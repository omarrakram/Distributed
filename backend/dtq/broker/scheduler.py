"""Custom scheduler / dispatcher of the broker.

Transport is *pull*: a worker with free slots long-polls ``claim``. Decisions are
*central*: the dispatcher batches all waiting claims and decides which task goes
to which worker.

Policy
------
* Task order: FIFO by submission sequence (``seq``) after integer priority.
* Worker choice: each task goes to the least-utilised waiting worker
  ((active + planned) / capacity), ties broken by least-recently-assigned
  (round robin), then by how long the claim has been waiting.
* Capacity-aware: a worker never receives more than ``capacity - active`` tasks.

Concurrency control
-------------------
One dispatch cycle = one transaction:
  1. lock the claimants' worker rows (``FOR UPDATE``, ordered by id → no deadlocks),
  2. lock up to Σ free slots eligible tasks with ``FOR UPDATE SKIP LOCKED``,
  3. guarded UPDATE of each task to ASSIGNED + INSERT of its attempt,
  4. COMMIT, and only then hand the assignments to the waiting requests.
Concurrent dispatchers (several broker replicas) therefore take disjoint task
sets and can never over-assign a worker.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from dtq.common.config import BrokerSettings
from dtq.db import events_repo, lifecycle

log = logging.getLogger(__name__)


@dataclass
class Candidate:
    """A waiting worker as seen by the planner."""

    worker_id: Any
    capacity: int
    active: int
    free: int
    last_assigned: float | None = None   # epoch seconds, None = never assigned
    waiting_since: float = 0.0
    planned: int = 0
    _rank: tuple = field(default=(0, 0.0), repr=False)


def plan_assignments(candidates: list[Candidate], task_ids: list[Any]) -> list[tuple[Any, Any]]:
    """Pure planning function: FIFO tasks -> least-utilised, round-robin-fair workers.

    Returns a list of (task_id, worker_id) pairs. Never exceeds a worker's free slots.
    """
    pool = [c for c in candidates if c.free > 0 and c.capacity > 0]
    for c in pool:
        c.planned = 0
        c._rank = (0, c.last_assigned if c.last_assigned is not None else float("-inf"), c.waiting_since)
    counter = itertools.count()
    plan: list[tuple[Any, Any]] = []
    for task_id in task_ids:
        eligible = [c for c in pool if c.planned < c.free]
        if not eligible:
            break
        best = min(eligible, key=lambda c: ((c.active + c.planned) / c.capacity, c._rank))
        plan.append((task_id, best.worker_id))
        best.planned += 1
        best._rank = (1, float(next(counter)), 0.0)  # just served -> back of the queue on ties
    return plan


@dataclass
class Claimant:
    worker_id: UUID
    max_tasks: int
    future: asyncio.Future
    since: float


class Dispatcher:
    def __init__(self, pool: asyncpg.Pool, settings: BrokerSettings) -> None:
        self.pool = pool
        self.settings = settings
        self._claimants: dict[UUID, Claimant] = {}
        self._wake = asyncio.Event()
        self._stopping = False
        self.stats: dict[str, Any] = {
            "cycles": 0, "assignments": 0, "redeliveries": 0, "last_cycle_ms": 0.0,
            "last_assignment_at": None, "errors": 0,
        }

    # ------------------------------------------------------------- API used by routes
    def wake(self, *_: Any) -> None:
        self._wake.set()

    @property
    def waiting_workers(self) -> int:
        return sum(1 for c in self._claimants.values() if not c.future.done())

    def cancel_claim(self, worker_id: UUID) -> None:
        claimant = self._claimants.pop(worker_id, None)
        if claimant and not claimant.future.done():
            claimant.future.set_result([])

    async def claim(self, worker_id: UUID, max_tasks: int, wait_s: float) -> list[dict]:
        loop = asyncio.get_running_loop()
        self.cancel_claim(worker_id)  # a newer claim supersedes an older one
        claimant = Claimant(worker_id, max_tasks, loop.create_future(), time.time())
        self._claimants[worker_id] = claimant
        self.wake()
        try:
            return await asyncio.wait_for(asyncio.shield(claimant.future), timeout=wait_s)
        except asyncio.TimeoutError:
            # If assignments were committed for us at the last moment they are not lost:
            # the worker's next claim redelivers its ASSIGNED tasks.
            return claimant.future.result() if claimant.future.done() else []
        finally:
            if self._claimants.get(worker_id) is claimant:
                del self._claimants[worker_id]

    async def redeliver(self, worker_id: UUID, older_than_s: float = 1.0) -> list[dict]:
        """Assignments already committed to this worker but never acknowledged (lost response)."""
        rows = await self.pool.fetch(
            """SELECT id, current_attempt_id, attempt_count, type, payload, timeout_seconds,
                      max_attempts, lease_expires_at
                 FROM tasks t
                WHERE assigned_worker_id = $1 AND status = 'ASSIGNED' AND lease_expires_at > now()
                  AND EXISTS (SELECT 1 FROM task_attempts a WHERE a.id = t.current_attempt_id
                               AND a.assigned_at < now() - make_interval(secs => $2))
                ORDER BY seq""",
            worker_id, float(older_than_s),
        )
        if rows:
            self.stats["redeliveries"] += len(rows)
        return [self._assignment(r, r["current_attempt_id"]) for r in rows]

    # ------------------------------------------------------------------ main loop
    async def run(self) -> None:
        while not self._stopping:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.settings.dispatch_tick_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            try:
                assigned = await self.dispatch_cycle()
                if assigned:
                    self._wake.set()  # more work and more claimants may remain
            except asyncio.CancelledError:
                raise
            except Exception:
                self.stats["errors"] += 1
                log.exception("dispatch cycle failed")
                await asyncio.sleep(0.5)

    def stop(self) -> None:
        self._stopping = True
        for worker_id in list(self._claimants):
            self.cancel_claim(worker_id)

    @staticmethod
    def _assignment(row: asyncpg.Record, attempt_id: UUID) -> dict:
        return {
            "task_id": row["id"], "attempt_id": attempt_id, "fencing_token": row["attempt_count"],
            "type": row["type"], "payload": row["payload"], "timeout_seconds": row["timeout_seconds"],
            "max_attempts": row["max_attempts"], "lease_expires_at": row["lease_expires_at"],
        }

    async def dispatch_cycle(self) -> int:
        claimants = [c for c in self._claimants.values() if not c.future.done()]
        if not claimants:
            return 0
        started = time.perf_counter()
        by_worker = {c.worker_id: c for c in claimants}
        results: dict[UUID, list[dict]] = {}
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                wrows = await conn.fetch(
                    """SELECT w.id, w.name, w.capacity, w.last_assigned_at,
                              (SELECT count(*) FROM tasks t WHERE t.assigned_worker_id = w.id
                                  AND t.status IN ('ASSIGNED', 'RUNNING')) AS active
                         FROM workers w
                        WHERE w.id = ANY($1::uuid[]) AND w.status = 'ACTIVE'
                        ORDER BY w.id
                          FOR UPDATE""",
                    list(by_worker),
                )
                candidates = []
                names = {}
                for w in wrows:
                    claimant = by_worker[w["id"]]
                    free = min(claimant.max_tasks, w["capacity"] - w["active"])
                    names[w["id"]] = w["name"]
                    if free > 0:
                        candidates.append(Candidate(
                            worker_id=w["id"], capacity=w["capacity"], active=w["active"], free=free,
                            last_assigned=w["last_assigned_at"].timestamp() if w["last_assigned_at"] else None,
                            waiting_since=claimant.since,
                        ))
                total = sum(c.free for c in candidates)
                if total == 0:
                    return 0
                trows = await conn.fetch(
                    """SELECT id FROM tasks
                        WHERE status IN ('PENDING', 'RETRY_WAIT') AND available_at <= now()
                          AND attempt_count < max_attempts
                        ORDER BY priority DESC, seq
                        LIMIT $1
                          FOR UPDATE SKIP LOCKED""",
                    total,
                )
                if not trows:
                    return 0
                plan = plan_assignments(candidates, [r["id"] for r in trows])
                events = []
                for task_id, worker_id in plan:
                    attempt_id = uuid4()
                    row = await lifecycle.assign(
                        conn, task_id, attempt_id, worker_id, self.settings.assign_ack_timeout_s
                    )
                    if row is None:  # defensive: cannot happen while we hold the row lock
                        continue
                    results.setdefault(worker_id, []).append(self._assignment(row, attempt_id))
                    events.append((
                        "INFO", "broker", "task.assigned",
                        f"Attempt {row['attempt_count']}/{row['max_attempts']} of {row['type']} "
                        f"assigned to {names.get(worker_id, worker_id)}",
                        task_id, worker_id, row["namespace"],
                        {"attempt": row["attempt_count"], "worker": names.get(worker_id)},
                    ))
                if results:
                    await conn.execute(
                        "UPDATE workers SET last_assigned_at = now() WHERE id = ANY($1::uuid[])",
                        list(results),
                    )
                    await events_repo.emit_many(conn, events)
        # Committed: only now hand the assignments to the waiting requests.
        assigned = 0
        for worker_id, tasks in results.items():
            claimant = by_worker[worker_id]
            if not claimant.future.done():
                claimant.future.set_result(tasks)
            assigned += len(tasks)
        self.stats["cycles"] += 1
        self.stats["assignments"] += assigned
        self.stats["last_cycle_ms"] = round((time.perf_counter() - started) * 1000, 2)
        if assigned:
            self.stats["last_assignment_at"] = time.time()
        return assigned
