"""Failure detection and automatic recovery (runs inside the broker, leader only).

Each cycle (default every second):
  1. ACTIVE workers silent for > suspect_after  -> SUSPECT (no new work; may be a false suspicion)
  2. live workers silent for > dead_after      -> DEAD; their tasks are recovered immediately
  3. ASSIGNED/RUNNING tasks whose lease expired -> attempt LOST -> RETRY_WAIT (+backoff) or FAILED
  4. RUNNING tasks past their execution deadline -> attempt TIMED_OUT -> RETRY_WAIT or FAILED

Heartbeats are an *unreliable failure detector* (Chandra & Toueg, 1996): a slow
or partitioned worker can be declared dead while still running. That only costs
a duplicate execution; it cannot corrupt state, because the late worker's
messages carry an old fencing token and are rejected.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import asyncpg

from dtq.broker.retry_policy import backoff_delay
from dtq.common.config import BrokerSettings
from dtq.db import events_repo, lifecycle

log = logging.getLogger(__name__)

_TASK_FIELDS = """t.id, t.status, t.type, t.namespace, t.attempt_count, t.max_attempts,
                  t.current_attempt_id, t.assigned_worker_id, w.name AS worker_name, w.status AS worker_status"""


class RecoveryLoop:
    def __init__(self, pool: asyncpg.Pool, settings: BrokerSettings, wake_dispatcher, leader) -> None:
        self.pool = pool
        self.s = settings
        self.wake_dispatcher = wake_dispatcher
        self.leader = leader
        self.stats: dict[str, Any] = {
            "cycles": 0, "suspected": 0, "dead": 0, "recovered": 0, "timed_out": 0, "failed": 0,
        }

    def _backoff(self, attempts: int) -> float:
        return backoff_delay(attempts, self.s.retry_backoff_base_s, self.s.retry_backoff_max_s,
                             self.s.retry_jitter)

    # ------------------------------------------------------------ restart grace
    async def on_leadership_acquired(self) -> None:
        """Extend leases after a broker (re)start: workers could not renew while no broker
        was reachable, so expiring them now would cause spurious re-executions."""
        if not self.s.restart_grace:
            return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                leases = await conn.execute(
                    """UPDATE tasks
                          SET lease_expires_at = greatest(lease_expires_at,
                                                          now() + make_interval(secs => $1))
                        WHERE status IN ('ASSIGNED', 'RUNNING')""",
                    float(self.s.lease_duration_s),
                )
                workers = await conn.execute(
                    """UPDATE workers SET last_heartbeat_at = greatest(last_heartbeat_at, now())
                        WHERE status IN ('ACTIVE', 'SUSPECT', 'DRAINING')"""
                )
                n_leases, n_workers = int(leases.split()[-1]), int(workers.split()[-1])
                await events_repo.emit(
                    conn, "INFO", "recovery", "recovery.leader_acquired",
                    f"Recovery leadership acquired by {self.s.instance_id}; restart grace applied to "
                    f"{n_leases} in-flight task lease(s) and {n_workers} live worker(s)",
                    details={"instance": self.s.instance_id, "leases_extended": n_leases,
                             "workers_refreshed": n_workers},
                )

    # ----------------------------------------------------------------- the loop
    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.s.reaper_interval_s)
            if not self.leader.is_leader:
                continue
            try:
                await self.cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("recovery cycle failed")

    async def cycle(self) -> int:
        self.stats["cycles"] += 1
        changed = 0
        changed += await self._detect_suspects()
        changed += await self._detect_dead()
        changed += await self._recover_expired_leases()
        changed += await self._enforce_deadlines()
        if changed:
            self.wake_dispatcher()
        return changed

    # ------------------------------------------------------------------ steps
    async def _detect_suspects(self) -> int:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """UPDATE workers SET status = 'SUSPECT', status_changed_at = now()
                        WHERE status = 'ACTIVE'
                          AND last_heartbeat_at < now() - make_interval(secs => $1)
                    RETURNING id, name, EXTRACT(EPOCH FROM (now() - last_heartbeat_at))::float8 AS silent_s""",
                    float(self.s.worker_suspect_after_s),
                )
                await events_repo.emit_many(conn, [
                    ("WARNING", "recovery", "worker.suspect",
                     f"Worker {r['name']} suspected: no heartbeat for {float(r['silent_s']):.1f}s "
                     f"(may be a network delay — no new work until it reappears)",
                     None, r["id"], None, {"silent_s": round(float(r["silent_s"]), 2)})
                    for r in rows
                ])
        self.stats["suspected"] += len(rows)
        return 0  # suspicion alone frees nothing

    async def _detect_dead(self) -> int:
        recovered = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                dead = await conn.fetch(
                    """UPDATE workers
                          SET status = 'DEAD', status_changed_at = now(), stopped_at = now(),
                              stop_reason = 'heartbeat timeout'
                        WHERE status IN ('ACTIVE', 'SUSPECT', 'DRAINING')
                          AND last_heartbeat_at < now() - make_interval(secs => $1)
                    RETURNING id, name, EXTRACT(EPOCH FROM (now() - last_heartbeat_at))::float8 AS silent_s""",
                    float(self.s.worker_dead_after_s),
                )
                if not dead:
                    return 0
                events = [
                    ("ERROR", "recovery", "worker.dead",
                     f"Worker {r['name']} declared DEAD after {float(r['silent_s']):.1f}s without heartbeat",
                     None, r["id"], None, {"silent_s": round(float(r["silent_s"]), 2)})
                    for r in dead
                ]
                tasks = await conn.fetch(
                    f"""SELECT {_TASK_FIELDS}
                          FROM tasks t LEFT JOIN workers w ON w.id = t.assigned_worker_id
                         WHERE t.assigned_worker_id = ANY($1::uuid[])
                           AND t.status IN ('ASSIGNED', 'RUNNING')
                         ORDER BY t.seq
                           FOR UPDATE OF t""",
                    [r["id"] for r in dead],
                )
                for t in tasks:
                    outcome = await lifecycle.recover(
                        conn, t, "WORKER_DEAD",
                        f"worker {t['worker_name']} declared dead (no heartbeat for > {self.s.worker_dead_after_s}s)",
                        self._backoff(t["attempt_count"]),
                    )
                    events.append(self._recovery_event(t, outcome, "WORKER_DEAD"))
                    recovered += 1
                await events_repo.emit_many(conn, events)
        self.stats["dead"] += len(dead)
        self.stats["recovered"] += recovered
        return recovered

    async def _recover_expired_leases(self) -> int:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                tasks = await conn.fetch(
                    f"""SELECT {_TASK_FIELDS}
                          FROM tasks t LEFT JOIN workers w ON w.id = t.assigned_worker_id
                         WHERE t.status IN ('ASSIGNED', 'RUNNING') AND t.lease_expires_at < now()
                         ORDER BY t.lease_expires_at
                         LIMIT 200
                           FOR UPDATE OF t SKIP LOCKED"""
                )
                events = []
                for t in tasks:
                    error_type = "ASSIGN_ACK_TIMEOUT" if t["status"] == "ASSIGNED" else "LEASE_EXPIRED"
                    message = (
                        f"worker {t['worker_name']} did not acknowledge the assignment in time"
                        if error_type == "ASSIGN_ACK_TIMEOUT"
                        else f"lease held by {t['worker_name']} expired without renewal"
                    )
                    outcome = await lifecycle.recover(conn, t, error_type, message,
                                                      self._backoff(t["attempt_count"]))
                    events.append(self._recovery_event(t, outcome, error_type))
                await events_repo.emit_many(conn, events)
        self.stats["recovered"] += len(tasks)
        return len(tasks)

    async def _enforce_deadlines(self) -> int:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                tasks = await conn.fetch(
                    f"""SELECT {_TASK_FIELDS}
                          FROM tasks t LEFT JOIN workers w ON w.id = t.assigned_worker_id
                         WHERE t.status = 'RUNNING'
                           AND now() > t.started_at + make_interval(secs => t.timeout_seconds + $1)
                         LIMIT 200
                           FOR UPDATE OF t SKIP LOCKED""",
                    float(self.s.deadline_grace_s),
                )
                events = []
                for t in tasks:
                    outcome = await lifecycle.recover(
                        conn, t, "DEADLINE_EXCEEDED",
                        "execution exceeded timeout_seconds (enforced by broker)",
                        self._backoff(t["attempt_count"]), attempt_status="TIMED_OUT",
                    )
                    events.append(self._recovery_event(t, outcome, "DEADLINE_EXCEEDED"))
                await events_repo.emit_many(conn, events)
        self.stats["timed_out"] += len(tasks)
        return len(tasks)

    def _recovery_event(self, t: asyncpg.Record, outcome: lifecycle.Outcome, cause: str) -> tuple:
        if outcome.status == "failed":
            self.stats["failed"] += 1
            return ("ERROR", "recovery", "task.failed",
                    f"Task {t['type']} FAILED after {t['attempt_count']}/{t['max_attempts']} attempts "
                    f"(last cause: {cause})",
                    t["id"], t["assigned_worker_id"], t["namespace"],
                    {"cause": cause, "attempt": t["attempt_count"]})
        return ("WARNING", "recovery", "task.recovered",
                f"Attempt {t['attempt_count']} of {t['type']} on {t['worker_name']} lost ({cause}); "
                f"task scheduled for retry",
                t["id"], t["assigned_worker_id"], t["namespace"],
                {"cause": cause, "attempt": t["attempt_count"],
                 "next_attempt_at": outcome.next_attempt_at.isoformat() if outcome.next_attempt_at else None})
