"""Task lifecycle operations: the only code that changes a task's status.

Every function runs inside a caller-provided transaction and is written as a
*guarded* (compare-and-set) UPDATE: it only takes effect if the task is still
in the expected state **and** the caller presents the task's current fencing
pair ``(current_attempt_id, attempt_count)``. A message from an older attempt
therefore matches zero rows and is rejected — this is what prevents a stale
worker from overwriting a committed result.

State changes are additionally validated by the database trigger
``tasks_enforce_transition`` (allowed pairs in ``task_state_transitions``) and
recorded by ``tasks_log_transition`` with the reason set via ``set_reason``.

Ownership rule: a lease only decides *when the recovery loop may take a task
away*. Until recovery actually reassigns it, the current attempt still owns
the task and may renew, start or commit. Safety comes from fencing, liveness
from leases.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from dtq.common.protocol import StaleReason

ACTIVE_STATES = ("ASSIGNED", "RUNNING")


async def set_reason(conn: asyncpg.Connection, reason: str) -> None:
    """Attach a human-readable reason to the next status transition(s) in this transaction."""
    await conn.execute("SELECT set_config('dtq.reason', $1, true)", reason[:500])


@dataclass
class Outcome:
    ok: bool
    status: str = ""                   # committed | duplicate | retry_scheduled | failed | ok
    reason: str | None = None          # 409 reason when not ok
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime | None = None
    task: dict[str, Any] | None = None


async def _diagnose(conn: asyncpg.Connection, task_id: UUID, attempt_id: UUID, token: int,
                    grace_s: float = 0.0) -> str:
    row = await conn.fetchrow(
        """SELECT status, current_attempt_id, attempt_count, lease_expires_at,
                  started_at, timeout_seconds, now() AS now
             FROM tasks WHERE id = $1""",
        task_id,
    )
    if row is None:
        return StaleReason.NOT_FOUND
    if row["current_attempt_id"] != attempt_id or row["attempt_count"] != token:
        return StaleReason.STALE_ATTEMPT
    if row["status"] not in ACTIVE_STATES:
        # Same attempt, but the broker already took the task away: report why.
        attempt_status = await conn.fetchval("SELECT status FROM task_attempts WHERE id = $1", attempt_id)
        if attempt_status == "TIMED_OUT":
            return StaleReason.DEADLINE_EXCEEDED
        if attempt_status == "LOST":
            return StaleReason.LEASE_EXPIRED
        return StaleReason.INVALID_STATE
    if row["status"] == "RUNNING" and row["started_at"] is not None:
        deadline = row["started_at"].timestamp() + row["timeout_seconds"] + grace_s
        if row["now"].timestamp() > deadline:
            return StaleReason.DEADLINE_EXCEEDED
    return StaleReason.INVALID_STATE


# --------------------------------------------------------------------- dispatch
async def assign(conn: asyncpg.Connection, task_id: UUID, attempt_id: UUID, worker_id: UUID,
                 ack_timeout_s: float) -> asyncpg.Record | None:
    """PENDING/RETRY_WAIT -> ASSIGNED for a row the caller has locked. Creates the attempt record."""
    await set_reason(conn, "dispatched to worker")
    row = await conn.fetchrow(
        """UPDATE tasks
              SET status = 'ASSIGNED',
                  attempt_count = attempt_count + 1,
                  current_attempt_id = $2,
                  assigned_worker_id = $3,
                  lease_expires_at = now() + make_interval(secs => $4),
                  first_assigned_at = coalesce(first_assigned_at, now()),
                  started_at = NULL
            WHERE id = $1
              AND status IN ('PENDING', 'RETRY_WAIT')
              AND available_at <= now()
              AND attempt_count < max_attempts
        RETURNING id, attempt_count, type, payload, timeout_seconds, max_attempts,
                  lease_expires_at, namespace""",
        task_id, attempt_id, worker_id, float(ack_timeout_s),
    )
    if row is not None:
        await conn.execute(
            """INSERT INTO task_attempts (id, task_id, attempt_no, worker_id, status, assigned_at)
               VALUES ($1, $2, $3, $4, 'ASSIGNED', now())""",
            attempt_id, task_id, row["attempt_count"], worker_id,
        )
    return row


# ------------------------------------------------------------------ acknowledge
async def start(conn: asyncpg.Connection, task_id: UUID, worker_id: UUID, attempt_id: UUID,
                token: int, lease_s: float) -> Outcome:
    """ASSIGNED -> RUNNING (worker acknowledgement). Idempotent for a repeated START."""
    await set_reason(conn, "worker acknowledged and started")
    row = await conn.fetchrow(
        """UPDATE tasks
              SET status = 'RUNNING', started_at = now(),
                  lease_expires_at = now() + make_interval(secs => $5)
            WHERE id = $1 AND status = 'ASSIGNED'
              AND current_attempt_id = $3 AND attempt_count = $4 AND assigned_worker_id = $2
        RETURNING lease_expires_at, namespace""",
        task_id, worker_id, attempt_id, token, float(lease_s),
    )
    if row is not None:
        await conn.execute(
            """UPDATE task_attempts SET status = 'RUNNING', started_at = now(), last_renewed_at = now()
                WHERE id = $1""",
            attempt_id,
        )
        return Outcome(True, "ok", lease_expires_at=row["lease_expires_at"],
                       task={"namespace": row["namespace"]})
    # Duplicate START (response lost, worker retried): already RUNNING under this attempt.
    dup = await conn.fetchrow(
        """SELECT lease_expires_at FROM tasks
            WHERE id = $1 AND status = 'RUNNING' AND current_attempt_id = $2 AND attempt_count = $3""",
        task_id, attempt_id, token,
    )
    if dup is not None:
        return Outcome(True, "duplicate", lease_expires_at=dup["lease_expires_at"])
    return Outcome(False, reason=await _diagnose(conn, task_id, attempt_id, token))


async def renew(conn: asyncpg.Connection, task_id: UUID, worker_id: UUID, attempt_id: UUID,
                token: int, lease_s: float, deadline_grace_s: float,
                progress: float | None = None, message: str | None = None) -> Outcome:
    """Extend the lease of the current attempt (refused after the execution deadline)."""
    row = await conn.fetchrow(
        """UPDATE tasks
              SET lease_expires_at = now() + make_interval(secs => $5)
            WHERE id = $1 AND status = 'RUNNING'
              AND current_attempt_id = $3 AND attempt_count = $4 AND assigned_worker_id = $2
              AND now() <= started_at + make_interval(secs => timeout_seconds + $6)
        RETURNING lease_expires_at""",
        task_id, worker_id, attempt_id, token, float(lease_s), float(deadline_grace_s),
    )
    if row is None:
        return Outcome(False, reason=await _diagnose(conn, task_id, attempt_id, token, deadline_grace_s))
    await conn.execute(
        """UPDATE task_attempts
              SET last_renewed_at = now(),
                  progress = coalesce($2, progress),
                  progress_message = coalesce($3, progress_message)
            WHERE id = $1""",
        attempt_id, progress, message,
    )
    return Outcome(True, "ok", lease_expires_at=row["lease_expires_at"])


# --------------------------------------------------------------------- complete
async def complete(conn: asyncpg.Connection, task_id: UUID, worker_id: UUID, attempt_id: UUID,
                   token: int, result: dict[str, Any], duration_ms: int) -> Outcome:
    """RUNNING -> COMPLETED: the transactional, fenced result commit (exactly one visible result)."""
    await set_reason(conn, "result committed")
    row = await conn.fetchrow(
        """UPDATE tasks
              SET status = 'COMPLETED', result = $5, completed_at = now(), lease_expires_at = NULL
            WHERE id = $1 AND status = 'RUNNING'
              AND current_attempt_id = $3 AND attempt_count = $4 AND assigned_worker_id = $2
        RETURNING namespace, type, created_at, completed_at""",
        task_id, worker_id, attempt_id, token, result,
    )
    if row is not None:
        await conn.execute(
            """UPDATE task_attempts
                  SET status = 'SUCCEEDED', finished_at = now(), duration_ms = $2, progress = 1
                WHERE id = $1""",
            attempt_id, duration_ms,
        )
        await conn.execute(
            "UPDATE workers SET tasks_completed = tasks_completed + 1 WHERE id = $1", worker_id
        )
        return Outcome(True, "committed", task=dict(row))
    # Duplicate acknowledgement: this very attempt already committed -> no effect, success.
    dup = await conn.fetchval(
        "SELECT 1 FROM tasks WHERE id = $1 AND status = 'COMPLETED' AND current_attempt_id = $2",
        task_id, attempt_id,
    )
    if dup:
        return Outcome(True, "duplicate")
    return Outcome(False, reason=await _diagnose(conn, task_id, attempt_id, token))


# ------------------------------------------------------------- failure/recovery
async def retry_or_fail(conn: asyncpg.Connection, task_id: UUID, error: dict[str, Any],
                        retryable: bool, backoff_s: float, reason: str) -> Outcome:
    """ASSIGNED/RUNNING -> RETRY_WAIT, then -> FAILED in the same transaction when exhausted.

    The caller must hold the row lock and have closed the attempt record.
    """
    await set_reason(conn, reason)
    row = await conn.fetchrow(
        """UPDATE tasks
              SET status = 'RETRY_WAIT', lease_expires_at = NULL, last_error = $2,
                  available_at = now() + make_interval(secs => $3)
            WHERE id = $1 AND status IN ('ASSIGNED', 'RUNNING')
        RETURNING attempt_count, max_attempts, available_at, namespace""",
        task_id, error, float(backoff_s),
    )
    if row is None:
        return Outcome(False, reason=StaleReason.INVALID_STATE)
    if (not retryable) or row["attempt_count"] >= row["max_attempts"]:
        await set_reason(
            conn,
            "non-retryable error" if not retryable
            else f"retry limit exhausted ({row['attempt_count']}/{row['max_attempts']} attempts)",
        )
        await conn.execute(
            "UPDATE tasks SET status = 'FAILED', failed_at = now() WHERE id = $1", task_id
        )
        return Outcome(True, "failed", task={"namespace": row["namespace"],
                                             "attempt_count": row["attempt_count"]})
    return Outcome(True, "retry_scheduled", next_attempt_at=row["available_at"],
                   task={"namespace": row["namespace"], "attempt_count": row["attempt_count"]})


async def fail(conn: asyncpg.Connection, task_id: UUID, worker_id: UUID, attempt_id: UUID,
               token: int, error_type: str, message: str, retryable: bool, duration_ms: int,
               backoff_s: float) -> Outcome:
    """Worker-reported failure of the current attempt (also used to release an assignment)."""
    task = await conn.fetchrow(
        """SELECT id FROM tasks
            WHERE id = $1 AND status IN ('ASSIGNED', 'RUNNING')
              AND current_attempt_id = $3 AND attempt_count = $4 AND assigned_worker_id = $2
            FOR UPDATE""",
        task_id, worker_id, attempt_id, token,
    )
    if task is None:
        already = await conn.fetchval(
            "SELECT status FROM task_attempts WHERE id = $1 AND task_id = $2", attempt_id, task_id
        )
        if already in ("FAILED", "TIMED_OUT", "RELEASED"):
            return Outcome(True, "duplicate")
        return Outcome(False, reason=await _diagnose(conn, task_id, attempt_id, token))

    attempt_status = (
        "TIMED_OUT" if error_type == "TIMEOUT"
        else "RELEASED" if error_type == "WORKER_SHUTDOWN"
        else "FAILED"
    )
    await conn.execute(
        """UPDATE task_attempts
              SET status = $2::attempt_status, finished_at = now(), duration_ms = $3,
                  error_type = $4, error_message = $5
            WHERE id = $1""",
        attempt_id, attempt_status, duration_ms, error_type, message[:2000],
    )
    if attempt_status != "RELEASED":
        await conn.execute("UPDATE workers SET tasks_failed = tasks_failed + 1 WHERE id = $1", worker_id)
    error = {"type": error_type, "message": message[:2000], "attempt": token, "worker_id": str(worker_id)}
    return await retry_or_fail(
        conn, task_id, error, retryable, backoff_s,
        reason=f"attempt {token} {attempt_status.lower()}: {error_type}",
    )


async def recover(conn: asyncpg.Connection, task: asyncpg.Record, error_type: str, message: str,
                  backoff_s: float, attempt_status: str = "LOST") -> Outcome:
    """Recovery-loop path for a locked ASSIGNED/RUNNING task whose owner is gone or too slow."""
    await conn.execute(
        """UPDATE task_attempts
              SET status = $2::attempt_status, finished_at = now(), error_type = $3, error_message = $4
            WHERE id = $1 AND status IN ('ASSIGNED', 'RUNNING')""",
        task["current_attempt_id"], attempt_status, error_type, message,
    )
    error = {
        "type": error_type, "message": message, "attempt": task["attempt_count"],
        "worker_id": str(task["assigned_worker_id"]) if task["assigned_worker_id"] else None,
    }
    return await retry_or_fail(
        conn, task["id"], error, retryable=True, backoff_s=backoff_s,
        reason=f"attempt {task['attempt_count']} {attempt_status.lower()}: {error_type}",
    )
