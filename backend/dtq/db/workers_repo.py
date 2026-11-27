"""Worker registry persistence (one row per worker incarnation)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

LIVE_STATES = ("ACTIVE", "SUSPECT", "DRAINING")

WORKER_COLUMNS = """
    w.id, w.name, w.host, w.pid, w.agent_id, w.version, w.capacity, w.status, w.labels,
    w.registered_at, w.last_heartbeat_at, w.last_assigned_at, w.status_changed_at, w.stopped_at,
    w.stop_reason, w.reported_active, w.tasks_completed, w.tasks_failed,
    EXTRACT(EPOCH FROM (now() - w.last_heartbeat_at))::float8 AS heartbeat_age_s,
    (SELECT count(*) FROM tasks t
      WHERE t.assigned_worker_id = w.id AND t.status IN ('ASSIGNED', 'RUNNING')) AS active_tasks
"""


async def register(conn: asyncpg.Connection, *, name: str, host: str, pid: int | None, capacity: int,
                   agent_id: str | None, version: str | None, labels: dict[str, str]) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO workers (name, host, pid, capacity, agent_id, version, labels)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           RETURNING id, name, registered_at""",
        name, host, pid, capacity, agent_id, version, labels,
    )


async def heartbeat(conn: asyncpg.Connection, worker_id: UUID, draining: bool,
                    reported_active: int) -> tuple[str | None, str | None]:
    """Record liveness. Returns (previous_status, new_status); (None, None) if not live (dead/unknown).

    A heartbeat from a SUSPECT worker restores it to ACTIVE (the suspicion was false).
    DRAINING is sticky: a draining worker never becomes ACTIVE again.
    """
    previous = await conn.fetchval("SELECT status FROM workers WHERE id = $1 FOR UPDATE", worker_id)
    if previous not in LIVE_STATES:
        return None, None
    new = "DRAINING" if draining or previous == "DRAINING" else "ACTIVE"
    await conn.execute(
        """UPDATE workers
              SET last_heartbeat_at = now(), reported_active = $2, status = $3::worker_status,
                  status_changed_at = CASE WHEN status <> $3::worker_status THEN now()
                                           ELSE status_changed_at END
            WHERE id = $1""",
        worker_id, reported_active, new,
    )
    return previous, new


async def set_draining(conn: asyncpg.Connection, worker_id: UUID) -> str | None:
    return await conn.fetchval(
        """UPDATE workers SET status = 'DRAINING', status_changed_at = now()
            WHERE id = $1 AND status IN ('ACTIVE', 'SUSPECT', 'DRAINING')
        RETURNING status""",
        worker_id,
    )


async def set_stopped(conn: asyncpg.Connection, worker_id: UUID, reason: str) -> str | None:
    return await conn.fetchval(
        """UPDATE workers SET status = 'STOPPED', status_changed_at = now(), stopped_at = now(),
                              stop_reason = $2
            WHERE id = $1 AND status IN ('ACTIVE', 'SUSPECT', 'DRAINING')
        RETURNING status""",
        worker_id, reason[:200],
    )


async def get_status(conn: asyncpg.Connection, worker_id: UUID) -> str | None:
    return await conn.fetchval("SELECT status FROM workers WHERE id = $1", worker_id)


async def list_workers(conn: asyncpg.Connection | asyncpg.Pool, include_stopped: bool = False,
                       stopped_window_s: int = 600) -> list[dict[str, Any]]:
    where = "" if include_stopped else (
        f"WHERE w.status IN ('ACTIVE', 'SUSPECT', 'DRAINING') "
        f"OR w.stopped_at > now() - interval '{int(stopped_window_s)} seconds'"
    )
    rows = await conn.fetch(
        f"SELECT {WORKER_COLUMNS} FROM workers w {where} ORDER BY w.registered_at DESC"
    )
    workers = [dict(r) for r in rows]
    if not workers:
        return workers
    current = await conn.fetch(
        """SELECT t.assigned_worker_id AS worker_id, t.id AS task_id, t.type, t.status, t.attempt_count,
                  t.started_at, a.progress, a.progress_message
             FROM tasks t LEFT JOIN task_attempts a ON a.id = t.current_attempt_id
            WHERE t.status IN ('ASSIGNED', 'RUNNING') AND t.assigned_worker_id = ANY($1::uuid[])
            ORDER BY t.seq""",
        [w["id"] for w in workers],
    )
    by_worker: dict[UUID, list[dict]] = {}
    for row in current:
        by_worker.setdefault(row["worker_id"], []).append(
            {k: row[k] for k in ("task_id", "type", "status", "attempt_count", "started_at",
                                 "progress", "progress_message")}
        )
    for w in workers:
        w["heartbeat_age_s"] = round(float(w["heartbeat_age_s"]), 2)
        w["current_tasks"] = by_worker.get(w["id"], [])
        w["utilization"] = round(w["active_tasks"] / w["capacity"], 3) if w["capacity"] else 0.0
    return workers


async def get_worker(conn: asyncpg.Connection, worker_id: UUID) -> dict[str, Any] | None:
    row = await conn.fetchrow(f"SELECT {WORKER_COLUMNS} FROM workers w WHERE w.id = $1", worker_id)
    if row is None:
        return None
    worker = dict(row)
    worker["heartbeat_age_s"] = round(float(worker["heartbeat_age_s"]), 2)
    attempts = await conn.fetch(
        """SELECT a.id, a.task_id, t.type, a.attempt_no, a.status, a.assigned_at, a.started_at,
                  a.finished_at, a.duration_ms, a.error_type, a.progress
             FROM task_attempts a JOIN tasks t ON t.id = a.task_id
            WHERE a.worker_id = $1 ORDER BY a.assigned_at DESC LIMIT 100""",
        worker_id,
    )
    worker["recent_attempts"] = [dict(a) for a in attempts]
    return worker
