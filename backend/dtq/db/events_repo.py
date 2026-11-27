"""System events: the append-only observability log (streamed to the dashboard via NOTIFY)."""

from __future__ import annotations

from typing import Any, Iterable
from uuid import UUID

import asyncpg

SEVERITIES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

EVENT_COLUMNS = "id, ts, severity, component, event_type, message, task_id, worker_id, namespace, details"


async def emit(conn: asyncpg.Connection | asyncpg.Pool, severity: str, component: str, event_type: str,
               message: str, *, task_id: UUID | None = None, worker_id: UUID | None = None,
               namespace: str | None = None, details: dict[str, Any] | None = None) -> None:
    await conn.execute(
        """INSERT INTO system_events (severity, component, event_type, message, task_id, worker_id,
                                      namespace, details)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
        severity, component, event_type, message[:2000], task_id, worker_id, namespace, details or {},
    )


async def emit_many(conn: asyncpg.Connection, rows: Iterable[tuple]) -> None:
    """rows: (severity, component, event_type, message, task_id, worker_id, namespace, details)"""
    rows = list(rows)
    if not rows:
        return
    await conn.execute(
        """INSERT INTO system_events (severity, component, event_type, message, task_id, worker_id,
                                      namespace, details)
           SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::uuid[], $6::uuid[],
                                $7::text[], $8::jsonb[])""",
        [r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows], [r[3][:2000] for r in rows],
        [r[4] for r in rows], [r[5] for r in rows], [r[6] for r in rows], [r[7] or {} for r in rows],
    )


async def query(conn: asyncpg.Connection | asyncpg.Pool, *, severities: list[str] | None = None,
                components: list[str] | None = None, event_type: str | None = None,
                task_id: UUID | None = None, worker_id: UUID | None = None,
                namespace: str | None = None, search: str | None = None,
                after_id: int | None = None, before_id: int | None = None,
                limit: int = 200, ascending: bool = False) -> list[dict[str, Any]]:
    clauses, args = [], []

    def arg(value: Any) -> str:
        args.append(value)
        return f"${len(args)}"

    if severities:
        clauses.append(f"severity = ANY({arg(severities)}::text[])")
    if components:
        clauses.append(f"component = ANY({arg(components)}::text[])")
    if event_type:
        clauses.append(f"event_type LIKE {arg(event_type.rstrip('*') + '%')}")
    if task_id:
        clauses.append(f"task_id = {arg(task_id)}")
    if worker_id:
        clauses.append(f"worker_id = {arg(worker_id)}")
    if namespace:
        clauses.append(f"namespace = {arg(namespace)}")
    if search:
        clauses.append(f"message ILIKE {arg('%' + search + '%')}")
    if after_id is not None:
        clauses.append(f"id > {arg(after_id)}")
    if before_id is not None:
        clauses.append(f"id < {arg(before_id)}")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    direction = "ASC" if ascending or after_id is not None else "DESC"
    rows = await conn.fetch(
        f"SELECT {EVENT_COLUMNS} FROM system_events {where} ORDER BY id {direction} LIMIT {arg(limit)}",
        *args,
    )
    return [dict(r) for r in rows]


async def last_id(conn: asyncpg.Connection | asyncpg.Pool) -> int:
    return await conn.fetchval("SELECT coalesce(max(id), 0) FROM system_events")
