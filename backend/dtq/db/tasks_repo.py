"""Persistent task storage: idempotent submission and read models."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID

import asyncpg

from dtq.db import events_repo

TASK_COLUMNS = """
    t.id, t.seq, t.type, t.payload, t.namespace, t.batch_id, t.priority, t.status,
    t.max_attempts, t.attempt_count, t.timeout_seconds, t.idempotency_key,
    t.current_attempt_id, t.assigned_worker_id, w.name AS assigned_worker_name,
    t.lease_expires_at, t.available_at, t.result, t.last_error,
    t.created_at, t.updated_at, t.first_assigned_at, t.started_at, t.completed_at, t.failed_at,
    EXTRACT(EPOCH FROM (t.first_assigned_at - t.created_at))::float8 * 1000 AS queue_wait_ms,
    EXTRACT(EPOCH FROM (t.completed_at - t.started_at))::float8 * 1000 AS execution_ms,
    EXTRACT(EPOCH FROM (coalesce(t.completed_at, t.failed_at) - t.created_at))::float8 * 1000 AS latency_ms
"""


class IdempotencyConflict(Exception):
    """The idempotency key was already used for a different request."""

    def __init__(self, existing_id: UUID) -> None:
        super().__init__(f"idempotency key already used by task {existing_id} with a different request")
        self.existing_id = existing_id


def request_hash(task_type: str, payload: dict[str, Any], namespace: str, priority: int,
                 max_attempts: int, timeout_seconds: int) -> str:
    canonical = json.dumps(
        {"type": task_type, "payload": payload, "namespace": namespace, "priority": priority,
         "max_attempts": max_attempts, "timeout_seconds": timeout_seconds},
        sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _row(record: asyncpg.Record | None) -> dict[str, Any] | None:
    if record is None:
        return None
    out = dict(record)
    for key in ("queue_wait_ms", "execution_ms", "latency_ms"):
        if out.get(key) is not None:
            out[key] = round(float(out[key]), 1)
    return out


async def get_task(conn: asyncpg.Connection, task_id: UUID) -> dict[str, Any] | None:
    return _row(await conn.fetchrow(
        f"SELECT {TASK_COLUMNS} FROM tasks t LEFT JOIN workers w ON w.id = t.assigned_worker_id WHERE t.id = $1",
        task_id,
    ))


async def submit(conn: asyncpg.Connection, *, task_id: UUID, task_type: str, payload: dict[str, Any],
                 namespace: str, priority: int, max_attempts: int, timeout_seconds: int,
                 idempotency_key: str | None) -> tuple[dict[str, Any], bool]:
    """Insert a task durably. Returns (task, created). Acknowledge the client only after this commits.

    With an idempotency key, concurrent or repeated submissions collapse onto one row
    (unique index); a reused key with a different request raises IdempotencyConflict.
    """
    rhash = request_hash(task_type, payload, namespace, priority, max_attempts, timeout_seconds)
    async with conn.transaction():
        new_id = await conn.fetchval(
            """INSERT INTO tasks (id, type, payload, namespace, priority, max_attempts,
                                  timeout_seconds, idempotency_key, request_hash)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (idempotency_key) DO NOTHING
               RETURNING id""",
            task_id, task_type, payload, namespace, priority, max_attempts, timeout_seconds,
            idempotency_key, rhash,
        )
        if new_id is not None:
            await events_repo.emit(
                conn, "INFO", "api", "task.submitted",
                f"Task {task_type} submitted", task_id=new_id, namespace=namespace,
                details={"type": task_type, "priority": priority, "max_attempts": max_attempts,
                         "idempotency_key": idempotency_key},
            )
            return await get_task(conn, new_id), True  # type: ignore[return-value]

        existing = await conn.fetchrow(
            "SELECT id, request_hash FROM tasks WHERE idempotency_key = $1", idempotency_key
        )
        if existing["request_hash"] != rhash:
            raise IdempotencyConflict(existing["id"])
        await events_repo.emit(
            conn, "INFO", "api", "task.duplicate_submission",
            f"Duplicate submission with idempotency key {idempotency_key!r} returned the original task",
            task_id=existing["id"], namespace=namespace,
        )
        return await get_task(conn, existing["id"]), False  # type: ignore[return-value]


async def submit_batch(conn: asyncpg.Connection, *, batch_id: UUID,
                       items: list[dict[str, Any]], namespace: str) -> list[UUID]:
    """Insert many tasks atomically (all-or-nothing) with a single statement."""
    ids, types, payloads, nss, prios, maxes, timeouts, hashes = [], [], [], [], [], [], [], []
    for item in items:
        ids.append(item["id"])
        types.append(item["type"])
        payloads.append(item["payload"])
        nss.append(namespace)
        prios.append(item["priority"])
        maxes.append(item["max_attempts"])
        timeouts.append(item["timeout_seconds"])
        hashes.append(request_hash(item["type"], item["payload"], namespace, item["priority"],
                                   item["max_attempts"], item["timeout_seconds"]))
    async with conn.transaction():
        await conn.execute(
            """INSERT INTO tasks (id, type, payload, namespace, priority, max_attempts,
                                  timeout_seconds, request_hash, batch_id)
               SELECT u.id, u.type, u.payload, u.ns, u.prio, u.maxa, u.tmo, u.rh, $9
                 FROM unnest($1::uuid[], $2::text[], $3::jsonb[], $4::text[], $5::smallint[],
                             $6::int[], $7::int[], $8::text[])
                      AS u(id, type, payload, ns, prio, maxa, tmo, rh)""",
            ids, types, payloads, nss, prios, maxes, timeouts, hashes, batch_id,
        )
        counts: dict[str, int] = {}
        for t in types:
            counts[t] = counts.get(t, 0) + 1
        await events_repo.emit(
            conn, "INFO", "api", "batch.submitted",
            f"Batch of {len(ids)} tasks submitted", namespace=namespace,
            details={"batch_id": str(batch_id), "count": len(ids), "types": counts},
        )
    return ids


async def list_tasks(conn: asyncpg.Connection, *, statuses: list[str] | None = None,
                     task_type: str | None = None, namespace: str | None = None,
                     batch_id: UUID | None = None, worker_id: UUID | None = None,
                     q: str | None = None, retried: bool | None = None,
                     limit: int = 50, offset: int = 0, order: str = "created_desc") -> tuple[list[dict], int]:
    clauses, args = [], []

    def arg(value: Any) -> str:
        args.append(value)
        return f"${len(args)}"

    if statuses:
        clauses.append(f"t.status = ANY({arg(statuses)}::task_status[])")
    if task_type:
        clauses.append(f"t.type = {arg(task_type)}")
    if namespace:
        clauses.append(f"t.namespace = {arg(namespace)}")
    if batch_id:
        clauses.append(f"t.batch_id = {arg(batch_id)}")
    if worker_id:
        clauses.append(
            f"EXISTS (SELECT 1 FROM task_attempts a WHERE a.task_id = t.id AND a.worker_id = {arg(worker_id)})"
        )
    if retried is True:
        clauses.append("t.attempt_count > 1")
    if q:
        clauses.append(f"(t.id::text LIKE {arg(q.lower() + '%')} OR t.idempotency_key = {arg(q)})")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    order_sql = "t.seq ASC" if order == "created_asc" else "t.seq DESC"
    total = await conn.fetchval(f"SELECT count(*) FROM tasks t {where}", *args)
    rows = await conn.fetch(
        f"""SELECT {TASK_COLUMNS} FROM tasks t LEFT JOIN workers w ON w.id = t.assigned_worker_id
            {where} ORDER BY {order_sql} LIMIT {arg(limit)} OFFSET {arg(offset)}""",
        *args,
    )
    return [_row(r) for r in rows], total  # type: ignore[misc]


async def get_task_detail(conn: asyncpg.Connection, task_id: UUID) -> dict[str, Any] | None:
    task = await get_task(conn, task_id)
    if task is None:
        return None
    attempts = await conn.fetch(
        """SELECT a.id, a.attempt_no, a.worker_id, w.name AS worker_name, a.status, a.assigned_at,
                  a.started_at, a.finished_at, a.last_renewed_at, a.progress, a.progress_message,
                  a.duration_ms, a.error_type, a.error_message
             FROM task_attempts a JOIN workers w ON w.id = a.worker_id
            WHERE a.task_id = $1 ORDER BY a.attempt_no""",
        task_id,
    )
    transitions = await conn.fetch(
        """SELECT tt.id, tt.from_status, tt.to_status, tt.attempt_no, tt.worker_id, w.name AS worker_name,
                  tt.reason, tt.at
             FROM task_transitions tt LEFT JOIN workers w ON w.id = tt.worker_id
            WHERE tt.task_id = $1 ORDER BY tt.id""",
        task_id,
    )
    events = await events_repo.query(conn, task_id=task_id, limit=200, ascending=True)
    task["attempts"] = [dict(a) for a in attempts]
    task["transitions"] = [dict(t) for t in transitions]
    task["events"] = events
    return task


async def batch_summary(conn: asyncpg.Connection, batch_id: UUID, include_tasks: bool = False) -> dict | None:
    row = await conn.fetchrow(
        """SELECT count(*) AS total,
                  count(*) FILTER (WHERE status = 'COMPLETED') AS completed,
                  count(*) FILTER (WHERE status = 'FAILED') AS failed,
                  count(*) FILTER (WHERE status IN ('PENDING', 'RETRY_WAIT')) AS waiting,
                  count(*) FILTER (WHERE status IN ('ASSIGNED', 'RUNNING')) AS in_progress,
                  sum(attempt_count) AS attempts,
                  min(created_at) AS first_created_at,
                  max(coalesce(completed_at, failed_at)) AS last_finished_at
             FROM tasks WHERE batch_id = $1""",
        batch_id,
    )
    if row is None or row["total"] == 0:
        return None
    out = dict(row)
    out["batch_id"] = batch_id
    out["done"] = out["completed"] + out["failed"] == out["total"]
    if include_tasks:
        rows = await conn.fetch(
            """SELECT t.id, t.type, t.status, t.attempt_count, t.created_at, t.first_assigned_at,
                      t.started_at, t.completed_at, t.failed_at, t.assigned_worker_id,
                      w.name AS worker_name,
                      EXTRACT(EPOCH FROM (t.first_assigned_at - t.created_at))::float8 * 1000 AS queue_wait_ms,
                      EXTRACT(EPOCH FROM (t.completed_at - t.started_at))::float8 * 1000 AS execution_ms,
                      EXTRACT(EPOCH FROM (coalesce(t.completed_at, t.failed_at) - t.created_at))::float8 * 1000 AS latency_ms
                 FROM tasks t LEFT JOIN workers w ON w.id = t.assigned_worker_id
                WHERE t.batch_id = $1 ORDER BY t.seq""",
            batch_id,
        )
        out["tasks"] = [_row(r) for r in rows]
    return out
