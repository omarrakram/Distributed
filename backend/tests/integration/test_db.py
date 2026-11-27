"""Database integration: migrations, lifecycle enforcement, constraints, idempotency, atomic claims."""

import asyncio
import itertools
import shutil
import uuid

import asyncpg
import pytest

from dtq.db import lifecycle, tasks_repo
from dtq.db.migrate import MIGRATIONS_DIR, MigrationError, apply_migrations
from tests.helpers import submit

STATES = ["PENDING", "ASSIGNED", "RUNNING", "COMPLETED", "RETRY_WAIT", "FAILED"]
ALLOWED = {("PENDING", "ASSIGNED"), ("ASSIGNED", "RUNNING"), ("ASSIGNED", "RETRY_WAIT"), ("RUNNING", "COMPLETED"),
           ("RUNNING", "RETRY_WAIT"), ("RETRY_WAIT", "ASSIGNED"), ("RETRY_WAIT", "FAILED")}


async def test_migrations_are_idempotent_and_recorded(database_url, pool):
    assert await apply_migrations(database_url) == []
    versions = [r["version"] for r in await pool.fetch("SELECT version FROM schema_migrations ORDER BY 1")]
    assert versions == ["0001_initial_schema", "0002_analytics_views"]


async def test_modified_migration_is_detected(database_url, tmp_path):
    copy = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS_DIR, copy)
    (copy / "0001_initial_schema.sql").write_text((copy / "0001_initial_schema.sql").read_text() + "\n-- edited\n")
    with pytest.raises(MigrationError):
        await apply_migrations(database_url, copy)


async def test_state_machine_table_matches_specification(pool):
    rows = {(r["from_status"], r["to_status"]) for r in await pool.fetch("SELECT * FROM task_state_transitions")}
    assert rows == ALLOWED


async def _task_in_state(pool, state: str) -> uuid.UUID:
    """Create a task and force it into `state` through legal transitions only."""
    worker = await pool.fetchval("INSERT INTO workers (name, host, capacity) VALUES ('w', 'h', 4) RETURNING id")
    tid = await submit(pool, "delay", {"seconds": 1}, max_attempts=5)
    path = {"PENDING": [], "ASSIGNED": ["ASSIGNED"], "RUNNING": ["ASSIGNED", "RUNNING"],
            "COMPLETED": ["ASSIGNED", "RUNNING", "COMPLETED"], "RETRY_WAIT": ["ASSIGNED", "RETRY_WAIT"],
            "FAILED": ["ASSIGNED", "RETRY_WAIT", "FAILED"]}[state]
    for step in path:
        if step == "ASSIGNED":
            await pool.execute(
                """UPDATE tasks SET status='ASSIGNED', attempt_count=attempt_count+1, current_attempt_id=gen_random_uuid(),
                   assigned_worker_id=$2, lease_expires_at=now()+interval '1 minute' WHERE id=$1""", tid, worker)
        elif step == "RUNNING":
            await pool.execute("UPDATE tasks SET status='RUNNING', started_at=now() WHERE id=$1", tid)
        elif step == "COMPLETED":
            await pool.execute("UPDATE tasks SET status='COMPLETED', result='{}', completed_at=now(), "
                               "lease_expires_at=NULL WHERE id=$1", tid)
        elif step == "RETRY_WAIT":
            await pool.execute("UPDATE tasks SET status='RETRY_WAIT', lease_expires_at=NULL WHERE id=$1", tid)
        elif step == "FAILED":
            await pool.execute("UPDATE tasks SET status='FAILED', failed_at=now(), last_error='{}' WHERE id=$1", tid)
    return tid


@pytest.mark.parametrize("src,dst", [(a, b) for a, b in itertools.product(STATES, STATES)
                                     if a != b and (a, b) not in ALLOWED])
async def test_illegal_transitions_are_rejected_by_the_database(pool, src, dst):
    tid = await _task_in_state(pool, src)
    with pytest.raises(asyncpg.PostgresError) as exc:
        await pool.execute("UPDATE tasks SET status=$2::task_status WHERE id=$1", tid, dst)
    assert exc.value.sqlstate in ("DT001", "23514")  # trigger, or a CHECK constraint for the target state


async def test_tasks_must_be_created_pending_and_fencing_never_decreases(pool):
    with pytest.raises(asyncpg.PostgresError) as exc:
        await pool.execute("INSERT INTO tasks (id, type, payload, timeout_seconds, request_hash, status) "
                           "VALUES (gen_random_uuid(), 'delay', '{}', 5, 'h', 'RUNNING')")
    assert exc.value.sqlstate == "DT001"
    tid = await _task_in_state(pool, "RETRY_WAIT")
    with pytest.raises(asyncpg.PostgresError) as exc:
        await pool.execute("UPDATE tasks SET attempt_count = 0 WHERE id=$1", tid)
    assert exc.value.sqlstate in ("DT002", "23514")


async def test_result_constraints(pool):
    tid = await _task_in_state(pool, "RUNNING")
    with pytest.raises(asyncpg.CheckViolationError):  # COMPLETED needs a result
        await pool.execute("UPDATE tasks SET status='COMPLETED', completed_at=now(), lease_expires_at=NULL WHERE id=$1", tid)
    with pytest.raises(asyncpg.CheckViolationError):  # a result can only exist on COMPLETED
        await pool.execute("UPDATE tasks SET result='{}' WHERE id=$1", tid)


async def test_transitions_are_logged_with_reasons(pool):
    tid = await _task_in_state(pool, "FAILED")
    rows = await pool.fetch("SELECT from_status, to_status FROM task_transitions WHERE task_id=$1 ORDER BY id", tid)
    assert [(r["from_status"], r["to_status"]) for r in rows] == [
        (None, "PENDING"), ("PENDING", "ASSIGNED"), ("ASSIGNED", "RETRY_WAIT"), ("RETRY_WAIT", "FAILED")]
    t2 = await submit(pool, "delay", {"seconds": 1})
    async with pool.acquire() as conn, conn.transaction():
        worker = await conn.fetchval("SELECT id FROM workers LIMIT 1")
        assert await lifecycle.assign(conn, t2, uuid.uuid4(), worker, 5) is not None
    reason = await pool.fetchval("SELECT reason FROM task_transitions WHERE task_id=$1 AND to_status='ASSIGNED'", t2)
    assert reason == "dispatched to worker"


async def test_idempotent_submission_including_concurrent_duplicates(pool):
    a = await submit(pool, "delay", {"seconds": 1}, idempotency_key="k-1")
    b = await submit(pool, "delay", {"seconds": 1}, idempotency_key="k-1")
    assert a == b
    with pytest.raises(tasks_repo.IdempotencyConflict):
        await submit(pool, "delay", {"seconds": 2}, idempotency_key="k-1")
    ids = await asyncio.gather(*(submit(pool, "sha256", {"data": "x"}, idempotency_key="k-2") for _ in range(20)))
    assert len(set(ids)) == 1
    assert await pool.fetchval("SELECT count(*) FROM tasks WHERE idempotency_key='k-2'") == 1


async def test_batch_insert_is_atomic(pool):
    good = {"id": uuid.uuid4(), "type": "delay", "payload": {"seconds": 1}, "priority": 0, "max_attempts": 3,
            "timeout_seconds": 30}
    bad = dict(good, id=uuid.uuid4(), max_attempts=99)  # violates CHECK
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await tasks_repo.submit_batch(conn, batch_id=uuid.uuid4(), items=[good, bad], namespace="default")
    assert await pool.fetchval("SELECT count(*) FROM tasks") == 0
