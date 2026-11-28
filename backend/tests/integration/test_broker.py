"""Broker protocol integration tests (in-process broker app, real PostgreSQL)."""

import asyncio
import contextlib

import pytest

from dtq.broker.app import create_app
from tests.conftest import fast_broker_settings
from tests.helpers import running_app, submit, wait_until

TOKEN = {"X-Cluster-Token": "dev-cluster-token"}


@pytest.fixture
async def broker(pool, database_url):
    app = create_app(fast_broker_settings(database_url))
    async with running_app(app, TOKEN) as client:
        yield client


async def register(client, name="w1", capacity=1) -> str:
    r = await client.post("/v1/workers/register", json={"name": name, "host": "test", "capacity": capacity})
    assert r.status_code == 200, r.text
    return r.json()["worker_id"]


async def claim(client, wid, n=1, wait=1.0) -> list[dict]:
    r = await client.post(f"/v1/workers/{wid}/claim", json={"max_tasks": n, "wait_s": wait})
    assert r.status_code == 200, r.text
    return r.json()["tasks"]


def fenced(wid, a) -> dict:
    return {"worker_id": wid, "attempt_id": a["attempt_id"], "fencing_token": a["fencing_token"]}


@contextlib.asynccontextmanager
async def heartbeating(client, wid, interval=0.25):
    async def beat():
        while True:
            await client.post(f"/v1/workers/{wid}/heartbeat", json={"state": "active", "active": [], "free_slots": 0})
            await asyncio.sleep(interval)
    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_requires_cluster_token(broker):
    r = await broker.post("/v1/workers/register", json={"name": "x", "host": "h", "capacity": 1},
                          headers={"X-Cluster-Token": "wrong"})
    assert r.status_code == 401 and r.json()["error"]["code"] == "unauthorized"


async def test_full_protocol_roundtrip(broker, pool):
    tid = await submit(pool, "sha256", {"data": "x", "iterations": 10})
    wid = await register(broker)
    [a] = await claim(broker, wid)
    assert a["task_id"] == str(tid) and a["fencing_token"] == 1
    assert (await broker.post(f"/v1/tasks/{tid}/start", json=fenced(wid, a))).status_code == 200
    r = await broker.post(f"/v1/tasks/{tid}/renew", json=fenced(wid, a) | {"progress": 0.5, "message": "half"})
    assert r.status_code == 200
    r = await broker.post(f"/v1/tasks/{tid}/complete", json=fenced(wid, a) | {"result": {"ok": 1}, "duration_ms": 5})
    assert r.json() == {"status": "committed"}
    row = await pool.fetchrow("SELECT status, result, attempt_count FROM tasks WHERE id=$1", tid)
    assert (row["status"], row["result"], row["attempt_count"]) == ("COMPLETED", {"ok": 1}, 1)
    attempt = await pool.fetchrow("SELECT status, progress FROM task_attempts WHERE task_id=$1", tid)
    assert attempt["status"] == "SUCCEEDED" and attempt["progress"] == 1
    events = {r["event_type"] for r in await pool.fetch("SELECT event_type FROM system_events WHERE task_id=$1", tid)}
    assert {"task.submitted", "task.assigned", "task.started", "task.completed"} <= events


async def test_fifo_dispatch_order(broker, pool):
    ids = [await submit(pool, "delay", {"seconds": 1}) for _ in range(4)]
    wid = await register(broker, capacity=4)
    got = await claim(broker, wid, n=4)
    assert [a["task_id"] for a in got] == [str(i) for i in ids]


async def test_capacity_is_never_exceeded(broker, pool):
    for _ in range(5):
        await submit(pool, "delay", {"seconds": 1})
    wid = await register(broker, capacity=2)
    first = await claim(broker, wid, n=5)
    assert len(first) == 2
    assert await claim(broker, wid, n=5, wait=0.5) == []  # both slots still held


async def test_duplicate_complete_is_idempotent(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1})
    wid = await register(broker)
    [a] = await claim(broker, wid)
    await broker.post(f"/v1/tasks/{tid}/start", json=fenced(wid, a))
    body = fenced(wid, a) | {"result": {"v": 1}, "duration_ms": 1}
    assert (await broker.post(f"/v1/tasks/{tid}/complete", json=body)).json()["status"] == "committed"
    dup = await broker.post(f"/v1/tasks/{tid}/complete", json=body | {"result": {"v": 999}})
    assert dup.status_code == 200 and dup.json()["status"] == "duplicate"
    assert await pool.fetchval("SELECT result FROM tasks WHERE id=$1", tid) == {"v": 1}


async def test_stale_attempt_cannot_overwrite_committed_result(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1})
    w1, w2 = await register(broker, "w1"), await register(broker, "w2")
    [a1] = await claim(broker, w1)
    await broker.post(f"/v1/tasks/{tid}/start", json=fenced(w1, a1))
    await pool.execute("UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id=$1", tid)
    await wait_until(lambda: pool.fetchval("SELECT status = 'RETRY_WAIT' FROM tasks WHERE id=$1", tid),
                     message="lease recovery")
    async with heartbeating(broker, w2):
        [a2] = await claim(broker, w2, wait=3)
    assert a2["fencing_token"] == 2
    # the old owner renews and completes late: both rejected
    r = await broker.post(f"/v1/tasks/{tid}/renew", json=fenced(w1, a1))
    assert r.status_code == 409 and r.json()["error"]["code"] == "stale_attempt"
    await broker.post(f"/v1/tasks/{tid}/start", json=fenced(w2, a2))
    await broker.post(f"/v1/tasks/{tid}/complete", json=fenced(w2, a2) | {"result": {"by": "w2"}, "duration_ms": 1})
    stale = await broker.post(f"/v1/tasks/{tid}/complete", json=fenced(w1, a1) | {"result": {"by": "w1"}, "duration_ms": 1})
    assert stale.status_code == 409 and stale.json()["error"]["code"] == "stale_attempt"
    assert await pool.fetchval("SELECT result FROM tasks WHERE id=$1", tid) == {"by": "w2"}
    assert await pool.fetchval("SELECT count(*) FROM system_events WHERE task_id=$1 AND event_type LIKE 'task.stale_%'",
                               tid) == 2


async def test_retry_with_backoff_then_terminal_failure(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1}, max_attempts=2)
    wid = await register(broker)
    async with heartbeating(broker, wid):
        for attempt in (1, 2):
            [a] = await claim(broker, wid, wait=3)
            assert a["fencing_token"] == attempt
            await broker.post(f"/v1/tasks/{tid}/start", json=fenced(wid, a))
            r = await broker.post(f"/v1/tasks/{tid}/fail", json=fenced(wid, a) | {
                "error_type": "TRANSIENT", "message": "boom", "retryable": True, "duration_ms": 1})
            assert r.json()["status"] == ("retry_scheduled" if attempt == 1 else "failed")
    path = [(r["from_status"], r["to_status"]) for r in
            await pool.fetch("SELECT from_status, to_status FROM task_transitions WHERE task_id=$1 ORDER BY id", tid)]
    assert ("RETRY_WAIT", "ASSIGNED") in path and path[-1] == ("RETRY_WAIT", "FAILED")
    gap = await pool.fetchval("""SELECT EXTRACT(EPOCH FROM (b.assigned_at - a.finished_at))::float8
                                  FROM task_attempts a JOIN task_attempts b ON b.task_id=a.task_id AND b.attempt_no=2
                                 WHERE a.task_id=$1 AND a.attempt_no=1""", tid)
    assert gap >= 0.18  # base backoff 0.2 s


async def test_non_retryable_failure_skips_remaining_attempts(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1}, max_attempts=5)
    wid = await register(broker)
    [a] = await claim(broker, wid)
    await broker.post(f"/v1/tasks/{tid}/start", json=fenced(wid, a))
    r = await broker.post(f"/v1/tasks/{tid}/fail", json=fenced(wid, a) | {
        "error_type": "PERMANENT", "message": "bad", "retryable": False, "duration_ms": 1})
    assert r.json()["status"] == "failed"
    assert await pool.fetchval("SELECT status FROM tasks WHERE id=$1", tid) == "FAILED"


async def test_unacknowledged_assignment_is_redelivered(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1})
    wid = await register(broker)
    [a] = await claim(broker, wid)
    await asyncio.sleep(1.1)  # response "lost": the worker never STARTed it
    again = await claim(broker, wid)
    assert again and again[0]["attempt_id"] == a["attempt_id"]
    assert (await broker.post(f"/v1/tasks/{tid}/start", json=fenced(wid, a))).status_code == 200


async def test_worker_lifecycle_messages(broker, pool):
    import uuid
    r = await broker.post(f"/v1/workers/{uuid.uuid4()}/heartbeat", json={"state": "active", "active": [], "free_slots": 1})
    assert r.status_code == 410
    tid = await submit(pool, "delay", {"seconds": 1})
    wid = await register(broker, capacity=2)
    [a] = await claim(broker, wid)
    assert (await broker.post(f"/v1/workers/{wid}/drain", json={"reason": "test"})).json()["status"] == "DRAINING"
    r = await broker.post(f"/v1/workers/{wid}/claim", json={"max_tasks": 1, "wait_s": 0.1})
    assert r.status_code == 409 and r.json()["error"]["code"] == "worker_draining"
    r = await broker.post(f"/v1/workers/{wid}/deregister", json={"reason": "test"})
    assert r.json() == {"status": "STOPPED", "released": 1}
    row = await pool.fetchrow("SELECT status, available_at <= now() AS ready FROM tasks WHERE id=$1", tid)
    assert row["status"] == "RETRY_WAIT" and row["ready"]  # released without backoff
    assert await pool.fetchval("SELECT status FROM task_attempts WHERE task_id=$1", tid) == "RELEASED"


async def test_heartbeat_reports_revoked_attempts(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1})
    wid = await register(broker)
    [a] = await claim(broker, wid)
    await pool.execute("UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id=$1", tid)
    await wait_until(lambda: pool.fetchval("SELECT status = 'RETRY_WAIT' FROM tasks WHERE id=$1", tid))
    r = await broker.post(f"/v1/workers/{wid}/heartbeat", json={"state": "active", "free_slots": 0, "active": [
        {"task_id": a["task_id"], "attempt_id": a["attempt_id"], "fencing_token": 1}]})
    assert r.json()["revoked"] == [a["task_id"]]


async def test_oversized_result_is_refused(broker, pool):
    tid = await submit(pool, "delay", {"seconds": 1})
    wid = await register(broker)
    [a] = await claim(broker, wid)
    await broker.post(f"/v1/tasks/{tid}/start", json=fenced(wid, a))
    r = await broker.post(f"/v1/tasks/{tid}/complete", json=fenced(wid, a) | {"result": {"x": "y" * 70_000}, "duration_ms": 1})
    assert r.status_code == 413
