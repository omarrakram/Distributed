"""Broker crash/restart: no task is lost; leases get a restart grace period."""

from __future__ import annotations

import asyncio
import signal

import pytest

from tests.helpers import submit, wait_until
from tests.system.conftest import wait_workers

pytestmark = pytest.mark.system


async def test_broker_crash_and_restart_loses_no_task(stack, pool):
    stack.broker()
    await stack.wait_http(f"{stack.broker_url}/health")
    stack.worker("w1", capacity=2)
    stack.worker("w2", capacity=2)
    await wait_workers(pool, 2)
    ids = [await submit(pool, "delay", {"seconds": 3}) for _ in range(8)]
    await wait_until(lambda: pool.fetchval("SELECT count(*) >= 2 FROM tasks WHERE status='RUNNING'"))
    stack.signal("broker", signal.SIGKILL)
    await asyncio.sleep(2)
    stack.broker("broker-restarted")
    await stack.wait_http(f"{stack.broker_url}/health")
    await wait_until(lambda: pool.fetchval("SELECT bool_and(status='COMPLETED') FROM tasks WHERE id = ANY($1::uuid[])", ids),
                     timeout=60, message="all tasks completed after broker restart")
    assert await pool.fetchval("SELECT count(*) FROM tasks WHERE status='COMPLETED'") == 8
    assert await pool.fetchval("SELECT count(*) FROM system_events WHERE event_type='recovery.leader_acquired'") == 2
