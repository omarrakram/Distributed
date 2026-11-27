"""Shared fixtures: a throw-away PostgreSQL database per test session, cleaned before every test.

Set DTQ_TEST_ADMIN_DSN to a role that may CREATE/DROP DATABASE
(default: postgresql://dtq:dtq@127.0.0.1:5432/postgres).
"""

from __future__ import annotations

import asyncio
import os
import uuid

import asyncpg
import pytest

from dtq.common.config import ApiSettings, BrokerSettings
from dtq.db.migrate import apply_migrations
from dtq.db.pool import create_pool

ADMIN_DSN = os.environ.get("DTQ_TEST_ADMIN_DSN", "postgresql://dtq:dtq@127.0.0.1:5432/postgres")

DATA_TABLES = ("tasks", "task_attempts", "task_transitions", "workers", "system_events", "scaling_decisions",
               "control_commands", "metrics_samples", "benchmark_runs", "demo_runs")


def _dsn_for(name: str) -> str:
    base, _, _ = ADMIN_DSN.rpartition("/")
    return f"{base}/{name}"


@pytest.fixture(scope="session")
def database_url() -> str:
    name = f"dtq_test_{uuid.uuid4().hex[:10]}"
    dsn = _dsn_for(name)

    async def setup() -> None:
        conn = await asyncpg.connect(ADMIN_DSN)
        try:
            await conn.execute(f'CREATE DATABASE "{name}"')
        finally:
            await conn.close()
        await apply_migrations(dsn)

    async def teardown() -> None:
        conn = await asyncpg.connect(ADMIN_DSN)
        try:
            await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        finally:
            await conn.close()

    asyncio.run(setup())
    yield dsn
    asyncio.run(teardown())


async def reset_database(pool: asyncpg.Pool) -> None:
    await pool.execute(f"TRUNCATE {', '.join(DATA_TABLES)} RESTART IDENTITY CASCADE")
    await pool.execute("DELETE FROM autoscaler_config; INSERT INTO autoscaler_config (id) VALUES (1)")
    await pool.execute("DELETE FROM autoscaler_state; INSERT INTO autoscaler_state (id) VALUES (1)")


@pytest.fixture
async def pool(database_url: str):
    p = await create_pool(database_url, 1, 10)
    await reset_database(p)
    try:
        yield p
    finally:
        await p.close()


def fast_broker_settings(database_url: str, **overrides) -> BrokerSettings:
    """Short timing parameters so failure-detection tests run in seconds."""
    values = dict(
        database_url=database_url, heartbeat_interval_s=0.3, worker_suspect_after_s=0.9,
        worker_dead_after_s=1.5, lease_duration_s=2.0, assign_ack_timeout_s=1.5, claim_max_wait_s=2.0,
        dispatch_tick_s=0.1, reaper_interval_s=0.2, deadline_grace_s=0.5, retry_backoff_base_s=0.2,
        retry_backoff_max_s=1.0, retry_jitter=0.0,
    )
    values.update(overrides)
    return BrokerSettings(**values)


def api_settings(database_url: str, **overrides) -> ApiSettings:
    values = dict(database_url=database_url, broker_url="http://127.0.0.1:9", agent_urls="http://127.0.0.1:9",
                  request_timeout_s=1.0, ws_interval_s=0.5)
    values.update(overrides)
    return ApiSettings(**values)
