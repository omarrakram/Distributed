"""asyncpg connection pool with JSON codecs and start-up retry."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import asyncpg

log = logging.getLogger(__name__)


def _json_default(value: Any) -> Any:
    if isinstance(value, (uuid.UUID, Decimal)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, separators=(",", ":"))


async def init_connection(conn: asyncpg.Connection) -> None:
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(typename, encoder=dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(
    dsn: str, min_size: int = 1, max_size: int = 10, *, attempts: int = 30, delay_s: float = 1.0
) -> asyncpg.Pool:
    """Create a pool, retrying while PostgreSQL is still starting (e.g. under Docker Compose)."""
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(
                dsn, min_size=min_size, max_size=max_size, init=init_connection, command_timeout=30
            )
        except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
            last_exc = exc
            log.warning("database not ready (attempt %d/%d): %s", attempt, attempts, exc)
            await asyncio.sleep(delay_s)
    raise RuntimeError(f"could not connect to database: {last_exc}")


async def connect(dsn: str) -> asyncpg.Connection:
    conn = await asyncpg.connect(dsn)
    await init_connection(conn)
    return conn
