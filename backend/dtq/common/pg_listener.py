"""Resilient PostgreSQL LISTEN helper.

NOTIFY payloads are only wake-up hints: consumers always re-read the database,
and also poll periodically, so a missed notification can delay but never lose work.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable

import asyncpg

log = logging.getLogger(__name__)


async def listen_forever(dsn: str, channel: str, callback: Callable[[str], None],
                         reconnect_s: float = 2.0) -> None:
    while True:
        conn: asyncpg.Connection | None = None
        try:
            conn = await asyncpg.connect(dsn)
            closed = asyncio.Event()
            conn.add_termination_listener(lambda _c: closed.set())
            await conn.add_listener(channel, lambda _c, _pid, _ch, payload: callback(payload))
            log.info("listening on channel %s", channel)
            callback("")  # catch up on anything committed while we were not listening
            while not closed.is_set():
                try:
                    await asyncio.wait_for(closed.wait(), timeout=5)
                except asyncio.TimeoutError:
                    await conn.fetchval("SELECT 1")  # keep-alive / detect half-open sockets
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("listener on %s failed: %s", channel, exc)
        finally:
            if conn is not None and not conn.is_closed():
                conn.terminate()
        await asyncio.sleep(reconnect_s)
