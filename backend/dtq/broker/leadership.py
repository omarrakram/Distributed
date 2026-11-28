"""Leader election with PostgreSQL session advisory locks.

Exactly one process holding the lock acts as leader for a singleton control
loop (broker recovery, autoscaler). The lock is tied to a dedicated database
session: if the leader crashes or loses its connection, PostgreSQL releases
the lock and another instance acquires it on its next attempt.
Followers never act, but even if two leaders overlapped briefly, every action
they take is a guarded (compare-and-set) update, so safety is preserved.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

import asyncpg

log = logging.getLogger(__name__)


class LeaderElection:
    def __init__(self, dsn: str, key: str, *, retry_s: float = 2.0, check_s: float = 2.0,
                 on_acquired: Callable[[], Awaitable[None]] | None = None) -> None:
        self.dsn = dsn
        self.key = key
        self.retry_s = retry_s
        self.check_s = check_s
        self.on_acquired = on_acquired
        self.is_leader = False
        self._acquired_event = asyncio.Event()

    async def wait_until_leader(self, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._acquired_event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def run(self) -> None:
        while True:
            conn: asyncpg.Connection | None = None
            try:
                conn = await asyncpg.connect(self.dsn)
                while True:
                    got = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", self.key)
                    if got:
                        break
                    await asyncio.sleep(self.retry_s)
                self.is_leader = True
                self._acquired_event.set()
                log.info("acquired leadership", extra={"lock": self.key})
                if self.on_acquired is not None:
                    await self.on_acquired()
                while True:  # hold the session; detect loss of the connection
                    await asyncio.sleep(self.check_s)
                    await conn.fetchval("SELECT 1")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # connection lost or DB unavailable
                if self.is_leader:
                    log.warning("lost leadership: %s", exc, extra={"lock": self.key})
                self.is_leader = False
                self._acquired_event.clear()
                await asyncio.sleep(self.retry_s)
            finally:
                if conn is not None and not conn.is_closed():
                    self.is_leader = False
                    self._acquired_event.clear()
                    try:
                        await asyncio.wait_for(conn.close(), timeout=2)
                    except Exception:
                        conn.terminate()
