"""Multi-process test harness: every component runs as a separate OS process."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

BACKEND = Path(__file__).resolve().parents[2]

FAST_ENV = {
    "DTQ_HEARTBEAT_INTERVAL_S": "0.5", "DTQ_WORKER_SUSPECT_AFTER_S": "1.5", "DTQ_WORKER_DEAD_AFTER_S": "3",
    "DTQ_LEASE_DURATION_S": "4", "DTQ_ASSIGN_ACK_TIMEOUT_S": "3", "DTQ_REAPER_INTERVAL_S": "0.3",
    "DTQ_DISPATCH_TICK_S": "0.2", "DTQ_RETRY_BACKOFF_BASE_S": "0.3", "DTQ_RETRY_BACKOFF_MAX_S": "2",
    "DTQ_RETRY_JITTER": "0", "DTQ_CLAIM_MAX_WAIT_S": "5", "DTQ_DEADLINE_GRACE_S": "1",
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Stack:
    """Starts components with `python -m dtq ...` and cleans them up (whole process groups)."""

    def __init__(self, dsn: str, logdir: Path) -> None:
        self.dsn = dsn
        self.logdir = logdir
        self.procs: dict[str, subprocess.Popen] = {}
        self.broker_port = free_port()
        self.api_port = free_port()
        self.agent_port = free_port()

    @property
    def broker_url(self) -> str:
        return f"http://127.0.0.1:{self.broker_port}"

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.api_port}"

    @property
    def agent_url(self) -> str:
        return f"http://127.0.0.1:{self.agent_port}"

    def start(self, name: str, *args: str, **env: str) -> subprocess.Popen:
        full_env = dict(os.environ, DTQ_DATABASE_URL=self.dsn, DTQ_LOG_FORMAT="console", DTQ_LOG_LEVEL="INFO",
                        PYTHONPATH=str(BACKEND), **FAST_ENV)
        full_env.update(env)
        log = open(self.logdir / f"{name}.log", "ab")
        proc = subprocess.Popen([sys.executable, "-m", "dtq", *args], cwd=str(BACKEND), env=full_env,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        log.close()
        self.procs[name] = proc
        return proc

    def broker(self, name: str = "broker") -> subprocess.Popen:
        return self.start(name, "broker", DTQ_PORT=str(self.broker_port))

    def worker(self, name: str, capacity: int = 1, broker_url: str | None = None, **env: str) -> subprocess.Popen:
        return self.start(name, "worker", "--broker-url", broker_url or self.broker_url, "--name", name,
                          "--capacity", str(capacity), **env)

    def api(self, name: str = "api") -> subprocess.Popen:
        return self.start(name, "api", DTQ_PORT=str(self.api_port), DTQ_BROKER_URL=self.broker_url,
                          DTQ_AGENT_URLS=self.agent_url)

    def agent(self) -> subprocess.Popen:
        return self.start("agent", "agent", DTQ_PORT=str(self.agent_port), DTQ_AGENT_BACKEND="process",
                          DTQ_WORKER_BROKER_URL=self.broker_url, DTQ_WORKER_GRACEFUL_TIMEOUT_S="5",
                          DTQ_AGENT_WORKER_LOG_DIR=str(self.logdir / "agent-workers"))

    def autoscaler(self) -> subprocess.Popen:
        return self.start("autoscaler", "autoscaler", DTQ_BROKER_URL=self.broker_url, DTQ_AGENT_URLS=self.agent_url,
                          DTQ_AUTOSCALER_ALIVE_FILE=str(self.logdir / "alive"))

    def signal(self, name: str, sig: int, group: bool = True) -> None:
        proc = self.procs[name]
        if group:
            os.killpg(os.getpgid(proc.pid), sig)
        else:
            proc.send_signal(sig)

    async def wait_http(self, url: str, timeout: float = 30) -> None:
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient() as c:
            while time.monotonic() < deadline:
                with contextlib.suppress(httpx.HTTPError):
                    if (await c.get(url, timeout=2)).status_code == 200:
                        return
                await asyncio.sleep(0.2)
        raise AssertionError(f"{url} did not become healthy")

    def stop_all(self) -> None:
        for name in reversed(list(self.procs)):
            proc = self.procs[name]
            if proc.poll() is None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        deadline = time.monotonic() + 15
        for proc in self.procs.values():
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=max(0.1, deadline - time.monotonic()))
            if proc.poll() is None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=5)


@pytest.fixture
async def stack(pool, database_url, tmp_path):
    s = Stack(database_url, tmp_path)
    try:
        yield s
    finally:
        s.stop_all()


class TcpProxy:
    """A toggleable TCP forwarder used to simulate network partitions between a worker and the broker."""

    def __init__(self, target_port: int) -> None:
        self.target_port = target_port
        self.port = free_port()
        self.blocked = False
        self._writers: set[asyncio.StreamWriter] = set()
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", self.port)

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while not reader.at_eof():
                data = await reader.read(65536)
                if not data or self.blocked:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.blocked:
            writer.close()
            return
        try:
            up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.target_port)
        except OSError:
            writer.close()
            return
        self._writers.update({writer, up_writer})
        await asyncio.gather(self._pipe(reader, up_writer), self._pipe(up_reader, writer))
        self._writers.difference_update({writer, up_writer})

    def block(self) -> None:
        self.blocked = True
        for w in list(self._writers):
            with contextlib.suppress(Exception):
                w.close()

    def unblock(self) -> None:
        self.blocked = False

    async def close(self) -> None:
        self.block()
        if self._server:
            self._server.close()
            await self._server.wait_closed()


# ------------------------------------------------------------------ shared helpers for system tests
async def worker_row(pool, name: str):
    return await pool.fetchrow("SELECT * FROM workers WHERE name=$1 ORDER BY registered_at DESC LIMIT 1", name)


async def wait_workers(pool, n: int, timeout: float = 30) -> None:
    from tests.helpers import wait_until

    await wait_until(lambda: pool.fetchval("SELECT count(*) >= $1 FROM workers WHERE status='ACTIVE'", n),
                     timeout=timeout, message=f"{n} active workers")


async def owner_of(pool, task_id):
    from tests.helpers import wait_until

    async def running():
        return await pool.fetchrow(
            """SELECT w.name, w.pid FROM tasks t JOIN workers w ON w.id = t.assigned_worker_id
                WHERE t.id=$1 AND t.status='RUNNING'""", task_id)
    return await wait_until(running, timeout=20, message="task to be RUNNING")
