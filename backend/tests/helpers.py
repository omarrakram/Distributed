"""Helpers shared by integration and system tests."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Any, Awaitable, Callable

import httpx
from fastapi import FastAPI

from dtq.common.task_specs import validate_payload
from dtq.db import tasks_repo


async def wait_until(predicate: Callable[[], Awaitable[Any]], timeout: float = 20.0, interval: float = 0.1,
                     message: str = "condition") -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = await predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {message}")
        await asyncio.sleep(interval)


async def submit(pool, task_type: str = "delay", payload: dict | None = None, *, max_attempts: int = 3,
                 timeout_seconds: int | None = None, idempotency_key: str | None = None,
                 namespace: str = "default", priority: int = 0) -> uuid.UUID:
    normalised, default_timeout = validate_payload(task_type, payload or {})
    async with pool.acquire() as conn:
        task, _ = await tasks_repo.submit(
            conn, task_id=uuid.uuid4(), task_type=task_type, payload=normalised, namespace=namespace,
            priority=priority, max_attempts=max_attempts, timeout_seconds=timeout_seconds or default_timeout,
            idempotency_key=idempotency_key)
    return task["id"]


@contextlib.asynccontextmanager
async def running_app(app: FastAPI, headers: dict[str, str] | None = None):
    """Run an ASGI app's lifespan in-process and yield an httpx client bound to it."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=headers or {},
                                     timeout=30) as client:
            yield client
