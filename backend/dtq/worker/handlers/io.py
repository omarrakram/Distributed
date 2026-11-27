"""Waiting-bound and fault-injection handlers (run as coroutines inside the worker)."""

from __future__ import annotations

import asyncio
import time
from typing import Callable

from dtq.common.task_specs import DelayPayload, FailPermanentPayload, FailTransientPayload
from dtq.worker.errors import ExecContext, TaskError

Progress = Callable[[float, str | None], None]


async def _sleep_with_progress(seconds: float, progress: Progress) -> None:
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= seconds:
            break
        progress(min(1.0, elapsed / seconds) if seconds else 1.0, f"{elapsed:.1f}/{seconds:g}s")
        await asyncio.sleep(min(0.5, seconds - elapsed))


async def delay(p: DelayPayload, ctx: ExecContext, progress: Progress) -> dict:
    started = time.monotonic()
    await _sleep_with_progress(p.seconds, progress)
    return {"requested_seconds": p.seconds, "slept_seconds": round(time.monotonic() - started, 3)}


async def fail_transient(p: FailTransientPayload, ctx: ExecContext, progress: Progress) -> dict:
    await _sleep_with_progress(p.seconds, progress)
    if ctx.attempt_no <= p.fail_times:
        raise TaskError(f"{p.message} (attempt {ctx.attempt_no}; planned failures: {p.fail_times})",
                        "TRANSIENT", retryable=True)
    return {"succeeded_on_attempt": ctx.attempt_no, "planned_failures": p.fail_times}


async def fail_permanent(p: FailPermanentPayload, ctx: ExecContext, progress: Progress) -> dict:
    await _sleep_with_progress(p.seconds, progress)
    raise TaskError(p.message, "PERMANENT", retryable=p.retryable)


IO_HANDLERS = {
    "delay": delay,
    "fail_transient": fail_transient,
    "fail_permanent": fail_permanent,
}
