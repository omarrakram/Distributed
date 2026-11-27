import asyncio
import multiprocessing
import time

import pytest

from dtq.worker import executor
from dtq.worker.errors import ExecContext, TaskError, TaskTimeout

CTX = ExecContext(task_id="exec-test-task", attempt_no=1, worker_name="w")


async def test_cpu_task_runs_in_child_process_with_progress():
    seen = []
    out = await executor.execute("prime_count", {"start": 0, "end": 2_000_000}, CTX, 30,
                                 lambda f, m: seen.append(f))
    assert out["count"] == 148_933
    assert seen and seen[-1] == pytest.approx(1.0)


async def test_timeout_kills_the_child_process():
    started = time.monotonic()
    with pytest.raises(TaskTimeout):
        await executor.execute("matrix_multiply", {"size": 300}, CTX, 0.5, lambda *_: None)
    assert time.monotonic() - started < 5
    await asyncio.sleep(0.2)
    assert not [p for p in multiprocessing.active_children() if p.name.startswith("dtq-task")]


async def test_cancellation_kills_the_child_process():
    task = asyncio.create_task(executor.execute("matrix_multiply", {"size": 300}, CTX, 60, lambda *_: None))
    await asyncio.sleep(0.8)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.2)
    assert not [p for p in multiprocessing.active_children() if p.name.startswith("dtq-task")]


async def test_invalid_payload_is_not_retryable():
    with pytest.raises(TaskError) as exc:
        await executor.execute("matrix_multiply", {"size": 99999}, CTX, 5, lambda *_: None)
    assert exc.value.error_type == "INVALID_PAYLOAD" and exc.value.retryable is False


async def test_io_task_timeout():
    with pytest.raises(TaskTimeout):
        await executor.execute("delay", {"seconds": 5}, CTX, 0.2, lambda *_: None)
