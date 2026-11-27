"""Task execution with enforced timeouts and cancellation.

* CPU-bound handlers run in a dedicated child process per task (``forkserver``
  start method on POSIX: fast and safe with threads). The parent polls a pipe
  for progress/result messages and can SIGKILL the child on timeout or when the
  lease is lost. The child limits its own address space (RLIMIT_AS).
* Waiting-bound handlers run as coroutines; ``asyncio.wait_for`` enforces the
  timeout and cancellation interrupts them immediately.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import signal
import sys
from typing import Any, Callable

from pydantic import ValidationError

from dtq.common.task_specs import get_spec
from dtq.worker.errors import ExecContext, HandlerCrashed, TaskError, TaskTimeout
from dtq.worker.handlers.io import IO_HANDLERS

ProgressCb = Callable[[float, str | None], None]

_CTX: Any = None


def mp_context():
    """forkserver on POSIX (handlers preloaded once), spawn elsewhere (Windows/macOS native)."""
    global _CTX
    if _CTX is None:
        if sys.platform.startswith("linux") and "forkserver" in mp.get_all_start_methods():
            _CTX = mp.get_context("forkserver")
            _CTX.set_forkserver_preload(["dtq.worker.handlers.cpu", "PIL.Image", "PIL.ImageFilter"])
        else:
            _CTX = mp.get_context("spawn")
    return _CTX


def _child_main(task_type: str, payload: dict, ctx: dict, conn, memory_limit_mb: int) -> None:
    """Entry point of the CPU child process (must be importable for spawn/forkserver)."""
    import time

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        if memory_limit_mb > 0:
            try:
                import resource

                limit = memory_limit_mb * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
            except (ImportError, ValueError, OSError):
                pass
        from dtq.worker.handlers.cpu import CPU_HANDLERS

        model = get_spec(task_type).payload_model.model_validate(payload)
        last = {"t": 0.0, "f": -1.0}

        def progress(fraction: float, message: str | None = None) -> None:
            now = time.monotonic()
            if fraction >= 1.0 or now - last["t"] >= 0.25:
                last["t"], last["f"] = now, fraction
                conn.send(("progress", float(fraction), message))

        result = CPU_HANDLERS[task_type](model, ExecContext(**ctx), progress)
        conn.send(("result", result))
    except TaskError as exc:
        conn.send(("error", exc.error_type, str(exc), exc.retryable))
    except ValidationError as exc:
        conn.send(("error", "INVALID_PAYLOAD", str(exc)[:500], False))
    except MemoryError:
        conn.send(("error", "PERMANENT", f"memory limit of {memory_limit_mb} MiB exceeded", False))
    except Exception as exc:  # noqa: BLE001 - report anything else as a transient failure
        conn.send(("error", "TRANSIENT", f"{type(exc).__name__}: {exc}", True))
    finally:
        conn.close()


async def run_cpu(task_type: str, payload: dict, ctx: ExecContext, timeout_s: float,
                  on_progress: ProgressCb, memory_limit_mb: int) -> dict:
    mpc = mp_context()
    parent_conn, child_conn = mpc.Pipe(duplex=False)
    proc = mpc.Process(
        target=_child_main,
        args=(task_type, payload, ctx.__dict__, child_conn, memory_limit_mb),
        daemon=True,
        name=f"dtq-task-{ctx.task_id[:8]}",
    )
    await asyncio.to_thread(proc.start)
    child_conn.close()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    try:
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TaskTimeout(timeout_s)
            ready = await asyncio.to_thread(parent_conn.poll, min(0.25, remaining))
            if ready:
                try:
                    msg = parent_conn.recv()
                except EOFError:
                    await asyncio.to_thread(proc.join, 2)
                    raise HandlerCrashed(proc.exitcode) from None
                kind = msg[0]
                if kind == "progress":
                    on_progress(msg[1], msg[2])
                elif kind == "result":
                    return msg[1]
                elif kind == "error":
                    raise TaskError(msg[2], msg[1], msg[3])
            elif not proc.is_alive() and not parent_conn.poll():
                raise HandlerCrashed(proc.exitcode)
    finally:
        if proc.is_alive():
            try:
                proc.kill()  # SIGKILL on POSIX, TerminateProcess on Windows
            except (ProcessLookupError, OSError):
                pass
        await asyncio.to_thread(proc.join, 2)
        parent_conn.close()


async def execute(task_type: str, payload: dict, ctx: ExecContext, timeout_s: float,
                  on_progress: ProgressCb, memory_limit_mb: int = 1024) -> dict:
    """Validate and run one task. Raises TaskError subclasses on failure."""
    spec = get_spec(task_type)
    try:
        model = spec.payload_model.model_validate(payload)
    except ValidationError as exc:
        raise TaskError(f"invalid payload: {exc}"[:500], "INVALID_PAYLOAD", retryable=False) from None
    if spec.kind == "io":
        try:
            return await asyncio.wait_for(IO_HANDLERS[task_type](model, ctx, on_progress), timeout_s)
        except asyncio.TimeoutError:
            raise TaskTimeout(timeout_s) from None
    return await run_cpu(task_type, payload, ctx, timeout_s, on_progress, memory_limit_mb)
