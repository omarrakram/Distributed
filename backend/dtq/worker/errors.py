"""Errors raised while executing tasks (mapped to the protocol's FAIL error types)."""

from __future__ import annotations

from dataclasses import dataclass


class TaskError(Exception):
    """A handler-level failure. ``retryable`` decides between RETRY_WAIT and FAILED."""

    def __init__(self, message: str, error_type: str = "TRANSIENT", retryable: bool = True) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.retryable = retryable


class TaskTimeout(TaskError):
    def __init__(self, timeout_s: float) -> None:
        super().__init__(f"execution exceeded timeout of {timeout_s:g}s; handler terminated",
                         "TIMEOUT", retryable=True)


class HandlerCrashed(TaskError):
    def __init__(self, exitcode: int | None) -> None:
        super().__init__(f"handler process exited unexpectedly (exit code {exitcode})",
                         "HANDLER_CRASHED", retryable=True)


@dataclass(frozen=True)
class ExecContext:
    task_id: str
    attempt_no: int
    worker_name: str
