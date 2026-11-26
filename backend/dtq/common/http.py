"""Shared HTTP helpers: error envelope, token authentication, client retry with backoff."""

from __future__ import annotations

import asyncio
import hmac
import logging
import random
from typing import Any, Awaitable, Callable, TypeVar

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import SecretStr

log = logging.getLogger(__name__)
T = TypeVar("T")


class ServiceError(Exception):
    """Raised by route handlers; rendered as {"error": {"code", "message", "details"}}."""

    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


def error_body(code: str, message: str, details: Any = None) -> dict:
    return {"error": {"code": code, "message": message, "details": details}}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def _service_error(_: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(error_body(exc.code, exc.message, exc.details), status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {"loc": list(e.get("loc", [])), "msg": e.get("msg"), "type": e.get("type")}
            for e in exc.errors()
        ]
        return JSONResponse(error_body("validation_error", "request validation failed", details),
                            status_code=422)


def tokens_equal(provided: str | None, expected: SecretStr) -> bool:
    return provided is not None and hmac.compare_digest(provided.encode(), expected.get_secret_value().encode())


async def retry_async(fn: Callable[[], Awaitable[T]], *, attempts: int | None = None,
                      base_s: float = 0.25, max_s: float = 10.0,
                      retry_on: tuple[type[BaseException], ...] = (httpx.TransportError,),
                      describe: str = "request", stop: Callable[[], bool] | None = None) -> T:
    """Call ``fn`` until it succeeds, retrying transport errors with capped exponential backoff + jitter.

    ``attempts=None`` retries forever (used by workers that must eventually report a result).
    """
    n = 0
    while True:
        n += 1
        try:
            return await fn()
        except retry_on as exc:
            if (attempts is not None and n >= attempts) or (stop is not None and stop()):
                raise
            delay = min(max_s, base_s * 2 ** (n - 1)) * random.uniform(0.5, 1.0)
            log.warning("%s failed (%s: %s); retry %d in %.2fs", describe, type(exc).__name__, exc, n, delay)
            await asyncio.sleep(delay)
