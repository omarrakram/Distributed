"""HTTP client for the broker protocol, used by worker processes.

HTTP status codes map onto protocol semantics:
  409 -> StaleError   (fencing token not current / draining) — stop working on it
  410 -> GoneError    (this worker incarnation is dead/unknown) — re-register
  413 -> ResultTooLarge
  5xx / transport errors -> BrokerUnavailable / httpx.TransportError — retry with backoff
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx

from dtq.common.protocol import (
    ClaimResponse, CompleteResponse, FailResponse, HeartbeatResponse, LeaseResponse, RegisterResponse,
)


class BrokerError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code


class StaleError(BrokerError):
    pass


class GoneError(BrokerError):
    pass


class ResultTooLarge(BrokerError):
    pass


class BrokerUnavailable(Exception):
    """5xx from the broker: treat like a transport failure (retry)."""


RETRYABLE = (httpx.TransportError, BrokerUnavailable)


class BrokerClient:
    def __init__(self, base_url: str, token: str, timeout_s: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._http = httpx.AsyncClient(
            base_url=self.base_url, headers={"X-Cluster-Token": token},
            timeout=httpx.Timeout(timeout_s), limits=httpx.Limits(max_connections=50),
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def _post(self, path: str, body: dict[str, Any], timeout: float | None = None) -> dict:
        resp = await self._http.post(path, json=body, timeout=timeout or self.timeout_s)
        if resp.status_code < 300:
            return resp.json()
        try:
            err = resp.json().get("error", {})
        except ValueError:
            err = {}
        code, message = err.get("code", "error"), err.get("message", resp.text[:200])
        if resp.status_code == 409:
            raise StaleError(409, code, message)
        if resp.status_code == 410:
            raise GoneError(410, code, message)
        if resp.status_code == 413:
            raise ResultTooLarge(413, code, message)
        if resp.status_code >= 500:
            raise BrokerUnavailable(f"{resp.status_code} {code}: {message}")
        raise BrokerError(resp.status_code, code, message)

    async def register(self, **fields: Any) -> RegisterResponse:
        return RegisterResponse.model_validate(await self._post("/v1/workers/register", fields))

    async def heartbeat(self, worker_id: UUID, state: str, active: list[dict], free_slots: int) -> HeartbeatResponse:
        body = {"state": state, "active": active, "free_slots": free_slots}
        return HeartbeatResponse.model_validate(await self._post(f"/v1/workers/{worker_id}/heartbeat", body))

    async def claim(self, worker_id: UUID, max_tasks: int, wait_s: float) -> ClaimResponse:
        body = {"max_tasks": max_tasks, "wait_s": wait_s}
        data = await self._post(f"/v1/workers/{worker_id}/claim", body, timeout=wait_s + self.timeout_s)
        return ClaimResponse.model_validate(data)

    @staticmethod
    def _fenced(worker_id: UUID, attempt_id: UUID, token: int) -> dict:
        return {"worker_id": str(worker_id), "attempt_id": str(attempt_id), "fencing_token": token}

    async def start(self, task_id: UUID, worker_id: UUID, attempt_id: UUID, token: int) -> LeaseResponse:
        data = await self._post(f"/v1/tasks/{task_id}/start", self._fenced(worker_id, attempt_id, token))
        return LeaseResponse.model_validate(data)

    async def renew(self, task_id: UUID, worker_id: UUID, attempt_id: UUID, token: int,
                    progress: float | None, message: str | None) -> LeaseResponse:
        body = self._fenced(worker_id, attempt_id, token) | {"progress": progress, "message": message}
        return LeaseResponse.model_validate(await self._post(f"/v1/tasks/{task_id}/renew", body))

    async def complete(self, task_id: UUID, worker_id: UUID, attempt_id: UUID, token: int,
                       result: dict, duration_ms: int) -> CompleteResponse:
        body = self._fenced(worker_id, attempt_id, token) | {"result": result, "duration_ms": duration_ms}
        return CompleteResponse.model_validate(await self._post(f"/v1/tasks/{task_id}/complete", body))

    async def fail(self, task_id: UUID, worker_id: UUID, attempt_id: UUID, token: int, error_type: str,
                   message: str, retryable: bool, duration_ms: int) -> FailResponse:
        body = self._fenced(worker_id, attempt_id, token) | {
            "error_type": error_type, "message": message[:2000], "retryable": retryable,
            "duration_ms": duration_ms,
        }
        return FailResponse.model_validate(await self._post(f"/v1/tasks/{task_id}/fail", body))

    async def drain(self, worker_id: UUID, reason: str) -> dict:
        return await self._post(f"/v1/workers/{worker_id}/drain", {"reason": reason})

    async def deregister(self, worker_id: UUID, reason: str) -> dict:
        return await self._post(f"/v1/workers/{worker_id}/deregister", {"reason": reason})
