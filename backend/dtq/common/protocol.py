"""Broker <-> worker message protocol (shared by server and client).

Every message that mutates a task carries the *fencing pair*
``(attempt_id, fencing_token)``. The broker accepts it only if it matches the
task's current attempt; anything else is a stale message from an older
attempt and is rejected.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Msg(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------- registration
class RegisterRequest(_Msg):
    name: str = Field(min_length=1, max_length=100)
    host: str = Field(min_length=1, max_length=255)
    pid: int | None = None
    capacity: int = Field(ge=1, le=32)
    agent_id: str | None = Field(default=None, max_length=100)
    version: str | None = Field(default=None, max_length=40)
    labels: dict[str, str] = Field(default_factory=dict)


class RegisterResponse(_Msg):
    worker_id: UUID
    heartbeat_interval_s: float
    lease_duration_s: float
    renew_interval_s: float
    claim_wait_s: float
    server_time: datetime


# ------------------------------------------------------------------ heartbeat
class ActiveAttempt(_Msg):
    task_id: UUID
    attempt_id: UUID
    fencing_token: int


class HeartbeatRequest(_Msg):
    state: Literal["active", "draining"] = "active"
    active: list[ActiveAttempt] = Field(default_factory=list)
    free_slots: int = Field(ge=0, le=32)


class HeartbeatResponse(_Msg):
    status: str
    revoked: list[UUID] = Field(default_factory=list)
    server_time: datetime


# ---------------------------------------------------------------------- claim
class ClaimRequest(_Msg):
    max_tasks: int = Field(ge=1, le=32)
    wait_s: float = Field(default=20.0, ge=0, le=120)


class Assignment(_Msg):
    task_id: UUID
    attempt_id: UUID
    fencing_token: int
    type: str
    payload: dict[str, Any]
    timeout_seconds: int
    max_attempts: int
    lease_expires_at: datetime


class ClaimResponse(_Msg):
    tasks: list[Assignment] = Field(default_factory=list)


# ------------------------------------------------------------ task messages
class FencedRequest(_Msg):
    worker_id: UUID
    attempt_id: UUID
    fencing_token: int = Field(ge=1)


class StartRequest(FencedRequest):
    pass


class RenewRequest(FencedRequest):
    progress: float | None = Field(default=None, ge=0, le=1)
    message: str | None = Field(default=None, max_length=200)


class LeaseResponse(_Msg):
    lease_expires_at: datetime


class CompleteRequest(FencedRequest):
    result: dict[str, Any]
    duration_ms: int = Field(ge=0)


class CompleteResponse(_Msg):
    status: Literal["committed", "duplicate"]


ErrorType = Literal[
    "TRANSIENT",
    "PERMANENT",
    "TIMEOUT",
    "HANDLER_CRASHED",
    "INVALID_PAYLOAD",
    "RESULT_TOO_LARGE",
    "WORKER_SHUTDOWN",
    "INTERNAL",
]


class FailRequest(FencedRequest):
    error_type: ErrorType
    message: str = Field(max_length=2000)
    retryable: bool = True
    duration_ms: int = Field(default=0, ge=0)


class FailResponse(_Msg):
    status: Literal["retry_scheduled", "failed", "duplicate"]
    next_attempt_at: datetime | None = None


class DrainRequest(_Msg):
    reason: str = Field(default="requested", max_length=200)


class DeregisterRequest(_Msg):
    reason: str = Field(default="shutdown", max_length=200)


class StaleReason:
    """Reasons used in 409 responses."""

    STALE_ATTEMPT = "stale_attempt"
    LEASE_EXPIRED = "lease_expired"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    INVALID_STATE = "invalid_state"
    NOT_FOUND = "not_found"
