"""Typed specifications of the supported task types.

The API validates every submitted payload against these models (rejecting
anything unknown or out of range) and workers re-validate before execution.
Only names in ``TASK_TYPES`` can ever be executed: there is no way to submit
code. Limits keep every task bounded in CPU time and memory.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PrimeCountPayload(_Payload):
    """Count primes in [start, end) with a segmented sieve (CPU-bound)."""

    start: int = Field(0, ge=0, le=50_000_000)
    end: int = Field(2_000_000, ge=1, le=50_000_000)

    @model_validator(mode="after")
    def _range(self) -> "PrimeCountPayload":
        if self.end <= self.start:
            raise ValueError("end must be greater than start")
        if self.end - self.start > 20_000_000:
            raise ValueError("range (end - start) must be <= 20,000,000")
        return self


class MatrixMultiplyPayload(_Payload):
    """Multiply two seeded pseudo-random integer matrices (CPU-bound, pure Python)."""

    size: int = Field(120, ge=1, le=300)
    seed: int = Field(42, ge=0, le=2**31 - 1)
    value_range: int = Field(10, ge=1, le=1000)


ImageOp = Literal[
    "grayscale", "blur", "sharpen", "edge_detect", "emboss", "contour",
    "invert", "equalize", "threshold", "rotate", "resize",
]


class ImageProcessPayload(_Payload):
    """Generate a deterministic synthetic image and apply a whitelisted pipeline (CPU-bound)."""

    width: int = Field(512, ge=16, le=1024)
    height: int = Field(512, ge=16, le=1024)
    pattern: Literal["rings", "gradient", "checker", "noise"] = "rings"
    seed: int = Field(7, ge=0, le=2**31 - 1)
    operations: list[ImageOp] = Field(
        default_factory=lambda: ["grayscale", "blur", "edge_detect"], min_length=1, max_length=8
    )
    blur_radius: float = Field(2.0, ge=0.5, le=10.0)
    rotate_degrees: float = Field(30.0, ge=-360.0, le=360.0)
    threshold: int = Field(128, ge=0, le=255)
    resize_factor: float = Field(0.5, ge=0.1, le=2.0)
    return_thumbnail: bool = True


class Sha256Payload(_Payload):
    """Iterated SHA-256 hash chain h_{i+1} = sha256(h_i) (CPU-bound)."""

    data: str = Field("distributed-systems", max_length=1_000_000)
    iterations: int = Field(100_000, ge=1, le=2_000_000)


class DelayPayload(_Payload):
    """Wait for a configurable time (waiting-bound, no CPU)."""

    seconds: float = Field(5.0, ge=0, le=300)


class FailTransientPayload(_Payload):
    """Fail on the first `fail_times` attempts, then succeed (demonstrates retries)."""

    fail_times: int = Field(2, ge=1, le=10)
    seconds: float = Field(1.0, ge=0, le=60)
    message: str = Field("intentional transient failure", max_length=200)


class FailPermanentPayload(_Payload):
    """Always fail. retryable=True shows bounded retries; False fails immediately."""

    message: str = Field("intentional permanent failure", max_length=200)
    retryable: bool = True
    seconds: float = Field(0.5, ge=0, le=60)


@dataclass(frozen=True)
class TaskTypeSpec:
    name: str
    title: str
    kind: Literal["cpu", "io"]
    description: str
    payload_model: type[_Payload]
    default_timeout: Callable[[Any], int]
    example: dict[str, Any]

    def schema(self) -> dict[str, Any]:
        return self.payload_model.model_json_schema()


TASK_TYPES: dict[str, TaskTypeSpec] = {
    spec.name: spec
    for spec in (
        TaskTypeSpec(
            "prime_count", "Prime number calculation", "cpu",
            "Counts primes in [start, end) using a segmented sieve of Eratosthenes.",
            PrimeCountPayload, lambda p: 120, {"start": 0, "end": 2_000_000},
        ),
        TaskTypeSpec(
            "matrix_multiply", "Matrix multiplication", "cpu",
            "Multiplies two seeded size×size integer matrices; returns checksum, trace and digest.",
            MatrixMultiplyPayload, lambda p: 120, {"size": 120, "seed": 42},
        ),
        TaskTypeSpec(
            "image_process", "Image processing", "cpu",
            "Renders a synthetic image and applies a whitelisted Pillow pipeline; returns stats + thumbnail.",
            ImageProcessPayload, lambda p: 60,
            {"width": 512, "height": 512, "pattern": "rings", "operations": ["grayscale", "blur", "edge_detect"]},
        ),
        TaskTypeSpec(
            "sha256", "SHA-256 hashing", "cpu",
            "Computes an iterated SHA-256 hash chain over the given data.",
            Sha256Payload, lambda p: 60, {"data": "distributed-systems", "iterations": 100_000},
        ),
        TaskTypeSpec(
            "delay", "Delayed execution", "io",
            "Waits for the configured number of seconds (waiting-intensive workload).",
            DelayPayload, lambda p: int(p.seconds) + 30, {"seconds": 5},
        ),
        TaskTypeSpec(
            "fail_transient", "Intentional transient failure", "io",
            "Fails on the first `fail_times` attempts and succeeds afterwards.",
            FailTransientPayload, lambda p: int(p.seconds) + 30, {"fail_times": 2, "seconds": 1},
        ),
        TaskTypeSpec(
            "fail_permanent", "Intentional permanent failure", "io",
            "Always fails; retryable=true demonstrates bounded retries, false fails immediately.",
            FailPermanentPayload, lambda p: int(p.seconds) + 30, {"message": "boom", "retryable": True},
        ),
    )
}


class UnknownTaskType(ValueError):
    pass


def get_spec(task_type: str) -> TaskTypeSpec:
    try:
        return TASK_TYPES[task_type]
    except KeyError:
        raise UnknownTaskType(
            f"unknown task type {task_type!r}; supported: {', '.join(sorted(TASK_TYPES))}"
        ) from None


def validate_payload(task_type: str, payload: dict[str, Any] | None) -> tuple[dict[str, Any], int]:
    """Validate and normalise a payload. Returns (normalised payload, default timeout seconds).

    Raises ``UnknownTaskType`` or ``pydantic.ValidationError``.
    """
    spec = get_spec(task_type)
    model = spec.payload_model.model_validate(payload or {})
    return model.model_dump(mode="json"), spec.default_timeout(model)
