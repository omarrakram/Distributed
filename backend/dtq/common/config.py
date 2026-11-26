"""Validated configuration for every DTQ component.

All settings are read from environment variables with the ``DTQ_`` prefix
(e.g. ``DTQ_DATABASE_URL``). Invalid combinations fail fast at start-up with a
clear error instead of producing subtle runtime misbehaviour.
"""

from __future__ import annotations

import os
import socket
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV_CLUSTER_TOKEN = "dev-cluster-token"
DEV_AGENT_TOKEN = "dev-agent-token"
DEV_ADMIN_TOKEN = "dev-admin-token"


def _split_csv(value: str) -> list[str]:
    return [item.strip().rstrip("/") for item in value.split(",") if item.strip()]


class CommonSettings(BaseSettings):
    """Settings shared by every process."""

    model_config = SettingsConfigDict(env_prefix="DTQ_", extra="ignore", case_sensitive=False)

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    def insecure_defaults(self) -> list[str]:
        """Names of token settings still using development defaults."""
        found = []
        for name, default in (
            ("cluster_token", DEV_CLUSTER_TOKEN),
            ("agent_token", DEV_AGENT_TOKEN),
            ("admin_token", DEV_ADMIN_TOKEN),
        ):
            value = getattr(self, name, None)
            if isinstance(value, SecretStr) and value.get_secret_value() == default:
                found.append(f"DTQ_{name.upper()}")
        return found


class DatabaseSettings(CommonSettings):
    database_url: str = "postgresql://dtq:dtq@localhost:5432/dtq"
    db_pool_min: int = Field(1, ge=1, le=50)
    db_pool_max: int = Field(10, ge=1, le=100)

    @model_validator(mode="after")
    def _check_pool(self) -> "DatabaseSettings":
        if self.db_pool_min > self.db_pool_max:
            raise ValueError("DTQ_DB_POOL_MIN must be <= DTQ_DB_POOL_MAX")
        if not self.database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("DTQ_DATABASE_URL must be a postgresql:// URL")
        return self


class BrokerSettings(DatabaseSettings):
    """Custom broker: scheduling, leases, failure detection and recovery."""

    host: str = "0.0.0.0"
    port: int = Field(8001, ge=1, le=65535)
    cluster_token: SecretStr = SecretStr(DEV_CLUSTER_TOKEN)

    heartbeat_interval_s: float = Field(2.0, gt=0, le=60)
    worker_suspect_after_s: float = Field(6.0, gt=0, le=600)
    worker_dead_after_s: float = Field(10.0, gt=0, le=1200)
    lease_duration_s: float = Field(15.0, ge=0.5, le=3600)
    assign_ack_timeout_s: float = Field(10.0, ge=0.5, le=600)
    claim_max_wait_s: float = Field(20.0, ge=0, le=120)
    dispatch_tick_s: float = Field(0.5, gt=0, le=10)
    reaper_interval_s: float = Field(1.0, gt=0, le=60)
    deadline_grace_s: float = Field(5.0, ge=0, le=600)
    retry_backoff_base_s: float = Field(1.0, ge=0, le=600)
    retry_backoff_max_s: float = Field(30.0, ge=0, le=3600)
    retry_jitter: float = Field(0.1, ge=0, le=1)
    restart_grace: bool = True
    result_max_bytes: int = Field(65536, ge=1024, le=4 * 1024 * 1024)
    instance_id: str = Field(default_factory=lambda: f"broker-{socket.gethostname()}-{os.getpid()}")

    @model_validator(mode="after")
    def _check_timing(self) -> "BrokerSettings":
        if not self.heartbeat_interval_s < self.worker_suspect_after_s < self.worker_dead_after_s:
            raise ValueError(
                "require DTQ_HEARTBEAT_INTERVAL_S < DTQ_WORKER_SUSPECT_AFTER_S < DTQ_WORKER_DEAD_AFTER_S"
            )
        if self.retry_backoff_base_s > self.retry_backoff_max_s:
            raise ValueError("DTQ_RETRY_BACKOFF_BASE_S must be <= DTQ_RETRY_BACKOFF_MAX_S")
        return self

    @property
    def renew_interval_s(self) -> float:
        """Workers renew three times per lease period (tolerates two lost renewals)."""
        return max(0.2, self.lease_duration_s / 3.0)


class WorkerSettings(CommonSettings):
    """Independent worker process. Timing parameters come from the broker at registration."""

    broker_url: str = "http://localhost:8001"
    cluster_token: SecretStr = SecretStr(DEV_CLUSTER_TOKEN)
    worker_name: str | None = None
    worker_capacity: int = Field(2, ge=1, le=32)
    agent_id: str | None = None
    graceful_timeout_s: float = Field(30.0, ge=0, le=3600)
    request_timeout_s: float = Field(10.0, gt=0, le=120)
    reconnect_max_backoff_s: float = Field(10.0, gt=0, le=300)
    cpu_memory_limit_mb: int = Field(1024, ge=64, le=65536)

    @model_validator(mode="after")
    def _default_name(self) -> "WorkerSettings":
        if not self.worker_name:
            self.worker_name = f"worker-{socket.gethostname()[:12]}-{os.getpid()}"
        self.broker_url = self.broker_url.rstrip("/")
        return self


class ApiSettings(DatabaseSettings):
    """Public API: validation, idempotent submission, queries, control, WebSocket."""

    host: str = "0.0.0.0"
    port: int = Field(8000, ge=1, le=65535)
    broker_url: str = "http://localhost:8001"
    cluster_token: SecretStr = SecretStr(DEV_CLUSTER_TOKEN)
    agent_urls: str = "http://localhost:8003"
    agent_token: SecretStr = SecretStr(DEV_AGENT_TOKEN)
    admin_token: SecretStr = SecretStr(DEV_ADMIN_TOKEN)
    demo_mode: bool = True
    cors_origins: str = "*"
    max_batch: int = Field(1000, ge=1, le=10000)
    ws_interval_s: float = Field(2.0, ge=0.2, le=60)
    request_timeout_s: float = Field(10.0, gt=0, le=120)

    @property
    def agent_url_list(self) -> list[str]:
        return _split_csv(self.agent_urls)

    @property
    def cors_origin_list(self) -> list[str]:
        return _split_csv(self.cors_origins) or ["*"]


class AgentSettings(CommonSettings):
    """Worker-management agent: the only component allowed to create/kill worker processes."""

    host: str = "0.0.0.0"
    port: int = Field(8003, ge=1, le=65535)
    agent_id: str = "local"
    agent_token: SecretStr = SecretStr(DEV_AGENT_TOKEN)
    agent_backend: Literal["docker", "process"] = "process"
    agent_max_workers: int = Field(12, ge=1, le=64)
    demo_mode: bool = True

    # How spawned workers reach the broker and authenticate
    worker_broker_url: str = "http://localhost:8001"
    cluster_token: SecretStr = SecretStr(DEV_CLUSTER_TOKEN)
    worker_default_capacity: int = Field(2, ge=1, le=32)
    worker_log_level: str = "INFO"
    worker_graceful_timeout_s: float = Field(30.0, ge=0, le=3600)

    # Docker backend
    worker_image: str = "dtq-backend:latest"
    docker_network: str = "dtq-net"
    worker_cpus: float = Field(0.0, ge=0, le=64)
    worker_memory_mb: int = Field(768, ge=0, le=65536)
    compose_project: str = "dtq"
    restartable_services: str = "broker,api"

    # Process backend
    python_executable: str | None = None

    @property
    def restartable_service_list(self) -> list[str]:
        return _split_csv(self.restartable_services)


class AutoscalerSettings(DatabaseSettings):
    """Autoscaling controller (measure → decide → act → wait)."""

    broker_url: str = "http://localhost:8001"
    cluster_token: SecretStr = SecretStr(DEV_CLUSTER_TOKEN)
    agent_urls: str = "http://localhost:8003"
    agent_token: SecretStr = SecretStr(DEV_AGENT_TOKEN)
    initial_workers: int = Field(3, ge=0, le=16)
    metrics_sample_interval_s: float = Field(5.0, ge=0.5, le=600)
    max_starts_per_cycle: int = Field(2, ge=1, le=16)
    request_timeout_s: float = Field(10.0, gt=0, le=120)
    instance_id: str = Field(default_factory=lambda: f"autoscaler-{socket.gethostname()}-{os.getpid()}")

    @property
    def agent_url_list(self) -> list[str]:
        return _split_csv(self.agent_urls)
