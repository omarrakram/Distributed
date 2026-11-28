"""Broker service: the worker-facing protocol of the custom task broker.

Messages (all JSON over HTTP, authenticated with X-Cluster-Token):
REGISTER, HEARTBEAT, CLAIM (long-poll), START (assignment ack), RENEW (lease),
COMPLETE, FAIL, DRAIN, DEREGISTER.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
from fastapi import Depends, FastAPI, Header

from dtq import __version__
from dtq.broker.leadership import LeaderElection
from dtq.broker.recovery import RecoveryLoop
from dtq.broker.retry_policy import backoff_delay
from dtq.broker.scheduler import Dispatcher
from dtq.common.config import BrokerSettings
from dtq.common.http import ServiceError, install_error_handlers, tokens_equal
from dtq.common.pg_listener import listen_forever
from dtq.common.protocol import (
    Assignment, ClaimRequest, ClaimResponse, CompleteRequest, CompleteResponse, DeregisterRequest,
    DrainRequest, FailRequest, FailResponse, HeartbeatRequest, HeartbeatResponse, LeaseResponse,
    RegisterRequest, RegisterResponse, RenewRequest, StartRequest,
)
from dtq.db import events_repo, lifecycle, workers_repo
from dtq.db.migrate import wait_for_schema
from dtq.db.pool import create_pool, dumps

log = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class BrokerState:
    def __init__(self, settings: BrokerSettings) -> None:
        self.settings = settings
        self.pool: asyncpg.Pool | None = None
        self.dispatcher: Dispatcher | None = None
        self.recovery: RecoveryLoop | None = None
        self.leader: LeaderElection | None = None
        self.tasks: list[asyncio.Task] = []
        self.rejections = {"stale": 0, "duplicate_acks": 0}


def create_app(settings: BrokerSettings | None = None) -> FastAPI:
    settings = settings or BrokerSettings()
    st = BrokerState(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        st.pool = await create_pool(settings.database_url, settings.db_pool_min, settings.db_pool_max)
        await wait_for_schema(st.pool)
        st.dispatcher = Dispatcher(st.pool, settings)
        st.leader = LeaderElection(settings.database_url, "dtq.recovery")
        st.recovery = RecoveryLoop(st.pool, settings, st.dispatcher.wake, st.leader)
        st.leader.on_acquired = st.recovery.on_leadership_acquired
        st.tasks = [
            asyncio.create_task(st.dispatcher.run(), name="dispatcher"),
            asyncio.create_task(listen_forever(settings.database_url, "dtq_tasks", st.dispatcher.wake),
                                name="listener"),
            asyncio.create_task(st.leader.run(), name="leader"),
            asyncio.create_task(st.recovery.run(), name="recovery"),
        ]
        await events_repo.emit(st.pool, "INFO", "broker", "broker.started",
                               f"Broker {settings.instance_id} started (v{__version__})",
                               details={"instance": settings.instance_id})
        log.info("broker ready", extra={"instance": settings.instance_id})
        try:
            yield
        finally:
            st.dispatcher.stop()
            for task in st.tasks:
                task.cancel()
            for task in st.tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            with contextlib.suppress(Exception):
                await events_repo.emit(st.pool, "INFO", "broker", "broker.stopped",
                                       f"Broker {settings.instance_id} stopped")
            await st.pool.close()

    app = FastAPI(title="DTQ Broker", version=__version__, lifespan=lifespan)
    app.state.broker = st
    install_error_handlers(app)

    async def require_cluster_token(x_cluster_token: str | None = Header(default=None)) -> None:
        if not tokens_equal(x_cluster_token, settings.cluster_token):
            raise ServiceError(401, "unauthorized", "missing or invalid X-Cluster-Token")

    auth = [Depends(require_cluster_token)]

    def backoff(attempts: int) -> float:
        return backoff_delay(attempts, settings.retry_backoff_base_s, settings.retry_backoff_max_s,
                             settings.retry_jitter)

    async def worker_name(conn, worker_id: UUID) -> str:
        return await conn.fetchval("SELECT name FROM workers WHERE id = $1", worker_id) or str(worker_id)

    async def reject(conn, req, task_id: UUID, reason: str, what: str) -> None:
        """Record and refuse a message whose fencing pair is not current.

        The event is written through the pool (autocommit), because raising the 409
        rolls back the caller's transaction (which changed nothing anyway).
        """
        st.rejections["stale"] += 1
        name = await worker_name(conn, req.worker_id)
        await events_repo.emit(
            st.pool, "WARNING", "broker", f"task.stale_{what}_rejected",
            f"Rejected {what.upper()} from {name} for attempt token {req.fencing_token}: {reason}",
            task_id=task_id, worker_id=req.worker_id,
            details={"reason": reason, "fencing_token": req.fencing_token, "attempt_id": str(req.attempt_id)},
        )
        raise ServiceError(409, reason, f"{what} rejected: {reason}",
                           {"task_id": str(task_id), "fencing_token": req.fencing_token})

    # ------------------------------------------------------------------ health
    @app.get("/health")
    async def health():
        try:
            await asyncio.wait_for(st.pool.fetchval("SELECT 1"), timeout=3)
            db = "ok"
        except Exception as exc:  # pragma: no cover - exercised manually
            raise ServiceError(503, "database_unavailable", str(exc))
        return {"status": "ok", "database": db, "instance": settings.instance_id,
                "recovery_leader": bool(st.leader and st.leader.is_leader), "version": __version__}

    @app.get("/v1/stats", dependencies=auth)
    async def stats():
        return {
            "instance": settings.instance_id,
            "recovery_leader": bool(st.leader and st.leader.is_leader),
            "waiting_workers": st.dispatcher.waiting_workers,
            "dispatcher": st.dispatcher.stats,
            "recovery": st.recovery.stats,
            "rejections": st.rejections,
            "config": {
                "heartbeat_interval_s": settings.heartbeat_interval_s,
                "worker_suspect_after_s": settings.worker_suspect_after_s,
                "worker_dead_after_s": settings.worker_dead_after_s,
                "lease_duration_s": settings.lease_duration_s,
                "assign_ack_timeout_s": settings.assign_ack_timeout_s,
                "retry_backoff_base_s": settings.retry_backoff_base_s,
                "retry_backoff_max_s": settings.retry_backoff_max_s,
            },
        }

    # ---------------------------------------------------------- registration
    @app.post("/v1/workers/register", response_model=RegisterResponse, dependencies=auth)
    async def register(req: RegisterRequest):
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                row = await workers_repo.register(
                    conn, name=req.name, host=req.host, pid=req.pid, capacity=req.capacity,
                    agent_id=req.agent_id, version=req.version, labels=req.labels,
                )
                await events_repo.emit(
                    conn, "INFO", "broker", "worker.registered",
                    f"Worker {req.name} registered on {req.host} (capacity {req.capacity})",
                    worker_id=row["id"],
                    details={"host": req.host, "pid": req.pid, "capacity": req.capacity,
                             "agent_id": req.agent_id},
                )
        st.dispatcher.wake()
        return RegisterResponse(
            worker_id=row["id"], heartbeat_interval_s=settings.heartbeat_interval_s,
            lease_duration_s=settings.lease_duration_s, renew_interval_s=settings.renew_interval_s,
            claim_wait_s=settings.claim_max_wait_s, server_time=_now(),
        )

    @app.post("/v1/workers/{worker_id}/heartbeat", response_model=HeartbeatResponse, dependencies=auth)
    async def heartbeat(worker_id: UUID, req: HeartbeatRequest):
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                previous, current = await workers_repo.heartbeat(
                    conn, worker_id, req.state == "draining", len(req.active)
                )
                if current is None:
                    raise ServiceError(410, "worker_not_live",
                                       "worker incarnation is dead, stopped or unknown; re-register")
                if previous == "SUSPECT" and current == "ACTIVE":
                    await events_repo.emit(
                        conn, "INFO", "recovery", "worker.recovered",
                        f"Worker {await worker_name(conn, worker_id)} heartbeat resumed; suspicion cleared",
                        worker_id=worker_id,
                    )
                revoked: list[UUID] = []
                if req.active:
                    rows = await conn.fetch(
                        """SELECT id, current_attempt_id, status, assigned_worker_id
                             FROM tasks WHERE id = ANY($1::uuid[])""",
                        [a.task_id for a in req.active],
                    )
                    current_by_task = {r["id"]: r for r in rows}
                    for a in req.active:
                        r = current_by_task.get(a.task_id)
                        if (r is None or r["current_attempt_id"] != a.attempt_id
                                or r["status"] not in lifecycle.ACTIVE_STATES
                                or r["assigned_worker_id"] != worker_id):
                            revoked.append(a.task_id)
        if previous == "SUSPECT":
            st.dispatcher.wake()
        return HeartbeatResponse(status=current, revoked=revoked, server_time=_now())

    # ------------------------------------------------------------------ claim
    @app.post("/v1/workers/{worker_id}/claim", response_model=ClaimResponse, dependencies=auth)
    async def claim(worker_id: UUID, req: ClaimRequest):
        status = await workers_repo.get_status(st.pool, worker_id)
        if status is None or status in ("DEAD", "STOPPED"):
            raise ServiceError(410, "worker_not_live", "worker incarnation is dead, stopped or unknown")
        if status == "DRAINING":
            raise ServiceError(409, "worker_draining", "draining workers receive no new tasks")
        redelivered = await st.dispatcher.redeliver(worker_id)
        if redelivered:
            return ClaimResponse(tasks=[Assignment(**a) for a in redelivered[: req.max_tasks]])
        tasks = await st.dispatcher.claim(worker_id, req.max_tasks, min(req.wait_s, settings.claim_max_wait_s))
        return ClaimResponse(tasks=[Assignment(**a) for a in tasks])

    # --------------------------------------------------------- task messages
    @app.post("/v1/tasks/{task_id}/start", response_model=LeaseResponse, dependencies=auth)
    async def start(task_id: UUID, req: StartRequest):
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                out = await lifecycle.start(conn, task_id, req.worker_id, req.attempt_id,
                                            req.fencing_token, settings.lease_duration_s)
                if not out.ok:
                    await reject(conn, req, task_id, out.reason, "start")
                if out.status == "ok":
                    await events_repo.emit(
                        conn, "INFO", "broker", "task.started",
                        f"Attempt {req.fencing_token} started on {await worker_name(conn, req.worker_id)}",
                        task_id=task_id, worker_id=req.worker_id, namespace=(out.task or {}).get("namespace"),
                        details={"attempt": req.fencing_token},
                    )
        return LeaseResponse(lease_expires_at=out.lease_expires_at)

    @app.post("/v1/tasks/{task_id}/renew", response_model=LeaseResponse, dependencies=auth)
    async def renew(task_id: UUID, req: RenewRequest):
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                out = await lifecycle.renew(conn, task_id, req.worker_id, req.attempt_id, req.fencing_token,
                                            settings.lease_duration_s, settings.deadline_grace_s,
                                            req.progress, req.message)
                if not out.ok:
                    await reject(conn, req, task_id, out.reason, "renew")
        return LeaseResponse(lease_expires_at=out.lease_expires_at)

    @app.post("/v1/tasks/{task_id}/complete", response_model=CompleteResponse, dependencies=auth)
    async def complete(task_id: UUID, req: CompleteRequest):
        size = len(dumps(req.result))
        if size > settings.result_max_bytes:
            raise ServiceError(413, "result_too_large",
                               f"result is {size} bytes; limit is {settings.result_max_bytes}")
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                out = await lifecycle.complete(conn, task_id, req.worker_id, req.attempt_id,
                                               req.fencing_token, req.result, req.duration_ms)
                if not out.ok:
                    await reject(conn, req, task_id, out.reason, "result")
                name = await worker_name(conn, req.worker_id)
                if out.status == "committed":
                    await events_repo.emit(
                        conn, "INFO", "broker", "task.completed",
                        f"Task {out.task['type']} completed by {name} on attempt {req.fencing_token} "
                        f"({req.duration_ms} ms)",
                        task_id=task_id, worker_id=req.worker_id, namespace=out.task["namespace"],
                        details={"attempt": req.fencing_token, "duration_ms": req.duration_ms},
                    )
                else:
                    st.rejections["duplicate_acks"] += 1
                    await events_repo.emit(
                        conn, "INFO", "broker", "task.duplicate_ack",
                        f"Duplicate COMPLETE from {name} for attempt {req.fencing_token} acknowledged "
                        f"without re-applying the result",
                        task_id=task_id, worker_id=req.worker_id, details={"attempt": req.fencing_token},
                    )
        st.dispatcher.wake()
        return CompleteResponse(status=out.status)

    @app.post("/v1/tasks/{task_id}/fail", response_model=FailResponse, dependencies=auth)
    async def fail(task_id: UUID, req: FailRequest):
        delay = 0.0 if req.error_type == "WORKER_SHUTDOWN" else backoff(req.fencing_token)
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                out = await lifecycle.fail(conn, task_id, req.worker_id, req.attempt_id, req.fencing_token,
                                           req.error_type, req.message, req.retryable, req.duration_ms, delay)
                if not out.ok:
                    await reject(conn, req, task_id, out.reason, "failure")
                if out.status != "duplicate":
                    name = await worker_name(conn, req.worker_id)
                    ns = (out.task or {}).get("namespace")
                    await events_repo.emit(
                        conn, "WARNING", "worker", "task.attempt_failed",
                        f"Attempt {req.fencing_token} on {name} failed: {req.error_type}: {req.message[:200]}",
                        task_id=task_id, worker_id=req.worker_id, namespace=ns,
                        details={"error_type": req.error_type, "retryable": req.retryable,
                                 "attempt": req.fencing_token},
                    )
                    if out.status == "failed":
                        await events_repo.emit(
                            conn, "ERROR", "broker", "task.failed",
                            f"Task FAILED permanently after attempt {req.fencing_token} "
                            f"({'retry limit exhausted' if req.retryable else 'non-retryable error'})",
                            task_id=task_id, worker_id=req.worker_id, namespace=ns,
                            details={"attempt": req.fencing_token, "error_type": req.error_type},
                        )
                    else:
                        await events_repo.emit(
                            conn, "INFO", "broker", "task.retry_scheduled",
                            f"Retry scheduled after {delay:.2f}s backoff (attempt {req.fencing_token} failed)",
                            task_id=task_id, worker_id=req.worker_id, namespace=ns,
                            details={"backoff_s": round(delay, 3),
                                     "next_attempt_at": out.next_attempt_at.isoformat()},
                        )
        st.dispatcher.wake()
        return FailResponse(status=out.status, next_attempt_at=out.next_attempt_at)

    # ------------------------------------------------------- worker lifecycle
    @app.post("/v1/workers/{worker_id}/drain", dependencies=auth)
    async def drain(worker_id: UUID, req: DrainRequest):
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                status = await workers_repo.set_draining(conn, worker_id)
                if status is None:
                    raise ServiceError(410, "worker_not_live", "worker is not live")
                await events_repo.emit(
                    conn, "INFO", "broker", "worker.draining",
                    f"Worker {await worker_name(conn, worker_id)} is draining ({req.reason}); no new work",
                    worker_id=worker_id, details={"reason": req.reason},
                )
        st.dispatcher.cancel_claim(worker_id)
        return {"status": status}

    @app.post("/v1/workers/{worker_id}/deregister", dependencies=auth)
    async def deregister(worker_id: UUID, req: DeregisterRequest):
        released = 0
        async with st.pool.acquire() as conn:
            async with conn.transaction():
                status = await workers_repo.set_stopped(conn, worker_id, req.reason)
                if status is None:
                    raise ServiceError(410, "worker_not_live", "worker is not live")
                leftovers = await conn.fetch(
                    """SELECT t.id, t.status, t.type, t.namespace, t.attempt_count, t.max_attempts,
                              t.current_attempt_id, t.assigned_worker_id, w.name AS worker_name,
                              w.status AS worker_status
                         FROM tasks t JOIN workers w ON w.id = t.assigned_worker_id
                        WHERE t.assigned_worker_id = $1 AND t.status IN ('ASSIGNED', 'RUNNING')
                          FOR UPDATE OF t""",
                    worker_id,
                )
                for t in leftovers:
                    await lifecycle.recover(conn, t, "WORKER_STOPPED",
                                            "worker deregistered before finishing", 0.0,
                                            attempt_status="RELEASED")
                    released += 1
                await events_repo.emit(
                    conn, "INFO", "broker", "worker.stopped",
                    f"Worker {await worker_name(conn, worker_id)} deregistered ({req.reason}); "
                    f"released {released} task(s)",
                    worker_id=worker_id, details={"reason": req.reason, "released": released},
                )
        st.dispatcher.cancel_claim(worker_id)
        if released:
            st.dispatcher.wake()
        return {"status": "STOPPED", "released": released}

    return app
