"""Worker runtime: an independent process that executes tasks for the broker.

Concurrent activities (asyncio):
* heartbeat loop – liveness + list of held attempts; the broker answers with revoked
  attempts (cancel them) or 410 Gone (this incarnation was declared dead → re-register).
* claim loop     – long-polls CLAIM whenever a slot is free (capacity-aware pull).
* per task       – START (ack) → execute (child process / coroutine) with a RENEW loop
                   → COMPLETE or FAIL, retried with backoff until the broker answers.
* SIGTERM        – graceful drain: stop claiming, let running tasks finish within
                   graceful_timeout, release leftovers (FAIL WORKER_SHUTDOWN), DEREGISTER.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from uuid import UUID

from dtq import __version__
from dtq.common.config import WorkerSettings
from dtq.common.http import retry_async
from dtq.common.protocol import Assignment, RegisterResponse
from dtq.worker import executor
from dtq.worker.client import RETRYABLE, BrokerClient, GoneError, ResultTooLarge, StaleError
from dtq.worker.errors import ExecContext, TaskError

log = logging.getLogger("dtq.worker")


@dataclass
class RunningTask:
    a: Assignment
    worker_id: UUID                     # incarnation that owns this attempt
    started_at: float = field(default_factory=time.monotonic)
    handler: asyncio.Task | None = None
    outer: asyncio.Task | None = None
    progress: float | None = None
    message: str | None = None
    cancel_reason: str | None = None

    def on_progress(self, fraction: float, message: str | None = None) -> None:
        self.progress = max(0.0, min(1.0, float(fraction)))
        self.message = message


class Worker:
    def __init__(self, settings: WorkerSettings, client: BrokerClient | None = None) -> None:
        self.s = settings
        self.client = client or BrokerClient(settings.broker_url, settings.cluster_token.get_secret_value(),
                                             settings.request_timeout_s)
        self.capacity = settings.worker_capacity
        self.running: dict[UUID, RunningTask] = {}
        self.worker_id: UUID | None = None
        self.reg: RegisterResponse | None = None
        self.draining = False
        self.counters = {"completed": 0, "failed": 0, "stale": 0, "registrations": 0}
        self._reg_lock = asyncio.Lock()
        self._slot_event = asyncio.Event()
        self._stop = asyncio.Event()
        self._abandon_reports = False
        self._hb_task: asyncio.Task | None = None
        self._claim_task: asyncio.Task | None = None

    # ------------------------------------------------------------------ helpers
    @property
    def free_slots(self) -> int:
        return max(0, self.capacity - len(self.running))

    def _log(self, level: int, msg: str, *args, **extra) -> None:
        log.log(level, msg, *args, extra={"worker": self.s.worker_name, **extra})

    def request_stop(self) -> None:
        self._log(logging.INFO, "stop requested")
        self._stop.set()

    def _cancel(self, task_id: UUID, reason: str) -> None:
        rt = self.running.get(task_id)
        if rt and rt.handler and not rt.handler.done():
            rt.cancel_reason = reason
            rt.handler.cancel()
            self._log(logging.WARNING, "cancelling task %s: %s", task_id, reason, task_id=str(task_id))

    # ------------------------------------------------------------- lifecycle
    async def run(self) -> int:
        await self.ensure_registered(force=True)
        self._hb_task = asyncio.create_task(self.heartbeat_loop(), name="heartbeat")
        self._claim_task = asyncio.create_task(self.claim_loop(), name="claim")
        await self._stop.wait()
        await self.shutdown()
        return 0

    async def ensure_registered(self, force: bool = False, stale_id: UUID | None = None) -> None:
        async with self._reg_lock:
            if not force and self.worker_id is not None and self.worker_id != stale_id:
                return  # another loop already re-registered
            reg = await retry_async(
                lambda: self.client.register(
                    name=self.s.worker_name, host=socket.gethostname(), pid=os.getpid(),
                    capacity=self.capacity, agent_id=self.s.agent_id, version=__version__,
                    labels={"runtime": "python"},
                ),
                attempts=None, base_s=0.5, max_s=self.s.reconnect_max_backoff_s, retry_on=RETRYABLE,
                describe="register", stop=self._stop.is_set,
            )
            self.reg, self.worker_id = reg, reg.worker_id
            self.counters["registrations"] += 1
            self._log(logging.INFO, "registered with broker as %s (heartbeat %.1fs, lease %.1fs)",
                      reg.worker_id, reg.heartbeat_interval_s, reg.lease_duration_s,
                      worker_id=str(reg.worker_id))

    async def heartbeat_loop(self) -> None:
        while True:
            wid = self.worker_id
            try:
                active = [
                    {"task_id": str(rt.a.task_id), "attempt_id": str(rt.a.attempt_id),
                     "fencing_token": rt.a.fencing_token}
                    for rt in self.running.values() if rt.worker_id == wid
                ]
                resp = await self.client.heartbeat(wid, "draining" if self.draining else "active",
                                                   active, self.free_slots)
                for task_id in resp.revoked:
                    self._cancel(task_id, "lease revoked by broker")
                if resp.status == "DRAINING" and not self.draining and not self._stop.is_set():
                    # Remote drain through the protocol (works for unmanaged workers on any host).
                    self._log(logging.INFO, "broker marked this worker DRAINING: shutting down gracefully")
                    self.request_stop()
            except GoneError:
                if not self.draining:
                    self._log(logging.WARNING,
                              "broker declared incarnation %s dead (false suspicion or partition); "
                              "re-registering — results of old attempts will be rejected as stale", wid)
                    await self.ensure_registered(stale_id=wid)
            except RETRYABLE as exc:
                self._log(logging.WARNING, "heartbeat failed: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("heartbeat error")
            await asyncio.sleep(self.reg.heartbeat_interval_s if self.reg else 2.0)

    async def claim_loop(self) -> None:
        backoff = 0.5
        while not self.draining:
            while self.free_slots == 0 and not self.draining:
                self._slot_event.clear()
                await self._slot_event.wait()
            if self.draining:
                break
            wid = self.worker_id
            try:
                resp = await self.client.claim(wid, self.free_slots, self.reg.claim_wait_s)
                backoff = 0.5
            except GoneError:
                await self.ensure_registered(stale_id=wid)
                continue
            except StaleError:  # broker marked us DRAINING (scale-down in progress)
                if not self._stop.is_set():
                    self._log(logging.INFO, "broker refused claim: worker is DRAINING; shutting down gracefully")
                    self.request_stop()
                await asyncio.sleep(1.0)
                continue
            except RETRYABLE as exc:
                self._log(logging.WARNING, "claim failed (%s); retrying in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.s.reconnect_max_backoff_s)
                continue
            for a in resp.tasks:
                if a.task_id in self.running:
                    continue  # redelivery of an assignment we already hold
                if self.free_slots == 0:
                    break
                rt = RunningTask(a=a, worker_id=wid)
                self.running[a.task_id] = rt
                rt.outer = asyncio.create_task(self._run_task(rt), name=f"task-{a.task_id}")

    # ------------------------------------------------------------- execution
    async def _run_task(self, rt: RunningTask) -> None:
        a, wid = rt.a, rt.worker_id
        extra = {"task_id": str(a.task_id), "attempt": a.fencing_token, "type": a.type}
        try:
            try:
                await retry_async(lambda: self.client.start(a.task_id, wid, a.attempt_id, a.fencing_token),
                                  attempts=5, retry_on=RETRYABLE, describe="start")
            except (StaleError, GoneError) as exc:
                self._log(logging.WARNING, "assignment no longer valid at START (%s)", exc.code, **extra)
                return
            except RETRYABLE:
                self._log(logging.ERROR, "could not acknowledge assignment; the broker will reassign it",
                          **extra)
                return
            self._log(logging.INFO, "started %s attempt %d", a.type, a.fencing_token, **extra)
            renew = asyncio.create_task(self._renew_loop(rt))
            t0 = time.monotonic()
            ctx = ExecContext(task_id=str(a.task_id), attempt_no=a.fencing_token, worker_name=self.s.worker_name)
            rt.handler = asyncio.create_task(
                executor.execute(a.type, a.payload, ctx, a.timeout_seconds, rt.on_progress,
                                 self.s.cpu_memory_limit_mb)
            )
            try:
                outcome: tuple = ("complete", await rt.handler)
            except asyncio.CancelledError:
                if rt.cancel_reason is None:
                    raise
                if rt.cancel_reason != "shutdown":
                    return  # lease lost / revoked: the task is no longer ours; report nothing
                outcome = ("fail", "WORKER_SHUTDOWN", "worker shut down before finishing", True)
            except TaskError as exc:
                outcome = ("fail", exc.error_type, str(exc), exc.retryable)
            except Exception as exc:  # noqa: BLE001
                outcome = ("fail", "INTERNAL", f"{type(exc).__name__}: {exc}", True)
            finally:
                renew.cancel()
            await self._report(rt, outcome, int((time.monotonic() - t0) * 1000), extra)
        finally:
            self.running.pop(a.task_id, None)
            self._slot_event.set()

    async def _renew_loop(self, rt: RunningTask) -> None:
        a = rt.a
        while True:
            await asyncio.sleep(self.reg.renew_interval_s)
            try:
                await self.client.renew(a.task_id, rt.worker_id, a.attempt_id, a.fencing_token,
                                        rt.progress, rt.message)
            except (StaleError, GoneError) as exc:
                self._cancel(a.task_id, f"lease lost ({exc.code})")
                return
            except RETRYABLE as exc:
                self._log(logging.WARNING, "lease renewal failed (%s); will retry", exc, task_id=str(a.task_id))

    async def _report(self, rt: RunningTask, outcome: tuple, duration_ms: int, extra: dict) -> None:
        a, wid = rt.a, rt.worker_id
        common = dict(attempts=None, base_s=0.25, max_s=self.s.reconnect_max_backoff_s, retry_on=RETRYABLE,
                      stop=lambda: self._abandon_reports)
        try:
            if outcome[0] == "complete":
                try:
                    resp = await retry_async(
                        lambda: self.client.complete(a.task_id, wid, a.attempt_id, a.fencing_token,
                                                     outcome[1], duration_ms),
                        describe="complete", **common,
                    )
                    self.counters["completed"] += 1
                    self._log(logging.INFO, "completed in %d ms (%s)", duration_ms, resp.status, **extra)
                    return
                except ResultTooLarge as exc:
                    outcome = ("fail", "RESULT_TOO_LARGE", str(exc), False)
            _, error_type, message, retryable = outcome
            resp = await retry_async(
                lambda: self.client.fail(a.task_id, wid, a.attempt_id, a.fencing_token, error_type,
                                         message, retryable, duration_ms),
                describe="fail", **common,
            )
            self.counters["failed"] += 1
            self._log(logging.WARNING, "reported failure %s -> %s", error_type, resp.status, **extra)
        except (StaleError, GoneError) as exc:
            self.counters["stale"] += 1
            self._log(logging.WARNING, "broker rejected report as stale (%s): the task was reassigned, "
                      "this result is discarded", exc.code, **extra)
        except RETRYABLE:
            self._log(logging.ERROR, "gave up reporting during shutdown; lease expiry will recover the task",
                      **extra)

    # -------------------------------------------------------------- shutdown
    async def shutdown(self) -> None:
        self.draining = True
        self._slot_event.set()
        if self._claim_task:
            self._claim_task.cancel()
        wid = self.worker_id
        self._log(logging.INFO, "draining: %d running task(s), graceful timeout %.0fs",
                  len(self.running), self.s.graceful_timeout_s)
        try:
            await asyncio.wait_for(self.client.drain(wid, "worker received stop signal"), 5)
        except Exception as exc:  # noqa: BLE001
            self._log(logging.WARNING, "drain notification failed: %s", exc)
        deadline = time.monotonic() + self.s.graceful_timeout_s
        while self.running and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        if self.running:
            self._log(logging.WARNING, "graceful timeout reached; releasing %d task(s)", len(self.running))
            for task_id in list(self.running):
                self._cancel(task_id, "shutdown")
            end = time.monotonic() + 10
            while self.running and time.monotonic() < end:
                await asyncio.sleep(0.2)
        self._abandon_reports = True
        try:
            await asyncio.wait_for(self.client.deregister(wid, "graceful shutdown"), 5)
        except Exception as exc:  # noqa: BLE001
            self._log(logging.WARNING, "deregister failed: %s", exc)
        if self._hb_task:
            self._hb_task.cancel()
        await self.client.close()
        self._log(logging.INFO, "stopped (completed=%d failed=%d stale=%d)", self.counters["completed"],
                  self.counters["failed"], self.counters["stale"])
