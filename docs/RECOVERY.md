# Workers, failure detection and recovery

Owner: Malak Mohamed (workers + recovery logic). This document maps every failure mode to the code that
handles it and the test that proves it.

## 1. Worker runtime (`backend/dtq/worker/runtime.py`)

A worker is an independent OS process (`python -m dtq worker`; one Docker container each in the compose
deployment). Its only interface to the rest of the system is the broker protocol (`dtq/worker/client.py`).

| Activity | Behaviour |
|---|---|
| Registration | `REGISTER` with name, host, pid and capacity; retried with capped exponential backoff until the broker answers. Timing parameters (heartbeat, lease, renew interval) come **from the broker's response**, so they are configured in one place. |
| Heartbeat loop | Every `heartbeat_interval_s` (2 s) it sends state (`active`/`draining`), free slots and the attempts it holds. The response may list **revoked** attempts (cancel them), report `DRAINING` (shut down gracefully), or be **410 Gone** (this incarnation was declared dead → re-register as a new incarnation). |
| Claim loop | Long-polls `CLAIM` with `max_tasks = free slots`. Ignores redelivered assignments it already holds (deduplication by task id). |
| Execution | `START` (acknowledge), then the handler runs: CPU tasks in a forkserver child process (killable, `RLIMIT_AS`-limited), waiting tasks as coroutines. A renew loop runs every `lease/3` (5 s) and carries progress. |
| Reporting | `COMPLETE` or `FAIL`, retried on transport errors until the broker answers. 409 means the result is stale (discarded and logged). |
| Shutdown (SIGTERM) | Drain: notify the broker, stop claiming, let running tasks finish within `graceful_timeout_s`, release leftovers with `FAIL(WORKER_SHUTDOWN, retryable)` (immediate retry, no backoff), `DEREGISTER`, exit 0. |

## 2. Failure detection (`backend/dtq/broker/recovery.py`)

The recovery loop runs inside the broker on the instance holding the `dtq.recovery` advisory lock, every second:

1. **Suspect:** no heartbeat for 6 s → `SUSPECT` (no new work). A later heartbeat clears it (`worker.recovered`):
   a missing heartbeat only *suggests* failure (proposal slide 11).
2. **Dead:** no heartbeat for 10 s → `DEAD`. Its `ASSIGNED`/`RUNNING` tasks are recovered at once (attempt `LOST`, `WORKER_DEAD`).
3. **Lease expiry:** an attempt not renewed within 15 s (or not acknowledged within 10 s) → `LOST` (`LEASE_EXPIRED` / `ASSIGN_ACK_TIMEOUT`).
4. **Deadline:** `RUNNING` longer than `timeout_seconds + 5 s` → `TIMED_OUT` (backup for the worker's own timeout).
5. **Restart grace:** when a broker instance acquires leadership it extends all active leases by one lease
   period, because workers could not renew while no broker was reachable (the idea follows Chubby's grace period).

## 3. Recovery and retries

`lifecycle.retry_or_fail` moves the task to `RETRY_WAIT` with `available_at = now() + backoff`, where
`backoff(n) = min(30 s, 1 s · 2^(n−1)) · (1 ± 10 % jitter)` (`dtq/broker/retry_policy.py`). If no attempts remain,
or the error is non-retryable, `RETRY_WAIT → FAILED` happens in the same transaction. The dispatcher re-assigns
eligible `RETRY_WAIT` tasks in FIFO order (`RETRY_WAIT → ASSIGNED`).

## 4. Stale-attempt protection (fencing)

Every task message carries `(attempt_id, fencing_token = attempt number)`. All state changes are compare-and-set
updates that require the pair to be current (`dtq/db/lifecycle.py`). After a reassignment the old pair matches
nothing, so RENEW, START, COMPLETE and FAIL from the old attempt get **409** (`stale_attempt`, `lease_expired`
or `deadline_exceeded`) and an event `task.stale_*_rejected` is written. A worker whose lease is lost cancels
its local execution (it kills the child process).

## 5. Failure matrix

| Failure | Detected by | Result | Test |
|---|---|---|---|
| Crash before execution (after claim) | ack timeout / dead worker | reassigned | `test_recovery.py::test_unacknowledged_assignment_times_out` |
| Crash during execution | heartbeats → DEAD | `LOST`, re-executed elsewhere | `system/test_fault_tolerance.py::test_sigkill_worker_mid_task_is_recovered_by_another_worker`, Scenario B |
| Crash after execution, before COMPLETE | same as above | re-executed (at-least-once), one committed result | covered by the same mechanism |
| Unresponsive (paused) worker | SUSPECT → DEAD | reassigned; revived worker's messages rejected; it re-registers | `test_frozen_worker_cannot_overwrite_the_committed_result`, Scenario G |
| Short connectivity loss | — (below the suspect threshold) | continues; no duplicate | `test_network_partitions_short_and_long` (part 1) |
| Long connectivity loss | DEAD / lease | reassigned; partitioned worker re-registers after healing | `test_network_partitions_short_and_long` (part 2) |
| Handler exception | worker `FAIL` | retry with backoff, or FAILED | `test_broker.py::test_retry_with_backoff_then_terminal_failure` |
| Runtime exceeded | worker kills child; broker deadline | `TIMED_OUT`, retry | `test_execution_timeout_terminates_handler_and_retries`, `test_recovery.py::test_execution_deadline_is_enforced_by_the_broker` |
| Retry exhaustion | attempt counter | `RETRY_WAIT → FAILED` | Scenario C, `test_lost_attempts_exhaust_the_retry_budget` |
| Graceful stop | SIGTERM | running task finishes; worker `STOPPED` | `test_sigterm_drains_gracefully_without_losing_the_running_task` |
| Broker restart | — | nothing lost; leases extended | `system/test_broker_restart.py`, `test_recovery.py::test_restart_grace_extends_in_flight_leases`, Scenario F |

## 6. Measured behaviour (Experiment B)

See `benchmarks/results/RESULTS.md` §B. With default settings the detection delay after a SIGKILL is bounded by
the 10 s dead timeout (measured 9.3–9.9 s). The task restarts on another worker once a slot is free. All
affected tasks completed, and none failed.
