# Testing Strategy

Principle (S10): *every distributed-systems concept has an implementation artifact and a demonstrable, automated test.* Tests use real infrastructure — a real PostgreSQL server, real worker **processes**, real HTTP — never mocks of the component under test.

## 1. Test pyramid

| Layer | Location | Infrastructure | Examples |
|---|---|---|---|
| Unit | `backend/tests/unit` | none | task spec validation, handler determinism, backoff, autoscaler policy (pure function), config validation, scheduler ordering |
| Database integration | `backend/tests/integration/test_db.py` | PostgreSQL (temporary database per session) | migrations, constraints, transition trigger, repositories, idempotent insert, atomic batches |
| API integration | `backend/tests/integration/test_api.py`, `test_api_ws.py` | PostgreSQL + in-process ASGI app | submit/batch/idempotency/validation, queries, admin auth, WebSocket |
| Broker integration | `backend/tests/integration/test_broker.py`, `test_scheduler_concurrency.py`, `test_recovery.py` | PostgreSQL + in-process ASGI broker | register/heartbeat/claim/start/renew/complete/fail, fencing, duplicate ack, recovery loop, restart grace, fairness, 20 concurrent dispatchers |
| System (multi-process) | `backend/tests/system` | PostgreSQL + broker/API/worker/agent/autoscaler as **separate OS processes** | distribution over 3 workers, SIGKILL recovery, SIGSTOP stale result, TCP-proxy network partition, timeout, graceful drain, broker restart, API restart, autoscale up/down with the process agent |
| Frontend unit | `frontend/src/**/*.test.ts(x)` | jsdom | formatting helpers, status badges, reconnect backoff, API client (error envelope, admin token), live-event merging |
| End-to-end | `backend/tests/e2e` + Playwright | full Docker Compose stack | demo scenarios A–G via API, dashboard renders live data, submit task from UI |

## 2. Concept → test mapping (implemented tests)

| Concept / requirement | Test(s) (`backend/tests/…`) |
|---|---|
| Atomic claims (FR-13) | `integration/test_scheduler_concurrency.py::test_concurrent_dispatchers_never_double_assign` (20 dispatchers, 200 tasks) |
| Scheduling FIFO + fairness (FR-12) | `integration/test_broker.py::test_fifo_dispatch_order`, `unit/test_scheduler.py` |
| Lifecycle enforcement (FR-20) | `integration/test_db.py::test_illegal_transitions_are_rejected_by_the_database` (23 cases) |
| Lease + fencing (FR-16, FR-44) | `integration/test_broker.py::test_stale_attempt_cannot_overwrite_committed_result`, `system/test_fault_tolerance.py::test_frozen_worker_cannot_overwrite_the_committed_result` |
| Duplicate ack (FR-15) | `integration/test_broker.py::test_duplicate_complete_is_idempotent` |
| Idempotent submission (FR-05) | `integration/test_db.py::test_idempotent_submission_including_concurrent_duplicates`, `integration/test_api.py::test_idempotency_key_header_and_body` |
| Heartbeat failure detection (FR-40) | `integration/test_recovery.py::test_silent_worker_is_suspected_then_declared_dead_and_its_task_recovered`, `::test_false_suspicion_is_cleared_by_a_heartbeat` |
| Retry/backoff/exhaustion (FR-41) | `integration/test_broker.py::test_retry_with_backoff_then_terminal_failure`, `integration/test_recovery.py::test_lost_attempts_exhaust_the_retry_budget`, `unit/test_retry_policy.py` |
| Timeout (FR-42) | `system/test_fault_tolerance.py::test_execution_timeout_terminates_handler_and_retries`, `integration/test_recovery.py::test_execution_deadline_is_enforced_by_the_broker`, `unit/test_executor.py` |
| Worker crash (S11) | `system/test_fault_tolerance.py::test_sigkill_worker_mid_task_is_recovered_by_another_worker` |
| Connectivity loss | `system/test_fault_tolerance.py::test_network_partitions_short_and_long` (TCP proxy toggled) |
| Broker restart (FR-43) | `system/test_broker_restart.py`, `integration/test_recovery.py::test_restart_grace_extends_in_flight_leases` |
| API restart | `system/test_e2e_flow.py::test_api_restart_keeps_committed_state` |
| Graceful shutdown (FR-33) | `system/test_fault_tolerance.py::test_sigterm_drains_gracefully_without_losing_the_running_task` |
| Autoscaling (FR-50..53) | `unit/test_autoscaler_policy.py`, `system/test_autoscaling.py` |
| WebSocket (FR-60) | `integration/test_api_ws.py` |
| Dashboard | `frontend/src/**/*.test.ts(x)` (Vitest), `frontend/e2e/dashboard.spec.ts` (Playwright) |
| Demo scenarios (FR-63) | `e2e/test_compose_stack.py` against the compose stack |

## 3. Running

```bash
# backend (needs a PostgreSQL role that may create a throw-away database)
cd backend && DTQ_TEST_ADMIN_DSN=postgresql://postgres:postgres@localhost:5432/postgres pytest -q tests/unit tests/integration tests/system
# frontend
cd frontend && npm test
# end-to-end (stack must be up: docker compose up -d --build)
cd backend && DTQ_E2E_API=http://localhost:8000 pytest -q tests/e2e
cd frontend && npx playwright test
```

`scripts/run_tests.sh` runs everything that the environment supports and writes a log to `docs/test-results/` (git-ignored, regenerated on every run).

## 4. Rules

* Never weaken an assertion to get green; fix the root cause and re-run.
* Timing-based tests use explicit, configurable short intervals (e.g. heartbeat 0.5 s) and poll with deadlines instead of fixed sleeps.
* Every test database is created fresh; tests are order-independent.
* Results of every executed run (command, date, pass/fail counts) are recorded in `PROGRESS.md` and `FINAL_DELIVERY.md`.
