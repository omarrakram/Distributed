# Acceptance Criteria

Each criterion is measurable and is verified by the referenced test or procedure. "Done" requires all **Must** criteria.

| ID | Priority | Criterion | Verification |
|---|---|---|---|
| AC-01 | Must | `docker compose up --build` starts postgres, migrate, api, broker, agent, autoscaler, frontend and **3 worker containers** with all health checks green within 3 minutes on the reference machine | clean startup test (FINAL_DELIVERY) |
| AC-02 | Must | A submitted task returns a UUID and reaches `COMPLETED` with a result visible via `GET /tasks/{id}` | system + e2e tests |
| AC-03 | Must | A batch of ≥ 12 tasks is executed by ≥ 3 distinct workers with overlapping execution intervals | Scenario A, system test |
| AC-04 | Must | Under 20 concurrent dispatchers, no task is ever assigned to two workers concurrently and attempt numbers are unique | `test_concurrent_dispatchers_never_double_assign` |
| AC-05 | Must | Illegal lifecycle transitions are rejected by the database | `test_illegal_transitions_are_rejected_by_the_database` |
| AC-06 | Must | Killing a busy worker (SIGKILL) leads to its task being completed by a different worker; attempt history shows `LOST` then `SUCCEEDED` | Scenario B, system test |
| AC-07 | Must | A permanently failing task with `max_attempts=3` ends `FAILED` after exactly 3 attempts with increasing backoff | Scenario C, tests |
| AC-08 | Must | A stale worker's late COMPLETE is rejected (409) and the committed result is unchanged; duplicate COMPLETE from the committing attempt returns `duplicate` | Scenario G, tests |
| AC-09 | Must | Re-submitting with the same idempotency key returns the same task id; reusing the key with a different payload returns 409 | tests, Scenario G |
| AC-10 | Must | Completed tasks and results survive broker, API and full-stack restarts; in-flight tasks complete after a broker restart | Scenario F, restart tests |
| AC-11 | Must | Sustained backlog makes the autoscaler start real additional worker containers (> 3) up to max 8; decisions with reasons are recorded | Scenario D, system test |
| AC-12 | Must | When idle, capacity returns to min 3 by gracefully draining idle workers; no task fails because of scale-down | Scenario E, system test |
| AC-13 | Must | Task execution exceeding `timeout_seconds` is terminated and retried/failed | system test |
| AC-14 | Must | Dashboard pages Overview, Architecture, Tasks, Workers, Autoscaling, Analytics, Logs, Demo display live backend data, update via WebSocket, and every button performs its function | Playwright E2E + manual checklist |
| AC-15 | Must | Destructive controls are refused without admin token or with demo mode disabled | API tests |
| AC-16 | Must | Automated tests execute and pass; command and results recorded | test logs |
| AC-17 | Must | Experiments A–D executed with ≥ 3 trials where applicable; CSV + PNG produced; environment recorded; nothing fabricated | `benchmarks/results/` |
| AC-18 | Must | Report, diagrams, API docs, install instructions, demo script, study guides (5 + shared), viva Q&A, traceability matrix, audit report and FINAL_DELIVERY.md exist and match the code | audit |
| AC-19 | Should | Workers can run on another host against the published broker port | documented procedure + host-process simulation |
| AC-20 | Must | TEAM_HANDOVER packages reconstruct a tree identical to the verified project and its tests pass | reconstruction test |
