# Requirements Specification

**Project:** Distributed Task Queue with Fault Tolerance and Dynamic Worker Scaling
**Source of truth:** the instructor-approved proposal deck (20 slides, `DS.TaskQueue.Proposal.pptx`) plus the team's execution brief.
Slide references are written as `[S7]` (= proposal slide 7).

---

## 1. Proposal analysis (slide by slide)

| Slide | Topic | Requirement content extracted |
|---|---|---|
| S1 | Title | Distributed task queue; *fault tolerance* + *dynamic worker scaling*; pillars: architecture, reliability, evaluation. Diagram: task queue → W1/W2/W3 → persistent state; badges RECOVER / RETRY / SCALE. |
| S2 | Motivation | A single executor is a bottleneck and a single point of failure. Distributed workers share work; failures can be detected and retried. Objective: *faster processing, recoverable failures, observable task execution*. |
| S3 | Concept | Pipeline Producer → Queue → Broker → Worker → Database. **A crashed worker's task must become eligible for another attempt without silently disappearing.** |
| S4 | Objectives | (1) Distribution: ≥ 3 workers process independent jobs in parallel. (2) Reliability: unfinished tasks recover after a worker crash. (3) Observability: task states, retries, results and worker activity are visible. (4) Scalability: compare worker counts and measure objectively. Academic focus: concurrency, message passing, coordination, persistence, fault tolerance. |
| S5 | Logical architecture | Client (create job) → API (validate + ID) → Broker (queue + dispatch) → Workers 1..3 (execute). API and broker persist task state + results. Optional monitoring UI and autoscaler. *Nodes are separate processes.* Workers acknowledge completion. |
| S6 | Components | Core: Producer/API, Task broker, Worker processes, Persistent storage (task, attempts, status, result), Failure recovery (timeout / re-deliver). Extensions: Monitoring dashboard, Scaling controller. |
| S7 | State machine | `PENDING → ASSIGNED → RUNNING → COMPLETED` (success + committed result); `RUNNING → RETRY_WAIT` (timeout / crash / retriable error); `RETRY_WAIT → ASSIGNED`; `RETRY_WAIT → FAILED` (retry limit exceeded). Task id and attempt history remain inspectable. |
| S8 | Trade-off | Approach A (RabbitMQ) vs Approach B (custom broker over sockets/HTTP + persistent storage). **Brief selects B.** |
| S9 | Open questions | External broker? → custom. Separate machines? → single host by default, configurable multi-host. Failures to handle? → worker failures + broker restart (HA optional). Autoscaling mandatory? → manual and automatic both implemented. |
| S10 | Theory ↔ implementation | Concurrency, message passing, load balancing, coordination (leases/acks limit conflicting claims), persistence (committed state survives restart), fault tolerance (abandoned work retried). Every concept needs an implementation artifact **and** a demonstrable test. |
| S11 | Failure scenario | Assign T-101 → W1 processes → W1 crashes → timeout/redelivery → W2 executes → result persisted. A missing heartbeat only *suggests* failure; network delay can cause false suspicion. |
| S12 | Delivery guarantees | At-least-once execution is the chosen model; duplicates are possible; "exactly-once **effect**" (one visible outcome) requires idempotency/transactional design. Example: result saved but ack lost → retry must not apply the effect twice. |
| S13 | Data design | Tables `tasks(task_id, type, payload, status, result)`, `attempts(task_id, attempt_no, worker_id, timestamps)`, `workers(worker_id, last_seen, state, capacity)`. API: `POST /tasks`, `GET /tasks/{id}`, `GET /workers`, `GET /metrics`. |
| S14 | Dynamic scaling | Control loop MEASURE (queue length + busy workers) → DECIDE (thresholds) → ACT (scale within min/max) → WAIT (cooldown). Backlog stays high → add 1 worker (≤ max); backlog stays low → remove an idle worker (≥ min). Extra processes on one laptop do not create extra CPU cores. |
| S15 | Scope control | MVS: uniquely identified jobs, multiple independent workers, status/results tracking, ack + retry of unfinished work, persisted committed records, crash-recovery demo. Extensions: live dashboard, heartbeat monitoring, bounded autoscaling, advanced scheduling, multi-machine, broker failover. |
| S16 | Plan | Incremental milestones, each tested: prototype → parallel workers → tracking → retry/recovery → persistence → monitoring → scaling → evaluation. |
| S17 | Evaluation | Exp. A scalability (1/2/3/5 workers: duration, throughput, speedup); Exp. B fault injection (kill a worker: recovered tasks, retries, recovery delay); Exp. C workload growth (queue wait, throughput trend). Throughput = completed / elapsed; Speedup(N) = T1/TN. Repeat trials, report hardware/variability/limitations. **No invented results.** |
| S18 | Demonstration | Submit → distribute across 3 workers → kill a busy worker → unfinished task retried → verify results + metrics. Optional finale: bounded scale-out. |
| S19 | Team | Omar Akram (2023/01749) architecture + integration; Reem Abobakr (2023/01478) broker + task assignment; Malak Mohamed (2023/02654) workers + recovery logic; Lama Hassan Ataturk (2023/00580) database + task lifecycle; Habiba Ahmed Mahmoud (2023/06098) dashboard + scaling + evaluation. |
| S20 | Closing | "Build for recovery. Measure the scale." Confirm requirements, choose broker, implement + verify MVS, demonstrate faults with reproducible metrics. |

---

## 2. Functional requirements

IDs are referenced by the traceability matrix (`docs/TRACEABILITY_MATRIX.md`).

### 2.1 Task submission (Producer/API)
| ID | Requirement | Source |
|---|---|---|
| FR-01 | Clients submit a task (type + payload + options) over HTTP; the API validates it and returns a unique task id. | S5, S6, S13 |
| FR-02 | Batch submission of up to 1 000 tasks in one atomic request. | Brief Ph.8 |
| FR-03 | Supported task types: prime computation, matrix multiplication, image processing, SHA-256 hashing, configurable delay, intentional transient failure, intentional permanent failure. | Brief Ph.5 |
| FR-04 | Payloads are validated per type with resource limits; arbitrary code execution is impossible. | Brief Ph.5 |
| FR-05 | Duplicate submission with the same idempotency key returns the original task; a different request with a reused key is rejected (409). | Brief Ph.6, S12 |
| FR-06 | A task is acknowledged to the client only after it is durably committed (no window where an accepted task can vanish). | S3, Brief Ph.4 |

### 2.2 Broker, scheduling and coordination
| ID | Requirement | Source |
|---|---|---|
| FR-10 | A custom broker (our code, not RabbitMQ/Celery) owns the queue and dispatch. | S8, Brief Ph.3 |
| FR-11 | Workers register, advertise capacity, and request work; the broker assigns tasks only up to free capacity. | S5, S6 |
| FR-12 | FIFO scheduling (with optional integer priority) and least-loaded, round-robin-fair assignment across waiting workers. | S10 (load balancing) |
| FR-13 | Concurrent claims never assign one task to two workers at the same time (atomic claim). | S10 (coordination) |
| FR-14 | Each assignment creates an attempt record and a lease; ownership is tracked per task. | S7, S13 |
| FR-15 | Workers acknowledge assignment (`ASSIGNED → RUNNING`) and acknowledge completion; duplicate acknowledgements are idempotent. | S5, S12 |
| FR-16 | Only the current attempt (fencing token) can renew, complete or fail a task; stale attempts are rejected. | S12, Brief Ph.6 |

### 2.3 Lifecycle and persistence
| ID | Requirement | Source |
|---|---|---|
| FR-20 | Lifecycle exactly as S7; every other transition is rejected by the database. | S7 |
| FR-21 | Tasks, attempts, workers, transitions, events, scaling decisions and benchmark runs are persisted in PostgreSQL. | S13, Brief Ph.4 |
| FR-22 | Complete attempt history (worker, timestamps, outcome, error) is kept for every task. | S7, S13 |
| FR-23 | Committed tasks and results survive API, broker and full-stack restarts. | S10, S15 |

### 2.4 Workers
| ID | Requirement | Source |
|---|---|---|
| FR-30 | Workers are independent OS processes (one container each under Docker) communicating with the broker over the network only. | S5, Brief Ph.5 |
| FR-31 | Workers send periodic heartbeats, renew leases, report progress, submit results/failures. | S11, S15 |
| FR-32 | Workers survive temporary broker unavailability (retry with backoff, re-register when declared dead). | Brief Ph.5/6 |
| FR-33 | Workers shut down gracefully on request (drain: stop claiming, finish or release work, deregister). | Brief Ph.5/7 |
| FR-34 | ≥ 3 workers run concurrently by default; worker count is configurable. | S4 |

### 2.5 Fault tolerance
| ID | Requirement | Source |
|---|---|---|
| FR-40 | Worker failure detection via heartbeats (suspect → dead) and expiring task leases evaluated on database time. | S11 |
| FR-41 | Abandoned work is retried automatically with exponential backoff, up to `max_attempts`; then `FAILED`. | S7, S10 |
| FR-42 | Execution timeouts are enforced by the worker (kills the child process) and by the broker (deadline). | Brief Ph.6 |
| FR-43 | Broker restart loses no tasks; in-flight leases get a grace period after restart. | S9, Brief Ph.6 |
| FR-44 | Stale results from a worker that was (falsely) declared dead cannot overwrite the committed result. | S11, S12 |

### 2.6 Scaling
| ID | Requirement | Source |
|---|---|---|
| FR-50 | An autoscaler measures queue depth, busy/idle workers and utilization; decides with thresholds sustained over time; acts within [min, max]; waits for a cooldown. | S14 |
| FR-51 | Scaling starts/stops **real** worker processes/containers through a controlled worker-management agent; the public API has no Docker access. | Brief Ph.7 |
| FR-52 | Scale-down drains idle workers first and never kills busy workers in automatic mode. | S14 |
| FR-53 | Manual and automatic modes; every decision and its reason is recorded. | S16, Brief Ph.7 |

### 2.7 Observability and demonstration
| ID | Requirement | Source |
|---|---|---|
| FR-60 | Dashboard (React/TS/Tailwind) with Overview, Architecture, Tasks, Workers, Autoscaling, Analytics, Logs and Demo pages fed by live REST + WebSocket data. | S4, S6, Brief Ph.8 |
| FR-61 | Metrics: counts by state, throughput, execution time, queue wait, utilization, retries, failures, recovery delay. | S13, S17 |
| FR-62 | Structured system events with severity/component/task/worker filters. | Brief Ph.8 |
| FR-63 | Automated demo scenarios A–G and a reset limited to demo data. | S18, Brief Ph.9 |
| FR-64 | Destructive controls (kill/pause workers, demo reset, scaling changes) require demo mode **and** an admin token. | Brief Ph.8 |

### 2.8 Evaluation
| ID | Requirement | Source |
|---|---|---|
| FR-70 | Executable benchmark scripts for Experiments A (scalability), B (fault injection), C (workload growth), D (fixed vs autoscaling). | S17, Brief Ph.11 |
| FR-71 | Real CSV datasets + graphs; hardware, configuration, trials and limitations recorded. | S17 |

---

## 3. Non-functional requirements

| ID | Category | Requirement |
|---|---|---|
| NFR-01 | Correctness | No task is lost after a 201/200 response; no task has two concurrent valid owners; at most one committed result per task. |
| NFR-02 | Reliability | Recovery from a single worker crash completes without operator action; detection delay bounded by configuration (default ≤ 10 s heartbeat timeout / 15 s lease). |
| NFR-03 | Performance | Dispatch latency for an idle system < 100 ms (LISTEN/NOTIFY wake-up); broker sustains ≥ 50 assignments/s on the reference laptop. |
| NFR-04 | Scalability | Throughput increases with workers until CPU cores saturate (CPU-bound) and near-linearly for waiting-bound tasks. |
| NFR-05 | Portability | `docker compose up --build` on Linux, macOS and Windows (Docker Desktop, Linux containers). Workers also runnable as plain Python processes on other hosts. |
| NFR-06 | Security | Shared-secret tokens between internal components; workers never receive database credentials; payload limits; no arbitrary code; only the agent holds the Docker socket; non-root containers except the agent. |
| NFR-07 | Maintainability | Modular packages per component, typed Python, pinned dependencies, SQL migrations, structured JSON logs. |
| NFR-08 | Observability | Every state change produces a persisted transition and/or event visible in the dashboard within ~1 s. |
| NFR-09 | Reproducibility | Deterministic task handlers, seeded inputs, scripted experiments, recorded environment. |
| NFR-10 | Honesty | Documentation never claims exactly-once execution or fabricated results. |

---

## 4. Ambiguities and how they were resolved

| # | Ambiguity | Resolution (and rationale) |
|---|---|---|
| A1 | RabbitMQ vs custom broker (S8, S9) | Custom broker over HTTP + PostgreSQL (brief mandates; maximises learning value per S8 Approach B). |
| A2 | Does `RETRY_WAIT` return to `PENDING`? | No. Following S7 literally: `RETRY_WAIT → ASSIGNED` when backoff elapses; `PENDING` is used only for never-assigned tasks. |
| A3 | Can a crash during `ASSIGNED` (before start) be recovered? | Yes; brief lists the recovery path `ASSIGNED or RUNNING → RETRY_WAIT`. Added `ASSIGNED → RETRY_WAIT`. |
| A4 | What counts as an attempt? | Every assignment increments `attempt_count` (the fencing token). Lost/expired assignments therefore count toward `max_attempts` — this bounds poison tasks that crash workers. |
| A5 | Is a failed attempt always retried? | Retryable errors, timeouts and lost leases → retry with backoff. Non-retryable errors (`retryable=false`) go `RETRY_WAIT → FAILED` immediately. |
| A6 | "Permanent failure" task vs retry exhaustion demo | `fail_permanent` raises a retryable error by default so Scenario C shows bounded retries; `retryable=false` demonstrates immediate terminal failure. |
| A7 | Separate machines? (S9 Q2) | Default single-host Docker deployment; multi-host supported by running workers/agents elsewhere against the published broker port. True multi-machine measurements are labelled separately from local process scaling. |
| A8 | Broker HA (S15 extension) | Not required. Broker is stateless (soft state only) and restartable; DB is the single authority. Multiple broker replicas are *safe* (atomic claims, advisory-lock leadership) but not the default. |
| A9 | Autoscaler ownership of capacity vs manual buttons | The autoscaler is the single owner of worker count. Manual mode = fixed target. UI "start/stop" change the manual target; kill/pause are fault injection and do not change the target (the controller replaces crashed workers like a supervisor). |
| A10 | Exactly-once | Not claimed. At-least-once *execution* attempts, bounded by `max_attempts`; exactly-once *visible result* via fenced, transactional commit. |

## 5. Dependencies

* PostgreSQL ≥ 13 (uses `gen_random_uuid()`, `FOR UPDATE SKIP LOCKED`, `LISTEN/NOTIFY`, advisory locks); deployed: 16.
* Docker Engine ≥ 24 / Docker Desktop with Compose v2 for the containerised deployment.
* Python 3.12 (container) — runtime libraries pinned in `backend/requirements.txt`.
* Node 22 for building the dashboard.

## 6. Technical risks (summary — full register in `RISK_REGISTER.md`)

Race conditions in claiming; false failure suspicion; lease expiry during broker downtime; autoscaler oscillation; Docker-socket security; benchmark noise on a shared 4-vCPU VM; Windows path/line-ending issues; scope size.
