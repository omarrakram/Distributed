# Study guide — Lama Hassan Ataturk (2023/00580): Database and task lifecycle

## 1. Relevant source files

| File | What it is |
|---|---|
| `backend/migrations/0001_initial_schema.sql` | all tables, enums, constraints, indexes, the state machine table and triggers |
| `backend/migrations/0002_analytics_views.sql` | `task_timings`, `worker_load`, `recovery_delays` views |
| `backend/dtq/db/migrate.py` | migration runner (advisory lock, checksums, `schema_migrations`) |
| `backend/dtq/db/pool.py` | asyncpg pool with JSON codecs and start-up retry |
| `backend/dtq/db/lifecycle.py` | **the only code that changes a task's status**: assign, start, renew, complete, fail, retry_or_fail, recover |
| `backend/dtq/db/tasks_repo.py` | idempotent submission, batch insert, read models (list, detail, batch summary) |
| `backend/dtq/db/workers_repo.py`, `events_repo.py` | worker registry (incarnations), system events |
| `backend/tests/conftest.py`, `tests/helpers.py` | throw-away database per test session; shared helpers |
| `backend/tests/integration/test_db.py` | migrations, every illegal transition, constraints, logging, idempotency, atomic batches |
| `docs/DATABASE_DESIGN.md` | ER diagram, constraints, transactions |

## 2. Responsibilities

PostgreSQL is the **authoritative source of truth** (slide 13: *Small database + clear external interface*). You
own the schema, the migrations, the task lifecycle state machine (slide 7), attempt records, worker records,
transactional result persistence and the idempotency mechanism.

## 3. Key implementation decisions

1. **The state machine is data plus a trigger.** `task_state_transitions` holds the 7 allowed edges;
   `tasks_enforce_transition` (BEFORE UPDATE) rejects everything else with SQLSTATE `DT001`, even if
   application code has a bug.
2. **The audit trail cannot be bypassed:** `tasks_log_transition` (AFTER trigger) writes every change to
   `task_transitions`, with the reason passed through `set_config('dtq.reason', …, true)`.
3. **Constraints encode invariants:** `ASSIGNED/RUNNING ⇒ owner + lease`; `COMPLETED ⇔ result`;
   `FAILED ⇒ error`; `attempt_count ≤ max_attempts`; `PENDING ⇒ never attempted`; the fencing token never decreases.
4. **Guarded compare-and-set updates** for every transition. They include the fencing pair, so stale messages
   match zero rows.
5. **The fencing token is `attempt_count`:** monotonic per task, incremented by every assignment.
6. **One row per worker incarnation**, so a revived worker cannot reuse its old identity.
7. **Plain SQL migrations with checksums**, transparent for review; an advisory lock serialises concurrent runners.
8. **Database time (`now()`)** for all leases and heartbeat ages, so worker clock skew is irrelevant.

## 4. Important algorithms / SQL

**Fenced result commit** (`lifecycle.complete`):
```sql
UPDATE tasks SET status='COMPLETED', result=$5, completed_at=now(), lease_expires_at=NULL
 WHERE id=$1 AND status='RUNNING' AND current_attempt_id=$3 AND attempt_count=$4 AND assigned_worker_id=$2
RETURNING …;
-- 0 rows → same attempt already COMPLETED? → 'duplicate' (idempotent ack) : 409 with diagnosed reason
```
The result, the attempt (`SUCCEEDED`), the worker counter and the event are written in one transaction.

**Failure path** (`retry_or_fail`): `→ RETRY_WAIT` (with `available_at = now() + backoff`), then, in the *same
transaction*, `RETRY_WAIT → FAILED` if attempts are exhausted or the error is non-retryable. A task is
therefore never left stuck in RETRY_WAIT without a future.

**Idempotent submission:** `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`. If no row comes
back, it compares `request_hash` → replay or `IdempotencyConflict`. Under concurrency the second inserter
waits for the first transaction and then sees the conflict. The test shows 20 concurrent submissions create 1 row.

**Migrations:** `pg_advisory_lock(hashtext('dtq.migrations'))`, each file in its own transaction, SHA-256 of
the file recorded. A modified applied file raises `MigrationError`.

## 5. Communication with other components

Every component except workers uses the database: the API (submission, queries), the broker (dispatch and
lifecycle calls), recovery, the autoscaler (metrics, decisions, commands). Triggers send `NOTIFY dtq_tasks`
(the broker wakes up) and `NOTIFY dtq_events` (the WebSocket pushes).

## 6. Theory

- **ACID transactions and isolation** (READ COMMITTED + row locks; EvalPlanQual re-check after waiting).
- **Write-ahead logging and durability:** committed means it survives a crash (`synchronous_commit=on`).
- **Optimistic concurrency / compare-and-set;** fencing tokens as a monotonic version.
- **State machines** as a correctness tool; invariants as constraints.
- **Idempotency keys** and exactly-once effects.

## 7. Failure scenarios

| Scenario | Protection |
|---|---|
| Bug tries `PENDING → COMPLETED` | trigger raises `DT001` (tested for all 23 illegal pairs) |
| Two results for one task | `result_only_when_completed` + the guarded UPDATE |
| Crash between "save result" and "ack" | one transaction: either both or neither |
| Concurrent duplicate submissions | unique index on `idempotency_key` |
| Two services run migrations at once | advisory lock serialises them |
| Someone edits an applied migration | checksum mismatch stops start-up |
| Batch with one invalid row | single statement → all-or-nothing |

## 8. Demonstration responsibilities

Scenario G (exactly one visible result: stale and duplicate COMPLETE replays) and Scenario F (persistence across
a broker restart). Show the task drawer's *Lifecycle transitions* and explain the trigger.

## 9. Code walkthrough

### 9.1 `migrations/0001_initial_schema.sql`
Read it top to bottom: enums → `workers` → `tasks` (look at every `CONSTRAINT`) → partial indexes (dispatch
index on `(priority DESC, seq) WHERE status IN ('PENDING','RETRY_WAIT')`; lease index) → `task_attempts`
(`UNIQUE (task_id, attempt_no)`) → `task_state_transitions` (7 rows) → the two triggers → `tasks_notify_new` →
`system_events` + statement-level NOTIFY → autoscaling tables (CHECK bounds) → benchmark/demo tables.

### 9.2 `db/lifecycle.py`
`assign` (from PENDING/RETRY_WAIT, increments the token, creates the attempt). `start` (idempotent for a repeated
START). `renew` (refused after the deadline). `complete`, `fail`, `retry_or_fail`, `recover`. `_diagnose` turns
"0 rows matched" into a precise 409 reason (`stale_attempt`, `lease_expired`, `deadline_exceeded`, `invalid_state`).

### 9.3 `db/tasks_repo.py`
`submit` (idempotent), `submit_batch` (`unnest` arrays → one INSERT), `list_tasks` (parameterised filters, never
string-concatenated values), `get_task_detail` (task + attempts + transitions + events), `batch_summary`.

### 9.4 `db/migrate.py`
`apply_migrations` and `wait_for_schema` (services wait until every migration on disk is recorded).

## 10. Expected questions and answers

**Q: How do you guarantee only valid state transitions?** Three layers: application functions only issue
specific guarded UPDATEs; the trigger checks every status change against `task_state_transitions`; CHECK
constraints enforce the per-state invariants. A test tries all 23 illegal pairs, and each is rejected.

**Q: What is stored for each attempt?** Worker, attempt number (= fencing token), status (ASSIGNED, RUNNING,
SUCCEEDED, FAILED, TIMED_OUT, LOST, RELEASED), assigned/started/finished timestamps, last renewal, progress,
duration and error type/message. The full history remains after success or terminal failure (slide 7).

**Q: Why is the queue a table and not a separate structure?** One source of truth: the same transaction that
assigns a task records ownership and the attempt. There is no window in which an accepted task exists only in
memory.

**Q: How do you avoid a race between the recovery loop and a late COMPLETE?** Both are guarded updates on the same
row under a row lock. Whichever commits first wins; the other's WHERE clause no longer matches (PostgreSQL
re-checks the condition after the lock). If COMPLETE wins, the task is done; if recovery wins, the late COMPLETE
gets 409.

**Q: Which isolation level do you use?** READ COMMITTED (PostgreSQL's default). It is sufficient because every
critical change is a single guarded UPDATE or uses explicit row locks (`FOR UPDATE [SKIP LOCKED]`).

**Q: How do the analytics get latency and queue wait?** From timestamps: queue wait =
`first_assigned_at − created_at`; execution = `completed_at − started_at`; latency =
`completed_at − created_at`. See the `task_timings` and `recovery_delays` views.
