# Study guide — Reem Abobakr (2023/01478): Broker and task assignment

## 1. Relevant source files

| File | What it is |
|---|---|
| `backend/dtq/broker/app.py` | the broker service: protocol routes, authentication, lifecycle of background tasks |
| `backend/dtq/broker/scheduler.py` | `plan_assignments` (pure policy) and `Dispatcher` (long-poll claims + transactional dispatch) |
| `backend/dtq/broker/leadership.py` | leader election with PostgreSQL advisory locks |
| `backend/tests/unit/test_scheduler.py` | planning policy: FIFO, least-utilised, round robin, capacity |
| `backend/tests/integration/test_broker.py` | the full protocol against a real database |
| `backend/tests/integration/test_scheduler_concurrency.py` | 20 dispatchers racing over 200 tasks: no double assignment |
| `backend/tests/system/test_broker_restart.py` | SIGKILL the broker mid-run, restart, nothing lost |
| `docs/TECHNICAL_DESIGN.md` §2 | scheduling and dispatch design |

## 2. Responsibilities

The broker is the **queue + dispatch** box of slide 5. It registers workers, tracks their free capacity, decides
which pending task goes to which worker, records ownership (attempt, lease, fencing token), accepts acknowledgements
and results, and refuses messages from stale attempts.

## 3. Key implementation decisions

1. **Pull transport, central decision.** Workers long-poll `CLAIM` with their number of free slots
   (capacity-aware, built-in back-pressure). The *broker* chooses the assignment, so the policy is central and explainable.
2. **The queue is the `tasks` table.** No in-memory queue can lose tasks. The broker holds only soft state
   (waiting long-polls), so it restarts freely.
3. **One dispatch cycle = one transaction.** Lock the workers, lock the tasks with `SKIP LOCKED`, assign, commit;
   only then answer the waiting HTTP requests. A crash before commit assigns nothing.
4. **LISTEN/NOTIFY + a 500 ms tick.** NOTIFY (sent by the submission transaction) gives low latency; the tick picks
   up tasks whose retry backoff has elapsed and covers lost notifications.
5. **Redelivery.** If a claim response is lost, the task is still `ASSIGNED` to that worker; the next claim
   redelivers it (after 1 s). Otherwise the acknowledgement timeout recovers it.
6. **Leader election** for the singleton recovery loop, so several broker replicas would not duplicate recovery work.

## 4. Important algorithms

**Policy (`plan_assignments`, pure function):**
```
for each task in FIFO order (priority DESC, seq ASC):
    eligible = waiting workers with planned < free
    best = argmin over eligible of ((active + planned) / capacity, rank)
    rank = (0, last_assigned_at, waiting_since) initially; (1, counter) after being served this cycle
    assign task → best; best.planned += 1
```
This is a greedy least-utilised choice with round-robin tie-breaking. It never exceeds `capacity − active`.

**Dispatch transaction (`Dispatcher.dispatch_cycle`):**
```sql
SELECT … FROM workers WHERE id = ANY(claimants) AND status='ACTIVE' ORDER BY id FOR UPDATE;   -- no deadlocks
SELECT id FROM tasks WHERE status IN ('PENDING','RETRY_WAIT') AND available_at <= now()
       AND attempt_count < max_attempts ORDER BY priority DESC, seq LIMIT Σfree FOR UPDATE SKIP LOCKED;
-- per (task, worker): lifecycle.assign → guarded UPDATE + INSERT task_attempts
UPDATE workers SET last_assigned_at = now() …; INSERT system_events …;
COMMIT;  → resolve the waiting futures
```

**Why it is race-free:** `SKIP LOCKED` makes concurrent dispatchers take disjoint task sets. The guarded `UPDATE`
only succeeds from `PENDING`/`RETRY_WAIT`. The database trigger rejects any other transition. The unique
`(task_id, attempt_no)` constraint rejects a duplicate attempt. Locking the worker rows prevents two brokers from
over-filling the same worker.

**Long-poll claim (`Dispatcher.claim`):** register a `Claimant(worker, max_tasks, future)`, wake the dispatcher,
`await wait_for(shield(future), wait_s)`. A newer claim from the same worker supersedes the older one.

## 5. Communication with other components

| Peer | Messages |
|---|---|
| Workers | REGISTER, HEARTBEAT (→ revoked list / 410), CLAIM (long-poll), START, RENEW, COMPLETE, FAIL, DRAIN, DEREGISTER |
| PostgreSQL | dispatch transaction; lifecycle functions (`dtq/db/lifecycle.py`, Lama); LISTEN `dtq_tasks` |
| Recovery loop | in-process: `RecoveryLoop` (Malak) runs on the leader and calls `dispatcher.wake()` |
| Autoscaler / API | `POST /v1/workers/{id}/drain`, `GET /v1/stats`, `GET /health` |

## 6. Theory

- **Mutual exclusion / atomic claim:** row locks, `SKIP LOCKED` (PostgreSQL docs, *SELECT … Locking Clause*).
- **Load balancing:** least-loaded + round robin vs. random/power-of-two; pull-based work distribution.
- **Leases** (Gray & Cheriton, 1989): time-bounded ownership; safety must not depend on clocks → fencing.
- **Leader election** via a lock service (advisory locks; compare with ZooKeeper/Chubby).
- **Back-pressure:** workers only ask for what they can run.

## 7. Failure scenarios

| Scenario | What the broker does |
|---|---|
| Two workers claim at the same instant | disjoint `SKIP LOCKED` sets; each task assigned once |
| Claim response lost | redelivery on the next claim, or ack-timeout recovery |
| Broker crashes during dispatch | transaction rolls back; nothing assigned; workers re-poll |
| Broker restarts | workers retry with backoff; new leader applies the restart grace (leases extended) |
| Stale START/RENEW/COMPLETE | guarded UPDATE matches 0 rows → 409 + `task.stale_*_rejected` event (written outside the rolled-back transaction) |
| Draining worker claims | 409 `worker_draining` (the worker then shuts down) |

## 8. Demonstration responsibilities

Scenario A (distribution over ≥ 3 workers). Explain the dispatch transaction on the Architecture page (broker
node: assignments, dispatch cycles, redeliveries, stale rejections).

## 9. Code walkthrough

### 9.1 `broker/app.py`
`create_app` builds `BrokerState`; `lifespan` creates the pool, waits for migrations, and starts four tasks:
dispatcher loop, LISTEN loop, leader election and recovery loop. Every `/v1` route depends on
`require_cluster_token` (constant-time compare). Each task message route opens a transaction, calls a lifecycle
function and emits an event. `reject()` records the stale event through the pool (autocommit) **before** raising
409 — raising rolls back the route's transaction. `complete` refuses results larger than 64 KiB (413).
`deregister` releases leftover tasks with `RELEASED` attempts and no backoff.

### 9.2 `broker/scheduler.py`
`Candidate` / `plan_assignments`: the policy. `Dispatcher.run()`: wait for wake-up or tick, run a cycle, loop
immediately when something was assigned. `redeliver()`: returns `ASSIGNED` tasks of this worker whose attempt is
older than 1 s and whose lease is still valid.

### 9.3 `broker/leadership.py`
A dedicated connection calls `pg_try_advisory_lock(hashtext(key))`. On success, `is_leader = True` and
`on_acquired()` runs (restart grace). It then checks the connection every 2 s; if the connection breaks,
PostgreSQL frees the lock and another instance can take over.

## 10. Expected questions and answers

**Q: What prevents two workers from getting the same task?** The dispatch transaction locks candidate tasks with
`FOR UPDATE SKIP LOCKED`, so concurrent dispatchers see disjoint rows. The UPDATE only applies to
`PENDING`/`RETRY_WAIT`, the trigger rejects illegal transitions, and the unique attempt number rejects
duplicates. The concurrency test (20 dispatchers, 200 tasks, 40 workers) found 0 duplicates and no worker over capacity.

**Q: Is the scheduling fair?** Tasks are FIFO by submission sequence (optionally by priority). Workers are chosen
least-utilised first, with ties broken by least-recently-assigned — round robin among equals.
`test_plan_never_exceeds_free_slots_and_balances_round_robin` shows 6 tasks spread 2/2/2.

**Q: Why long polling?** It avoids busy polling (latency close to a push, low load) and needs no connection to the
worker. A claim waits up to 20 s and returns as soon as tasks are assigned.

**Q: What happens to waiting claims when the broker restarts?** They are dropped (soft state). Workers get a
transport error, back off, and re-claim against the new instance. No task is lost because tasks live in
PostgreSQL; leases get a grace period.

**Q: Can you run two brokers?** Yes, safely: dispatch is serialised per row (SKIP LOCKED + worker row locks),
and recovery runs only on the advisory-lock leader. The default deployment runs one broker; broker
replication with failover is future work.

**Q: What is the difference between ASSIGNED and RUNNING?** ASSIGNED: the broker chose a worker; a short
acknowledgement lease (10 s) is running. RUNNING: the worker acknowledged with START; the execution lease is
15 s and is renewed every 5 s. A worker that crashes before START is detected by the ack timeout.
