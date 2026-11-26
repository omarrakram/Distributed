# Risk Register

Likelihood/Impact: L = low, M = medium, H = high. Status is updated during the project (see `PROGRESS.md`).

| ID | Risk | L | I | Mitigation | Status |
|---|---|---|---|---|---|
| R1 | Race conditions let two workers own one task | M | H | `FOR UPDATE SKIP LOCKED`, guarded compare-and-set updates, transition trigger, unique `(task_id, attempt_no)`, dedicated concurrency test | Mitigated |
| R2 | False failure suspicion causes duplicate execution | H | M | Accept (at-least-once); fencing tokens prevent duplicate *visible* results; SUSPECT state before DEAD; tuned timeouts | Mitigated (documented) |
| R3 | Broker downtime expires all leases → mass re-execution | M | M | Restart grace period extends leases when recovery leadership is acquired | Mitigated |
| R4 | Lost HTTP response after a claim strands an assignment | M | M | Claim redelivers ASSIGNED tasks for the same worker; ack timeout recovers otherwise | Mitigated |
| R5 | Autoscaler oscillation / runaway creation | M | H | Sustain windows, hysteresis, cooldown, per-cycle start limit, DB-enforced max ≤ 16, agent hard cap, counting not-yet-registered workers | Mitigated |
| R6 | Docker socket gives root-equivalent power | M | H | Only the agent mounts it; agent not published; token auth; acts only on labelled containers; service restart limited to allow-list in demo mode | Mitigated (documented) |
| R7 | Windows Docker Desktop incompatibilities (line endings, socket, ports) | M | M | `.gitattributes` forcing LF, Python entrypoints (no shell scripts in images), configurable host ports, PowerShell helper scripts | Mitigated; not executed on Windows here |
| R8 | Benchmark noise on a shared 4-vCPU VM | H | M | Repeated trials, report mean ± sd, warm-up, fixed seeds, record environment, separate CPU- vs wait-bound workloads | Mitigated (documented) |
| R9 | PostgreSQL is a single point of failure | M | H | Out of scope (S15 lists replication as extension); persistent volume; documented limitation | Accepted |
| R10 | Long-running CPU handlers block the event loop | M | H | CPU handlers run in separate killable processes; I/O handlers are coroutines | Mitigated |
| R11 | NOTIFY missed (connection drop) stalls dispatch | L | M | 500 ms polling tick + LISTEN reconnect loop | Mitigated |
| R12 | Scope too large for timeframe | H | H | Build MVS first (S15), integrate incrementally, automate tests early | Managed |
| R13 | Forked process + threads deadlock | L | M | `forkserver` start method with preloaded handler module | Mitigated |
| R14 | Payload abuse (huge inputs, code injection) | M | H | Pydantic limits, whitelist of operations, request body limit, no `eval`/pickle from clients | Mitigated |
| R15 | Clock skew between hosts breaks leases | M | M | All lease arithmetic uses PostgreSQL `now()` | Mitigated |
| R16 | Team members cannot explain the code | M | H | Per-member study guides, code walkthroughs, viva Q&A, review checklists; members must review and own their PRs | Mitigated (handover) |
