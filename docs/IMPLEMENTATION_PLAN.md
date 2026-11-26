# Implementation Plan

Mirrors the proposal's incremental plan (S16: "test after every milestone"). Each milestone has an exit test; progress is tracked in `PROGRESS.md`.

## 1. Repository structure

```
Distributed/
├── README.md, PROGRESS.md, FINAL_DELIVERY.md
├── docker-compose.yml            # full single-host stack
├── docker-compose.worker-node.yml# extra worker host (multi-machine)
├── .env.example, .gitignore, .gitattributes, .dockerignore, .editorconfig
├── scripts/                      # start/stop/test/demo helpers (.sh + .ps1)
├── backend/
│   ├── Dockerfile, requirements.txt, requirements-dev.txt, pyproject.toml
│   ├── migrations/               # 0001_*.sql …  (SQL, applied in order)
│   ├── dtq/
│   │   ├── common/   config, logging, protocol (shared message models), task_specs, http helpers
│   │   ├── db/       pool, migrate runner, repositories (tasks, workers, events, scaling, analytics)
│   │   ├── broker/   app, scheduler/dispatcher, protocol routes, recovery (reaper), leadership
│   │   ├── worker/   runtime, broker client, executor, handlers/
│   │   ├── agent/    app, docker backend, process backend
│   │   ├── autoscaler/ policy (pure), controller loop
│   │   ├── api/      app, routes/, websocket hub, demo scenarios, auth
│   │   └── __main__.py  (python -m dtq <component>)
│   └── tests/        unit/, integration/, system/
├── frontend/          Vite + React + TypeScript + Tailwind dashboard (nginx in Docker)
├── benchmarks/        experiment runner, analysis, results/ (CSV, PNG)
└── docs/              design docs, report, guides, diagrams
```

## 2. Milestones

| # | Milestone (S16) | Deliverables | Exit test |
|---|---|---|---|
| M0 | Foundations | repo skeleton, pinned deps, config validation, logging, shared models | `pytest tests/unit/test_config.py` |
| M1 | Database | migrations, triggers, repositories | migration applies twice idempotently; illegal transition rejected |
| M2 | Local prototype | API submit + broker + 1 worker executes 1 task | `test_full_protocol_roundtrip` (broker integration), `test_api_to_workers_end_to_end_with_parallelism` (system) |
| M3 | Parallel workers | capacity-aware fair dispatch, ≥3 workers | concurrency + distribution tests |
| M4 | Task tracking | task detail, attempts, transitions, events | API tests |
| M5 | Retry + recovery | leases, heartbeats, reaper, backoff, fencing, stale rejection, timeouts | crash / pause / partition / exhaustion tests |
| M6 | Persistence | broker/API restart recovery, grace period | restart tests |
| M7 | Monitoring | WebSocket, metrics, analytics, dashboard pages | WS test, frontend unit tests, Playwright E2E |
| M8 | Scaling | agent (docker/process), autoscaler (manual → auto) | autoscaler policy unit tests + scale up/down system test |
| M9 | Demo | scenarios A–G + reset | scenario runs PASS in the compose stack |
| M10 | Evaluation | experiments A–D executed, CSV + graphs | files present, numbers traceable to CSVs |
| M11 | Documentation | report, diagrams, guides, demo script, traceability, audit | doc review against code |
| M12 | Handover | TEAM_HANDOVER packages, reconstruction test | rebuilt tree identical + tests pass |

## 3. Technology decisions (pinned in manifests)

| Area | Choice | Reason |
|---|---|---|
| Language | Python 3.12 | asyncio, rich stdlib (multiprocessing, hashlib) |
| HTTP services | FastAPI + Uvicorn | typed request validation, OpenAPI docs, WebSocket support |
| DB driver | asyncpg | fast native protocol, LISTEN/NOTIFY, explicit transactions |
| Validation | Pydantic v2 | shared schemas for API, broker protocol and task payloads |
| HTTP client | httpx (async) | timeouts, connection pooling |
| Container control | Docker SDK for Python | Engine API with version negotiation |
| Images | Pillow | image-processing handler |
| Frontend | React 18 + TypeScript + Vite + Tailwind 3 + TanStack Query + Recharts | brief stack; typed; fast build |
| Tests | pytest + pytest-asyncio; Vitest + Testing Library; Playwright | real infrastructure where needed |
| Analysis | pandas + matplotlib | brief stack |

## 4. Work allocation (S19) — used later for the handover packages

| Member | Components |
|---|---|
| Omar Akram | architecture, `dtq/common`, `dtq/api` (except analytics), compose/Docker, config, integration & system tests, deployment docs |
| Reem Abobakr | `dtq/broker` (app, scheduler, protocol routes, leadership), broker tests, broker docs |
| Malak Mohamed | `dtq/worker`, handlers, `dtq/broker/recovery.py`, retry/backoff, fencing tests, fault tests, recovery docs |
| Lama Hassan Ataturk | `migrations/`, `dtq/db`, lifecycle state machine, idempotency storage, DB tests, DB docs |
| Habiba Ahmed Mahmoud | `frontend/`, `dtq/autoscaler`, `dtq/agent`, analytics endpoints, `benchmarks/`, scaling/frontend tests, evaluation docs |

The exact file-level ownership is produced from the final codebase in `TEAM_HANDOVER/00_MASTER_HANDOVER/CONTRIBUTION_MATRIX.md`.
