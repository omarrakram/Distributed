"""Command-line entry point: ``python -m dtq <component>``.

Every component is a separate OS process communicating only over the network
(HTTP) and through PostgreSQL:

    python -m dtq migrate       apply SQL migrations, then exit
    python -m dtq api           public REST/WebSocket API          (:8000)
    python -m dtq broker        custom task broker                 (:8001)
    python -m dtq agent         worker-management agent            (:8003)
    python -m dtq autoscaler    autoscaling controller loop
    python -m dtq worker        one worker process
    python -m dtq healthcheck X container health probe for X
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
import urllib.request

from dtq.common.logging import configure_logging

AUTOSCALER_ALIVE_FILE = os.environ.get("DTQ_AUTOSCALER_ALIVE_FILE", "/tmp/dtq-autoscaler.alive")


def _serve(app, host: str, port: int, level: str) -> None:
    import uvicorn

    uvicorn.run(app, host=host, port=port, log_config=None, log_level=level.lower(), access_log=False,
                timeout_graceful_shutdown=5)


def _warn_insecure(settings) -> None:
    insecure = settings.insecure_defaults()
    if insecure:
        logging.getLogger("dtq").warning(
            "using development default tokens for %s — set them in .env for any shared deployment",
            ", ".join(insecure),
        )


def cmd_migrate() -> int:
    from dtq.common.config import DatabaseSettings
    from dtq.db.migrate import apply_migrations

    s = DatabaseSettings()
    configure_logging("migrate", s.log_level, s.log_format)

    async def run() -> list[str]:
        from dtq.db.pool import create_pool

        pool = await create_pool(s.database_url, 1, 1)  # waits for PostgreSQL to accept connections
        await pool.close()
        return await apply_migrations(s.database_url)

    applied = asyncio.run(run())
    logging.getLogger("dtq.migrate").info("migrations applied: %s", ", ".join(applied) or "none (up to date)")
    return 0


def cmd_broker() -> int:
    from dtq.broker.app import create_app
    from dtq.common.config import BrokerSettings

    s = BrokerSettings()
    configure_logging("broker", s.log_level, s.log_format)
    _warn_insecure(s)
    _serve(create_app(s), s.host, s.port, s.log_level)
    return 0


def cmd_api() -> int:
    from dtq.api.app import create_app
    from dtq.common.config import ApiSettings

    s = ApiSettings()
    configure_logging("api", s.log_level, s.log_format)
    _warn_insecure(s)
    _serve(create_app(s), s.host, s.port, s.log_level)
    return 0


def cmd_agent() -> int:
    from dtq.agent.app import create_app
    from dtq.common.config import AgentSettings

    s = AgentSettings()
    configure_logging(f"agent:{s.agent_id}", s.log_level, s.log_format)
    _warn_insecure(s)
    _serve(create_app(s), s.host, s.port, s.log_level)
    return 0


def cmd_autoscaler() -> int:
    from dtq.autoscaler.controller import run_forever
    from dtq.common.config import AutoscalerSettings

    s = AutoscalerSettings()
    configure_logging("autoscaler", s.log_level, s.log_format)
    _warn_insecure(s)
    asyncio.run(run_forever(s, alive_file=AUTOSCALER_ALIVE_FILE))
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    if args.broker_url:
        os.environ["DTQ_BROKER_URL"] = args.broker_url
    if args.capacity:
        os.environ["DTQ_WORKER_CAPACITY"] = str(args.capacity)
    if args.name:
        os.environ["DTQ_WORKER_NAME"] = args.name
    from dtq.common.config import WorkerSettings
    from dtq.worker import executor
    from dtq.worker.runtime import Worker

    s = WorkerSettings()
    configure_logging(f"worker:{s.worker_name}", s.log_level, s.log_format)
    executor.mp_context()  # start method chosen before any thread exists

    async def run() -> int:
        worker = Worker(s)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, worker.request_stop)
            except (NotImplementedError, RuntimeError):  # Windows
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(worker.request_stop))
        return await worker.run()

    return asyncio.run(run())


def cmd_healthcheck(target: str) -> int:
    if target == "autoscaler":
        try:
            age = time.time() - os.path.getmtime(AUTOSCALER_ALIVE_FILE)
        except OSError:
            return 1
        return 0 if age < 30 else 1
    ports = {"api": ("DTQ_PORT", 8000, "/api/v1/healthz"), "broker": ("DTQ_PORT", 8001, "/health"),
             "agent": ("DTQ_PORT", 8003, "/health")}
    env, default, path = ports[target]
    port = int(os.environ.get(env, default))
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=4) as resp:
            return 0 if resp.status == 200 else 1
    except Exception:
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m dtq", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("migrate", "api", "broker", "agent", "autoscaler"):
        sub.add_parser(name)
    w = sub.add_parser("worker")
    w.add_argument("--broker-url")
    w.add_argument("--capacity", type=int)
    w.add_argument("--name")
    hc = sub.add_parser("healthcheck")
    hc.add_argument("target", choices=["api", "broker", "agent", "autoscaler"])
    args = parser.parse_args(argv)
    if args.cmd == "worker":
        return cmd_worker(args)
    if args.cmd == "healthcheck":
        return cmd_healthcheck(args.target)
    return {"migrate": cmd_migrate, "api": cmd_api, "broker": cmd_broker, "agent": cmd_agent,
            "autoscaler": cmd_autoscaler}[args.cmd]()


if __name__ == "__main__":
    sys.exit(main())
