"""Minimal, transparent SQL migration runner.

* Applies ``migrations/NNNN_name.sql`` in lexical order, each in its own transaction.
* Records ``(version, checksum)`` in ``schema_migrations``; refuses to run if an
  applied file was modified afterwards (protects against silent schema drift).
* Serialises concurrent runners with a PostgreSQL advisory lock, so several
  services starting at once cannot apply the same migration twice.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path

import asyncpg

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
_LOCK_KEY = "dtq.migrations"


class MigrationError(RuntimeError):
    pass


def _discover(directory: Path) -> list[tuple[str, str, str]]:
    files = sorted(directory.glob("*.sql"))
    if not files:
        raise MigrationError(f"no migrations found in {directory}")
    out = []
    for path in files:
        sql = path.read_text(encoding="utf-8").replace("\r\n", "\n")
        out.append((path.stem, sql, hashlib.sha256(sql.encode()).hexdigest()))
    return out


async def apply_migrations(dsn: str, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply pending migrations. Returns the versions applied by this call."""
    migrations = _discover(directory)
    conn = await asyncpg.connect(dsn)
    applied_now: list[str] = []
    try:
        await conn.execute("SELECT pg_advisory_lock(hashtext($1))", _LOCK_KEY)
        try:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                       version    TEXT PRIMARY KEY,
                       checksum   TEXT NOT NULL,
                       applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"""
            )
            applied = {
                r["version"]: r["checksum"]
                for r in await conn.fetch("SELECT version, checksum FROM schema_migrations")
            }
            for version, sql, checksum in migrations:
                if version in applied:
                    if applied[version] != checksum:
                        raise MigrationError(
                            f"migration {version} was modified after it was applied "
                            f"(db checksum {applied[version][:12]}, file {checksum[:12]})"
                        )
                    continue
                log.info("applying migration %s", version)
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)",
                        version, checksum,
                    )
                applied_now.append(version)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", _LOCK_KEY)
    finally:
        await conn.close()
    return applied_now


async def wait_for_schema(pool: asyncpg.Pool, directory: Path = MIGRATIONS_DIR, timeout_s: float = 120) -> None:
    """Block until every migration on disk is recorded as applied (used by services at start-up)."""
    expected = {version for version, _, _ in _discover(directory)}
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        try:
            rows = await pool.fetch("SELECT version FROM schema_migrations")
            missing = expected - {r["version"] for r in rows}
            if not missing:
                return
            log.info("waiting for migrations: %s", ", ".join(sorted(missing)))
        except asyncpg.UndefinedTableError:
            log.info("waiting for schema_migrations table")
        if asyncio.get_running_loop().time() > deadline:
            raise MigrationError("timed out waiting for database migrations")
        await asyncio.sleep(1.0)
