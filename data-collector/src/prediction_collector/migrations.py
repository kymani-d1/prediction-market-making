from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import psycopg


LOGGER = logging.getLogger(__name__)


def migrations_directory() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations"


async def migrate_database(dsn: str, directory: Path | None = None) -> list[str]:
    migration_dir = directory or migrations_directory()
    files = sorted(migration_dir.glob("*.sql"))
    if not files:
        raise RuntimeError(f"No SQL migrations found in {migration_dir}")
    applied: list[str] = []
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                filename TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        for path in files:
            contents = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(contents.encode("utf-8")).hexdigest()
            cursor = await connection.execute(
                "SELECT checksum FROM schema_migrations WHERE filename = %s", (path.name,)
            )
            existing = await cursor.fetchone()
            if existing:
                if existing[0] != checksum:
                    raise RuntimeError(
                        f"Applied migration {path.name} was modified; add a new migration"
                    )
                continue
            LOGGER.info("Applying database migration", extra={"migration": path.name})
            await connection.execute(contents)
            await connection.execute(
                "INSERT INTO schema_migrations (filename, checksum) VALUES (%s, %s)",
                (path.name, checksum),
            )
            applied.append(path.name)
    return applied


async def verify_database_migrations(
    dsn: str, directory: Path | None = None
) -> dict[str, object]:
    """Verify migration state without creating or modifying database objects."""
    migration_dir = directory or migrations_directory()
    files = sorted(migration_dir.glob("*.sql"))
    expected = {
        path.name: hashlib.sha256(
            path.read_text(encoding="utf-8").encode("utf-8")
        ).hexdigest()
        for path in files
    }
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
        exists = await (
            await connection.execute(
                "SELECT to_regclass(current_schema() || '.schema_migrations')"
            )
        ).fetchone()
        if not exists or exists[0] is None:
            return {
                "current": False,
                "applied": [],
                "pending": list(expected),
                "checksum_mismatches": [],
            }
        rows = await (
            await connection.execute(
                "SELECT filename, checksum FROM schema_migrations ORDER BY filename"
            )
        ).fetchall()
    applied = {str(row[0]): str(row[1]) for row in rows}
    pending = [name for name in expected if name not in applied]
    mismatches = [
        name
        for name, checksum in expected.items()
        if name in applied and applied[name] != checksum
    ]
    return {
        "current": not pending and not mismatches,
        "applied": sorted(applied),
        "pending": pending,
        "checksum_mismatches": mismatches,
    }
