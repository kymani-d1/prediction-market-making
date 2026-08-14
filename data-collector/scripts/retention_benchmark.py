"""Accelerated PostgreSQL hot-retention and bloat benchmark.

Run only against a disposable migrated database containing at least one market
and outcome. The script intentionally creates and ages synthetic hot rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import selectors
import sys
from pathlib import Path
from typing import Any

from psycopg import AsyncConnection

from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.logging_config import ThroughputMetrics


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=4)
    parser.add_argument("--rows-per-cycle", type=int, default=50_000)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


async def sample(database: Database) -> dict[str, Any]:
    async with database.pool.connection() as connection:
        database_size = await (
            await connection.execute(
                "SELECT pg_database_size(current_database()) AS bytes, "
                "pg_current_wal_lsn()::TEXT AS lsn"
            )
        ).fetchone()
        rows = await (
            await connection.execute(
                """
                SELECT relname, pg_relation_size(relid) AS heap_bytes,
                       pg_indexes_size(relid) AS index_bytes,
                       n_live_tup, n_dead_tup, vacuum_count, autovacuum_count,
                       last_vacuum, last_autovacuum
                FROM pg_stat_user_tables
                WHERE relname IN (
                    'reference_price_updates', 'microstructure_observations'
                )
                ORDER BY relname
                """
            )
        ).fetchall()
    return {
        "database_bytes": int(database_size["bytes"]),
        "wal_lsn": str(database_size["lsn"]),
        "tables": {str(row["relname"]): dict(row) for row in rows},
    }


async def wal_bytes(database: Database, before: str, after: str) -> int:
    async with database.pool.connection() as connection:
        row = await (
            await connection.execute(
                "SELECT pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)::BIGINT AS bytes",
                (after, before),
            )
        ).fetchone()
    return int(row["bytes"] or 0)


async def insert_cycle(database: Database, rows: int, cycle: int) -> None:
    async with database.pool.connection() as connection:
        identity = await (
            await connection.execute(
                """
                SELECT market.id AS market_id, outcome.id AS outcome_id
                FROM markets market
                JOIN outcomes outcome ON outcome.market_id = market.id
                WHERE market.exchange = 'polymarket'
                ORDER BY market.id, outcome.id LIMIT 1
                """
            )
        ).fetchone()
        if identity is None:
            raise RuntimeError("retention benchmark requires one discovered market/outcome")
        # Age the prior cycle beyond both configured windows. This models time
        # advancing while keeping the benchmark short enough for CI/dev use.
        await connection.execute(
            "UPDATE reference_price_updates "
            "SET received_at = received_at - INTERVAL '48 hours'"
        )
        await connection.execute(
            "UPDATE microstructure_observations "
            "SET observed_at = observed_at - INTERVAL '48 hours', "
            "received_at = received_at - INTERVAL '48 hours'"
        )
        await connection.execute(
            """
            INSERT INTO reference_price_updates
                (delivery_exchange, provider, external_instrument_id,
                 received_at, received_monotonic_ns, price, source_status,
                 raw_data)
            SELECT 'polymarket', 'retention_benchmark',
                   'cycle-' || %s::TEXT,
                   clock_timestamp() - (series %% 3600) * INTERVAL '1 second',
                   (%s::BIGINT * 1000000000) + series,
                   100 + (series %% 1000)::NUMERIC / 100,
                   'synthetic', '{}'::JSONB
            FROM generate_series(1, %s) AS series
            """,
            (cycle, cycle, rows),
        )
        await connection.execute(
            """
            INSERT INTO microstructure_observations
                (market_id, outcome_id, tier, observed_at, received_at,
                 best_bid, best_ask, bid_depth_total, ask_depth_total,
                 observation_kind)
            SELECT %s, %s, 'full_l2',
                   clock_timestamp() - series * INTERVAL '1 microsecond'
                       - %s * INTERVAL '1 second',
                   clock_timestamp() - series * INTERVAL '1 microsecond'
                       - %s * INTERVAL '1 second',
                   0.40, 0.60, 10, 10, 'periodic_hot'
            FROM generate_series(1, %s) AS series
            """,
            (identity["market_id"], identity["outcome_id"], cycle, cycle, rows),
        )


async def run(cycles: int, rows_per_cycle: int) -> dict[str, Any]:
    settings = Settings.from_env()
    database = Database(settings, ThroughputMetrics())
    await database.open()
    initial = await sample(database)
    cycle_results: list[dict[str, Any]] = []
    try:
        for cycle in range(cycles):
            await insert_cycle(database, rows_per_cycle, cycle + 1)
            before_retention = await sample(database)
            deleted = await database.apply_retention()
            after_retention = await sample(database)
            cycle_results.append(
                {
                    "cycle": cycle + 1,
                    "rows_inserted_per_table": rows_per_cycle,
                    "deleted": deleted,
                    "before_retention": before_retention,
                    "after_retention": after_retention,
                }
            )
        before_vacuum = await sample(database)
    finally:
        await database.close()

    connection = await AsyncConnection.connect(settings.database_dsn, autocommit=True)
    try:
        await connection.execute("VACUUM (ANALYZE) reference_price_updates")
        await connection.execute("VACUUM (ANALYZE) microstructure_observations")
    finally:
        await connection.close()

    verified = Database(settings, ThroughputMetrics())
    await verified.open()
    try:
        after_vacuum = await sample(verified)
        await insert_cycle(verified, rows_per_cycle, cycles + 1)
        reuse_before_retention = await sample(verified)
        reuse_deleted = await verified.apply_retention()
        reuse_after_retention = await sample(verified)
    finally:
        await verified.close()

    connection = await AsyncConnection.connect(settings.database_dsn, autocommit=True)
    try:
        await connection.execute("VACUUM (ANALYZE) reference_price_updates")
        await connection.execute("VACUUM (ANALYZE) microstructure_observations")
    finally:
        await connection.close()

    final_database = Database(settings, ThroughputMetrics())
    await final_database.open()
    try:
        after_reuse_vacuum = await sample(final_database)
        total_wal = await wal_bytes(
            final_database, initial["wal_lsn"], after_reuse_vacuum["wal_lsn"]
        )
    finally:
        await final_database.close()
    return {
        "cycles": cycles,
        "rows_per_cycle_per_table": rows_per_cycle,
        "initial": initial,
        "cycle_results": cycle_results,
        "before_vacuum": before_vacuum,
        "after_vacuum": after_vacuum,
        "reuse_cycle": {
            "rows_inserted_per_table": rows_per_cycle,
            "deleted": reuse_deleted,
            "before_retention": reuse_before_retention,
            "after_retention": reuse_after_retention,
            "after_vacuum": after_reuse_vacuum,
        },
        "wal_bytes": total_wal,
    }


def main() -> None:
    args = arguments()
    loop_factory = (
        (lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        if sys.platform == "win32"
        else None
    )
    result = asyncio.run(
        run(args.cycles, args.rows_per_cycle), loop_factory=loop_factory
    )
    encoded = json.dumps(result, indent=2, default=str)
    if args.output:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)


if __name__ == "__main__":
    main()
