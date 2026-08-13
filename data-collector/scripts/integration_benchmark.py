"""Run a bounded real PostgreSQL/S3/public-Polymarket integration benchmark.

This script never provisions or deletes infrastructure. Point it at disposable
services through DATABASE_URL and the S3_* variables, then run it for a short
period. The production CLI remains the supported operational entry point.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import selectors
import sys
import time
from pathlib import Path
from typing import Any

from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.logging_config import ThroughputMetrics, configure_logging
from prediction_collector.main import _service, _tier_manager, _writer


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _working_set_bytes() -> int | None:
    if sys.platform != "win32":
        try:
            import resource

            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return int(value * (1024 if sys.platform != "darwin" else 1))
        except Exception:
            return None
    try:
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.windll.kernel32
        psapi = ctypes.windll.psapi
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(Counters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        process = kernel32.GetCurrentProcess()
        if psapi.GetProcessMemoryInfo(
            process, ctypes.byref(counters), counters.cb
        ):
            return int(counters.PeakWorkingSetSize)
    except Exception:
        return None
    return None


async def _database_sample(database: Database) -> dict[str, Any]:
    async with database.pool.connection() as connection:
        size = await (
            await connection.execute(
                "SELECT pg_database_size(current_database()) AS bytes"
            )
        ).fetchone()
        activity = await (
            await connection.execute(
                """
                SELECT COALESCE(sum(n_tup_ins + n_tup_upd + n_tup_del), 0) AS rows
                FROM pg_stat_user_tables
                """
            )
        ).fetchone()
        counts = await (
            await connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM markets) AS markets,
                    (SELECT count(*) FROM outcomes) AS outcomes,
                    (SELECT count(*) FROM trades) AS trades,
                    (SELECT count(*) FROM current_orderbooks) AS current_orderbooks,
                    (SELECT count(*) FROM current_orderbook_levels) AS current_levels,
                    (SELECT count(*) FROM microstructure_observations) AS observations,
                    (SELECT count(*) FROM raw_rest_payloads) AS raw_rest_provenance,
                    (SELECT count(*) FROM archive_objects WHERE status = 'uploaded')
                        AS archive_objects,
                    (SELECT count(*) FROM collector_write_failures)
                        AS collector_write_failures,
                    (SELECT count(*) FROM archive_degradation_events)
                        AS archive_degradation_events,
                    (SELECT COALESCE(sum(messages_received), 0)
                       FROM collector_connections) AS websocket_messages
                """
            )
        ).fetchone()
    return {
        "database_bytes": int(size["bytes"]),
        "write_activity": int(activity["rows"]),
        "counts": {key: int(value or 0) for key, value in dict(counts).items()},
    }


async def _run(seconds: int) -> dict[str, Any]:
    settings = Settings.from_env()
    settings.require_archive()
    configure_logging(settings.log_level, json_logs=settings.json_logs)
    metrics = ThroughputMetrics()
    database = Database(settings, metrics)
    await database.migrate()
    await database.open()
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    before = await _database_sample(database)
    try:
        async with AsyncHttpClient(
            concurrency=settings.http_concurrency,
            timeout_seconds=settings.http_timeout_seconds,
            max_attempts=settings.http_max_attempts,
        ) as http:
            tier_manager = _tier_manager(settings)
            writer = _writer(database, settings, tier_manager)
            service = _service(
                settings=settings, database=database, writer=writer, http=http
            )
            collector = LiveCollector(
                settings=settings,
                database=database,
                writer=writer,
                metrics=metrics,
                polymarket_service=service,
                tier_manager=tier_manager,
            )

            async def stop_later() -> None:
                await asyncio.sleep(seconds)
                collector.stop.set()

            stopper = asyncio.create_task(stop_later())
            try:
                await collector.run()
            finally:
                stopper.cancel()
                await asyncio.gather(stopper, return_exceptions=True)
            archive_metrics = writer.archive.metrics() if writer.archive else {}
            object_keys = (
                await writer.archive.object_store.list_keys(settings.s3_prefix)
                if writer.archive
                else []
            )
            coverage = collector.coverage.metrics()
            tiers = tier_manager.counts()
        after = await _database_sample(database)
    finally:
        await database.close()

    elapsed = max(time.perf_counter() - started_wall, 0.001)
    cpu = time.process_time() - started_cpu
    database_growth = after["database_bytes"] - before["database_bytes"]
    write_activity = after["write_activity"] - before["write_activity"]
    return {
        "workload": {
            "seconds": round(elapsed, 3),
            "public_polymarket": True,
            "postgresql": "18",
            "s3_compatible": True,
        },
        "configuration": settings.safe_summary(),
        "coverage": coverage,
        "tiers": tiers,
        "postgres": {
            "bytes_before": before["database_bytes"],
            "bytes_after": after["database_bytes"],
            "growth_bytes": database_growth,
            "growth_bytes_per_minute": round(database_growth * 60 / elapsed, 2),
            "write_activity": write_activity,
            "writes_per_minute": round(write_activity * 60 / elapsed, 2),
            "counts": after["counts"],
            "count_deltas": {
                key: value - int(before["counts"].get(key, 0))
                for key, value in after["counts"].items()
            },
        },
        "archive": {
            **archive_metrics,
            "listed_parquet_objects": len(object_keys),
            "sample_object_keys": object_keys[:5],
        },
        "process": {
            "cpu_seconds": round(cpu, 3),
            "average_single_core_percent": round(cpu / elapsed * 100, 2),
            "peak_working_set_bytes": _working_set_bytes(),
        },
        "estimated_from_short_sample": {
            "postgres_gib_per_30_days": round(
                max(database_growth, 0) / elapsed * 86400 * 30 / 1024**3, 3
            ),
            "archive_gib_per_30_days": round(
                int(archive_metrics.get("compressed_bytes_uploaded") or 0)
                / elapsed
                * 86400
                * 30
                / 1024**3,
                3,
            ),
        },
    }


def main() -> None:
    args = _arguments()
    loop_factory = (
        (lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        if sys.platform == "win32"
        else None
    )
    result = asyncio.run(_run(args.seconds), loop_factory=loop_factory)
    encoded = json.dumps(result, indent=2, default=str)
    if args.output:
        args.output.write_text(encoded + os.linesep, encoding="utf-8")
    print(encoded)


if __name__ == "__main__":
    main()
