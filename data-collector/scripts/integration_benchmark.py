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
from math import ceil
from dataclasses import replace
from pathlib import Path
from decimal import Decimal
from statistics import fmean
from typing import Any

from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.common.types import MarketCandidate
from prediction_collector.common.utils import parse_timestamp
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.logging_config import ThroughputMetrics, configure_logging
from prediction_collector.main import _service, _tier_manager, _writer


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(len(ordered) * quantile) - 1))
    return ordered[index]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--full-l2", type=int, default=25)
    parser.add_argument("--sampled", type=int, default=100)
    parser.add_argument(
        "--after-ready",
        action="store_true",
        help="exclude initial discovery/archive burst from the measured window",
    )
    parser.add_argument(
        "--readiness-timeout-seconds",
        type=int,
        default=900,
        help="maximum wait for a complete uncapped metadata crawl",
    )
    parser.add_argument("--prefix", default="integration-benchmark")
    parser.add_argument(
        "--candidate-snapshot",
        type=Path,
        help="use a read-only public-universe JSONL snapshot for scale stages",
    )
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _memory_bytes() -> tuple[int | None, int | None]:
    if sys.platform != "win32":
        try:
            import resource

            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            peak = int(value * (1024 if sys.platform != "darwin" else 1))
            return None, peak
        except Exception:
            return None, None
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
            return int(counters.WorkingSetSize), int(counters.PeakWorkingSetSize)
    except Exception:
        return None, None
    return None, None


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
        table_stats = await (
            await connection.execute(
                """
                SELECT relname,
                       pg_total_relation_size(relid) AS total_bytes,
                       n_live_tup, n_dead_tup,
                       vacuum_count, autovacuum_count
                FROM pg_stat_user_tables
                WHERE relname IN (
                    'current_orderbooks', 'current_orderbook_levels',
                    'microstructure_observations', 'reference_price_updates',
                    'archive_objects', 'raw_rest_payloads'
                )
                ORDER BY relname
                """
            )
        ).fetchall()
        wal = await (
            await connection.execute(
                "SELECT pg_current_wal_lsn()::TEXT AS lsn"
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
                    (SELECT count(*) FROM data_gaps) AS data_gaps,
                    (SELECT count(*) FROM data_gaps WHERE status = 'open')
                        AS open_data_gaps,
                    (SELECT count(*) FROM collector_connections)
                        AS connections,
                    (SELECT count(*) FROM collector_connections
                       WHERE reconnect_attempt > 0) AS reconnects,
                    (SELECT count(*) FROM market_collection_tier_history)
                        AS tier_changes,
                    (SELECT COALESCE(sum(messages_received), 0)
                       FROM collector_connections) AS websocket_messages
                """
            )
        ).fetchone()
    return {
        "database_bytes": int(size["bytes"]),
        "write_activity": int(activity["rows"]),
        "wal_lsn": str(wal["lsn"]),
        "table_stats": {
            str(row["relname"]): {
                "total_bytes": int(row["total_bytes"] or 0),
                "live_tuples": int(row["n_live_tup"] or 0),
                "dead_tuples": int(row["n_dead_tup"] or 0),
                "vacuum_count": int(row["vacuum_count"] or 0),
                "autovacuum_count": int(row["autovacuum_count"] or 0),
            }
            for row in table_stats
        },
        "counts": {key: int(value or 0) for key, value in dict(counts).items()},
    }


async def _wal_bytes(database: Database, before: str, after: str) -> int:
    async with database.pool.connection() as connection:
        row = await (
            await connection.execute(
                "SELECT pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)::BIGINT AS bytes",
                (after, before),
            )
        ).fetchone()
    return int(row["bytes"] or 0)


async def _run(
    seconds: int, *, full_l2: int, sampled: int, prefix: str,
    after_ready: bool = False, readiness_timeout_seconds: int = 900,
    candidate_snapshot: Path | None = None,
) -> dict[str, Any]:
    settings = replace(
        Settings.from_env(),
        full_l2_max_markets=full_l2,
        sampled_max_markets=sampled,
        # Benchmarks must fill the requested ceilings rather than measuring a
        # score-policy subset. Production retains the conservative thresholds.
        full_l2_min_score=0,
        full_l2_min_liquidity=0,
        full_l2_min_recent_trades=0,
        full_l2_min_book_updates=0,
        sampled_promotion_score=0,
        sampled_demotion_score=0,
        full_l2_demotion_score=0,
        full_l2_research_reserve=min(5, full_l2),
        s3_prefix=f"{prefix.strip('/')}/{full_l2}-{sampled}-{int(time.time())}",
    )
    settings.require_archive()
    configure_logging(settings.log_level, json_logs=settings.json_logs)
    metrics = ThroughputMetrics()
    database = Database(settings, metrics)
    await database.migrate()
    await database.open()
    process_started_wall = time.perf_counter()
    process_started_cpu = time.process_time()
    before = await _database_sample(database)
    startup_seconds = 0.0
    startup_confirmed_subscriptions = 0
    archive_baseline: dict[str, Any] = {}
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
            if candidate_snapshot is not None:
                cached_candidates = await asyncio.to_thread(
                    _load_candidate_snapshot, candidate_snapshot
                )

                async def cached_discovery(**options: Any) -> list[MarketCandidate]:
                    service._live_persisted_ids.clear()
                    on_page = options.get("on_page")
                    if on_page is not None:
                        await on_page(cached_candidates[:1000])
                    return list(cached_candidates)

                service.discover_live = cached_discovery  # type: ignore[method-assign]
            collector = LiveCollector(
                settings=settings,
                database=database,
                writer=writer,
                metrics=metrics,
                polymarket_service=service,
                tier_manager=tier_manager,
            )

            sampling_stop = asyncio.Event()
            cpu_samples: list[float] = []
            rss_samples: list[int] = []
            queue_rows: list[int] = []
            queue_bytes: list[int] = []
            event_loop_lag: list[float] = []

            async def sample_resources() -> None:
                previous_wall = time.perf_counter()
                previous_cpu = time.process_time()
                interval = 0.25
                while not sampling_stop.is_set():
                    await asyncio.sleep(interval)
                    current_wall = time.perf_counter()
                    current_cpu = time.process_time()
                    elapsed_sample = max(current_wall - previous_wall, 0.001)
                    cpu_samples.append(
                        (current_cpu - previous_cpu) / elapsed_sample * 100
                    )
                    event_loop_lag.append(max(0.0, elapsed_sample - interval))
                    current_rss, _peak_rss = _memory_bytes()
                    if current_rss is not None:
                        rss_samples.append(current_rss)
                    if writer.archive is not None:
                        current = writer.archive.metrics()
                        queue_rows.append(int(current["queue_depth"]))
                        queue_bytes.append(int(current["queue_bytes"]))
                    previous_wall = current_wall
                    previous_cpu = current_cpu

            collector_task = asyncio.create_task(
                collector.run(), name="integration-live-collector"
            )
            if after_ready:
                ready_started = time.perf_counter()
                deadline = ready_started + readiness_timeout_seconds
                while collector.discovery_state != "ready":
                    if collector_task.done():
                        await collector_task
                        raise RuntimeError("collector stopped before discovery became ready")
                    if time.perf_counter() >= deadline:
                        collector.stop.set()
                        await collector_task
                        raise TimeoutError(
                            "live discovery did not become ready within "
                            f"{readiness_timeout_seconds}s"
                        )
                    await asyncio.sleep(0.25)
                warmup_deadline = min(deadline, time.perf_counter() + 20)
                previous_confirmed = -1
                stable_since = time.perf_counter()
                while True:
                    current_confirmed = await database.active_subscribed_market_count(
                        collector.run_id
                    )
                    if current_confirmed != previous_confirmed:
                        previous_confirmed = current_confirmed
                        stable_since = time.perf_counter()
                    startup_confirmed_subscriptions = current_confirmed
                    now = time.perf_counter()
                    if current_confirmed > 0 and now - stable_since >= 2:
                        break
                    if collector_task.done():
                        await collector_task
                        raise RuntimeError(
                            "collector stopped before subscriptions became ready"
                        )
                    if now >= warmup_deadline:
                        if current_confirmed > 0:
                            break
                        collector.stop.set()
                        await collector_task
                        raise TimeoutError(
                            "live subscriptions produced no confirmed initial books "
                            "during the 20-second warmup"
                        )
                    await asyncio.sleep(0.25)
                await writer.flush()
                if writer.archive is not None:
                    await writer.archive.flush()
                startup_seconds = time.perf_counter() - ready_started
                before = await _database_sample(database)
                archive_baseline = (
                    writer.archive.metrics() if writer.archive else {}
                )
                started_wall = time.perf_counter()
                started_cpu = time.process_time()
            else:
                started_wall = process_started_wall
                started_cpu = process_started_cpu

            sampler = asyncio.create_task(sample_resources())
            try:
                await asyncio.sleep(seconds)
                # End the rate denominator at the requested wall-clock
                # boundary. Shutdown then cancels producers and durably drains
                # both FIFO writers, but its variable latency is not live
                # acquisition time.
                measurement_ended_wall = time.perf_counter()
                measurement_ended_cpu = time.process_time()
                sampling_stop.set()
                await sampler

                if collector.run_id is not None:
                    confirmed = await database.active_subscribed_market_ids(
                        collector.run_id
                    )
                    collector.coverage.confirmed_subscribed = len(confirmed)
                coverage = collector.coverage.metrics()
                tiers = tier_manager.counts()
                collector.stop.set()
                await collector_task
                archive_total = writer.archive.metrics() if writer.archive else {}
                archive_metrics = _archive_delta(archive_total, archive_baseline)
                object_keys = (
                    await writer.archive.object_store.list_keys(settings.s3_prefix)
                    if writer.archive
                    else []
                )
                after = await _database_sample(database)
                wal_bytes = await _wal_bytes(
                    database, before["wal_lsn"], after["wal_lsn"]
                )
            finally:
                sampling_stop.set()
                if not collector_task.done():
                    collector.stop.set()
                await asyncio.gather(collector_task, sampler, return_exceptions=False)
    finally:
        await database.close()

    elapsed = max(measurement_ended_wall - started_wall, 0.001)
    cpu = measurement_ended_cpu - started_cpu
    database_growth = after["database_bytes"] - before["database_bytes"]
    write_activity = after["write_activity"] - before["write_activity"]
    return {
        "workload": {
            "seconds": round(elapsed, 3),
            "requested_collection_seconds": seconds,
            "startup_seconds_excluded": round(startup_seconds, 3),
            "startup_confirmed_subscriptions": startup_confirmed_subscriptions,
            "subscription_warmup_policy": "2s stable, 20s maximum",
            "measurement_after_discovery_ready": after_ready,
            "public_polymarket": True,
            "candidate_snapshot": str(candidate_snapshot) if candidate_snapshot else None,
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
            "wal_growth_bytes": wal_bytes,
            "wal_bytes_per_minute": round(wal_bytes * 60 / elapsed, 2),
            "table_stats": after["table_stats"],
            "counts": after["counts"],
            "count_deltas": {
                key: value - int(before["counts"].get(key, 0))
                for key, value in after["counts"].items()
            },
        },
        "archive": {
            **archive_metrics,
            "listed_parquet_objects_total_prefix": len(object_keys),
            "objects_per_hour_projected": round(
                int(archive_metrics.get("objects_uploaded") or 0) * 3600 / elapsed,
                2,
            ),
            "input_rows_per_minute": round(
                int(archive_metrics.get("rows_uploaded") or 0) * 60 / elapsed, 2
            ),
            "compressed_bytes_per_minute": round(
                int(archive_metrics.get("compressed_bytes_uploaded") or 0)
                * 60 / elapsed,
                2,
            ),
            "sample_object_keys": object_keys[:5],
        },
        "process": {
            "cpu_seconds": round(cpu, 3),
            "average_single_core_percent": round(cpu / elapsed * 100, 2),
            "interval_cpu_average_percent": round(fmean(cpu_samples), 2)
                if cpu_samples else None,
            "interval_cpu_peak_percent": round(max(cpu_samples), 2)
                if cpu_samples else None,
            "rss_average_bytes": round(fmean(rss_samples)) if rss_samples else None,
            "rss_peak_bytes": max(rss_samples) if rss_samples else _memory_bytes()[1],
            "event_loop_lag_average_ms": round(fmean(event_loop_lag) * 1000, 3)
                if event_loop_lag else None,
            "event_loop_lag_peak_ms": round(max(event_loop_lag) * 1000, 3)
                if event_loop_lag else None,
            "event_loop_lag_p99_ms": (
                round(value * 1000, 3)
                if (value := _percentile(event_loop_lag, 0.99)) is not None
                else None
            ),
            "archive_queue_average_rows": round(fmean(queue_rows), 2)
                if queue_rows else 0,
            "archive_queue_peak_rows": max(queue_rows) if queue_rows else 0,
            "archive_queue_average_bytes": round(fmean(queue_bytes), 2)
                if queue_bytes else 0,
            "archive_queue_peak_bytes": max(queue_bytes) if queue_bytes else 0,
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


def _archive_delta(
    current: dict[str, Any], baseline: dict[str, Any]
) -> dict[str, Any]:
    if not baseline:
        return current
    result = dict(current)
    for key in (
        "objects_uploaded", "rows_uploaded", "uncompressed_bytes_uploaded",
        "compressed_bytes_uploaded", "upload_failures", "raw_rest_objects_reused",
    ):
        result[key] = int(current.get(key) or 0) - int(baseline.get(key) or 0)
    streams: dict[str, Any] = {}
    for stream in set(current.get("streams", {})) | set(baseline.get("streams", {})):
        now = current.get("streams", {}).get(stream, {})
        before = baseline.get("streams", {}).get(stream, {})
        streams[stream] = {
            key: int(now.get(key) or 0) - int(before.get(key) or 0)
            for key in (
                "rows_total", "uncompressed_bytes_total", "compressed_bytes_total"
            )
        }
    result["streams"] = streams
    compressed = int(result.get("compressed_bytes_uploaded") or 0)
    result["compression_ratio"] = (
        int(result.get("uncompressed_bytes_uploaded") or 0) / compressed
        if compressed else None
    )
    return result


def _load_candidate_snapshot(path: Path) -> list[MarketCandidate]:
    candidates: list[MarketCandidate] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            legacy_raw = value.get("raw_data") or {}
            legacy_event = legacy_raw.get("event") or {}
            candidates.append(
                MarketCandidate(
                    exchange=str(value["exchange"]),
                    external_id=str(value["external_id"]),
                    ticker=value.get("ticker"),
                    status=value.get("status"),
                    active=bool(value.get("active")),
                    tradable=bool(value.get("tradable")),
                    closed=bool(value.get("closed")),
                    archived=bool(value.get("archived")),
                    accepting_orders=bool(value.get("accepting_orders")),
                    enable_order_book=bool(value.get("enable_order_book")),
                    has_maker_rewards=bool(value.get("has_maker_rewards")),
                    spread=Decimal(str(value["spread"])) if value.get("spread") else None,
                    close_time=parse_timestamp(value.get("close_time")),
                    volume=Decimal(str(value["volume"])) if value.get("volume") else None,
                    volume_24h=(
                        Decimal(str(value["volume_24h"]))
                        if value.get("volume_24h") else None
                    ),
                    liquidity=(
                        Decimal(str(value["liquidity"]))
                        if value.get("liquidity") else None
                    ),
                    outcome_token_ids=tuple(value.get("outcome_token_ids") or ()),
                    aliases=tuple(value.get("aliases") or ()),
                    source_id=(
                        str(value.get("source_id") or legacy_raw.get("gamma_id"))
                        if value.get("source_id") or legacy_raw.get("gamma_id")
                        else None
                    ),
                    event_external_id=(
                        str(
                            value.get("event_external_id")
                            or legacy_event.get("id")
                        )
                        if value.get("event_external_id") or legacy_event.get("id")
                        else None
                    ),
                )
            )
    return candidates


def main() -> None:
    args = _arguments()
    loop_factory = (
        (lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        if sys.platform == "win32"
        else None
    )
    result = asyncio.run(
        _run(
            args.seconds,
            full_l2=args.full_l2,
            sampled=args.sampled,
            prefix=args.prefix,
            after_ready=args.after_ready,
            readiness_timeout_seconds=args.readiness_timeout_seconds,
            candidate_snapshot=args.candidate_snapshot,
        ),
        loop_factory=loop_factory,
    )
    encoded = json.dumps(result, indent=2, default=str)
    if args.output:
        args.output.write_text(encoded + os.linesep, encoding="utf-8")
    print(encoded)


if __name__ == "__main__":
    main()
