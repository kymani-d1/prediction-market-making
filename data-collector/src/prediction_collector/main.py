from __future__ import annotations

import argparse
import asyncio
import json
import logging
import selectors
import signal
import sys
from dataclasses import asdict
from typing import Any

from prediction_collector.archive import ArchiveWriter
from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.jobs.backfill import run_polymarket_backfill
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.logging_config import ThroughputMetrics, configure_logging
from prediction_collector.polymarket.rest import PolymarketRestClient
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.tiering import TierManager
from prediction_collector.writer import BatchWriter


LOGGER = logging.getLogger(__name__)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="python -m prediction_collector",
        description="Read-only Polymarket market-microstructure collector",
    )
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Apply pending plain-SQL database migrations")
    commands.add_parser(
        "backfill", help="Backfill public Polymarket metadata and historical data"
    )
    commands.add_parser("run", help="Run continuous live Polymarket collection")
    commands.add_parser(
        "status",
        help="Read-only database, tier, archive, and storage health report",
    )
    commands.add_parser(
        "smoke", help="Check current public Polymarket REST APIs without writes"
    )
    return root


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level, json_logs=settings.json_logs)
    try:
        loop_factory = (
            (lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
            if sys.platform == "win32"
            else None
        )
        exit_code = asyncio.run(_dispatch(args, settings), loop_factory=loop_factory)
        if exit_code:
            raise SystemExit(exit_code)
    except KeyboardInterrupt:
        LOGGER.info("Interrupted; shutdown complete")


async def _dispatch(args: argparse.Namespace, settings: Settings) -> int:
    metrics = ThroughputMetrics()
    database = Database(settings, metrics)
    if args.command == "migrate":
        applied = await database.migrate()
        LOGGER.info(
            "Database migrations complete",
            extra={"applied": applied, "applied_count": len(applied)},
        )
        return 0
    if args.command == "smoke":
        await _smoke(settings)
        return 0

    if args.command == "status":
        # Status is deliberately read-only. It reports pending/checksum drift and
        # never applies a migration as a side effect of a health check.
        migration_status = await database.verify_migrations()
        if not bool(migration_status.get("current")):
            print(
                json.dumps(
                    {
                        "scope": "polymarket_only",
                        "database_connected": True,
                        "migrations": migration_status,
                        "healthy": False,
                        "status_error": "pending or inconsistent migrations",
                    },
                    default=str,
                    indent=2,
                )
            )
            return 1
        await database.open()
        try:
            status = await database.status(migration_status=migration_status)
            print(json.dumps(status, default=str, indent=2))
            return 0 if status.get("healthy") else 1
        finally:
            await database.close()

    settings.require_archive()
    # Run/backfill startup owns migration responsibility. A status/health probe
    # cannot mutate schema.
    await database.migrate()
    await database.open()
    try:
        if args.command == "backfill":
            await _backfill(database, metrics, settings)
        elif args.command == "run":
            await _live(database, metrics, settings)
    finally:
        await database.close()
    return 0


def _tier_manager(settings: Settings) -> TierManager:
    return TierManager(
        full_l2_max_markets=settings.full_l2_max_markets,
        sampled_max_markets=settings.sampled_max_markets,
        full_l2_min_score=settings.full_l2_min_score,
        full_l2_min_liquidity=settings.full_l2_min_liquidity,
        full_l2_min_recent_trades=settings.full_l2_min_recent_trades,
        full_l2_min_book_updates=settings.full_l2_min_book_updates,
        sampled_promotion_score=settings.sampled_promotion_score,
        sampled_demotion_score=settings.sampled_demotion_score,
        full_l2_demotion_score=settings.full_l2_demotion_score,
        min_dwell_seconds=settings.tier_min_dwell_seconds,
        full_l2_research_reserve=settings.full_l2_research_reserve,
        allowlist=settings.full_l2_market_allowlist,
        blocklist=settings.live_market_blocklist,
        activity_window_seconds=settings.tier_activity_window_seconds,
    )


def _writer(
    database: Database,
    settings: Settings,
    tier_manager: TierManager,
) -> BatchWriter:
    archive = ArchiveWriter(settings, database)
    return BatchWriter(
        database,
        max_queue_size=settings.database_queue_size,
        batch_size=settings.database_batch_size,
        flush_interval_seconds=settings.database_flush_interval_seconds,
        archive=archive,
        tier_manager=tier_manager,
    )


def _service(
    *,
    settings: Settings,
    database: Database,
    writer: BatchWriter,
    http: AsyncHttpClient,
) -> PolymarketService:
    return PolymarketService(
        rest=PolymarketRestClient(
            http,
            gamma_url=settings.polymarket_gamma_url,
            data_url=settings.polymarket_data_url,
            clob_url=settings.polymarket_clob_url,
        ),
        database=database,
        writer=writer,
    )


async def _backfill(
    database: Database,
    metrics: ThroughputMetrics,
    settings: Settings,
) -> None:
    del metrics
    async with AsyncHttpClient(
        concurrency=settings.http_concurrency,
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
    ) as http:
        tier_manager = _tier_manager(settings)
        writer = _writer(database, settings, tier_manager)
        service = _service(
            settings=settings,
            database=database,
            writer=writer,
            http=http,
        )
        run_id = await database.start_run("backfill", "polymarket")
        writer.run_id = run_id
        try:
            result = await run_polymarket_backfill(service, writer)
            await database.finish_run(
                run_id,
                status=result.status,
                records_processed=result.records_processed,
                rows_written=result.rows_written,
            )
            LOGGER.info("Polymarket backfill complete", extra=asdict(result))
        except Exception as exc:
            await database.finish_run(
                run_id,
                status="failed",
                records_processed=0,
                rows_written=writer.rows_written,
                error_summary=f"{type(exc).__name__}: {exc}",
            )
            raise


async def _live(
    database: Database,
    metrics: ThroughputMetrics,
    settings: Settings,
) -> None:
    async with AsyncHttpClient(
        concurrency=settings.http_concurrency,
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
    ) as http:
        tier_manager = _tier_manager(settings)
        writer = _writer(database, settings, tier_manager)
        service = _service(
            settings=settings,
            database=database,
            writer=writer,
            http=http,
        )
        collector = LiveCollector(
            settings=settings,
            database=database,
            writer=writer,
            metrics=metrics,
            polymarket_service=service,
            tier_manager=tier_manager,
        )
        loop = asyncio.get_running_loop()

        def request_stop(*_: object) -> None:
            loop.call_soon_threadsafe(collector.stop.set)

        shutdown_signals = [signal.SIGINT, signal.SIGTERM]
        if hasattr(signal, "SIGBREAK"):
            shutdown_signals.append(signal.SIGBREAK)
        for sig in shutdown_signals:
            try:
                signal.signal(sig, request_stop)
            except (ValueError, OSError):
                pass
        await collector.run()


async def _smoke(settings: Settings) -> None:
    result: dict[str, Any] = {"scope": "polymarket_only"}
    async with AsyncHttpClient(
        concurrency=min(settings.http_concurrency, 4),
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=min(settings.http_max_attempts, 3),
    ) as http:
        client = PolymarketRestClient(
            http,
            gamma_url=settings.polymarket_gamma_url,
            data_url=settings.polymarket_data_url,
            clob_url=settings.polymarket_clob_url,
        )
        async for items, _, cursor in client.iter_live_events():
            nested_markets = sum(
                len(item.get("markets") or [])
                for item in items
                if item.get("active") and not item.get("closed")
            )
            result["polymarket"] = {
                "active_event_page_records": sum(
                    bool(item.get("active")) and not bool(item.get("closed"))
                    for item in items
                ),
                "nested_market_page_records": nested_markets,
                "has_next_cursor": bool(cursor),
            }
            break
    print(json.dumps(result, indent=2))
