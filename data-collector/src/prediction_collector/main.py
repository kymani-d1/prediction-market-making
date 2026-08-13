from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import selectors
import sys
from dataclasses import asdict
from typing import Any

from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.jobs.backfill import (
    run_kalshi_backfill,
    run_polymarket_backfill,
)
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.kalshi.rest import KalshiRestClient
from prediction_collector.kalshi.service import KalshiService
from prediction_collector.logging_config import ThroughputMetrics, configure_logging
from prediction_collector.polymarket.rest import PolymarketRestClient
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import BatchWriter


LOGGER = logging.getLogger(__name__)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="python -m prediction_collector",
        description="Read-only Polymarket and Kalshi research data collector",
    )
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Apply pending plain-SQL database migrations")

    backfill = commands.add_parser("backfill", help="Backfill public historical data")
    backfill.add_argument(
        "--exchange", choices=("polymarket", "kalshi", "all"), default="all"
    )

    run = commands.add_parser("run", help="Run continuous live collection")
    run.add_argument(
        "--exchange", choices=("polymarket", "kalshi", "all"), default="all"
    )

    commands.add_parser("status", help="Show database and collector health")

    smoke = commands.add_parser("smoke", help="Check current public REST APIs without writes")
    smoke.add_argument(
        "--exchange", choices=("polymarket", "kalshi", "all"), default="all"
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
        await _smoke(settings, args.exchange)
        return 0

    if args.command == "status":
        migration_status = await database.verify_migrations()
        if not bool(migration_status.get("current")):
            print(
                json.dumps(
                    {
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

    await database.migrate()
    await database.open()
    try:
        if args.command == "backfill":
            await _backfill(database, metrics, settings, args.exchange)
        elif args.command == "run":
            await _live(database, metrics, settings, args.exchange)
    finally:
        await database.close()
    return 0


def _writer(database: Database, settings: Settings) -> BatchWriter:
    return BatchWriter(
        database,
        max_queue_size=settings.database_queue_size,
        batch_size=settings.database_batch_size,
        flush_interval_seconds=settings.database_flush_interval_seconds,
    )


def _services(
    *,
    settings: Settings,
    database: Database,
    writer: BatchWriter,
    http: AsyncHttpClient,
    exchange: str,
) -> tuple[PolymarketService | None, KalshiService | None]:
    polymarket: PolymarketService | None = None
    kalshi: KalshiService | None = None
    if settings.polymarket_enabled and exchange in {"polymarket", "all"}:
        polymarket = PolymarketService(
            rest=PolymarketRestClient(
                http,
                gamma_url=settings.polymarket_gamma_url,
                data_url=settings.polymarket_data_url,
                clob_url=settings.polymarket_clob_url,
            ),
            database=database,
            writer=writer,
            store_raw_rest=settings.store_raw_rest,
        )
    if settings.kalshi_enabled and exchange in {"kalshi", "all"}:
        kalshi = KalshiService(
            rest=KalshiRestClient(http, base_url=settings.kalshi_api_url),
            database=database,
            writer=writer,
            store_raw_rest=settings.store_raw_rest,
        )
    if polymarket is None and kalshi is None:
        raise RuntimeError("No requested exchange is enabled")
    return polymarket, kalshi


async def _backfill(
    database: Database,
    metrics: ThroughputMetrics,
    settings: Settings,
    exchange: str,
) -> None:
    async with AsyncHttpClient(
        concurrency=settings.http_concurrency,
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
    ) as http:
        exchanges = (
            ["polymarket", "kalshi"] if exchange == "all" else [exchange]
        )
        for selected in exchanges:
            if selected == "polymarket" and not settings.polymarket_enabled:
                continue
            if selected == "kalshi" and not settings.kalshi_enabled:
                continue
            writer = _writer(database, settings)
            polymarket, kalshi = _services(
                settings=settings,
                database=database,
                writer=writer,
                http=http,
                exchange=selected,
            )
            run_id = await database.start_run("backfill", selected)
            writer.run_id = run_id
            try:
                result = (
                    await run_polymarket_backfill(polymarket, writer)
                    if polymarket is not None
                    else await run_kalshi_backfill(kalshi, writer)  # type: ignore[arg-type]
                )
                await database.finish_run(
                    run_id,
                    status=result.status,
                    records_processed=result.records_processed,
                    rows_written=result.rows_written,
                )
                LOGGER.info(
                    "Backfill complete",
                    extra={"exchange": selected, **asdict(result)},
                )
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
    exchange: str,
) -> None:
    async with AsyncHttpClient(
        concurrency=settings.http_concurrency,
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
    ) as http:
        writer = _writer(database, settings)
        polymarket, kalshi = _services(
            settings=settings,
            database=database,
            writer=writer,
            http=http,
            exchange=exchange,
        )
        collector = LiveCollector(
            settings=settings,
            database=database,
            writer=writer,
            metrics=metrics,
            polymarket_service=polymarket,
            kalshi_service=kalshi,
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


async def _smoke(settings: Settings, exchange: str) -> None:
    result: dict[str, Any] = {}
    async with AsyncHttpClient(
        concurrency=min(settings.http_concurrency, 4),
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=min(settings.http_max_attempts, 3),
    ) as http:
        if exchange in {"polymarket", "all"}:
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
        if exchange in {"kalshi", "all"}:
            client = KalshiRestClient(http, base_url=settings.kalshi_api_url)
            async for items, _, cursor in client.iter_markets(status="open"):
                result["kalshi"] = {
                    "market_page_records": len(items),
                    "has_next_cursor": bool(cursor),
                }
                break
    print(json.dumps(result, indent=2))
