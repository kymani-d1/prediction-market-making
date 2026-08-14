"""Measure content-addressed raw REST reuse across two complete live crawls."""

from __future__ import annotations

import asyncio
import json
import selectors
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from prediction_collector.archive import ArchiveWriter
from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.polymarket.rest import PolymarketRestClient
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import BatchWriter


def _delta(after: dict[str, Any], before: dict[str, Any]) -> dict[str, int]:
    return {
        key: int(after.get(key) or 0) - int(before.get(key) or 0)
        for key in (
            "objects_uploaded",
            "rows_uploaded",
            "compressed_bytes_uploaded",
            "raw_rest_objects_reused",
        )
    }


async def _database_counts(database: Database) -> dict[str, int]:
    async with database.pool.connection() as connection:
        row = await (
            await connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM raw_rest_payloads) AS provenance_rows,
                    (SELECT count(*) FROM archive_objects
                     WHERE object_role = 'raw_rest_blob') AS raw_rest_objects
                """
            )
        ).fetchone()
    return {key: int(value) for key, value in dict(row).items()}


async def run() -> dict[str, Any]:
    settings = replace(
        Settings.from_env(),
        archive_compaction_enabled=False,
        s3_prefix=f"rest-dedup-benchmark/{int(time.time())}",
    )
    settings.require_archive()
    metrics = ThroughputMetrics()
    database = Database(settings, metrics)
    await database.open()
    archive = ArchiveWriter(settings, database)
    writer = BatchWriter(
        database,
        max_queue_size=settings.database_queue_size,
        batch_size=settings.database_batch_size,
        flush_interval_seconds=settings.database_flush_interval_seconds,
        archive=archive,
    )
    await writer.start()
    results: list[dict[str, Any]] = []
    try:
        async with AsyncHttpClient(
            concurrency=settings.http_concurrency,
            timeout_seconds=settings.http_timeout_seconds,
            max_attempts=settings.http_max_attempts,
        ) as http:
            service = PolymarketService(
                rest=PolymarketRestClient(
                    http,
                    gamma_url=settings.polymarket_gamma_url,
                    data_url=settings.polymarket_data_url,
                    clob_url=settings.polymarket_clob_url,
                ),
                database=database,
                writer=writer,
            )
            for crawl in (1, 2):
                before = archive.metrics()
                started = time.perf_counter()
                candidates = await service.discover_live(reconcile_absent=False)
                await archive.flush()
                after = archive.metrics()
                results.append(
                    {
                        "crawl": crawl,
                        "candidates": len(candidates),
                        "seconds": round(time.perf_counter() - started, 3),
                        **_delta(after, before),
                        **await _database_counts(database),
                    }
                )
    finally:
        await writer.stop()
        await database.close()
    return {"crawls": results}


def main() -> None:
    loop_factory = (
        (lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        if sys.platform == "win32"
        else None
    )
    result = asyncio.run(run(), loop_factory=loop_factory)
    output = Path(".integration-runtime/rest-dedup-final.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(result, indent=2)
    output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)


if __name__ == "__main__":
    main()
