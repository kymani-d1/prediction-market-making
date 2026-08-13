from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest

from prediction_collector.archive import (
    ArchiveRecord,
    ArchiveWriter,
    LocalObjectStore,
)
from prediction_collector.archive_reader import (
    archive_partition_prefixes,
    load_archive,
)
from prediction_collector.tiering import CollectionTier
from prediction_collector.writer import BatchWriter
from prediction_collector.common.retry import RetryPolicy
from prediction_collector.config import Settings


NOW = datetime(2026, 8, 13, 12, 34, 56, tzinfo=UTC)


class ArchiveDatabase:
    def __init__(self) -> None:
        self.objects: dict[int, dict[str, Any]] = {}
        self.by_hash: dict[str, int] = {}
        self.provenance: list[dict[str, Any]] = []
        self.degradations: list[dict[str, Any]] = []

    async def register_archive_object(self, **value: Any) -> int:
        digest = value["content_hash"]
        if digest in self.by_hash:
            return self.by_hash[digest]
        object_id = len(self.objects) + 1
        self.by_hash[digest] = object_id
        self.objects[object_id] = {"id": object_id, "status": "prepared", **value}
        return object_id

    async def mark_archive_upload_attempt(self, object_id: int, attempt: int) -> None:
        self.objects[object_id].update(status="uploading", upload_attempts=attempt)

    async def mark_archive_uploaded(self, object_id: int) -> None:
        self.objects[object_id]["status"] = "uploaded"

    async def mark_archive_retrying(self, object_id: int, error: str) -> None:
        self.objects[object_id].update(status="retrying", last_error=error)

    async def mark_archive_failed(self, object_id: int, error: str) -> None:
        self.objects[object_id].update(status="failed", last_error=error)

    async def archive_object_counts(self, object_id: int) -> dict[str, Any]:
        return self.objects[object_id]

    async def pending_archive_objects(self, *, limit: int) -> list[dict[str, Any]]:
        return [
            value
            for value in self.objects.values()
            if value["status"] in {"prepared", "retrying", "failed"}
            and value.get("local_spool_path")
        ][:limit]

    async def record_raw_rest_provenance(self, **value: Any) -> None:
        self.provenance.append(value)

    async def record_archive_degradation(self, **value: Any) -> None:
        self.degradations.append(value)


class FlakyStore(LocalObjectStore):
    def __init__(self, root: Path, failures: int) -> None:
        super().__init__(root)
        self.failures = failures
        self.attempts = 0

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise OSError("temporary object-store outage")
        await super().put_file(local_path, object_key, content_hash)


def archive_settings(workspace_tmp_path: Path, **changes: Any) -> Settings:
    base = Settings.from_env({}, load_dotenv_file=False)
    values = {
        "archive_spool_directory": workspace_tmp_path / "spool",
        "s3_prefix": "research",
        "archive_batch_rows": 2,
        "archive_batch_bytes": 1_000_000,
        "archive_flush_seconds": 0.05,
        "archive_upload_max_attempts": 3,
    }
    values.update(changes)
    return replace(base, **values)


def update_record(market: str, price: str, *, timestamp: datetime = NOW) -> ArchiveRecord:
    return ArchiveRecord.create(
        "orderbook_updates",
        {
            "exchange": "polymarket",
            "market_external_id": market,
            "outcome_external_id": f"{market}-yes",
            "connection_id": 1,
            "received_at": timestamp,
            "received_monotonic_ns": 123,
            "side": "buy",
            "price": price,
            "size": "10.125",
            "operation": "set",
            "event_type": "price_change",
        },
        priority=3,
    )


@pytest.mark.asyncio
async def test_row_threshold_flushes_readable_zstd_parquet(workspace_tmp_path: Path) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)
    await writer.start()
    await writer.put(update_record("market-a", "0.41"))
    await writer.put(update_record("market-b", "0.42"))
    await writer.queue.join()
    await writer.stop()

    keys = await store.list_keys("research/")
    assert len(keys) == 1
    assert "stream=orderbook_updates/date=2026-08-13/hour=12" in keys[0]
    path = store.root / keys[0]
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_rows == 2
    assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"
    table = load_archive([path], markets=["market-b"])
    assert table.column("market_external_id").to_pylist() == ["market-b"]
    assert table.column("price").to_pylist() == ["0.42"]
    manifest = next(iter(database.objects.values()))
    assert manifest["status"] == "uploaded"
    assert manifest["compressed_bytes"] > 0


@pytest.mark.asyncio
async def test_byte_and_interval_and_shutdown_flushes_are_bounded(workspace_tmp_path: Path) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=100,
        archive_batch_bytes=100,
        archive_flush_seconds=0.03,
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    await writer.put(update_record("byte-threshold", "0.43"))
    await writer.queue.join()
    # The first record exceeds the byte threshold and flushes without a second row.
    assert writer.counters.objects_uploaded == 1
    await writer.put(update_record("shutdown", "0.44", timestamp=NOW + timedelta(hours=1)))
    await writer.stop()
    assert writer.queue.empty()
    assert writer.counters.objects_uploaded == 2
    assert writer.counters.max_queue_rows <= settings.archive_queue_max_rows


@pytest.mark.asyncio
async def test_retry_is_idempotent_and_manifest_finishes_uploaded(
    workspace_tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = ArchiveDatabase()
    store = FlakyStore(workspace_tmp_path / "objects", failures=2)
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)
    writer._retry = RetryPolicy(3, 0.001, 0.001, 0)
    await writer.start()
    await writer.put(update_record("retry", "0.45"))
    await writer.stop()
    assert store.attempts == 3
    assert len(database.objects) == 1
    assert next(iter(database.objects.values()))["status"] == "uploaded"
    assert writer.counters.upload_failures == 2


@pytest.mark.asyncio
async def test_permanent_failure_spools_durably_and_restart_recovers(
    workspace_tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_upload_max_attempts=1,
        archive_queue_max_rows=2,
        archive_queue_max_bytes=10_000,
    )
    failing = FlakyStore(workspace_tmp_path / "objects", failures=100)
    first = ArchiveWriter(settings, database, object_store=failing)
    first._retry = RetryPolicy(1, 0, 0, 0)
    await first.start()
    await first.put(update_record("durable", "0.46"))
    await first.stop()
    manifest = next(iter(database.objects.values()))
    assert manifest["status"] == "retrying"
    assert Path(manifest["local_spool_path"]).is_file()
    assert first.degraded
    assert first.counters.max_queue_rows <= 2

    recovered = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await recovered.start()
    await recovered.stop()
    assert manifest["status"] == "uploaded"
    assert not Path(manifest["local_spool_path"]).exists()
    assert len(database.objects) == 1


@pytest.mark.asyncio
async def test_large_raw_rest_payload_is_archived_with_compact_provenance(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)
    payload = [{"id": index, "description": "x" * 2_000} for index in range(100)]
    await writer.start()
    await writer.put(
        ArchiveRecord.create(
            "raw_rest",
            {
                "source": "gamma",
                "endpoint": "/markets",
                "entity_type": "markets",
                "external_key": "page-1",
                "requested_at": NOW,
                "received_at": NOW,
                "parameters": {"limit": 100},
                "http_status": 200,
                "content_hash": "semantic-hash",
                "record_count": 100,
                "response_bytes": 200_000,
                "payload": payload,
            },
            priority=2,
        )
    )
    await writer.stop()
    assert len(database.provenance) == 1
    provenance = database.provenance[0]["value"]
    assert "payload" in provenance  # passed to DB method for column projection
    # The database method inserts an explicit compact column list and cannot persist payload.
    path = store.root / next(iter(await store.list_keys("research/")))
    restored = load_archive([path]).column("payload").to_pylist()[0]
    assert '"description"' in restored
    assert next(iter(database.objects.values()))["content_hash"]


def test_hour_partition_prefixes_cover_only_intersecting_hours() -> None:
    assert archive_partition_prefixes(
        prefix="research",
        stream="orderbook_updates",
        start=NOW,
        end=NOW + timedelta(hours=2),
    ) == [
        "research/schema_version=1/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=12/",
        "research/schema_version=1/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=13/",
        "research/schema_version=1/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=14/",
    ]
def test_errors_only_raw_policy_does_not_archive_known_rtds_transport_frames() -> None:
    writer = BatchWriter.__new__(BatchWriter)
    writer.archive = SimpleNamespace(settings=SimpleNamespace(raw_ws_policy="errors"))
    assert not writer._retain_raw_ws(
        {
            "channel": "rtds:crypto_prices_chainlink",
            "message_type": "update",
        },
        CollectionTier.METADATA_ONLY,
    )
    assert writer._retain_raw_ws(
        {"channel": "rtds:new_topic", "message_type": "update"},
        CollectionTier.METADATA_ONLY,
    )
