from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest

from prediction_collector.archive import (
    ArchiveRecord,
    ArchiveWriter,
    LocalObjectStore,
    RAW_REST_FRESH_ADMISSION_BURST,
    _directory_size,
    stable_archive_key,
)
from prediction_collector.archive_reader import (
    archive_partition_prefixes,
    load_archive,
)
from prediction_collector.database import Database
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.tiering import CollectionTier
from prediction_collector.writer import BatchWriter
from prediction_collector.common.retry import RetryPolicy
from prediction_collector.config import Settings


NOW = datetime(2026, 8, 13, 12, 34, 56, tzinfo=UTC)


class ArchiveDatabase:
    def __init__(self, *, emit_dictionary: bool = False) -> None:
        self.objects: dict[int, dict[str, Any]] = {}
        self.by_hash: dict[str, int] = {}
        self.provenance: list[dict[str, Any]] = []
        self.degradations: list[dict[str, Any]] = []
        self.identifiers: dict[tuple[str, int], str] = {}
        self.dictionary_emitted: set[tuple[str, int]] = set()
        self.emit_dictionary = emit_dictionary
        self.compactions: dict[int, dict[str, Any]] = {}
        self.transient_resolutions = 0

    async def register_archive_object(self, **value: Any) -> int:
        digest = value["content_hash"]
        if digest in self.by_hash:
            return self.by_hash[digest]
        object_id = len(self.objects) + 1
        self.by_hash[digest] = object_id
        self.objects[object_id] = {"id": object_id, "status": "prepared", **value}
        return object_id

    async def mark_archive_upload_attempt(self, object_id: int, attempt: int) -> None:
        if self.objects[object_id]["status"] != "uploaded":
            self.objects[object_id].update(status="uploading", upload_attempts=attempt)

    async def archive_object_state(self, object_id: int) -> dict[str, Any]:
        return self.objects[object_id]

    async def archive_object_by_content_hash(
        self, digest: str
    ) -> dict[str, Any] | None:
        object_id = self.by_hash.get(digest)
        return self.objects.get(object_id) if object_id is not None else None

    async def mark_archive_uploaded(self, object_id: int) -> None:
        self.objects[object_id].update(
            status="uploaded", local_spool_path=None, last_error=None
        )

    async def mark_archive_retrying(self, object_id: int, error: str) -> None:
        if self.objects[object_id]["status"] != "uploaded":
            self.objects[object_id].update(status="retrying", last_error=error)

    async def mark_archive_failed(self, object_id: int, error: str) -> None:
        if self.objects[object_id]["status"] != "uploaded":
            self.objects[object_id].update(status="failed", last_error=error)

    async def archive_object_counts(self, object_id: int) -> dict[str, Any]:
        return self.objects[object_id]

    async def pending_archive_objects(
        self,
        *,
        limit: int,
        local_content_hashes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        local_hashes = set(local_content_hashes or [])
        values = [
            value
            for value in self.objects.values()
            if value["status"] != "uploaded"
            and value.get("local_spool_path")
        ]
        values.sort(
            key=lambda value: (value["content_hash"] not in local_hashes, value["id"])
        )
        return values[:limit]

    async def record_raw_rest_provenance(self, **value: Any) -> None:
        self.provenance.append(value)

    async def record_archive_degradation(self, **value: Any) -> None:
        self.degradations.append({**value, "resolved": False})

    async def resolve_archive_record_degradations(
        self, record_ids: list[str]
    ) -> None:
        identities = set(record_ids)
        for event in self.degradations:
            if (event.get("details") or {}).get("record_id") in identities:
                event["resolved"] = True

    async def resolve_transient_archive_degradations(
        self, *, run_id: int | None
    ) -> None:
        self.transient_resolutions += 1
        for event in self.degradations:
            if event.get("run_id") == run_id:
                event["resolved"] = True

    async def raw_rest_archive_by_content_hash(
        self, content_hash: str
    ) -> dict[str, Any] | None:
        return next(
            (
                value for value in self.objects.values()
                if value.get("payload_content_hash") == content_hash
                and value["status"] == "uploaded"
            ),
            None,
        )

    async def archive_compaction_candidates(self, **_: Any) -> list[dict[str, Any]]:
        return [
            value for value in self.objects.values()
            if value["status"] == "uploaded"
            and value.get("superseded_at") is None
            and value["stream"] == "orderbook_updates"
        ]

    async def begin_archive_compaction(
        self, candidates: list[dict[str, Any]]
    ) -> int:
        compaction_id = len(self.compactions) + 1
        self.compactions[compaction_id] = {
            "id": compaction_id,
            "status": "running",
            "source_object_ids": [value["id"] for value in candidates],
            "replacement_object_id": None,
        }
        return compaction_id

    async def set_archive_compaction_replacement(
        self, compaction_id: int, object_id: int
    ) -> None:
        self.compactions[compaction_id]["replacement_object_id"] = object_id

    async def complete_archive_compaction(
        self, compaction_id: int, replacement_id: int, source_ids: list[int]
    ) -> None:
        self.objects[replacement_id]["status"] = "uploaded"
        for source_id in source_ids:
            self.objects[source_id]["superseded_at"] = NOW
            self.objects[source_id]["superseded_by_object_id"] = replacement_id
        self.compactions[compaction_id]["status"] = "completed"

    async def fail_archive_compaction(self, compaction_id: int, error: str) -> None:
        self.compactions[compaction_id].update(status="failed", error=error)

    async def abandon_archive_object(self, object_id: int, error: str) -> None:
        if self.objects[object_id]["status"] != "uploaded":
            self.objects[object_id].update(
                status="failed", local_spool_path=None, last_error=error
            )

    async def running_archive_compactions(self) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        for compaction in self.compactions.values():
            if compaction["status"] != "running":
                continue
            replacement_id = compaction["replacement_object_id"]
            values.append(
                {
                    **compaction,
                    "replacement_status": (
                        self.objects[replacement_id]["status"]
                        if replacement_id is not None else None
                    ),
                    "source_object_keys": [
                        self.objects[source_id]["object_key"]
                        for source_id in compaction["source_object_ids"]
                    ],
                }
            )
        return values

    async def ensure_archive_identifier(self, **value: Any) -> bool:
        identity = (value["entity_kind"], value["archive_key"])
        previous = self.identifiers.setdefault(identity, value["external_id"])
        assert previous == value["external_id"]
        created = self.emit_dictionary and identity not in self.dictionary_emitted
        self.dictionary_emitted.add(identity)
        return created


class LiveRunArchiveDatabase(ArchiveDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.finished_runs: list[dict[str, Any]] = []

    async def start_run(self, *_: Any) -> int:
        return 1

    async def finish_run(self, run_id: int, **value: Any) -> None:
        self.finished_runs.append({"run_id": run_id, **value})


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


class SlowStore(LocalObjectStore):
    def __init__(self, root: Path, delay_seconds: float) -> None:
        super().__init__(root)
        self.delay_seconds = delay_seconds

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        await asyncio.sleep(self.delay_seconds)
        await super().put_file(local_path, object_key, content_hash)


class SlowRawRestStore(LocalObjectStore):
    def __init__(self, root: Path, delay_seconds: float) -> None:
        super().__init__(root)
        self.delay_seconds = delay_seconds

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        if "stream=raw_rest" in object_key:
            await asyncio.sleep(self.delay_seconds)
        await super().put_file(local_path, object_key, content_hash)


class GatedStore(LocalObjectStore):
    def __init__(self, root: Path, blocked_stream: str) -> None:
        super().__init__(root)
        self.blocked_stream = blocked_stream
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.attempted_keys: list[str] = []

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        self.attempted_keys.append(object_key)
        if f"stream={self.blocked_stream}" in object_key:
            self.started.set()
            await self.release.wait()
        await super().put_file(local_path, object_key, content_hash)


class DualGatedStore(LocalObjectStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.started = {
            "orderbook_updates": asyncio.Event(),
            "raw_rest": asyncio.Event(),
        }
        self.release = asyncio.Event()
        self.attempted_keys: list[str] = []

    async def put_file(
        self, local_path: Path, object_key: str, content_hash: str
    ) -> None:
        self.attempted_keys.append(object_key)
        for stream, event in self.started.items():
            if f"stream={stream}" in object_key:
                event.set()
                await self.release.wait()
                break
        await super().put_file(local_path, object_key, content_hash)


class CoordinatedArchiveDatabase(ArchiveDatabase):
    """Return one stale pending snapshot while a second writer commits it."""

    def __init__(self) -> None:
        super().__init__()
        self.pending_read = asyncio.Event()
        self.release_pending = asyncio.Event()

    async def pending_archive_objects(
        self,
        *,
        limit: int,
        local_content_hashes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        values = [
            dict(value)
            for value in await super().pending_archive_objects(
                limit=limit,
                local_content_hashes=local_content_hashes,
            )
        ]
        self.pending_read.set()
        await self.release_pending.wait()
        return values


class RecordingArchiveConnection:
    def __init__(self) -> None:
        self.query = ""

    async def execute(self, query: str, _: tuple[Any, ...]) -> SimpleNamespace:
        self.query = " ".join(query.split())
        return SimpleNamespace(rowcount=0)


class RecordingArchiveConnectionContext:
    def __init__(self, connection: RecordingArchiveConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> RecordingArchiveConnection:
        return self.connection

    async def __aexit__(self, *_: Any) -> None:
        return None


class RecordingArchivePool:
    def __init__(self, connection: RecordingArchiveConnection) -> None:
        self.connection_value = connection

    def connection(self) -> RecordingArchiveConnectionContext:
        return RecordingArchiveConnectionContext(self.connection_value)


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


@pytest.mark.asyncio
async def test_journal_metrics_report_lock_append_and_fsync_by_stream(
    workspace_tmp_path: Path,
) -> None:
    settings = archive_settings(workspace_tmp_path)
    settings.archive_spool_directory.mkdir(parents=True)
    writer = ArchiveWriter(
        settings,
        ArchiveDatabase(),
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )

    await writer._append_journal(update_record("journal-metrics", "0.42"))

    metrics = writer.metrics()
    append = metrics["journal_append"]["orderbook_updates"]
    assert append["count"] == 1
    assert append["fsync_seconds_total"] >= 0
    assert append["total_seconds_max"] >= append["fsync_seconds_total"]
    lock = metrics["journal_lock"]
    assert lock["locked"] is False
    assert lock["owner"] is None
    assert lock["stages"]["append"]["count"] == 1


def add_archive_manifest(
    database: ArchiveDatabase,
    workspace_tmp_path: Path,
    payload: bytes,
    *,
    status: str = "retrying",
    local_file: bool = False,
    object_key: str = "research/recovery/object.parquet",
) -> tuple[dict[str, Any], Path]:
    digest = hashlib.sha256(payload).hexdigest()
    local_path = workspace_tmp_path / "spool" / f"{digest}.parquet"
    if local_file:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(payload)
    manifest = {
        "id": 1,
        "status": status,
        "stream": "orderbook_updates",
        "object_key": object_key,
        "content_hash": digest,
        "compressed_bytes": len(payload),
        "uncompressed_bytes": len(payload),
        "row_count": 1,
        "local_spool_path": str(local_path),
        "last_error": "previous upload interruption",
    }
    database.objects[1] = manifest
    database.by_hash[digest] = 1
    return manifest, local_path


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


def raw_rest_record(identity: str, *, payload_size: int = 0) -> ArchiveRecord:
    return ArchiveRecord.create(
        "raw_rest",
        {
            "source": "gamma",
            "endpoint": "/markets",
            "entity_type": "markets",
            "external_key": identity,
            "requested_at": NOW,
            "received_at": NOW,
            "parameters": {"limit": 100},
            "http_status": 200,
            "content_hash": f"raw-rest-{identity}",
            "record_count": 1,
            "response_bytes": payload_size,
            "payload": [{"id": identity, "padding": "x" * payload_size}],
        },
        priority=2,
    )


def reference_record(identity: str, price: str) -> ArchiveRecord:
    return ArchiveRecord.create(
        "reference_prices",
        {
            "provider": "chainlink_spot",
            "external_instrument_id": identity,
            "external_update_id": f"{identity}-{price}",
            "connection_id": 2,
            "source_timestamp": NOW,
            "exchange_timestamp": NOW,
            "received_at": NOW,
            "received_monotonic_ns": 456,
            "price": price,
            "source_status": "live",
        },
        priority=2,
    )


def observation_record(identity: str, price: str) -> ArchiveRecord:
    return ArchiveRecord.create(
        "microstructure_observations",
        {
            "market_external_id": identity,
            "outcome_external_id": f"{identity}-yes",
            "observed_at": NOW,
            "observation_kind": "change",
            "best_bid": price,
            "best_ask": str(Decimal(price) + Decimal("0.01")),
            "bid_depth_total": "100",
            "ask_depth_total": "100",
            "recent_trade_count": 1,
            "recent_update_count": 2,
        },
        priority=5,
    )


@pytest.mark.asyncio
async def test_row_threshold_flushes_readable_zstd_parquet(workspace_tmp_path: Path) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)
    await writer.start()
    await writer.put(update_record("market-a", "0.41"))
    await writer.put(update_record("market-b", "0.42"))
    await writer.join()
    await writer.stop()

    keys = await store.list_keys("research/")
    update_keys = [key for key in keys if "stream=orderbook_updates" in key]
    assert len(update_keys) == 1
    assert "stream=orderbook_updates/date=2026-08-13/hour=12" in update_keys[0]
    path = store.root / update_keys[0]
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_rows == 2
    assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"
    table = load_archive([path])
    assert table.column("price_mantissa").to_pylist() == [41, 42]
    assert table.column("price_scale").to_pylist() == [2, 2]
    manifest = next(
        value for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
    )
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
    await writer.join()
    # The first record exceeds the byte threshold and flushes without a second row.
    assert writer.counters.stream_rows["orderbook_updates"] == 1
    await writer.put(update_record("shutdown", "0.44", timestamp=NOW + timedelta(hours=1)))
    await writer.stop()
    assert writer.queue.empty()
    assert writer.counters.stream_rows["orderbook_updates"] == 2
    assert writer.counters.max_queue_rows <= settings.archive_queue_max_rows


@pytest.mark.asyncio
async def test_explicit_flush_is_bounded_and_writer_remains_usable(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=100,
        archive_batch_bytes=1_000_000,
        archive_flush_seconds=300,
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    await writer.put(update_record("forced-flush", "0.43"))
    await asyncio.wait_for(writer.flush(), timeout=2)
    assert writer.queue.empty()
    assert writer.counters.stream_rows["orderbook_updates"] == 1

    # A benchmark boundary is not a shutdown boundary. Records arriving after
    # the flush must still be durable and drain during normal stop.
    await writer.put(
        update_record("after-flush", "0.44", timestamp=NOW + timedelta(hours=1))
    )
    await asyncio.wait_for(writer.stop(), timeout=2)
    assert writer.queue.empty()
    assert writer.counters.stream_rows["orderbook_updates"] == 2


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
    updates = [
        value for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
    ]
    assert len(updates) == 1
    assert updates[0]["status"] == "uploaded"
    assert writer.counters.upload_failures == 2


@pytest.mark.asyncio
async def test_uploaded_manifest_cannot_be_downgraded_to_failed() -> None:
    database = ArchiveDatabase()
    database.objects[1] = {
        "id": 1,
        "status": "uploaded",
        "local_spool_path": None,
        "last_error": None,
    }

    await database.mark_archive_failed(1, "spool file is missing")
    await database.mark_archive_retrying(1, "late retry")
    await database.mark_archive_upload_attempt(1, 99)
    await database.abandon_archive_object(1, "late abandonment")
    assert database.objects[1] == {
        "id": 1,
        "status": "uploaded",
        "local_spool_path": None,
        "last_error": None,
    }

    connection = RecordingArchiveConnection()
    real_database = Database(Settings())
    real_database.pool = RecordingArchivePool(connection)  # type: ignore[assignment]
    await real_database.mark_archive_failed(1, "stale retry")
    assert "WHERE id = %s AND status <> 'uploaded'" in connection.query


@pytest.mark.asyncio
async def test_missing_local_spool_reconciles_verified_remote_object(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    payload = b"verified immutable parquet bytes"
    manifest, _ = add_archive_manifest(
        database, workspace_tmp_path, payload, status="failed"
    )
    store = LocalObjectStore(workspace_tmp_path / "objects")
    source = workspace_tmp_path / "remote-source.parquet"
    source.write_bytes(payload)
    await store.put_file(source, manifest["object_key"], manifest["content_hash"])
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)

    await writer._retry_pending_once()

    assert manifest["status"] == "uploaded"
    assert manifest["local_spool_path"] is None
    assert manifest["last_error"] is None


@pytest.mark.asyncio
async def test_missing_local_spool_wrong_remote_size_stays_unresolved(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    manifest, _ = add_archive_manifest(database, workspace_tmp_path, b"expected")
    store = LocalObjectStore(workspace_tmp_path / "objects")
    remote = store.root / manifest["object_key"]
    remote.parent.mkdir(parents=True, exist_ok=True)
    remote.write_bytes(b"wrong-size")
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)

    await writer._retry_pending_once()

    assert manifest["status"] == "retrying"
    assert manifest["last_error"] == "previous upload interruption"
    assert "size mismatch" in writer._remote_recovery_errors[1]


@pytest.mark.asyncio
async def test_missing_local_spool_wrong_remote_sha256_stays_unresolved(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    manifest, _ = add_archive_manifest(database, workspace_tmp_path, b"expected")
    store = LocalObjectStore(workspace_tmp_path / "objects")
    remote = store.root / manifest["object_key"]
    remote.parent.mkdir(parents=True, exist_ok=True)
    remote.write_bytes(b"tampered")
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)

    await writer._retry_pending_once()

    assert manifest["status"] == "retrying"
    assert manifest["last_error"] == "previous upload interruption"
    assert "sha256" in writer._remote_recovery_errors[1]


@pytest.mark.asyncio
async def test_missing_local_spool_and_remote_object_stays_visibly_unresolved(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    manifest, _ = add_archive_manifest(database, workspace_tmp_path, b"not-uploaded")
    writer = ArchiveWriter(
        archive_settings(workspace_tmp_path),
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )

    await writer._retry_pending_once()

    assert manifest["status"] == "retrying"
    assert manifest["local_spool_path"] is not None
    assert manifest["last_error"] == "previous upload interruption"
    assert writer._remote_recovery_errors[1] == "remote object is missing"


@pytest.mark.asyncio
async def test_two_archive_writers_cannot_end_uploaded_object_as_failed(
    workspace_tmp_path: Path,
) -> None:
    database = CoordinatedArchiveDatabase()
    payload = b"cross-container-race"
    manifest, local_path = add_archive_manifest(
        database,
        workspace_tmp_path,
        payload,
        status="prepared",
        local_file=True,
    )
    settings = archive_settings(workspace_tmp_path)
    store = LocalObjectStore(workspace_tmp_path / "objects")
    stale_writer = ArchiveWriter(settings, database, object_store=store)
    uploading_writer = ArchiveWriter(settings, database, object_store=store)

    stale_retry = asyncio.create_task(stale_writer._retry_pending_once())
    await asyncio.wait_for(database.pending_read.wait(), timeout=1)
    assert await uploading_writer._upload_with_retry(
        1,
        local_path,
        manifest["object_key"],
        manifest["content_hash"],
    )
    assert not local_path.exists()
    database.release_pending.set()
    await asyncio.wait_for(stale_retry, timeout=1)

    assert manifest["status"] == "uploaded"
    assert manifest["last_error"] is None


@pytest.mark.asyncio
async def test_maintenance_skips_lane_owned_spool_and_archive_task_stays_alive(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_upload_max_attempts=1,
    )
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    await writer.put(update_record("owned-by-live-lane", "0.41"))
    await asyncio.wait_for(store.started.wait(), timeout=1)

    manifest = next(iter(database.objects.values()))
    digest = str(manifest["content_hash"])
    assert digest in writer._spool_owners
    for _ in range(3):
        await writer._retry_pending_once()
    # Exercise the continuously running maintenance worker too.
    await asyncio.sleep(1.05)

    assert len(store.attempted_keys) == 1
    assert writer.task is not None and not writer.task.done()
    assert writer.counters.upload_failures == 0

    store.release.set()
    await asyncio.wait_for(writer.join(), timeout=2)
    await writer.stop()

    assert manifest["status"] == "uploaded"
    assert manifest["local_spool_path"] is None
    assert len(store.attempted_keys) == 1
    assert writer._spool_owners == {}
    assert writer.last_error is None


@pytest.mark.asyncio
async def test_failed_lane_upload_releases_spool_for_maintenance_recovery(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path, archive_upload_max_attempts=1
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    store = FlakyStore(workspace_tmp_path / "objects", failures=1)
    writer = ArchiveWriter(settings, database, object_store=store)

    assert await writer._prepare_and_upload(
        [update_record("failed-lane-owner", "0.42")]
    )
    manifest = next(iter(database.objects.values()))
    spool_path = Path(str(manifest["local_spool_path"]))
    assert manifest["status"] == "retrying"
    assert spool_path.is_file()
    assert writer._spool_owners == {}
    assert store.attempts == 1

    await writer._retry_pending_once()

    assert store.attempts == 2
    assert manifest["status"] == "uploaded"
    assert manifest["local_spool_path"] is None
    assert not spool_path.exists()
    assert writer._spool_owners == {}


@pytest.mark.asyncio
async def test_cancelled_lane_upload_releases_spool_for_recovery(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path, archive_upload_max_attempts=1
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(settings, database, object_store=store)

    upload = asyncio.create_task(
        writer._prepare_and_upload(
            [update_record("cancelled-lane-owner", "0.43")]
        )
    )
    await asyncio.wait_for(store.started.wait(), timeout=1)
    upload.cancel()
    with pytest.raises(asyncio.CancelledError):
        await upload

    manifest = next(iter(database.objects.values()))
    spool_path = Path(str(manifest["local_spool_path"]))
    assert writer._spool_owners == {}
    assert spool_path.is_file()

    store.release.set()
    await writer._retry_pending_once()
    assert manifest["status"] == "uploaded"
    assert not spool_path.exists()
    assert len(store.attempted_keys) == 2


@pytest.mark.asyncio
async def test_live_and_raw_rest_spool_owners_upload_independently(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_upload_max_attempts=1,
    )
    store = DualGatedStore(workspace_tmp_path / "objects")
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()

    await asyncio.gather(
        writer.put(update_record("parallel-live", "0.44")),
        writer.put(raw_rest_record("parallel-rest")),
    )
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in store.started.values())),
        timeout=1,
    )

    assert len(writer._spool_owners) == 2
    assert len(store.attempted_keys) == 2
    assert writer.task is not None and not writer.task.done()

    store.release.set()
    await asyncio.wait_for(writer.join(), timeout=2)
    await writer.stop()

    assert writer._spool_owners == {}
    assert {value["stream"] for value in database.objects.values()} == {
        "orderbook_updates",
        "raw_rest",
    }
    assert all(
        value["status"] == "uploaded" for value in database.objects.values()
    )


@pytest.mark.asyncio
async def test_concurrent_same_digest_preparation_has_one_file_owner_and_upload(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path, archive_upload_max_attempts=1
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    first = update_record("same-digest", "0.45")
    second = update_record("same-digest", "0.45")

    preparations = [
        asyncio.create_task(writer._prepare_and_upload([first])),
        asyncio.create_task(writer._prepare_and_upload([second])),
    ]
    await asyncio.wait_for(store.started.wait(), timeout=1)
    await asyncio.sleep(0.05)
    assert len(writer._spool_owners) == 1
    assert len(store.attempted_keys) == 1

    store.release.set()
    assert await asyncio.gather(*preparations) == [True, True]

    assert len(database.objects) == 1
    manifest = next(iter(database.objects.values()))
    assert manifest["status"] == "uploaded"
    assert len(store.attempted_keys) == 1
    assert writer.counters.upload_failures == 0
    assert writer._spool_owners == {}
    assert not list(settings.archive_spool_directory.glob("*.parquet"))


@pytest.mark.asyncio
async def test_compaction_spool_owner_excludes_maintenance_retry(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    source_store = LocalObjectStore(workspace_tmp_path / "objects")
    source_settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
    )
    source_writer = ArchiveWriter(
        source_settings, database, object_store=source_store
    )
    await source_writer.start()
    for index in range(3):
        await source_writer.put(
            update_record(f"compaction-owner-{index}", f"0.4{index}")
        )
    await source_writer.join()
    await source_writer.stop()

    settings = archive_settings(
        workspace_tmp_path,
        archive_compaction_min_objects=3,
        archive_compaction_min_age_seconds=0,
    )
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    compactor = ArchiveWriter(settings, database, object_store=store)
    compacting = asyncio.create_task(compactor.compact_once())
    await asyncio.wait_for(store.started.wait(), timeout=2)

    replacement = max(database.objects.values(), key=lambda value: value["id"])
    assert replacement["object_role"] == "compacted"
    assert str(replacement["content_hash"]) in compactor._spool_owners
    for _ in range(3):
        await compactor._retry_pending_once()
    assert len(store.attempted_keys) == 1

    store.release.set()
    assert await asyncio.wait_for(compacting, timeout=2)
    assert replacement["status"] == "uploaded"
    assert compactor._spool_owners == {}
    assert len(store.attempted_keys) == 1


@pytest.mark.asyncio
async def test_slow_upload_backpressure_stays_bounded_without_row_loss(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=10,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=30_000,
        archive_flush_seconds=0.01,
    )
    store = SlowStore(workspace_tmp_path / "objects", delay_seconds=0.01)
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    for index in range(200):
        await writer.put(
            update_record(
                "slow-market",
                f"0.{index % 100:02d}",
                timestamp=NOW + timedelta(microseconds=index),
            )
        )
    await writer.join()
    await writer.stop()

    update_objects = [
        value for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
    ]
    assert sum(int(value["row_count"]) for value in update_objects) == 200
    assert all(value["status"] == "uploaded" for value in update_objects)
    assert writer.counters.max_queue_rows <= settings.archive_queue_max_rows
    assert writer.counters.max_queue_bytes <= settings.archive_queue_max_bytes


@pytest.mark.asyncio
async def test_live_batch_reserves_headroom_and_reports_inflight_separately(
    workspace_tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=20,
        archive_batch_bytes=100_000,
        archive_flush_seconds=0.1,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=40_000,
    )
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    monkeypatch.setattr(
        "prediction_collector.archive.SLOW_LIVE_BATCH_SECONDS", 0.0
    )
    caplog.set_level("WARNING", logger="prediction_collector.archive")
    await writer.start()
    initial = [
        replace(
            update_record(f"headroom-{index}", f"0.4{index}"),
            estimated_bytes=1_000,
        )
        for index in range(4)
    ]
    await asyncio.gather(*(writer.put(record) for record in initial))
    await asyncio.wait_for(store.started.wait(), timeout=1)

    metrics = writer.metrics()
    live = metrics["queue_lanes"]["live"]
    assert live["queued_rows"] == 0
    assert live["queued_bytes"] == 0
    assert live["inflight_rows"] == len(initial)
    assert live["inflight_bytes"] == 4_000
    assert live["inflight_bytes"] <= live["max_resident_bytes"] // 2
    assert metrics["queue_depth"] == 0
    assert metrics["queue_bytes"] == 0
    assert metrics["inflight_rows"] == len(initial)
    assert metrics["total_resident_bytes"] == 4_000

    current = replace(
        update_record("headroom-current", "0.49"), estimated_bytes=1_000
    )
    await asyncio.wait_for(writer.put(current), timeout=0.25)
    after_admission = writer.metrics()["queue_lanes"]["live"]
    assert after_admission["queued_rows"] == 1
    assert after_admission["queued_bytes"] == 1_000
    assert after_admission["total_resident_bytes"] == 5_000
    assert (
        after_admission["total_resident_bytes"]
        <= after_admission["max_resident_bytes"]
    )

    store.release.set()
    await asyncio.wait_for(writer.join(), timeout=5)
    await writer.stop()
    timing = writer.metrics()["batch_processing"]["last"]["live"]
    assert timing["batch_rows"] >= 1
    assert timing["estimated_uncompressed_bytes"] >= 1_000
    for field in (
        "serialization_seconds",
        "spool_publication_manifest_seconds",
        "s3_put_seconds",
        "remote_verification_seconds",
        "provenance_db_commit_seconds",
        "journal_acknowledgement_seconds",
        "total_batch_processing_seconds",
        "streams",
        "groups",
    ):
        assert field in timing
    assert "Slow live archive batch" in caplog.text


@pytest.mark.asyncio
async def test_durable_live_spill_returns_promptly_and_replays_exactly_once(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=4,
        archive_queue_max_bytes=4_000,
        archive_enqueue_timeout_seconds=5,
    )
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    blocker = replace(
        update_record("prompt-spill-blocker", "0.40"), estimated_bytes=1_000
    )
    await writer.put(blocker)
    await asyncio.wait_for(store.started.wait(), timeout=1)

    spilled = replace(
        update_record("prompt-spill-current", "0.41"), estimated_bytes=500
    )
    started = time.monotonic()
    await writer.put(spilled)
    elapsed = time.monotonic() - started
    assert elapsed < 0.25
    assert spilled.record_id in writer._spilled_record_ids
    assert spilled.record_id in writer._journal_path.read_text(encoding="utf-8")
    assert writer.counters.live_admission_spills == 1
    assert len(database.degradations) == 1
    assert (
        database.degradations[0]["reason"]
        == "durable_journal_live_admission_spill"
    )

    store.release.set()
    await asyncio.wait_for(writer.join(), timeout=5)
    await writer.stop()
    assert writer._journal_path.read_text(encoding="utf-8") == ""
    assert not writer._spilled_record_ids
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
    ) == 2
    assert database.transient_resolutions == 1


@pytest.mark.asyncio
async def test_sustained_mixed_live_burst_spills_promptly_and_drains_bounded(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=10,
        archive_batch_bytes=100_000,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=30,
        archive_queue_max_bytes=20_000,
        archive_enqueue_timeout_seconds=5,
    )
    writer = ArchiveWriter(
        settings,
        database,
        object_store=SlowStore(
            workspace_tmp_path / "objects", delay_seconds=0.02
        ),
    )
    await writer.start()
    records = [
        *(
            replace(
                update_record(f"burst-book-{index}", f"0.{index + 10}"),
                estimated_bytes=600,
            )
            for index in range(15)
        ),
        *(
            replace(
                reference_record(f"BURST-{index}", f"{60_000 + index}.25"),
                estimated_bytes=600,
            )
            for index in range(15)
        ),
        *(
            replace(
                observation_record(f"burst-observation-{index}", "0.45"),
                estimated_bytes=600,
            )
            for index in range(15)
        ),
    ]

    async def submit(record: ArchiveRecord) -> float:
        started = time.monotonic()
        await writer.put(record)
        return time.monotonic() - started

    durations = await asyncio.wait_for(
        asyncio.gather(*(submit(record) for record in records)), timeout=5
    )
    assert max(durations) < 1.0
    await asyncio.wait_for(writer.join(), timeout=20)
    await writer.stop()

    metrics = writer.metrics()
    assert metrics["max_resident_rows"] <= sum(writer._lane_max_rows.values())
    assert metrics["max_resident_bytes"] <= settings.archive_queue_max_bytes
    assert metrics["spilled_records_pending"] == 0
    assert len(database.degradations) <= 3
    assert all(value["resolved"] for value in database.degradations)
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] in {
            "orderbook_updates",
            "reference_prices",
            "microstructure_observations",
        }
    ) == len(records)
    assert writer._journal_path.read_text(encoding="utf-8") == ""


@pytest.mark.asyncio
async def test_large_raw_rest_burst_cannot_starve_continuous_live_l2(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=20,
        archive_batch_bytes=80_000,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=40,
        archive_queue_max_bytes=200_000,
        archive_enqueue_timeout_seconds=0.05,
    )
    store = SlowRawRestStore(
        workspace_tmp_path / "objects", delay_seconds=0.03
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()

    raw_records = [
        raw_rest_record(f"burst-{index}", payload_size=30_000)
        for index in range(12)
    ]

    async def produce_raw_rest() -> None:
        for record in raw_records:
            await writer.put(record)

    raw_producer = asyncio.create_task(produce_raw_rest())
    await asyncio.sleep(0.01)
    live_records = [
        update_record(
            f"live-{index}",
            f"0.{index % 100:02d}",
            timestamp=NOW + timedelta(microseconds=index),
        )
        for index in range(200)
    ]
    for record in live_records:
        await writer.put(record)

    await asyncio.wait_for(raw_producer, timeout=5)
    await asyncio.wait_for(writer.join(), timeout=5)
    await writer.stop()

    assert not [
        event
        for event in database.degradations
        if event["reason"] == "bounded_queue_timeout"
    ]
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
    ) == len(live_records)
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "raw_rest"
    ) == len(raw_records)
    assert len(database.provenance) == len(raw_records)
    assert len({value["value"]["content_hash"] for value in database.provenance}) == len(
        raw_records
    )
    assert writer.counters.max_queue_rows <= settings.archive_queue_max_rows
    assert writer.counters.max_queue_bytes <= settings.archive_queue_max_bytes
    assert writer._journal_path.read_text(encoding="utf-8") == ""


@pytest.mark.asyncio
async def test_durable_live_spill_resolves_only_after_upload_and_ack(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=2,
        archive_queue_max_bytes=10_000,
        archive_enqueue_timeout_seconds=0.01,
    )
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    writer.run_id = 34
    await writer.start()

    blocker = update_record("degradation-blocker", "0.40")
    await writer.put(blocker)
    await asyncio.wait_for(store.started.wait(), timeout=1)
    queued = update_record("degradation-queued", "0.405")
    await writer.put(queued)
    spilled = update_record("degradation-spill", "0.41")
    await writer.put(spilled)

    assert spilled.record_id in writer._spilled_record_ids
    assert len(database.degradations) == 1
    assert (
        database.degradations[0]["reason"]
        == "durable_journal_live_admission_spill"
    )
    assert database.degradations[0]["resolved"] is False
    assert database.transient_resolutions == 0

    store.release.set()
    for _ in range(100):
        if (
            database.degradations[0]["resolved"]
            and database.transient_resolutions == 1
            and not writer._spilled_record_ids
            and not writer._active_record_ids
            and writer._queues_empty()
        ):
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("spilled archive record did not recover")

    assert database.transient_resolutions == 1
    assert writer._journal_path.read_text(encoding="utf-8") == ""
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
    ) == 3
    await writer.stop()


@pytest.mark.asyncio
async def test_raw_rest_backpressure_does_not_consume_live_lane_capacity(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_queue_max_rows=1,
        archive_queue_max_bytes=1_000_000,
        archive_enqueue_timeout_seconds=0.01,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    writer = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    raw_blocker = raw_rest_record("raw-blocker")
    await writer.put(raw_blocker)
    raw_waiter = raw_rest_record("raw-waiter")
    raw_put = asyncio.create_task(writer.put(raw_waiter))
    await asyncio.sleep(0.03)
    assert not raw_put.done()

    live = update_record("live-while-rest-blocked", "0.41")
    await asyncio.wait_for(writer.put(live), timeout=1)
    assert writer.queue.get_nowait().record_id == live.record_id
    writer.queue.task_done()
    await writer._release_bytes(live.estimated_bytes, lane="live")
    assert database.degradations == []

    dequeued = writer.raw_rest_queue.get_nowait()
    assert dequeued.record_id == raw_blocker.record_id
    writer.raw_rest_queue.task_done()
    await writer._release_bytes(dequeued.estimated_bytes, lane="raw_rest")
    await asyncio.wait_for(raw_put, timeout=1)
    assert next(iter(writer.raw_rest_queue._queue)).record_id == raw_waiter.record_id


@pytest.mark.asyncio
async def test_cancelled_raw_rest_backpressure_is_recovered_from_journal(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_queue_max_rows=1,
        archive_queue_max_bytes=1_000_000,
        archive_enqueue_timeout_seconds=0.01,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await interrupted.put(raw_rest_record("journal-blocker"))
    raw_rest = raw_rest_record("cancelled-page")
    pending = asyncio.create_task(interrupted.put(raw_rest))
    await asyncio.sleep(0.03)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert interrupted._raw_rest_fresh_waiters == 0
    assert raw_rest.record_id in interrupted._spilled_record_ids
    assert raw_rest.record_id in interrupted._journal_path.read_text(encoding="utf-8")

    recovered = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await recovered.start()
    await recovered.join()
    await recovered.stop()
    assert any(
        value["stream"] == "raw_rest" and value["status"] == "uploaded"
        for value in database.objects.values()
    )


@pytest.mark.asyncio
async def test_internal_spilled_raw_rest_replay_does_not_wait_on_its_own_queue(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_queue_max_rows=1,
        archive_queue_max_bytes=1_000_000,
        archive_enqueue_timeout_seconds=0.01,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    writer = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await writer.put(raw_rest_record("replay-blocker"))
    spilled = raw_rest_record("spilled-page")
    await writer._append_journal(spilled)
    writer._spilled_record_ids.add(spilled.record_id)

    await asyncio.wait_for(writer._retry_spilled_journal_once(), timeout=1)
    assert spilled.record_id in writer._spilled_record_ids
    assert database.degradations == []


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
    spool_path = Path(manifest["local_spool_path"])
    assert spool_path.is_file()
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
    assert manifest["local_spool_path"] is None
    assert not spool_path.exists()
    assert len(
        [value for value in database.objects.values()
         if value["stream"] == "orderbook_updates"]
    ) == 1


@pytest.mark.asyncio
async def test_restart_cleans_file_left_after_manifest_commit(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(workspace_tmp_path)
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    payload = b"manifest committed before local cleanup"
    digest = hashlib.sha256(payload).hexdigest()
    leftover = settings.archive_spool_directory / f"{digest}.parquet"
    leftover.write_bytes(payload)
    database.by_hash[digest] = 1
    database.objects[1] = {
        "id": 1,
        "content_hash": digest,
        "object_key": "research/already-uploaded.parquet",
        "status": "uploaded",
        "stream": "orderbook_updates",
    }

    writer = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await writer.start()
    await writer.stop()

    assert not leftover.exists()


@pytest.mark.asyncio
async def test_restart_recovers_journaled_record_before_parquet_serialization(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(workspace_tmp_path)
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await interrupted._append_journal(update_record("journal-crash", "0.47"))

    recovered = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await recovered.start()
    await recovered.join()
    await recovered.stop()

    uploaded = [
        value for value in database.objects.values()
        if value["status"] == "uploaded"
        and value["stream"] == "orderbook_updates"
    ]
    assert len(uploaded) == 1
    assert uploaded[0]["row_count"] == 1


@pytest.mark.asyncio
async def test_failed_inherited_record_retries_from_its_source_before_completion(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = raw_rest_record("recovery-fails-once")
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await interrupted._append_journal(inherited)

    recovered = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    prepare = recovered._prepare_and_upload
    failed_once = asyncio.Event()
    allow_retry = asyncio.Event()
    attempts = 0

    async def fail_once(records: list[ArchiveRecord]) -> bool:
        nonlocal attempts
        if any(record.record_id == inherited.record_id for record in records):
            attempts += 1
            if attempts == 1:
                failed_once.set()
                return False
            await allow_retry.wait()
        return await prepare(records)

    recovered._prepare_and_upload = fail_once  # type: ignore[method-assign]
    await recovered.start()
    await asyncio.wait_for(failed_once.wait(), timeout=1)
    for _ in range(100):
        if (
            inherited.record_id in recovered._spilled_record_ids
            or attempts >= 2
        ):
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("failed inherited row never entered spill replay")

    source = recovered._recovery_record_sources[inherited.record_id]
    assert source.is_file()
    assert inherited.record_id in source.read_text(encoding="utf-8")
    await asyncio.wait_for(
        recovered._journal_recovery_admission_complete.wait(), timeout=1
    )
    assert recovered._journal_recovery_admission_complete.is_set()
    assert not recovered._journal_recovery_complete.is_set()
    assert recovered.metrics()["journal_recovery_pending"] is True

    # Maintenance must retry from the same immutable recovery journal without
    # copying the row into the active-process journal or restarting the writer.
    joining = asyncio.create_task(recovered.join())
    await asyncio.sleep(0.05)
    assert not joining.done()
    allow_retry.set()
    await asyncio.wait_for(joining, timeout=5)
    assert attempts == 2
    assert recovered._journal_recovery_complete.is_set()
    assert recovered.metrics()["journal_recovery_pending"] is False
    assert not source.exists()
    assert not recovered._recovery_journal_paths()
    assert (
        not recovered._journal_path.exists()
        or recovered._journal_path.read_text(encoding="utf-8") == ""
    )
    assert len(database.provenance) == 1
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "raw_rest" and value["status"] == "uploaded"
    ) == 1
    await recovered.stop()


@pytest.mark.asyncio
async def test_fresh_live_evidence_retains_capacity_during_inherited_live_recovery(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=4,
        archive_queue_max_bytes=1_000_000,
        archive_enqueue_timeout_seconds=0.05,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = [
        update_record(f"inherited-live-{index}", f"0.{index + 10}")
        for index in range(12)
    ]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in inherited:
        await interrupted._append_journal(record)

    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    recovered = ArchiveWriter(settings, database, object_store=store)
    await recovered.start()
    await asyncio.wait_for(store.started.wait(), timeout=1)
    assert recovered._live_recovery_outstanding == 1

    current_update = update_record("current-live-update", "0.91")
    current_reference = reference_record("BTC-USD", "63250.125")
    await asyncio.wait_for(
        asyncio.gather(
            recovered.put(current_update),
            recovered.put(current_reference),
        ),
        timeout=0.5,
    )
    assert recovered.queue.qsize() == 2
    assert not any(
        value["reason"] == "bounded_queue_timeout"
        and (value.get("details") or {}).get("record_id")
            in {current_update.record_id, current_reference.record_id}
        for value in database.degradations
    )

    store.release.set()
    await asyncio.wait_for(recovered.join(), timeout=10)
    await recovered.stop()
    assert recovered._journal_recovery_complete.is_set()
    assert not recovered._recovery_journal_paths()
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
        and value["status"] == "uploaded"
    ) == len(inherited) + 1
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "reference_prices"
        and value["status"] == "uploaded"
    ) == 1


@pytest.mark.asyncio
async def test_bounded_fresh_live_priority_cannot_starve_recovery(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=4,
        archive_queue_max_bytes=1_000_000,
        archive_enqueue_timeout_seconds=1,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = [
        update_record(f"live-fair-recovery-{index}", f"0.{index + 10}")
        for index in range(10)
    ]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in inherited:
        await interrupted._append_journal(record)

    store = SlowStore(workspace_tmp_path / "objects", delay_seconds=0.02)
    recovered = ArchiveWriter(settings, database, object_store=store)
    await recovered.start()
    fresh = [
        update_record(f"live-continuous-fresh-{index}", f"0.{index + 40}")
        for index in range(12)
    ]
    await asyncio.wait_for(
        asyncio.gather(*(recovered.put(record) for record in fresh)),
        timeout=5,
    )
    await asyncio.wait_for(recovered.join(), timeout=10)
    await recovered.stop()

    fresh_market_keys = {
        stable_archive_key("market", f"live-continuous-fresh-{index}")
        for index in range(len(fresh))
    }
    processing_order: list[str] = []
    for value in sorted(database.objects.values(), key=lambda item: item["id"]):
        if value["stream"] != "orderbook_updates":
            continue
        table = pq.read_table(store.root / value["object_key"])
        market_key = int(table.column("market_key")[0].as_py())
        processing_order.append(
            "fresh" if market_key in fresh_market_keys else "recovery"
        )
    first_fresh = processing_order.index("fresh")
    last_fresh = len(processing_order) - 1 - processing_order[::-1].index(
        "fresh"
    )
    concurrent_window = processing_order[first_fresh:last_fresh + 1]
    assert "recovery" in concurrent_window
    assert len(processing_order) == len(inherited) + len(fresh)
    assert recovered._journal_recovery_complete.is_set()


@pytest.mark.asyncio
async def test_restart_drains_multiple_recovery_journals_without_loss_or_duplication(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    first_records = [
        update_record(f"multi-recovery-a-{index}", f"0.{index + 20}")
        for index in range(3)
    ]
    second_records = [
        update_record(f"multi-recovery-b-{index}", f"0.{index + 30}")
        for index in range(4)
    ]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in first_records:
        await interrupted._append_journal(record)
    interrupted._journal_path.replace(
        settings.archive_spool_directory
        / "ingress-journal.recovery-first-crash.jsonl"
    )
    for record in second_records:
        await interrupted._append_journal(record)
    interrupted._journal_path.replace(
        settings.archive_spool_directory
        / "ingress-journal.recovery-second-crash.jsonl"
    )

    recovered = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await recovered.start()
    await asyncio.wait_for(recovered.join(), timeout=10)
    await recovered.stop()

    expected = len(first_records) + len(second_records)
    assert sum(
        int(value["row_count"])
        for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
        and value["status"] == "uploaded"
    ) == expected
    assert recovered.counters.stream_rows["orderbook_updates"] == expected
    assert recovered._journal_recovery_complete.is_set()
    assert not recovered._recovery_journal_paths()


@pytest.mark.asyncio
async def test_inherited_raw_rest_recovery_is_async_bounded_and_does_not_starve_live(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=1_000_000,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = [raw_rest_record(f"inherited-{index}") for index in range(25)]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in inherited:
        await interrupted._append_journal(record)

    store = GatedStore(workspace_tmp_path / "objects", blocked_stream="raw_rest")
    recovered = ArchiveWriter(settings, database, object_store=store)
    await asyncio.wait_for(recovered.start(), timeout=0.25)
    await asyncio.wait_for(store.started.wait(), timeout=1)

    assert not recovered._journal_recovery_complete.is_set()
    assert recovered.task is not None and not recovered.task.done()
    assert len(recovered._recovery_record_sources) <= (
        recovered.raw_rest_queue.maxsize + 2
    )
    assert recovered.counters.max_queue_rows <= settings.archive_queue_max_rows
    assert recovered.counters.max_queue_bytes <= settings.archive_queue_max_bytes

    live = update_record("live-during-inherited-rest", "0.48")
    await asyncio.wait_for(recovered.put(live), timeout=0.25)
    for _ in range(100):
        if any(
            value["stream"] == "orderbook_updates"
            and value["status"] == "uploaded"
            for value in database.objects.values()
        ):
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("live archive lane was starved by inherited raw REST")

    store.release.set()
    await asyncio.wait_for(recovered.join(), timeout=10)
    await recovered.stop()

    raw_objects = [
        value for value in database.objects.values()
        if value["stream"] == "raw_rest"
    ]
    assert len(raw_objects) == len(inherited)
    assert all(value["status"] == "uploaded" for value in raw_objects)
    assert len(database.provenance) == len(inherited)
    assert not recovered._recovery_journal_paths()
    assert recovered._journal_path.read_text(encoding="utf-8") == ""


@pytest.mark.asyncio
async def test_live_discovery_raw_page_preempts_inherited_raw_rest_recovery(
    workspace_tmp_path: Path,
    load_fixture: Any,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=1_000_000,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = [
        raw_rest_record(f"critical-recovery-{index}") for index in range(12)
    ]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in inherited:
        await interrupted._append_journal(record)

    store = GatedStore(workspace_tmp_path / "objects", blocked_stream="raw_rest")
    archive = ArchiveWriter(settings, database, object_store=store)
    await asyncio.wait_for(archive.start(), timeout=0.25)
    await asyncio.wait_for(store.started.wait(), timeout=1)

    raw_market = load_fixture("polymarket_market.json")
    event_page = [
        {
            "id": "current-live-event",
            "active": True,
            "closed": False,
            "markets": [raw_market],
        }
    ]
    result = SimpleNamespace(
        requested_at=NOW,
        response_timestamp=NOW,
        status_code=200,
        url=(
            "https://gamma-api.polymarket.com/events/keyset"
            "?active=true&closed=false&limit=100"
        ),
    )

    class LiveDiscoveryRest:
        async def iter_live_events(self) -> Any:
            yield event_page, result, "current-live-cursor"

    batch_writer = BatchWriter(
        database,
        max_queue_size=10,
        batch_size=10,
        flush_interval_seconds=0.01,
        archive=archive,
    )
    service = PolymarketService(
        rest=LiveDiscoveryRest(),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=batch_writer,
    )
    collector = LiveCollector.__new__(LiveCollector)
    collector._last_discovery = []
    collector._selection_lock = asyncio.Lock()
    collector.run_id = 1
    collector.coverage = type(
        "Coverage", (), {"selection": None, "confirmed_subscribed": 0}
    )()
    collector.tier_manager = type(
        "TierPolicy",
        (),
        {
            "full_l2_max_markets": 10,
            "sampled_max_markets": 50,
            "counts": lambda self: {
                CollectionTier.FULL_L2.value: 0,
                CollectionTier.SAMPLED.value: 0,
            },
        },
    )()
    collector._persist_selected_candidates = (  # type: ignore[method-assign]
        lambda _: asyncio.sleep(0)
    )
    shard_reconciliation_reached = asyncio.Event()

    async def apply_tiers(markets: list[Any], **_: Any) -> None:
        assert markets[0].external_id == raw_market["conditionId"]
        shard_reconciliation_reached.set()

    collector._apply_tiers = apply_tiers  # type: ignore[method-assign]

    discovering = asyncio.create_task(
        service.discover_live(
            reconcile_absent=False,
            on_page=collector._merge_discovery_page,
        )
    )
    for _ in range(100):
        if archive._raw_rest_fresh_waiters:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("live discovery raw page never reached admission")
    assert not shard_reconciliation_reached.is_set()

    store.release.set()
    await asyncio.wait_for(shard_reconciliation_reached.wait(), timeout=1)
    discovered = await asyncio.wait_for(discovering, timeout=1)
    assert len(discovered) == 1
    assert collector._last_discovery[0].external_id == raw_market["conditionId"]
    assert not archive._journal_recovery_complete.is_set()

    await asyncio.wait_for(archive.join(), timeout=10)
    await archive.stop()
    provenance_order = [
        value["value"]["external_key"] for value in database.provenance
    ]
    fresh_index = provenance_order.index("current-live-cursor")
    assert fresh_index < max(
        provenance_order.index(f"critical-recovery-{index}")
        for index in range(len(inherited))
    )
    assert len(database.provenance) == len(inherited) + 1


@pytest.mark.asyncio
async def test_bounded_fresh_raw_rest_priority_cannot_starve_recovery(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=1_000_000,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = [
        raw_rest_record(f"fair-recovery-{index}") for index in range(20)
    ]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in inherited:
        await interrupted._append_journal(record)

    store = GatedStore(workspace_tmp_path / "objects", blocked_stream="raw_rest")
    archive = ArchiveWriter(settings, database, object_store=store)
    await archive.start()
    await asyncio.wait_for(store.started.wait(), timeout=1)
    fresh = [raw_rest_record(f"continuous-fresh-{index}") for index in range(20)]
    producers = [asyncio.create_task(archive.put(record)) for record in fresh]
    for _ in range(200):
        if archive._raw_rest_fresh_waiters == len(fresh):
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("fresh raw REST producers did not reach the arbiter")

    store.release.set()
    await asyncio.wait_for(asyncio.gather(*producers), timeout=10)
    await asyncio.wait_for(archive.join(), timeout=15)
    await archive.stop()

    provenance_order = [
        str(value["value"]["external_key"]) for value in database.provenance
    ]
    first_fresh = next(
        index for index, value in enumerate(provenance_order)
        if value.startswith("continuous-fresh-")
    )
    last_fresh = max(
        index for index, value in enumerate(provenance_order)
        if value.startswith("continuous-fresh-")
    )
    concurrent_window = provenance_order[first_fresh:last_fresh + 1]
    assert any(value.startswith("fair-recovery-") for value in concurrent_window)
    longest_fresh_run = 0
    current_fresh_run = 0
    for value in concurrent_window:
        if value.startswith("continuous-fresh-"):
            current_fresh_run += 1
            longest_fresh_run = max(longest_fresh_run, current_fresh_run)
        else:
            current_fresh_run = 0
    assert longest_fresh_run <= RAW_REST_FRESH_ADMISSION_BURST
    assert len(database.provenance) == len(inherited) + len(fresh)
    assert len([
        value for value in database.objects.values()
        if value["stream"] == "raw_rest" and value["status"] == "uploaded"
    ]) == len(inherited) + len(fresh)
    assert archive._journal_recovery_complete.is_set()
    assert not archive._recovery_journal_paths()


@pytest.mark.asyncio
async def test_recovery_shutdown_preserves_unadmitted_rows_for_restart(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=1_000_000,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    inherited = [raw_rest_record(f"cancelled-recovery-{index}") for index in range(12)]
    interrupted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for record in inherited:
        await interrupted._append_journal(record)

    gated = GatedStore(workspace_tmp_path / "objects", blocked_stream="raw_rest")
    first = ArchiveWriter(settings, database, object_store=gated)
    await first.start()
    await asyncio.wait_for(gated.started.wait(), timeout=1)
    stopping = asyncio.create_task(first.stop())
    await asyncio.sleep(0.02)
    assert not stopping.done()
    gated.release.set()
    await asyncio.wait_for(stopping, timeout=3)

    remaining_paths = first._recovery_journal_paths()
    assert remaining_paths
    remaining_ids = {
        str(json.loads(line)["record_id"])
        for path in remaining_paths
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    assert remaining_ids

    restarted = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await restarted.start()
    await asyncio.wait_for(restarted.join(), timeout=10)
    await restarted.stop()
    assert len([
        value for value in database.objects.values()
        if value["stream"] == "raw_rest" and value["status"] == "uploaded"
    ]) == len(inherited)
    assert not restarted._recovery_journal_paths()


@pytest.mark.asyncio
async def test_journal_recovery_failure_is_supervised_by_archive_parent(
    workspace_tmp_path: Path,
) -> None:
    settings = archive_settings(workspace_tmp_path)
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    journal = settings.archive_spool_directory / "ingress-journal.jsonl"
    journal.write_text("{malformed-json\n", encoding="utf-8")
    writer = ArchiveWriter(
        settings,
        ArchiveDatabase(),
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await writer.start()
    assert writer.task is not None
    with pytest.raises(json.JSONDecodeError):
        await asyncio.wait_for(writer.task, timeout=1)
    assert writer._recovery_journal_paths()


@pytest.mark.asyncio
async def test_live_collector_background_tasks_start_while_recovery_is_pending(
    workspace_tmp_path: Path,
) -> None:
    database = LiveRunArchiveDatabase()
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=20,
    )
    settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
    seed = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    for index in range(5):
        await seed._append_journal(raw_rest_record(f"live-start-{index}"))

    store = GatedStore(workspace_tmp_path / "objects", blocked_stream="raw_rest")
    archive = ArchiveWriter(settings, database, object_store=store)
    batch_writer = BatchWriter(
        database, max_queue_size=10, batch_size=10,
        flush_interval_seconds=0.01, archive=archive,
    )
    collector = LiveCollector.__new__(LiveCollector)
    collector.database = database  # type: ignore[assignment]
    collector.writer = batch_writer
    collector.stop = asyncio.Event()
    collector.run_id = None
    collector.coverage = type("Coverage", (), {"metrics": lambda self: {}})()
    collector._task_failure = None
    started = asyncio.Event()

    async def start_background() -> None:
        assert not archive._journal_recovery_complete.is_set()
        started.set()

    collector._start_background_tasks = start_background  # type: ignore[method-assign]
    collector._shutdown_tasks = lambda: asyncio.sleep(0)  # type: ignore[method-assign]
    running = asyncio.create_task(collector.run())
    await asyncio.wait_for(started.wait(), timeout=1)
    collector.stop.set()
    store.release.set()
    await asyncio.wait_for(running, timeout=3)
    assert database.finished_runs[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_oldest_queued_seconds_tracks_the_actual_oldest_record(
    workspace_tmp_path: Path,
) -> None:
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_flush_seconds=0.01,
        archive_queue_max_rows=20,
        archive_queue_max_bytes=1_000_000,
    )
    store = GatedStore(
        workspace_tmp_path / "objects", blocked_stream="orderbook_updates"
    )
    writer = ArchiveWriter(
        settings,
        ArchiveDatabase(),
        object_store=store,
    )
    await writer.start()
    oldest = update_record("oldest-age", "0.41")
    newest = update_record("newest-age", "0.42")
    await writer.put(oldest)
    await asyncio.wait_for(store.started.wait(), timeout=1)
    await asyncio.sleep(0.15)
    await writer.put(newest)
    await asyncio.sleep(0.01)
    age = writer.metrics()["oldest_queued_seconds"]

    # The first record is uploading, not queued. Its 150 ms wait must not be
    # attributed to the second record, which entered the queue only just now.
    assert 0 < age < 0.1
    store.release.set()
    await asyncio.wait_for(writer.join(), timeout=2)
    assert writer.metrics()["oldest_queued_seconds"] == 0.0
    await writer.stop()


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


@pytest.mark.asyncio
async def test_raw_rest_body_is_content_addressed_and_provenance_is_not_lost(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    writer = ArchiveWriter(archive_settings(workspace_tmp_path), database, object_store=store)
    body = {
        "source": "gamma",
        "endpoint": "/markets",
        "entity_type": "markets",
        "external_key": "same-page",
        "requested_at": NOW,
        "received_at": NOW,
        "parameters": {"limit": 100},
        "http_status": 200,
        "content_hash": "same-semantic-body",
        "record_count": 1,
        "response_bytes": 17,
        "payload": [{"id": "a"}],
    }
    await writer.start()
    await writer.put(ArchiveRecord.create("raw_rest", body, priority=2))
    await writer.join()
    await writer.put(
        ArchiveRecord.create(
            "raw_rest",
            {**body, "requested_at": NOW + timedelta(seconds=1)},
            priority=2,
        )
    )
    await writer.stop()

    raw_objects = [
        value for value in database.objects.values() if value["stream"] == "raw_rest"
    ]
    assert len(raw_objects) == 1
    assert raw_objects[0]["row_count"] == 1
    assert len(database.provenance) == 2
    assert writer.counters.raw_rest_objects_reused == 1


@pytest.mark.asyncio
async def test_compaction_replaces_small_objects_only_after_verified_upload(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase()
    store = LocalObjectStore(workspace_tmp_path / "objects")
    settings = archive_settings(
        workspace_tmp_path,
        archive_batch_rows=1,
        archive_compaction_min_objects=3,
        archive_compaction_min_age_seconds=0,
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    for index in range(4):
        await writer.put(update_record(f"compact-{index}", f"0.4{index}"))
    await writer.join()
    assert len(await database.archive_compaction_candidates()) == 4
    assert await writer.compact_once()
    await writer.stop()

    active = [
        value for value in database.objects.values()
        if value["stream"] == "orderbook_updates"
        and value.get("superseded_at") is None
        and value["status"] == "uploaded"
    ]
    assert len(active) == 1
    assert active[0]["row_count"] == 4
    assert active[0]["object_role"] == "compacted"
    assert writer.counters.compaction_objects_before == 4
    assert writer.counters.compaction_objects_after == 1


@pytest.mark.asyncio
async def test_archive_reader_resolves_compact_keys_through_dictionary(
    workspace_tmp_path: Path,
) -> None:
    database = ArchiveDatabase(emit_dictionary=True)
    store = LocalObjectStore(workspace_tmp_path / "objects")
    settings = archive_settings(
        workspace_tmp_path, archive_batch_rows=100, archive_flush_seconds=0.03
    )
    writer = ArchiveWriter(settings, database, object_store=store)
    await writer.start()
    await writer.put(update_record("market-a", "0.41"))
    await writer.put(update_record("market-b", "0.42"))
    await writer.join()
    await writer.stop()
    keys = await store.list_keys("research/")
    update_paths = [store.root / key for key in keys if "stream=orderbook_updates" in key]
    dictionary_paths = [
        store.root / key for key in keys if "stream=archive_dictionary" in key
    ]
    table = load_archive(
        update_paths,
        markets=["market-b"],
        columns=["market_external_id", "outcome_external_id", "price_mantissa"],
        dictionary_paths=dictionary_paths,
    )
    assert table.to_pylist() == [
        {
            "market_external_id": "market-b",
            "outcome_external_id": "market-b-yes",
            "price_mantissa": 42,
        }
    ]


def test_hour_partition_prefixes_cover_only_intersecting_hours() -> None:
    assert archive_partition_prefixes(
        prefix="research",
        stream="orderbook_updates",
        start=NOW,
        end=NOW + timedelta(hours=2),
    ) == [
        "research/schema_version=2/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=12/",
        "research/schema_version=2/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=13/",
        "research/schema_version=2/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=14/",
    ]


def test_spool_size_scan_tolerates_concurrent_scratch_file_removal(
    workspace_tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scratch = workspace_tmp_path / "preparing-race.parquet"
    scratch.write_bytes(b"temporary")
    original_stat = Path.stat

    def disappearing_stat(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path == scratch:
            scratch.unlink(missing_ok=True)
            raise FileNotFoundError(str(scratch))
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", disappearing_stat)
    assert _directory_size(workspace_tmp_path) == 0


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
