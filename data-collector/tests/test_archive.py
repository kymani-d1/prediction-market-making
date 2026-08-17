from __future__ import annotations

import asyncio
import hashlib
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
    _directory_size,
)
from prediction_collector.archive_reader import (
    archive_partition_prefixes,
    load_archive,
)
from prediction_collector.database import Database
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

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        if f"stream={self.blocked_stream}" in object_key:
            self.started.set()
            await self.release.wait()
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
async def test_bounded_timeout_resolves_only_after_spill_is_uploaded_and_acked(
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
    assert database.degradations[0]["reason"] == "bounded_queue_timeout"
    assert database.degradations[0]["resolved"] is False
    assert database.transient_resolutions == 0

    store.release.set()
    for _ in range(100):
        if (
            database.degradations[0]["resolved"]
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
    assert raw_rest.record_id in interrupted._spilled_record_ids
    assert raw_rest.record_id in interrupted._journal_path.read_text(encoding="utf-8")

    recovered = ArchiveWriter(
        settings,
        database,
        object_store=LocalObjectStore(workspace_tmp_path / "objects"),
    )
    await recovered.start()
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
    await recovered.stop()

    uploaded = [
        value for value in database.objects.values()
        if value["status"] == "uploaded"
        and value["stream"] == "orderbook_updates"
    ]
    assert len(uploaded) == 1
    assert uploaded[0]["row_count"] == 1


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
