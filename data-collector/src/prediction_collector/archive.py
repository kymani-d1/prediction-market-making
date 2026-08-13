from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import shutil
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Protocol

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

from prediction_collector.common.retry import RetryPolicy
from prediction_collector.common.utils import canonical_json, parse_timestamp, utc_now
from prediction_collector.config import Settings


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 1


class ArchiveBackpressureError(RuntimeError):
    pass


class ObjectStore(Protocol):
    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None: ...
    async def head(self, object_key: str) -> Mapping[str, Any]: ...
    async def download(self, object_key: str, local_path: Path) -> None: ...
    async def list_keys(self, prefix: str) -> list[str]: ...


class S3ObjectStore:
    def __init__(self, settings: Settings) -> None:
        settings.require_archive()
        style = settings.s3_url_style
        if style == "auto":
            style = "virtual"
        self.bucket = str(settings.s3_bucket)
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": style},
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        await asyncio.to_thread(
            self.client.upload_file,
            str(local_path),
            self.bucket,
            object_key,
            ExtraArgs={
                "ContentType": "application/vnd.apache.parquet",
                "Metadata": {"sha256": content_hash, "schema-version": str(SCHEMA_VERSION)},
            },
        )

    async def head(self, object_key: str) -> Mapping[str, Any]:
        return await asyncio.to_thread(
            self.client.head_object, Bucket=self.bucket, Key=object_key
        )

    async def download(self, object_key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(
            self.client.download_file, self.bucket, object_key, str(local_path)
        )

    async def list_keys(self, prefix: str) -> list[str]:
        def collect() -> list[str]:
            paginator = self.client.get_paginator("list_objects_v2")
            return [
                str(item["Key"])
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
                for item in page.get("Contents", [])
            ]

        return await asyncio.to_thread(collect)


class LocalObjectStore:
    """Filesystem object store for deterministic integration tests, never production."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None:
        target = self.root / object_key
        target.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copyfile, local_path, target)

    async def head(self, object_key: str) -> Mapping[str, Any]:
        target = self.root / object_key
        if not target.is_file():
            raise FileNotFoundError(target)
        return {"ContentLength": target.stat().st_size, "Metadata": {"sha256": _sha256(target)}}

    async def download(self, object_key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copyfile, self.root / object_key, local_path)

    async def list_keys(self, prefix: str) -> list[str]:
        return sorted(
            str(path.relative_to(self.root)).replace("\\", "/")
            for path in self.root.rglob("*.parquet")
            if str(path.relative_to(self.root)).replace("\\", "/").startswith(prefix)
        )


@dataclass(frozen=True, slots=True)
class ArchiveRecord:
    stream: str
    data: Mapping[str, Any]
    priority: int
    partition_timestamp: datetime
    estimated_bytes: int

    @classmethod
    def create(
        cls,
        stream: str,
        data: Mapping[str, Any],
        *,
        priority: int,
        partition_timestamp: datetime | None = None,
    ) -> "ArchiveRecord":
        timestamp = partition_timestamp or parse_timestamp(data.get("received_at")) or utc_now()
        estimate = len(canonical_json(dict(data)).encode("utf-8"))
        return cls(stream, dict(data), priority, timestamp, estimate)


_COMMON_FIELDS: list[tuple[str, pa.DataType]] = [
    ("exchange", pa.string()),
    ("market_external_id", pa.string()),
    ("outcome_external_id", pa.string()),
    ("connection_id", pa.int64()),
    ("source_timestamp", pa.timestamp("us", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("us", tz="UTC")),
    ("source_timestamp_raw", pa.string()),
    ("exchange_timestamp_raw", pa.string()),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("received_monotonic_ns", pa.int64()),
    ("sequence_number", pa.int64()),
    ("book_hash", pa.string()),
]


def _schema(fields: list[tuple[str, pa.DataType]]) -> pa.Schema:
    return pa.schema(
        [pa.field(name, kind) for name, kind in fields],
        metadata={b"schema_version": str(SCHEMA_VERSION).encode(), b"exchange": b"polymarket"},
    )


STREAM_SCHEMAS: dict[str, pa.Schema] = {
    "orderbook_updates": _schema(
        _COMMON_FIELDS
        + [
            ("side", pa.string()),
            ("price", pa.string()),
            ("size", pa.string()),
            ("size_delta", pa.string()),
            ("operation", pa.string()),
            ("event_type", pa.string()),
        ]
    ),
    "orderbook_snapshots": _schema(
        _COMMON_FIELDS
        + [
            ("snapshot_type", pa.string()),
            ("bids", pa.string()),
            ("asks", pa.string()),
            ("best_bid", pa.string()),
            ("best_ask", pa.string()),
            ("is_reconciliation", pa.bool_()),
        ]
    ),
    "raw_ws": _schema(
        _COMMON_FIELDS
        + [
            ("channel", pa.string()),
            ("message_type", pa.string()),
            ("payload", pa.string()),
            ("payload_hash", pa.string()),
        ]
    ),
    "raw_rest": _schema(
        [
            ("source", pa.string()),
            ("endpoint", pa.string()),
            ("entity_type", pa.string()),
            ("external_key", pa.string()),
            ("requested_at", pa.timestamp("us", tz="UTC")),
            ("received_at", pa.timestamp("us", tz="UTC")),
            ("response_timestamp", pa.timestamp("us", tz="UTC")),
            ("response_timestamp_raw", pa.string()),
            ("parameters", pa.string()),
            ("http_status", pa.int16()),
            ("content_hash", pa.string()),
            ("record_count", pa.int32()),
            ("response_bytes", pa.int64()),
            ("payload", pa.string()),
        ]
    ),
    "reference_prices": _schema(
        [
            ("delivery_exchange", pa.string()),
            ("provider", pa.string()),
            ("external_instrument_id", pa.string()),
            ("external_update_id", pa.string()),
            ("connection_id", pa.int64()),
            ("source_timestamp", pa.timestamp("us", tz="UTC")),
            ("exchange_timestamp", pa.timestamp("us", tz="UTC")),
            ("received_at", pa.timestamp("us", tz="UTC")),
            ("received_monotonic_ns", pa.int64()),
            ("price", pa.string()),
            ("bid", pa.string()),
            ("ask", pa.string()),
            ("confidence_interval", pa.string()),
            ("publish_slot", pa.int64()),
            ("source_status", pa.string()),
        ]
    ),
    # This is the permanent Tier-B history. Tier A also writes the same
    # research-ready projection so PostgreSQL can retain only a short hot
    # window without losing derived observations.
    "microstructure_observations": _schema(
        _COMMON_FIELDS
        + [
            ("observed_at", pa.timestamp("us", tz="UTC")),
            ("tier", pa.string()),
            ("best_bid", pa.string()),
            ("best_ask", pa.string()),
            ("midpoint", pa.string()),
            ("spread", pa.string()),
            ("spread_bps", pa.string()),
            ("bid_depth_top", pa.string()),
            ("ask_depth_top", pa.string()),
            ("bid_depth_1pct", pa.string()),
            ("ask_depth_1pct", pa.string()),
            ("bid_depth_total", pa.string()),
            ("ask_depth_total", pa.string()),
            ("book_imbalance", pa.string()),
            ("last_trade_price", pa.string()),
            ("recent_trade_count", pa.int64()),
            ("recent_trade_volume", pa.string()),
            ("recent_update_count", pa.int64()),
        ]
    ),
}


@dataclass(slots=True)
class ArchiveCounters:
    rows_uploaded: int = 0
    objects_uploaded: int = 0
    uncompressed_bytes: int = 0
    compressed_bytes: int = 0
    upload_failures: int = 0
    degraded_rows: int = 0
    max_queue_rows: int = 0
    max_queue_bytes: int = 0
    upload_latency_seconds_total: float = 0.0


class ArchiveWriter:
    """Bounded async Parquet spool and idempotent S3 uploader."""

    def __init__(
        self,
        settings: Settings,
        database: Any,
        *,
        object_store: ObjectStore | None = None,
    ) -> None:
        self.settings = settings
        self.database = database
        self.object_store = object_store or S3ObjectStore(settings)
        self.queue: asyncio.Queue[ArchiveRecord] = asyncio.Queue(
            maxsize=settings.archive_queue_max_rows
        )
        self._queued_bytes = 0
        self._bytes_condition = asyncio.Condition()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._oldest_enqueued_monotonic: float | None = None
        self.counters = ArchiveCounters()
        self.run_id: int | None = None
        self.last_error: str | None = None
        self.degraded = False
        self._retry = RetryPolicy(
            max_attempts=settings.archive_upload_max_attempts,
            base_delay_seconds=0.5,
            max_delay_seconds=30,
            jitter_ratio=0.25,
        )

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    async def start(self) -> None:
        self.settings.archive_spool_directory.mkdir(parents=True, exist_ok=True)
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="parquet-archive-writer")

    async def put(self, record: ArchiveRecord) -> None:
        if self._task is not None and self._task.done():
            await self._task
            raise RuntimeError("archive writer stopped unexpectedly")
        try:
            async with asyncio.timeout(self.settings.archive_enqueue_timeout_seconds):
                async with self._bytes_condition:
                    await self._bytes_condition.wait_for(
                        lambda: self._queued_bytes + record.estimated_bytes
                        <= self.settings.archive_queue_max_bytes
                    )
                    self._queued_bytes += record.estimated_bytes
                try:
                    await self.queue.put(record)
                except BaseException:
                    await self._release_bytes(record.estimated_bytes)
                    raise
        except TimeoutError as exc:
            self.degraded = True
            self.last_error = "archive queue backpressure timeout"
            await self.database.record_archive_degradation(
                run_id=self.run_id,
                stream=record.stream,
                priority=record.priority,
                reason="bounded_queue_timeout",
                rows_affected=1,
                bytes_affected=record.estimated_bytes,
            )
            raise ArchiveBackpressureError(
                f"archive queue remained full for {self.settings.archive_enqueue_timeout_seconds}s"
            ) from exc
        self._oldest_enqueued_monotonic = (
            self._oldest_enqueued_monotonic or time.monotonic()
        )
        self.counters.max_queue_rows = max(self.counters.max_queue_rows, self.queue.qsize())
        self.counters.max_queue_bytes = max(self.counters.max_queue_bytes, self._queued_bytes)

    async def run(self) -> None:
        await self._retry_pending_once()
        while not self._stop.is_set() or not self.queue.empty():
            records: list[ArchiveRecord] = []
            bytes_buffered = 0
            try:
                first = await asyncio.wait_for(
                    self.queue.get(), timeout=self.settings.archive_flush_seconds
                )
                records.append(first)
                bytes_buffered += first.estimated_bytes
            except TimeoutError:
                await self._retry_pending_once()
                continue
            deadline = time.monotonic() + self.settings.archive_flush_seconds
            while (
                len(records) < self.settings.archive_batch_rows
                and bytes_buffered < self.settings.archive_batch_bytes
                and time.monotonic() < deadline
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(
                        self.queue.get(), timeout=min(remaining, 0.25)
                    )
                except TimeoutError:
                    if self._stop.is_set():
                        break
                    continue
                records.append(item)
                bytes_buffered += item.estimated_bytes
            try:
                grouped: dict[tuple[str, str, int], list[ArchiveRecord]] = defaultdict(list)
                for record in records:
                    timestamp = record.partition_timestamp.astimezone(UTC)
                    grouped[(record.stream, timestamp.date().isoformat(), timestamp.hour)].append(record)
                for group in grouped.values():
                    await self._prepare_and_upload(group)
            finally:
                for record in records:
                    self.queue.task_done()
                    await self._release_bytes(record.estimated_bytes)
                if self.queue.empty():
                    self._oldest_enqueued_monotonic = None
        await self._retry_pending_once()

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _prepare_and_upload(self, records: list[ArchiveRecord]) -> None:
        stream = records[0].stream
        schema = STREAM_SCHEMAS.get(stream)
        if schema is None:
            raise ValueError(f"unsupported archive stream {stream!r}")
        timestamp = records[0].partition_timestamp.astimezone(UTC)
        provisional = self.settings.archive_spool_directory / f"preparing-{uuid.uuid4().hex}.parquet"
        uncompressed = sum(record.estimated_bytes for record in records)
        spool_bytes = await asyncio.to_thread(
            _directory_size, self.settings.archive_spool_directory
        )
        if spool_bytes + uncompressed > self.settings.archive_spool_max_bytes:
            self.degraded = True
            self.last_error = "archive spool capacity exceeded"
            await self.database.record_archive_degradation(
                run_id=self.run_id,
                stream=stream,
                priority=min(record.priority for record in records),
                reason="archive_spool_capacity_exceeded",
                rows_affected=len(records),
                bytes_affected=uncompressed,
                details={
                    "spool_bytes": spool_bytes,
                    "spool_max_bytes": self.settings.archive_spool_max_bytes,
                },
            )
            raise ArchiveBackpressureError(self.last_error)
        try:
            await asyncio.to_thread(
                _write_parquet,
                provisional,
                schema,
                [record.data for record in records],
                self.settings.archive_compression,
            )
        except Exception as exc:
            await self._quarantine_serialization_failure(records, exc)
            return
        digest = await asyncio.to_thread(_sha256, provisional)
        object_key = self._object_key(stream, timestamp, digest)
        final_path = self.settings.archive_spool_directory / f"{digest}.parquet"
        if final_path.exists():
            provisional.unlink(missing_ok=True)
        else:
            provisional.replace(final_path)
        compressed = final_path.stat().st_size
        source_times = [
            parsed
            for record in records
            if (parsed := parse_timestamp(record.data.get("source_timestamp"))) is not None
        ]
        received_times = [
            parsed
            for record in records
            if (parsed := parse_timestamp(record.data.get("received_at"))) is not None
        ]
        object_id = await self.database.register_archive_object(
            stream=stream,
            schema_version=SCHEMA_VERSION,
            object_key=object_key,
            content_hash=digest,
            compression=self.settings.archive_compression,
            row_count=len(records),
            uncompressed_bytes=uncompressed,
            compressed_bytes=compressed,
            min_source_timestamp=min(source_times) if source_times else None,
            max_source_timestamp=max(source_times) if source_times else None,
            min_received_at=min(received_times) if received_times else None,
            max_received_at=max(received_times) if received_times else None,
            partition_date=timestamp.date(),
            partition_hour=timestamp.hour,
            local_spool_path=str(final_path),
        )
        uploaded = await self._upload_with_retry(object_id, final_path, object_key, digest)
        if uploaded and stream == "raw_rest":
            for record in records:
                await self.database.record_raw_rest_provenance(
                    archive_object_id=object_id,
                    object_key=object_key,
                    value=record.data,
                )

    async def _upload_with_retry(
        self, object_id: int, local_path: Path, object_key: str, digest: str
    ) -> bool:
        for attempt in range(1, self.settings.archive_upload_max_attempts + 1):
            started = time.monotonic()
            try:
                await self.database.mark_archive_upload_attempt(object_id, attempt)
                await self.object_store.put_file(local_path, object_key, digest)
                head = await self.object_store.head(object_key)
                metadata = {str(k).lower(): str(v) for k, v in dict(head.get("Metadata") or {}).items()}
                if int(head.get("ContentLength", -1)) != local_path.stat().st_size:
                    raise IOError("uploaded object size verification failed")
                if metadata.get("sha256") not in {None, digest}:
                    raise IOError("uploaded object hash metadata verification failed")
                await self.database.mark_archive_uploaded(object_id)
                local_path.unlink(missing_ok=True)
                self.counters.objects_uploaded += 1
                row = await self.database.archive_object_counts(object_id)
                self.counters.rows_uploaded += int(row["row_count"])
                self.counters.uncompressed_bytes += int(row["uncompressed_bytes"])
                self.counters.compressed_bytes += int(row["compressed_bytes"])
                self.counters.upload_latency_seconds_total += time.monotonic() - started
                self.last_error = None
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.counters.upload_failures += 1
                self.degraded = True
                self.last_error = f"{type(exc).__name__}: {exc}"
                await self.database.mark_archive_retrying(object_id, self.last_error)
                if attempt < self.settings.archive_upload_max_attempts:
                    await asyncio.sleep(self._retry.delay(attempt))
        LOGGER.error(
            "Archive object remains spooled after upload retries",
            extra={"object_key": object_key, "error": self.last_error},
        )
        return False

    async def _retry_pending_once(self) -> None:
        for value in await self.database.pending_archive_objects(limit=20):
            local_path = Path(str(value["local_spool_path"] or ""))
            if not local_path.is_file():
                await self.database.mark_archive_failed(
                    int(value["id"]), "spool file is missing"
                )
                self.degraded = True
                continue
            await self._upload_with_retry(
                int(value["id"]),
                local_path,
                str(value["object_key"]),
                str(value["content_hash"]),
            )

    async def _quarantine_serialization_failure(
        self, records: list[ArchiveRecord], exc: Exception
    ) -> None:
        self.degraded = True
        self.last_error = f"Parquet serialization failed: {type(exc).__name__}: {exc}"
        target = self.settings.archive_spool_directory / f"quarantine-{uuid.uuid4().hex}.jsonl"

        def write() -> None:
            target.write_text(
                "\n".join(canonical_json(dict(record.data)) for record in records),
                encoding="utf-8",
            )

        await asyncio.to_thread(write)
        await self.database.record_archive_degradation(
            run_id=self.run_id,
            stream=records[0].stream,
            priority=min(record.priority for record in records),
            reason="parquet_serialization_failed_quarantined",
            rows_affected=len(records),
            bytes_affected=sum(record.estimated_bytes for record in records),
            details={"path": str(target), "error": self.last_error},
        )

    async def _release_bytes(self, amount: int) -> None:
        async with self._bytes_condition:
            self._queued_bytes = max(0, self._queued_bytes - amount)
            self._bytes_condition.notify_all()

    def _object_key(self, stream: str, timestamp: datetime, digest: str) -> str:
        prefix = f"{self.settings.s3_prefix}/" if self.settings.s3_prefix else ""
        return (
            f"{prefix}schema_version={SCHEMA_VERSION}/exchange=polymarket/"
            f"stream={stream}/date={timestamp.date().isoformat()}/hour={timestamp.hour:02d}/"
            f"part-{digest[:24]}.parquet"
        )

    def metrics(self) -> dict[str, Any]:
        compressed = self.counters.compressed_bytes
        return {
            "healthy": not self.degraded,
            "queue_depth": self.queue.qsize(),
            "queue_bytes": self._queued_bytes,
            "oldest_queued_seconds": (
                max(0.0, time.monotonic() - self._oldest_enqueued_monotonic)
                if self._oldest_enqueued_monotonic is not None
                else 0.0
            ),
            "objects_uploaded": self.counters.objects_uploaded,
            "rows_uploaded": self.counters.rows_uploaded,
            "uncompressed_bytes_uploaded": self.counters.uncompressed_bytes,
            "compressed_bytes_uploaded": compressed,
            "compression_ratio": (
                self.counters.uncompressed_bytes / compressed if compressed else None
            ),
            "upload_failures": self.counters.upload_failures,
            "max_queue_depth": self.counters.max_queue_rows,
            "max_queue_bytes": self.counters.max_queue_bytes,
            "last_error": self.last_error,
            "spool_bytes": _directory_size(self.settings.archive_spool_directory),
        }


def _normalise_value(value: Any, data_type: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_timestamp(data_type):
        parsed = parse_timestamp(value)
        return parsed.astimezone(UTC) if parsed is not None else None
    if pa.types.is_string(data_type):
        if isinstance(value, (dict, list, tuple)):
            return canonical_json(value)
        if isinstance(value, Decimal):
            return format(value, "f")
        return str(value)
    if pa.types.is_integer(data_type):
        return int(value)
    if pa.types.is_boolean(data_type):
        return bool(value)
    return value


def _write_parquet(
    path: Path,
    schema: pa.Schema,
    values: list[Mapping[str, Any]],
    compression: str,
) -> None:
    rows = [
        {
            field.name: _normalise_value(value.get(field.name), field.type)
            for field in schema
        }
        for value in values
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(
        table,
        path,
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
        data_page_size=1024 * 1024,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
