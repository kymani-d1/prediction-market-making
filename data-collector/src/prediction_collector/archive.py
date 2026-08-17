from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import shutil
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Protocol

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

from prediction_collector.common.retry import RetryPolicy
from prediction_collector.common.utils import (
    as_decimal,
    canonical_json,
    parse_timestamp,
    utc_now,
)
from prediction_collector.config import Settings


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 2

SIDE_CODES = {"buy": 1, "bid": 1, "bids": 1, "yes": 1,
              "sell": 2, "ask": 2, "asks": 2, "no": 2}
ACTION_CODES = {"set": 1, "absolute": 1, "update": 1, "delete": 2,
                "remove": 2, "reset": 3, "delta": 4, "add": 4}
SNAPSHOT_TYPE_CODES = {"exchange": 1, "initial": 1, "recovery": 2,
                       "reconciliation": 3, "closing": 4, "checkpoint": 5}
ENTITY_KIND_CODES = {"market": 1, "token": 2}


class ArchiveBackpressureError(RuntimeError):
    pass


class ObjectStore(Protocol):
    async def put_file(self, local_path: Path, object_key: str, content_hash: str) -> None: ...
    async def head(self, object_key: str) -> Mapping[str, Any]: ...
    async def download(self, object_key: str, local_path: Path) -> None: ...
    async def list_keys(self, prefix: str) -> list[str]: ...
    async def delete(self, object_key: str) -> None: ...


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

    async def delete(self, object_key: str) -> None:
        await asyncio.to_thread(self.client.delete_object, Bucket=self.bucket, Key=object_key)


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

    async def delete(self, object_key: str) -> None:
        (self.root / object_key).unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class ArchiveRecord:
    record_id: str
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
        return cls(uuid.uuid4().hex, stream, dict(data), priority, timestamp, estimate)


_BOOK_COMMON_FIELDS: list[tuple[str, pa.DataType]] = [
    ("market_key", pa.int64()),
    ("token_key", pa.int64()),
    ("connection_id", pa.int64()),
    ("source_ts_ns", pa.int64()),
    ("exchange_ts_ns", pa.int64()),
    ("received_ts_ns", pa.int64()),
    ("received_monotonic_ns", pa.int64()),
    ("book_hash", pa.string()),
]

_LEVEL_TYPE = pa.struct(
    [
        pa.field("side", pa.int8(), nullable=False),
        pa.field("price_mantissa", pa.int64(), nullable=False),
        pa.field("price_scale", pa.int8(), nullable=False),
        pa.field("size_mantissa", pa.int64(), nullable=False),
        pa.field("size_scale", pa.int8(), nullable=False),
    ]
)


def _schema(fields: list[tuple[str, pa.DataType]]) -> pa.Schema:
    return pa.schema(
        [pa.field(name, kind) for name, kind in fields],
        metadata={b"schema_version": str(SCHEMA_VERSION).encode(), b"exchange": b"polymarket"},
    )


STREAM_SCHEMAS: dict[str, pa.Schema] = {
    "orderbook_updates": _schema(
        _BOOK_COMMON_FIELDS
        + [
            ("side", pa.int8()),
            ("action", pa.int8()),
            ("price_mantissa", pa.int64()),
            ("price_scale", pa.int8()),
            ("size_mantissa", pa.int64()),
            ("size_scale", pa.int8()),
        ]
    ),
    "orderbook_snapshots": _schema(
        _BOOK_COMMON_FIELDS
        + [
            ("snapshot_type", pa.int8()),
            ("levels", pa.list_(_LEVEL_TYPE)),
            ("is_reconciliation", pa.bool_()),
        ]
    ),
    "raw_ws": _schema(
        [
            ("market_key", pa.int64()),
            ("token_key", pa.int64()),
            ("connection_id", pa.int64()),
            ("source_ts_ns", pa.int64()),
            ("exchange_ts_ns", pa.int64()),
            ("received_ts_ns", pa.int64()),
            ("channel", pa.string()),
            ("message_type", pa.string()),
            ("payload", pa.string()),
            ("payload_hash", pa.string()),
        ]
    ),
    "raw_rest": _schema(
        [
            ("content_hash", pa.string()),
            ("response_bytes", pa.int64()),
            ("payload", pa.string()),
        ]
    ),
    "reference_prices": _schema(
        [
            ("provider", pa.dictionary(pa.int16(), pa.string())),
            ("external_instrument_id", pa.dictionary(pa.int32(), pa.string())),
            ("external_update_id", pa.string()),
            ("connection_id", pa.int64()),
            ("source_ts_ns", pa.int64()),
            ("exchange_ts_ns", pa.int64()),
            ("received_ts_ns", pa.int64()),
            ("received_monotonic_ns", pa.int64()),
            # RTDS TWAPs currently carry more significant digits than fit in
            # an int64 mantissa. Decimal256 preserves the source value exactly
            # without falling back to verbose strings or float rounding.
            ("price", pa.decimal256(76, 36)),
            ("bid", pa.decimal256(76, 36)),
            ("ask", pa.decimal256(76, 36)),
            ("confidence", pa.decimal256(76, 36)),
            ("publish_slot", pa.int64()),
            ("source_status", pa.dictionary(pa.int8(), pa.string())),
        ]
    ),
    # This is only the permanent SAMPLED representation. FULL_L2 observations
    # are hot convenience rows and are deterministically regenerated from L2.
    "microstructure_observations": _schema(
        [
            ("market_key", pa.int64()),
            ("token_key", pa.int64()),
            ("received_ts_ns", pa.int64()),
            ("observation_kind", pa.int8()),
            ("best_bid_mantissa", pa.int64()), ("best_bid_scale", pa.int8()),
            ("best_ask_mantissa", pa.int64()), ("best_ask_scale", pa.int8()),
            ("bid_depth_mantissa", pa.int64()), ("bid_depth_scale", pa.int8()),
            ("ask_depth_mantissa", pa.int64()), ("ask_depth_scale", pa.int8()),
            ("last_trade_mantissa", pa.int64()), ("last_trade_scale", pa.int8()),
            ("recent_trade_count", pa.int32()),
            ("recent_update_count", pa.int32()),
        ]
    ),
    "archive_dictionary": _schema(
        [
            ("entity_kind", pa.int8()),
            ("archive_key", pa.int64()),
            ("parent_archive_key", pa.int64()),
            ("external_id", pa.string()),
            ("observed_ts_ns", pa.int64()),
        ]
    ),
}


def stable_archive_key(entity_kind: str, external_id: str) -> int:
    """Return a restart-stable positive 63-bit key; DB uniqueness detects collisions."""
    digest = hashlib.sha256(
        f"polymarket-archive-v2\0{entity_kind}\0{external_id}".encode("utf-8")
    ).digest()
    return (int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)) or 1


def decimal_components(value: Any) -> tuple[int | None, int | None]:
    """Encode a finite decimal exactly as signed mantissa plus base-10 scale."""
    if value is None:
        return None, None
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("archive decimals must be finite")
    sign, digits, exponent = decimal.as_tuple()
    mantissa = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        mantissa = -mantissa
    scale = -exponent
    if not -(1 << 63) <= mantissa < (1 << 63):
        raise OverflowError(f"decimal mantissa does not fit int64: {value!r}")
    if not -128 <= scale <= 127:
        raise OverflowError(f"decimal scale does not fit int8: {value!r}")
    return mantissa, scale


def decimal_from_components(mantissa: int | None, scale: int | None) -> Decimal | None:
    if mantissa is None or scale is None:
        return None
    return Decimal(mantissa).scaleb(-scale)


def _timestamp_ns(value: Any) -> int | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    delta = parsed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _decimal_fields(target: dict[str, Any], prefix: str, value: Any) -> None:
    mantissa, scale = decimal_components(value)
    target[f"{prefix}_mantissa"] = mantissa
    target[f"{prefix}_scale"] = scale


def compact_archive_row(stream: str, value: Mapping[str, Any]) -> dict[str, Any]:
    """Project verbose normalized records onto compact analytical schemas."""
    market = value.get("market_external_id")
    token = value.get("outcome_external_id")
    market_key = stable_archive_key("market", str(market)) if market else None
    token_key = stable_archive_key("token", str(token)) if token else None
    if stream in {"orderbook_updates", "orderbook_snapshots"}:
        result: dict[str, Any] = {
            "market_key": market_key,
            "token_key": token_key,
            "connection_id": value.get("connection_id"),
            "source_ts_ns": _timestamp_ns(value.get("source_timestamp")),
            "exchange_ts_ns": _timestamp_ns(value.get("exchange_timestamp")),
            "received_ts_ns": _timestamp_ns(value.get("received_at")),
            "received_monotonic_ns": value.get("received_monotonic_ns"),
            "book_hash": value.get("book_hash"),
        }
        if stream == "orderbook_updates":
            side = SIDE_CODES.get(str(value.get("side") or "").lower())
            size_value = value.get("size")
            operation = str(value.get("operation") or "set").lower()
            if size_value is None and value.get("size_delta") is not None:
                size_value = value.get("size_delta")
                operation = "delta"
            action = 2 if size_value is not None and Decimal(str(size_value)) <= 0 else ACTION_CODES.get(operation, 1)
            result.update({"side": side, "action": action})
            _decimal_fields(result, "price", value.get("price"))
            _decimal_fields(result, "size", size_value)
        else:
            levels: list[dict[str, Any]] = []
            for side_name, source in (("buy", value.get("bids") or []), ("sell", value.get("asks") or [])):
                for level in source:
                    if not isinstance(level, (list, tuple)) or len(level) < 2:
                        continue
                    price_mantissa, price_scale = decimal_components(level[0])
                    size_mantissa, size_scale = decimal_components(level[1])
                    levels.append(
                        {
                            "side": SIDE_CODES[side_name],
                            "price_mantissa": price_mantissa,
                            "price_scale": price_scale,
                            "size_mantissa": size_mantissa,
                            "size_scale": size_scale,
                        }
                    )
            result.update(
                {
                    "snapshot_type": SNAPSHOT_TYPE_CODES.get(str(value.get("snapshot_type") or "exchange"), 1),
                    "levels": levels,
                    "is_reconciliation": bool(value.get("is_reconciliation")),
                }
            )
        return result
    if stream == "microstructure_observations":
        result = {
            "market_key": market_key,
            "token_key": token_key,
            "received_ts_ns": _timestamp_ns(value.get("observed_at") or value.get("received_at")),
            "observation_kind": 2 if value.get("observation_kind") == "heartbeat" else 1,
            "recent_trade_count": value.get("recent_trade_count") or 0,
            "recent_update_count": value.get("recent_update_count") or 0,
        }
        for prefix, source in (
            ("best_bid", "best_bid"), ("best_ask", "best_ask"),
            ("bid_depth", "bid_depth_total"), ("ask_depth", "ask_depth_total"),
            ("last_trade", "last_trade_price"),
        ):
            _decimal_fields(result, prefix, value.get(source))
        return result
    if stream == "reference_prices":
        result = {
            "provider": value.get("provider"),
            "external_instrument_id": value.get("external_instrument_id"),
            "external_update_id": value.get("external_update_id"),
            "connection_id": value.get("connection_id"),
            "source_ts_ns": _timestamp_ns(value.get("source_timestamp")),
            "exchange_ts_ns": _timestamp_ns(value.get("exchange_timestamp")),
            "received_ts_ns": _timestamp_ns(value.get("received_at")),
            "received_monotonic_ns": value.get("received_monotonic_ns"),
            "publish_slot": value.get("publish_slot"),
            "source_status": value.get("source_status"),
            "price": as_decimal(value.get("price")),
            "bid": as_decimal(value.get("bid")),
            "ask": as_decimal(value.get("ask")),
            "confidence": as_decimal(value.get("confidence_interval")),
        }
        return result
    if stream == "raw_ws":
        return {
            "market_key": market_key,
            "token_key": token_key,
            "connection_id": value.get("connection_id"),
            "source_ts_ns": _timestamp_ns(value.get("source_timestamp")),
            "exchange_ts_ns": _timestamp_ns(value.get("exchange_timestamp")),
            "received_ts_ns": _timestamp_ns(value.get("received_at")),
            "channel": value.get("channel"),
            "message_type": value.get("message_type"),
            "payload": value.get("payload"),
            "payload_hash": value.get("payload_hash"),
        }
    if stream == "raw_rest":
        return {
            "content_hash": value.get("content_hash"),
            "response_bytes": value.get("response_bytes"),
            "payload": value.get("payload"),
        }
    if stream == "archive_dictionary":
        return dict(value)
    raise ValueError(f"unsupported archive stream {stream!r}")


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
    raw_rest_objects_reused: int = 0
    stream_rows: dict[str, int] = field(default_factory=dict)
    stream_uncompressed_bytes: dict[str, int] = field(default_factory=dict)
    stream_compressed_bytes: dict[str, int] = field(default_factory=dict)
    objects_compacted: int = 0
    compaction_objects_before: int = 0
    compaction_objects_after: int = 0
    compaction_bytes_before: int = 0
    compaction_bytes_after: int = 0
    compaction_failures: int = 0


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
        # Raw REST consists of a small number of large, replayable records;
        # live evidence consists of many small, latency-sensitive records.
        # Give each an independently draining lane while keeping their maximum
        # queued rows/bytes within the existing aggregate limits. Durability
        # priority is deliberately not used here: raw REST priority=2 means
        # "must retain", not "must run ahead of irreplaceable live L2".
        raw_rest_max_rows = max(1, settings.archive_queue_max_rows // 20)
        live_max_rows = max(
            1, settings.archive_queue_max_rows - raw_rest_max_rows
        )
        raw_rest_max_bytes = max(1, settings.archive_queue_max_bytes * 3 // 4)
        live_max_bytes = max(
            1, settings.archive_queue_max_bytes - raw_rest_max_bytes
        )
        self.queue: asyncio.Queue[ArchiveRecord] = asyncio.Queue(
            maxsize=live_max_rows
        )
        self.raw_rest_queue: asyncio.Queue[ArchiveRecord] = asyncio.Queue(
            maxsize=raw_rest_max_rows
        )
        self._queued_bytes = 0
        self._lane_queued_bytes = {"live": 0, "raw_rest": 0}
        self._lane_max_bytes = {
            "live": live_max_bytes,
            "raw_rest": raw_rest_max_bytes,
        }
        self._bytes_condition = asyncio.Condition()
        self._stop = asyncio.Event()
        self._flush_requested = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._compaction_task: asyncio.Task[None] | None = None
        self._oldest_enqueued_monotonic: float | None = None
        self._dictionary_emitted: set[tuple[str, int]] = set()
        self._active_record_ids: set[str] = set()
        self._spilled_record_ids: set[str] = set()
        self._replayed_record_ids: set[str] = set()
        self._journal_lock = asyncio.Lock()
        self._spill_replay_lock = asyncio.Lock()
        # A content-addressed spool filename is a process-local mutable resource
        # until its immutable remote object and manifest are committed. Lanes,
        # maintenance recovery, and compaction may run concurrently, but only
        # one of them may publish/stat/upload/unlink a given digest at a time.
        # Different digests remain fully concurrent.
        self._spool_owners: dict[str, object] = {}
        self._spool_owner_released: dict[str, asyncio.Event] = {}
        self._journal_path = settings.archive_spool_directory / "ingress-journal.jsonl"
        self._spool_bytes = 0
        self._spool_reserved_bytes = 0
        self.counters = ArchiveCounters()
        self.run_id: int | None = None
        self.last_error: str | None = None
        self.degraded = False
        self._transient_degradation_pending = False
        self._remote_recovery_errors: dict[int, str] = {}
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
            for path in self.settings.archive_spool_directory.glob(
                "preparing-*.parquet"
            ):
                path.unlink(missing_ok=True)
            await self._cleanup_committed_spool_files()
            self._spool_bytes = await asyncio.to_thread(
                _directory_size, self.settings.archive_spool_directory
            )
            # Sweep legacy timeout rows from a superseded run once this
            # service's persistent journal, queues, and uploads prove clean.
            # The resolver is job-type scoped in PostgreSQL.
            self._transient_degradation_pending = True
            self._task = asyncio.create_task(self.run(), name="parquet-archive-writer")
            await self._recover_journal()
        if (
            self.settings.archive_compaction_enabled
            and self._compaction_task is None
            and hasattr(self.database, "archive_compaction_candidates")
        ):
            self._compaction_task = asyncio.create_task(
                self._compaction_loop(), name="parquet-archive-compactor"
            )

    async def _cleanup_committed_spool_files(self) -> None:
        """Finish crash-interrupted cleanup after the manifest was committed."""
        if not hasattr(self.database, "archive_object_by_content_hash"):
            return
        for path in self.settings.archive_spool_directory.glob("*.parquet"):
            digest = path.stem.lower()
            if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
                continue
            state = await self.database.archive_object_by_content_hash(digest)
            if state is None or state["status"] != "uploaded":
                continue
            actual = await asyncio.to_thread(_sha256, path)
            if actual != digest:
                self.degraded = True
                self.last_error = "committed archive spool hash mismatch"
                continue
            path.unlink(missing_ok=True)

    async def put(self, record: ArchiveRecord) -> None:
        await self._ensure_identifiers(record)
        if record.stream == "raw_rest":
            existing = await self._existing_raw_rest(record)
            if existing is not None:
                await self.database.record_raw_rest_provenance(
                    archive_object_id=int(existing["id"]),
                    object_key=str(existing["object_key"]),
                    value=record.data,
                )
                self.counters.raw_rest_objects_reused += 1
                return
        await self._durable_enqueue(record)

    async def _durable_enqueue(self, record: ArchiveRecord) -> None:
        try:
            await self._append_journal(record)
        except ArchiveBackpressureError:
            self.degraded = True
            self.last_error = "archive spool hard capacity exceeded"
            await self.database.record_archive_degradation(
                run_id=self.run_id,
                stream=record.stream,
                priority=record.priority,
                reason="archive_spool_hard_capacity_exceeded",
                rows_affected=1,
                bytes_affected=record.estimated_bytes,
                details={
                    "spool_bytes": (
                        self._spool_bytes + self._spool_reserved_bytes
                    ),
                    "spool_max_bytes": self.settings.archive_spool_max_bytes,
                },
            )
            raise ArchiveBackpressureError(self.last_error)
        try:
            await self._enqueue(record)
        except asyncio.CancelledError:
            # Source-task cancellation can race with a clean collector stop
            # after the journal fsync but before queue ownership. Preserve the
            # record for the archive drain/restart path.
            self._active_record_ids.discard(record.record_id)
            self._spilled_record_ids.add(record.record_id)
            raise

    async def _enqueue(
        self,
        record: ArchiveRecord,
        *,
        wait_for_raw_rest_capacity: bool = True,
        report_timeout_degradation: bool = True,
        timeout_seconds: float | None = None,
    ) -> bool:
        if self._task is not None and self._task.done():
            await self._task
            raise RuntimeError("archive writer stopped unexpectedly")
        self._active_record_ids.add(record.record_id)
        lane = self._lane_for(record)
        target_queue = self._queue_for_lane(lane)

        async def enqueue_when_capacity_available() -> None:
            async with self._bytes_condition:
                while not (
                    (
                        self._queued_bytes == 0
                        or self._queued_bytes + record.estimated_bytes
                           <= self.settings.archive_queue_max_bytes
                    )
                    and (
                        self._lane_queued_bytes[lane] == 0
                        or self._lane_queued_bytes[lane] + record.estimated_bytes
                           <= self._lane_max_bytes[lane]
                    )
                ):
                    if self._task is not None and self._task.done():
                        await self._task
                        raise RuntimeError("archive writer stopped unexpectedly")
                    # The bounded wait lets us notice a failed consumer even
                    # when no future byte-release notification can arrive.
                    try:
                        await asyncio.wait_for(
                            self._bytes_condition.wait(), timeout=1.0
                        )
                    except TimeoutError:
                        continue
                self._queued_bytes += record.estimated_bytes
                self._lane_queued_bytes[lane] += record.estimated_bytes
            try:
                while True:
                    try:
                        await asyncio.wait_for(
                            target_queue.put(record), timeout=1.0
                        )
                        break
                    except TimeoutError:
                        if self._task is not None and self._task.done():
                            await self._task
                            raise RuntimeError("archive writer stopped unexpectedly")
            except BaseException:
                await self._release_bytes(record.estimated_bytes, lane=lane)
                raise

        try:
            if record.stream == "raw_rest" and wait_for_raw_rest_capacity:
                # The ingress journal already owns the record durably. REST is
                # replayable and not latency-sensitive, so exert real producer
                # backpressure until both row and byte capacity become free.
                await enqueue_when_capacity_available()
            else:
                async with asyncio.timeout(
                    timeout_seconds
                    if timeout_seconds is not None
                    else self.settings.archive_enqueue_timeout_seconds
                ):
                    await enqueue_when_capacity_available()
        except TimeoutError:
            self._active_record_ids.discard(record.record_id)
            self._spilled_record_ids.add(record.record_id)
            if report_timeout_degradation:
                self.degraded = True
                self._transient_degradation_pending = True
                self.last_error = "archive queue backpressure timeout"
                await self.database.record_archive_degradation(
                    run_id=self.run_id,
                    stream=record.stream,
                    priority=record.priority,
                    reason="bounded_queue_timeout",
                    rows_affected=1,
                    bytes_affected=record.estimated_bytes,
                    details={"record_id": record.record_id, "lane": lane},
                )
            # The append-only ingress journal already owns the record.  Do not
            # disconnect the source merely because the in-memory queue is
            # temporarily full; the archive task replays spilled journal rows
            # as capacity becomes available. Hard spool capacity still fails
            # explicitly before the journal append above.
            return False
        self._oldest_enqueued_monotonic = (
            self._oldest_enqueued_monotonic or time.monotonic()
        )
        self.counters.max_queue_rows = max(
            self.counters.max_queue_rows, self._queue_depth()
        )
        self.counters.max_queue_bytes = max(self.counters.max_queue_bytes, self._queued_bytes)
        if record.record_id in self._spilled_record_ids:
            self._replayed_record_ids.add(record.record_id)
        self._spilled_record_ids.discard(record.record_id)
        return True

    @staticmethod
    def _lane_for(record: ArchiveRecord) -> str:
        return "raw_rest" if record.stream == "raw_rest" else "live"

    def _queue_for_lane(self, lane: str) -> asyncio.Queue[ArchiveRecord]:
        return self.raw_rest_queue if lane == "raw_rest" else self.queue

    def _queue_depth(self) -> int:
        return self.queue.qsize() + self.raw_rest_queue.qsize()

    def _queues_empty(self) -> bool:
        return self.queue.empty() and self.raw_rest_queue.empty()

    async def _ensure_identifiers(self, record: ArchiveRecord) -> None:
        if record.stream in {"raw_rest", "reference_prices", "archive_dictionary"}:
            return
        market = record.data.get("market_external_id")
        token = record.data.get("outcome_external_id")
        parent_key = stable_archive_key("market", str(market)) if market else None
        for entity_kind, external_id, parent in (
            ("market", market, None), ("token", token, parent_key)
        ):
            if not external_id:
                continue
            archive_key = stable_archive_key(entity_kind, str(external_id))
            identity = (entity_kind, archive_key)
            if identity in self._dictionary_emitted:
                continue
            created: bool | None = None
            if hasattr(self.database, "ensure_archive_identifier"):
                created = await self.database.ensure_archive_identifier(
                    entity_kind=entity_kind,
                    archive_key=archive_key,
                    external_id=str(external_id),
                    parent_archive_key=parent,
                )
            self._dictionary_emitted.add(identity)
            if created is False:
                continue
            now = utc_now()
            dictionary_record = ArchiveRecord.create(
                    "archive_dictionary",
                    {
                        "entity_kind": ENTITY_KIND_CODES[entity_kind],
                        "archive_key": archive_key,
                        "parent_archive_key": parent,
                        "external_id": str(external_id),
                        "observed_ts_ns": _timestamp_ns(now),
                    },
                    priority=1,
                    partition_timestamp=now,
                )
            await self._durable_enqueue(dictionary_record)

    async def _append_journal(self, record: ArchiveRecord) -> None:
        entry = canonical_json(
            {
                "record_id": record.record_id,
                "stream": record.stream,
                "data": dict(record.data),
                "priority": record.priority,
                "partition_timestamp": record.partition_timestamp,
                "estimated_bytes": record.estimated_bytes,
            }
        )
        encoded_bytes = len((entry + "\n").encode("utf-8"))

        def append() -> None:
            with self._journal_path.open("a", encoding="utf-8") as handle:
                handle.write(entry + "\n")
                handle.flush()
                os.fsync(handle.fileno())

        async with self._journal_lock:
            if (
                self._spool_bytes
                + self._spool_reserved_bytes
                + encoded_bytes
                + self.settings.archive_batch_bytes
                > self.settings.archive_spool_max_bytes
            ):
                raise ArchiveBackpressureError(
                    "archive spool hard capacity exceeded before journal append"
                )
            await asyncio.to_thread(append)
            self._spool_bytes += encoded_bytes

    async def _recover_journal(self) -> None:
        if not self._journal_path.is_file():
            return

        def read() -> list[dict[str, Any]]:
            values: list[dict[str, Any]] = []
            for line in self._journal_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    values.append(json.loads(line))
            return values

        values = await asyncio.to_thread(read)
        if values:
            # A restarted service may be recovering timeout spills recorded by
            # its superseded run. Resolve those legacy run-scoped events only
            # after this journal is completely uploaded and acknowledged.
            self._transient_degradation_pending = True
        for value in values:
            record = ArchiveRecord(
                record_id=str(value["record_id"]),
                stream=str(value["stream"]),
                data=dict(value["data"]),
                priority=int(value["priority"]),
                partition_timestamp=parse_timestamp(value["partition_timestamp"]) or utc_now(),
                estimated_bytes=int(value["estimated_bytes"]),
            )
            await self._ensure_identifiers(record)
            await self._enqueue(record)

    async def _acknowledge_journal(self, records: list[ArchiveRecord]) -> None:
        if not records or not self._journal_path.is_file():
            return
        acknowledged = {record.record_id for record in records}

        def rewrite() -> int:
            previous_size = self._journal_path.stat().st_size
            retained: list[str] = []
            for line in self._journal_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    record_id = str(json.loads(line).get("record_id"))
                except json.JSONDecodeError:
                    retained.append(line)
                    continue
                if record_id not in acknowledged:
                    retained.append(line)
            temporary = self._journal_path.with_suffix(".tmp")
            temporary.write_text(
                "".join(f"{line}\n" for line in retained), encoding="utf-8"
            )
            temporary.replace(self._journal_path)
            current_size = self._journal_path.stat().st_size
            return current_size - previous_size

        async with self._journal_lock:
            size_delta = await asyncio.to_thread(rewrite)
            self._spool_bytes = max(0, self._spool_bytes + size_delta)

    async def _refresh_spool_bytes(self) -> int:
        async with self._journal_lock:
            size = await asyncio.to_thread(
                _directory_size, self.settings.archive_spool_directory
            )
            self._spool_bytes = size
        return size

    async def _reserve_spool_serialization(self, amount: int) -> bool:
        async with self._journal_lock:
            if (
                self._spool_bytes + self._spool_reserved_bytes + amount
                > self.settings.archive_spool_max_bytes
            ):
                return False
            self._spool_reserved_bytes += amount
            return True

    async def _release_spool_serialization(self, amount: int) -> None:
        async with self._journal_lock:
            self._spool_reserved_bytes = max(
                0, self._spool_reserved_bytes - amount
            )
            self._spool_bytes = await asyncio.to_thread(
                _directory_size, self.settings.archive_spool_directory
            )

    async def _existing_raw_rest(
        self, record: ArchiveRecord
    ) -> Mapping[str, Any] | None:
        content = str(record.data.get("content_hash") or "")
        if not content or not hasattr(self.database, "raw_rest_archive_by_content_hash"):
            return None
        return await self.database.raw_rest_archive_by_content_hash(content)

    async def _claim_spool_object(
        self, digest: str, *, wait: bool
    ) -> object | None:
        """Claim one content-addressed local object without blocking other hashes."""
        while digest in self._spool_owners:
            if not wait:
                return None
            released = self._spool_owner_released.setdefault(
                digest, asyncio.Event()
            )
            await released.wait()
        token = object()
        self._spool_owners[digest] = token
        return token

    def _release_spool_object(self, digest: str, token: object) -> None:
        """Release ownership synchronously so cancellation cannot strand it."""
        if self._spool_owners.get(digest) is not token:
            raise RuntimeError(f"archive spool ownership mismatch for {digest}")
        del self._spool_owners[digest]
        released = self._spool_owner_released.pop(digest, None)
        if released is not None:
            released.set()

    async def run(self) -> None:
        await self._retry_pending_once()
        await self._recover_compactions()
        workers = [
            asyncio.create_task(
                self._run_lane("live"), name="parquet-archive-live-lane"
            ),
            asyncio.create_task(
                self._run_lane("raw_rest"), name="parquet-archive-rest-lane"
            ),
            asyncio.create_task(
                self._maintenance_loop(), name="parquet-archive-maintenance"
            ),
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        await self._retry_pending_once()

    async def _run_lane(self, lane: str) -> None:
        queue = self._queue_for_lane(lane)
        while not self._stop.is_set() or not queue.empty():
            records: list[ArchiveRecord] = []
            bytes_buffered = 0
            try:
                first = await asyncio.wait_for(queue.get(), timeout=1.0)
                if first.stream == "__flush__":
                    queue.task_done()
                    first.data["event"].set()
                    continue
                records.append(first)
                bytes_buffered += first.estimated_bytes
            except TimeoutError:
                continue
            deadline = time.monotonic() + self.settings.archive_flush_seconds
            effective_batch_bytes = min(
                self.settings.archive_batch_bytes,
                self.settings.archive_queue_warn_bytes,
                self._lane_max_bytes[lane],
            )
            effective_batch_rows = min(
                self.settings.archive_batch_rows, queue.maxsize
            )
            flush_marker: ArchiveRecord | None = None
            while (
                lane == "live"
                and
                not self._flush_requested.is_set()
                and
                len(records) < effective_batch_rows
                and bytes_buffered < effective_batch_bytes
                and time.monotonic() < deadline
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(
                        queue.get(), timeout=min(remaining, 0.25)
                    )
                except TimeoutError:
                    if self._stop.is_set() or self._flush_requested.is_set():
                        break
                    continue
                if item.stream == "__flush__":
                    flush_marker = item
                    break
                records.append(item)
                bytes_buffered += item.estimated_bytes
            try:
                await self._process_records(records)
            finally:
                for record in records:
                    self._active_record_ids.discard(record.record_id)
                    queue.task_done()
                    await self._release_bytes(record.estimated_bytes, lane=lane)
                if flush_marker is not None:
                    queue.task_done()
                    flush_marker.data["event"].set()
                await self._resolve_recovered_degradations_if_clean()

    async def _process_records(self, records: list[ArchiveRecord]) -> None:
        grouped: dict[
            tuple[str, str, int, str | None], list[ArchiveRecord]
        ] = defaultdict(list)
        for record in records:
            timestamp = record.partition_timestamp.astimezone(UTC)
            # Raw REST bodies are independently content-addressed. All other
            # streams coalesce markets into coarse hourly files.
            payload_identity = (
                str(record.data.get("content_hash"))
                if record.stream == "raw_rest"
                else None
            )
            grouped[
                (
                    record.stream,
                    timestamp.date().isoformat(),
                    timestamp.hour,
                    payload_identity,
                )
            ].append(record)
        completed: list[ArchiveRecord] = []
        for group in grouped.values():
            if await self._prepare_and_upload(group):
                completed.extend(group)
            else:
                self._spilled_record_ids.update(
                    record.record_id for record in group
                )
        await self._acknowledge_journal(completed)
        recovered_ids = [
            record.record_id
            for record in completed
            if record.record_id in self._replayed_record_ids
        ]
        if recovered_ids and hasattr(
            self.database, "resolve_archive_record_degradations"
        ):
            await self.database.resolve_archive_record_degradations(
                recovered_ids
            )
        self._replayed_record_ids.difference_update(recovered_ids)
        # Refresh once per durable batch, not once per source record. This
        # includes failed-upload Parquet files and compactor scratch files.
        await self._refresh_spool_bytes()

    async def _maintenance_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=1.0)
            except TimeoutError:
                await self._retry_pending_once()
                await self._retry_spilled_journal_once()
                await self._resolve_recovered_degradations_if_clean()

    async def _resolve_recovered_degradations_if_clean(self) -> None:
        if not self._queues_empty() or self._active_record_ids:
            return
        self._oldest_enqueued_monotonic = None
        if (
            self._transient_degradation_pending
            and not self._spilled_record_ids
            and self.last_error in {None, "archive queue backpressure timeout"}
        ):
            if hasattr(self.database, "resolve_transient_archive_degradations"):
                await self.database.resolve_transient_archive_degradations(
                    run_id=self.run_id
                )
            self._transient_degradation_pending = False
            self.last_error = None
            self.degraded = False

    async def stop(self) -> None:
        # Producers have already stopped. Give any record journaled during
        # their cancellation boundary one final in-process drain attempt.
        await self._retry_spilled_journal_once()
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self._compaction_task is not None:
            await self._compaction_task
            self._compaction_task = None

    async def join(self) -> None:
        """Wait until records currently owned by both scheduling lanes finish."""
        await asyncio.gather(self.queue.join(), self.raw_rest_queue.join())

    async def flush(self) -> None:
        """Flush records preceding lane-local FIFO markers."""
        if self._task is not None and self._task.done():
            await self._task
        live_event = asyncio.Event()
        raw_rest_event = asyncio.Event()
        live_marker = ArchiveRecord.create(
            "__flush__",
            {"event": live_event},
            priority=0,
            partition_timestamp=utc_now(),
        )
        raw_rest_marker = ArchiveRecord.create(
            "__flush__",
            {"event": raw_rest_event},
            priority=0,
            partition_timestamp=utc_now(),
        )
        await asyncio.gather(
            self.queue.put(live_marker),
            self.raw_rest_queue.put(raw_rest_marker),
        )
        await asyncio.gather(live_event.wait(), raw_rest_event.wait())

    async def _compaction_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.settings.archive_compaction_interval_seconds,
                )
            except TimeoutError:
                try:
                    await self.compact_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.counters.compaction_failures += 1
                    LOGGER.exception("Archive compaction pass failed")

    async def compact_once(self) -> bool:
        """Safely replace one compatible group of small immutable objects."""
        if not hasattr(self.database, "archive_compaction_candidates"):
            return False
        candidates = await self.database.archive_compaction_candidates(
            min_age_seconds=self.settings.archive_compaction_min_age_seconds,
            min_objects=self.settings.archive_compaction_min_objects,
            target_bytes=self.settings.archive_compaction_target_bytes,
        )
        if len(candidates) < self.settings.archive_compaction_min_objects:
            return False
        compaction_id = await self.database.begin_archive_compaction(candidates)
        compaction_reservation = 2 * sum(
            int(value["compressed_bytes"]) for value in candidates
        )
        if not await self._reserve_spool_serialization(compaction_reservation):
            await self.database.fail_archive_compaction(
                compaction_id, "insufficient local spool capacity for compaction"
            )
            return False
        local_inputs: list[Path] = []
        replacement: Path | None = None
        replacement_id: int | None = None
        digest: str | None = None
        owner_token: object | None = None
        try:
            stream = str(candidates[0]["stream"])
            schema_version = int(candidates[0]["schema_version"])
            if schema_version != SCHEMA_VERSION:
                raise ValueError("compaction only merges the current schema version")
            tables: list[pa.Table] = []
            for candidate in candidates:
                local = self.settings.archive_spool_directory / (
                    f"compact-source-{compaction_id}-{int(candidate['id'])}.parquet"
                )
                await self.object_store.download(str(candidate["object_key"]), local)
                local_inputs.append(local)
                tables.append(await asyncio.to_thread(pq.read_table, local))
            expected_rows = sum(int(value["row_count"]) for value in candidates)
            table = pa.concat_tables(tables, promote_options="none")
            if table.num_rows != expected_rows:
                raise IOError(
                    f"compaction row mismatch: expected {expected_rows}, got {table.num_rows}"
                )
            provisional = self.settings.archive_spool_directory / (
                f"compacting-{compaction_id}-{uuid.uuid4().hex}.parquet"
            )
            await asyncio.to_thread(
                pq.write_table,
                table,
                provisional,
                compression=self.settings.archive_compression,
                compression_level=self.settings.archive_zstd_level,
                use_dictionary=_parquet_dictionary_enabled(stream),
                write_statistics=True,
                row_group_size=self.settings.archive_row_group_rows,
                data_page_size=1024 * 1024,
            )
            digest = await asyncio.to_thread(_sha256, provisional)
            owner_token = await self._claim_spool_object(digest, wait=True)
            assert owner_token is not None
            replacement = self.settings.archive_spool_directory / f"{digest}.parquet"
            if replacement.exists():
                provisional.unlink(missing_ok=True)
            else:
                provisional.replace(replacement)
            first = candidates[0]
            timestamp = datetime.combine(
                first["partition_date"],
                datetime.min.time(),
                tzinfo=UTC,
            ).replace(hour=int(first["partition_hour"]))
            object_key = self._object_key(stream, timestamp, digest)
            replacement_id = await self.database.register_archive_object(
                stream=stream,
                schema_version=SCHEMA_VERSION,
                object_key=object_key,
                content_hash=digest,
                compression=self.settings.archive_compression,
                row_count=table.num_rows,
                uncompressed_bytes=sum(
                    int(value["uncompressed_bytes"]) for value in candidates
                ),
                compressed_bytes=replacement.stat().st_size,
                min_source_timestamp=min(
                    (value["min_source_timestamp"] for value in candidates
                     if value["min_source_timestamp"] is not None),
                    default=None,
                ),
                max_source_timestamp=max(
                    (value["max_source_timestamp"] for value in candidates
                     if value["max_source_timestamp"] is not None),
                    default=None,
                ),
                min_received_at=min(
                    (value["min_received_at"] for value in candidates
                     if value["min_received_at"] is not None),
                    default=None,
                ),
                max_received_at=max(
                    (value["max_received_at"] for value in candidates
                     if value["max_received_at"] is not None),
                    default=None,
                ),
                partition_date=first["partition_date"],
                partition_hour=first["partition_hour"],
                local_spool_path=str(replacement),
                payload_content_hash=None,
                object_role="compacted",
                compaction_generation=max(
                    int(value["compaction_generation"]) for value in candidates
                ) + 1,
            )
            await self.database.set_archive_compaction_replacement(
                compaction_id, replacement_id
            )
            await self.object_store.put_file(replacement, object_key, digest)
            head = await self.object_store.head(object_key)
            if int(head.get("ContentLength", -1)) != replacement.stat().st_size:
                raise IOError("compacted object size verification failed")
            metadata = {
                str(key).lower(): str(value)
                for key, value in dict(head.get("Metadata") or {}).items()
            }
            if metadata.get("sha256") not in {None, digest}:
                raise IOError("compacted object hash verification failed")
            source_ids = [int(value["id"]) for value in candidates]
            await self.database.complete_archive_compaction(
                compaction_id, replacement_id, source_ids
            )
            replacement.unlink(missing_ok=True)
            for candidate in candidates:
                try:
                    await self.object_store.delete(str(candidate["object_key"]))
                except Exception:
                    LOGGER.warning(
                        "Superseded archive object deletion deferred",
                        extra={"object_key": candidate["object_key"]},
                    )
            self.counters.objects_compacted += len(candidates)
            self.counters.compaction_objects_before += len(candidates)
            self.counters.compaction_objects_after += 1
            self.counters.compaction_bytes_before += sum(
                int(value["compressed_bytes"]) for value in candidates
            )
            self.counters.compaction_bytes_after += int(head["ContentLength"])
            return True
        except Exception as exc:
            self.counters.compaction_failures += 1
            if replacement_id is not None and hasattr(
                self.database, "abandon_archive_object"
            ):
                await self.database.abandon_archive_object(
                    replacement_id,
                    f"compaction failed before atomic replacement: {type(exc).__name__}: {exc}",
                )
            await self.database.fail_archive_compaction(
                compaction_id, f"{type(exc).__name__}: {exc}"
            )
            raise
        finally:
            for path in local_inputs:
                path.unlink(missing_ok=True)
            if replacement is not None:
                replacement.unlink(missing_ok=True)
            if digest is not None and owner_token is not None:
                self._release_spool_object(digest, owner_token)
            await self._release_spool_serialization(compaction_reservation)

    async def _prepare_and_upload(self, records: list[ArchiveRecord]) -> bool:
        stream = records[0].stream
        if stream == "raw_rest":
            existing = await self._existing_raw_rest(records[0])
            if existing is not None:
                for record in records:
                    await self.database.record_raw_rest_provenance(
                        archive_object_id=int(existing["id"]),
                        object_key=str(existing["object_key"]),
                        value=record.data,
                    )
                self.counters.raw_rest_objects_reused += len(records)
                return True
        schema = STREAM_SCHEMAS.get(stream)
        if schema is None:
            raise ValueError(f"unsupported archive stream {stream!r}")
        # A content-addressed REST object contains exactly one immutable body.
        # Multiple concurrent observations of that body remain separate rows
        # in raw_rest_payloads, but must not duplicate the bytes in Parquet.
        payload_records = records[:1] if stream == "raw_rest" else records
        timestamp = records[0].partition_timestamp.astimezone(UTC)
        provisional = self.settings.archive_spool_directory / f"preparing-{uuid.uuid4().hex}.parquet"
        uncompressed = sum(record.estimated_bytes for record in payload_records)
        if not await self._reserve_spool_serialization(uncompressed):
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
                    "spool_bytes": self._spool_bytes,
                    "spool_reserved_bytes": self._spool_reserved_bytes,
                    "spool_max_bytes": self.settings.archive_spool_max_bytes,
                },
            )
            return False
        try:
            try:
                await asyncio.to_thread(
                    _write_parquet,
                    provisional,
                    stream,
                    schema,
                    [record.data for record in payload_records],
                    self.settings.archive_compression,
                    self.settings.archive_zstd_level,
                    self.settings.archive_row_group_rows,
                )
            except Exception as exc:
                await self._quarantine_serialization_failure(records, exc)
                return True
        finally:
            await self._release_spool_serialization(uncompressed)
        digest = await asyncio.to_thread(_sha256, provisional)
        object_key = self._object_key(
            stream,
            timestamp,
            digest,
            payload_content_hash=(
                str(records[0].data.get("content_hash")) if stream == "raw_rest" else None
            ),
        )
        owner_token = await self._claim_spool_object(digest, wait=True)
        assert owner_token is not None
        try:
            final_path = self.settings.archive_spool_directory / f"{digest}.parquet"
            if final_path.exists():
                provisional.unlink(missing_ok=True)
            else:
                provisional.replace(final_path)
            compressed = final_path.stat().st_size
            source_times = [
                parsed
                for record in records
                if (
                    parsed := parse_timestamp(record.data.get("source_timestamp"))
                ) is not None
            ]
            received_times = [
                parsed
                for record in records
                if (parsed := parse_timestamp(record.data.get("received_at")))
                is not None
            ]
            object_id = await self.database.register_archive_object(
                stream=stream,
                schema_version=SCHEMA_VERSION,
                object_key=object_key,
                content_hash=digest,
                compression=self.settings.archive_compression,
                row_count=len(payload_records),
                uncompressed_bytes=uncompressed,
                compressed_bytes=compressed,
                min_source_timestamp=min(source_times) if source_times else None,
                max_source_timestamp=max(source_times) if source_times else None,
                min_received_at=min(received_times) if received_times else None,
                max_received_at=max(received_times) if received_times else None,
                partition_date=timestamp.date(),
                partition_hour=timestamp.hour,
                local_spool_path=str(final_path),
                payload_content_hash=(
                    str(records[0].data.get("content_hash"))
                    if stream == "raw_rest"
                    else None
                ),
                object_role=(
                    "raw_rest_blob" if stream == "raw_rest"
                    else "dictionary" if stream == "archive_dictionary"
                    else "data"
                ),
                compaction_generation=0,
            )
            uploaded = False
            if hasattr(self.database, "archive_object_state"):
                state = await self.database.archive_object_state(object_id)
                object_key = str(state["object_key"])
                if state["status"] == "uploaded":
                    # Manifest commit won a prior retry/crash race. The immutable
                    # content already exists at the manifest key; discard only the
                    # redundant local serialization and acknowledge the journal.
                    final_path.unlink(missing_ok=True)
                    uploaded = True
            if not uploaded:
                uploaded = await self._upload_with_retry(
                    object_id, final_path, object_key, digest
                )
            if uploaded and stream == "raw_rest":
                for record in records:
                    await self.database.record_raw_rest_provenance(
                        archive_object_id=object_id,
                        object_key=object_key,
                        value=record.data,
                    )
            if uploaded and stream == "orderbook_snapshots" and hasattr(
                self.database, "mark_market_final_snapshot_archived"
            ):
                for record in records:
                    if record.data.get("snapshot_type") == "closing":
                        await self.database.mark_market_final_snapshot_archived(
                            market_external_id=str(
                                record.data["market_external_id"]
                            ),
                            archive_object_id=object_id,
                        )
            return True
        finally:
            provisional.unlink(missing_ok=True)
            self._release_spool_object(digest, owner_token)

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
                stream = str(row.get("stream") or "unknown")
                self.counters.stream_rows[stream] = (
                    self.counters.stream_rows.get(stream, 0) + int(row["row_count"])
                )
                self.counters.stream_uncompressed_bytes[stream] = (
                    self.counters.stream_uncompressed_bytes.get(stream, 0)
                    + int(row["uncompressed_bytes"])
                )
                self.counters.stream_compressed_bytes[stream] = (
                    self.counters.stream_compressed_bytes.get(stream, 0)
                    + int(row["compressed_bytes"])
                )
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
        local_hashes = [
            path.stem.lower()
            for path in self.settings.archive_spool_directory.glob("*.parquet")
            if len(path.stem) == 64
            and all(character in "0123456789abcdefABCDEF" for character in path.stem)
        ]
        pending = await self.database.pending_archive_objects(
            limit=20,
            local_content_hashes=local_hashes,
        )
        for value in pending:
            object_id = int(value["id"])
            digest = str(value["content_hash"])
            owner_token = await self._claim_spool_object(digest, wait=False)
            if owner_token is None:
                # A lane or compactor owns this local immutable object. It will
                # either commit+clean it or release it still pending for a later
                # maintenance pass.
                continue
            try:
                # The pending query is only a candidate snapshot. Another
                # collector may commit the shared manifest before this process
                # acts, or a lane may have completed before ownership was won.
                state = await self.database.archive_object_state(object_id)
                if state["status"] == "uploaded":
                    self._remote_recovery_errors.pop(object_id, None)
                    continue
                local_path = Path(str(state["local_spool_path"] or ""))
                if not local_path.is_file():
                    verification_error = await self._remote_verification_error(
                        state
                    )
                    if verification_error is None:
                        # A verified immutable object is authoritative. This also
                        # recovers a crash after S3 PUT but before the DB commit.
                        await self.database.mark_archive_uploaded(object_id)
                        self._remote_recovery_errors.pop(object_id, None)
                        LOGGER.info(
                            "Reconciled archive manifest from verified remote object",
                            extra={
                                "archive_object_id": object_id,
                                "object_key": str(state["object_key"]),
                            },
                        )
                        continue

                    # Absence in this container is not evidence that a spool file
                    # owned by another collector is gone. Keep the shared manifest
                    # unresolved and retry remote verification later. Deduplicate
                    # diagnostics to avoid a per-second cross-container log storm.
                    if (
                        self._remote_recovery_errors.get(object_id)
                        != verification_error
                    ):
                        self._remote_recovery_errors[object_id] = verification_error
                        log = (
                            LOGGER.warning
                            if "mismatch" in verification_error
                            else LOGGER.debug
                        )
                        log(
                            "Archive manifest remains unresolved after remote verification",
                            extra={
                                "archive_object_id": object_id,
                                "object_key": str(state["object_key"]),
                                "reason": verification_error,
                            },
                        )
                    continue
                await self._upload_with_retry(
                    object_id,
                    local_path,
                    str(state["object_key"]),
                    str(state["content_hash"]),
                )
            finally:
                self._release_spool_object(digest, owner_token)

    async def _remote_verification_error(
        self, value: Mapping[str, Any]
    ) -> str | None:
        """Return None only when the immutable remote object matches its manifest."""
        try:
            head = await self.object_store.head(str(value["object_key"]))
        except asyncio.CancelledError:
            raise
        except FileNotFoundError:
            return "remote object is missing"
        except Exception as exc:
            return f"remote HEAD failed: {type(exc).__name__}"

        try:
            actual_bytes = int(head.get("ContentLength", -1))
            expected_bytes = int(value["compressed_bytes"])
        except (TypeError, ValueError):
            return "remote object size is invalid"
        if actual_bytes != expected_bytes:
            return (
                "remote object size mismatch: "
                f"expected={expected_bytes} actual={actual_bytes}"
            )

        metadata = {
            str(key).lower(): str(metadata_value)
            for key, metadata_value in dict(head.get("Metadata") or {}).items()
        }
        remote_hash = metadata.get("sha256")
        expected_hash = str(value["content_hash"])
        if remote_hash is not None and remote_hash != expected_hash:
            return "remote object sha256 metadata mismatch"
        return None

    async def _retry_spilled_journal_once(self) -> None:
        async with self._spill_replay_lock:
            await self._retry_spilled_journal_locked()

    async def _retry_spilled_journal_locked(self) -> None:
        if not self._spilled_record_ids or not self._journal_path.is_file():
            return

        def read_spilled() -> list[dict[str, Any]]:
            values: list[dict[str, Any]] = []
            for line in self._journal_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record_id = str(value.get("record_id"))
                if (
                    record_id in self._spilled_record_ids
                    and record_id not in self._active_record_ids
                ):
                    values.append(value)
            return values

        values = await asyncio.to_thread(read_spilled)
        # Replay latency-sensitive live evidence first, then use the dedicated
        # REST lane for replayable pages. Numeric priority remains a useful
        # ordering within the live lane, but must not put raw REST ahead of L2.
        values.sort(
            key=lambda value: (
                str(value.get("stream")) == "raw_rest",
                int(value.get("priority") or 99),
            )
        )
        for value in values:
            record = ArchiveRecord(
                record_id=str(value["record_id"]),
                stream=str(value["stream"]),
                data=dict(value["data"]),
                priority=int(value["priority"]),
                partition_timestamp=(
                    parse_timestamp(value["partition_timestamp"]) or utc_now()
                ),
                estimated_bytes=int(value["estimated_bytes"]),
            )
            # Replay runs in maintenance, separate from both lane consumers.
            # Keep it bounded so one full lane cannot hold up recovery of the
            # other, and do not create a new degradation row for every retry of
            # an already-recorded spill.
            if not await self._enqueue(
                record,
                wait_for_raw_rest_capacity=False,
                report_timeout_degradation=False,
                timeout_seconds=min(
                    0.05, self.settings.archive_enqueue_timeout_seconds
                ),
            ):
                continue

    async def _recover_compactions(self) -> None:
        """Finish or safely abandon compactions interrupted by a crash.

        Source objects remain authoritative until the replacement is uploaded
        and the manifest swap commits.  Startup upload recovery runs first, so
        an uploaded replacement can now be atomically promoted; otherwise the
        interrupted compaction is failed without superseding its sources.
        """
        if not hasattr(self.database, "running_archive_compactions"):
            return
        for value in await self.database.running_archive_compactions():
            compaction_id = int(value["id"])
            replacement_id = value.get("replacement_object_id")
            if (
                replacement_id is not None
                and value.get("replacement_status") == "uploaded"
            ):
                source_ids = [int(item) for item in value["source_object_ids"]]
                await self.database.complete_archive_compaction(
                    compaction_id, int(replacement_id), source_ids
                )
                for object_key in value.get("source_object_keys") or []:
                    try:
                        await self.object_store.delete(str(object_key))
                    except Exception:
                        LOGGER.warning(
                            "Recovered compaction source deletion deferred",
                            extra={"object_key": object_key},
                        )
                continue
            if replacement_id is not None and hasattr(
                self.database, "abandon_archive_object"
            ):
                await self.database.abandon_archive_object(
                    int(replacement_id),
                    "interrupted compaction had no verified uploaded replacement",
                )
            await self.database.fail_archive_compaction(
                compaction_id,
                "interrupted compaction recovered with source objects unchanged",
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

    async def _release_bytes(self, amount: int, *, lane: str = "live") -> None:
        async with self._bytes_condition:
            self._queued_bytes = max(0, self._queued_bytes - amount)
            self._lane_queued_bytes[lane] = max(
                0, self._lane_queued_bytes[lane] - amount
            )
            self._bytes_condition.notify_all()

    def _object_key(
        self,
        stream: str,
        timestamp: datetime,
        digest: str,
        *,
        payload_content_hash: str | None = None,
    ) -> str:
        prefix = f"{self.settings.s3_prefix}/" if self.settings.s3_prefix else ""
        if stream == "raw_rest":
            payload_digest = payload_content_hash or digest
            return f"{prefix}schema_version={SCHEMA_VERSION}/stream=raw_rest/content_sha256={payload_digest[:2]}/body-{payload_digest}.parquet"
        return (
            f"{prefix}schema_version={SCHEMA_VERSION}/exchange=polymarket/"
            f"stream={stream}/date={timestamp.date().isoformat()}/hour={timestamp.hour:02d}/"
            f"part-{digest[:24]}.parquet"
        )

    def metrics(self) -> dict[str, Any]:
        compressed = self.counters.compressed_bytes
        return {
            "healthy": not self.degraded,
            "queue_depth": self._queue_depth(),
            "queue_bytes": self._queued_bytes,
            "queue_lanes": {
                "live": {
                    "rows": self.queue.qsize(),
                    "bytes": self._lane_queued_bytes["live"],
                },
                "raw_rest": {
                    "rows": self.raw_rest_queue.qsize(),
                    "bytes": self._lane_queued_bytes["raw_rest"],
                },
            },
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
            "raw_rest_objects_reused": self.counters.raw_rest_objects_reused,
            "max_queue_depth": self.counters.max_queue_rows,
            "max_queue_bytes": self.counters.max_queue_bytes,
            "last_error": self.last_error,
            "spool_bytes": self._spool_bytes + self._spool_reserved_bytes,
            "streams": {
                stream: {
                    "rows_total": rows,
                    "uncompressed_bytes_total": self.counters.stream_uncompressed_bytes.get(stream, 0),
                    "compressed_bytes_total": self.counters.stream_compressed_bytes.get(stream, 0),
                }
                for stream, rows in sorted(self.counters.stream_rows.items())
            },
            "compaction": {
                "objects_compacted": self.counters.objects_compacted,
                "objects_before": self.counters.compaction_objects_before,
                "objects_after": self.counters.compaction_objects_after,
                "bytes_before": self.counters.compaction_bytes_before,
                "bytes_after": self.counters.compaction_bytes_after,
                "failures": self.counters.compaction_failures,
            },
        }


async def reconcile_archive_objects(
    database: Any,
    object_store: ObjectStore,
    prefix: str,
    *,
    delete_orphans: bool = False,
) -> dict[str, list[str]]:
    """Compare immutable object keys with the manifest after crash recovery."""
    remote = set(await object_store.list_keys(prefix))
    manifest = set(await database.archive_manifest_keys())
    orphans = sorted(remote - manifest)
    missing = sorted(manifest - remote)
    if delete_orphans:
        for object_key in orphans:
            await object_store.delete(object_key)
    return {"orphan_objects": orphans, "missing_objects": missing}


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
    if pa.types.is_dictionary(data_type):
        return str(value)
    if pa.types.is_integer(data_type):
        return int(value)
    if pa.types.is_boolean(data_type):
        return bool(value)
    return value


def _parquet_dictionary_enabled(stream: str) -> bool:
    # The encoding benchmark shows Parquet dictionary pages enlarge the dense
    # numeric schemas while slowing serialization. Arrow dictionary-typed
    # reference columns remain dictionary encoded by their logical type.
    return stream not in {
        "orderbook_updates",
        "orderbook_snapshots",
        "microstructure_observations",
        "archive_dictionary",
    }


def _write_parquet(
    path: Path,
    stream: str,
    schema: pa.Schema,
    values: list[Mapping[str, Any]],
    compression: str,
    compression_level: int = 3,
    row_group_size: int = 100_000,
) -> None:
    compact_values = [compact_archive_row(stream, value) for value in values]
    rows = [
        {
            field.name: _normalise_value(value.get(field.name), field.type)
            for field in schema
        }
        for value in compact_values
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(
        table,
        path,
        compression=compression,
        compression_level=compression_level,
        use_dictionary=_parquet_dictionary_enabled(stream),
        write_statistics=True,
        data_page_size=1024 * 1024,
        row_group_size=row_group_size,
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
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except FileNotFoundError:
            # Upload and compaction paths atomically rename/delete scratch
            # files. A concurrent accounting pass may legitimately observe a
            # directory entry that is gone by stat time.
            continue
    return total
