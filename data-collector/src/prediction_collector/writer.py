from __future__ import annotations

import asyncio
import time
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from psycopg import DataError, IntegrityError, ProgrammingError

from prediction_collector.archive import (
    ArchiveBackpressureError,
    ArchiveRecord,
    ArchiveWriter,
)
from prediction_collector.common.diagnostics import process_memory_snapshot
from prediction_collector.common.utils import content_hash
from prediction_collector.tiering import CollectionTier, TierManager


if TYPE_CHECKING:
    from prediction_collector.database import Database


LOGGER = logging.getLogger(__name__)
SLOW_ROUTE_SECONDS = 0.5
MAX_ROUTE_KINDS = 32


@dataclass(frozen=True, slots=True)
class WriteItem:
    kind: str
    data: dict[str, Any]


class BatchWriter:
    """Bounded, backpressured, transactional high-volume database writer."""

    def __init__(
        self,
        database: "Database",
        *,
        max_queue_size: int,
        batch_size: int,
        flush_interval_seconds: float,
        archive: ArchiveWriter | None = None,
        tier_manager: TierManager | None = None,
    ) -> None:
        self.database = database
        self.queue: asyncio.Queue[WriteItem] = asyncio.Queue(maxsize=max_queue_size)
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.archive = archive
        self.tier_manager = tier_manager
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._has_started = False
        self.rows_written = 0
        self.failed_items = 0
        self.run_id: int | None = None
        self.postgres_pressure = "normal"
        self.storage_pressure_details: dict[str, Any] = {}
        self._reference_state: dict[tuple[str, str], tuple[str, float]] = {}
        self.reference_duplicates_suppressed = 0
        self._queued_at_monotonic: dict[int, float] = {}
        self._queue_put_wait_started: dict[asyncio.Task[Any], float] = {}
        self.queue_put_count = 0
        self.queue_put_wait_seconds_total = 0.0
        self.queue_put_wait_seconds_max = 0.0
        self.queue_put_wait_seconds_last = 0.0
        self.queue_put_blocked_count = 0
        self.max_queue_depth = 0
        self.queue_full_rejections = 0
        self.route_count = 0
        self.route_seconds_total = 0.0
        self.route_seconds_max = 0.0
        self.route_seconds_last = 0.0
        self._route_by_kind: dict[str, dict[str, float | int]] = {}

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    async def start(self) -> None:
        self._has_started = True
        if self.archive is not None:
            self.archive.run_id = self.run_id
            await self.archive.start()
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="database-batch-writer")

    async def put(self, item: WriteItem) -> None:
        if self._task is not None and self._task.done():
            await self._task
            raise RuntimeError("database writer stopped unexpectedly")
        route_started = time.monotonic()
        try:
            routed = await self._route(item)
        finally:
            route_seconds = time.monotonic() - route_started
            self.route_count += 1
            self.route_seconds_last = route_seconds
            self.route_seconds_total += route_seconds
            self.route_seconds_max = max(
                self.route_seconds_max, route_seconds
            )
            route_kind = item.kind
            if (
                route_kind not in self._route_by_kind
                and len(self._route_by_kind) >= MAX_ROUTE_KINDS
            ):
                route_kind = "__other__"
            timing = self._route_by_kind.setdefault(
                route_kind,
                {
                    "count": 0,
                    "seconds_total": 0.0,
                    "seconds_max": 0.0,
                    "seconds_last": 0.0,
                },
            )
            timing["count"] = int(timing["count"]) + 1
            timing["seconds_last"] = route_seconds
            timing["seconds_total"] = float(timing["seconds_total"]) + route_seconds
            timing["seconds_max"] = max(
                float(timing["seconds_max"]), route_seconds
            )
            if route_seconds >= SLOW_ROUTE_SECONDS:
                LOGGER.warning(
                    "Slow writer route",
                    extra={
                        "item_kind": item.kind,
                        "route_seconds": round(route_seconds, 6),
                        "process_memory": process_memory_snapshot(),
                    },
                )
        if routed is not None:
            wait_started = time.monotonic()
            task = asyncio.current_task()
            if task is not None:
                self._queue_put_wait_started[task] = wait_started
            try:
                await self.queue.put(routed)
            finally:
                if task is not None:
                    self._queue_put_wait_started.pop(task, None)
            wait_seconds = time.monotonic() - wait_started
            self.queue_put_count += 1
            self.queue_put_wait_seconds_last = wait_seconds
            self.queue_put_wait_seconds_total += wait_seconds
            self.queue_put_wait_seconds_max = max(
                self.queue_put_wait_seconds_max, wait_seconds
            )
            if wait_seconds >= 0.001:
                self.queue_put_blocked_count += 1
            self._queued_at_monotonic[id(routed)] = time.monotonic()
            self.max_queue_depth = max(self.max_queue_depth, self.queue.qsize())

    async def _route(self, item: WriteItem) -> WriteItem | None:
        data = item.data
        market = data.get("market_external_id")
        tier = (
            self.tier_manager.tier_for(str(market) if market is not None else None)
            if self.tier_manager is not None
            else CollectionTier.FULL_L2
        )
        if item.kind == "orderbook_updates":
            if tier is CollectionTier.FULL_L2:
                await self._archive("orderbook_updates", data, priority=3)
            return WriteItem("current_orderbook_updates", data)
        if item.kind == "orderbook_snapshots":
            # A terminal snapshot is the replay anchor for evicting a closed
            # market from PostgreSQL.  Preserve it even if a concurrent
            # lifecycle/tier update has already demoted the market.
            if (
                tier is CollectionTier.FULL_L2
                or data.get("snapshot_type") == "closing"
            ):
                await self._archive("orderbook_snapshots", data, priority=3)
            # Periodic REST reconciliation is useful immutable archive
            # evidence, but its response has no ordering relationship with
            # concurrent WebSocket deltas. Never let it regress the hot book.
            if data.get("archive_only"):
                return None
            return WriteItem("current_orderbook_snapshots", data)
        if item.kind in {"market_snapshots", "microstructure_observations"}:
            # FULL_L2 observations are a bounded PostgreSQL convenience view;
            # the permanent snapshot+delta stream regenerates them exactly.
            if tier is CollectionTier.SAMPLED:
                await self._archive("microstructure_observations", data, priority=5)
            if self.postgres_pressure == "critical":
                await self._record_pressure_degradation(item, priority=5)
                return None
            return WriteItem("microstructure_observations", data)
        if item.kind == "raw_ws_messages":
            if not self._retain_raw_ws(data, tier):
                return None
            archive_data = dict(data)
            archive_data["payload_hash"] = content_hash(data.get("payload"))
            try:
                await self._archive("raw_ws", archive_data, priority=6)
            except ArchiveBackpressureError:
                # Optional debug evidence degrades before normalized L2. The
                # archive writer has already persisted an explicit event.
                LOGGER.warning("Raw WebSocket evidence shed under archive pressure")
            return None
        if item.kind == "raw_rest_payloads":
            await self._archive("raw_rest", data, priority=2)
            return None
        if item.kind == "reference_price_updates":
            reference_key = (
                str(data.get("provider") or ""),
                str(data.get("external_instrument_id") or ""),
            )
            reference_fingerprint = content_hash(
                {
                    key: data.get(key)
                    for key in (
                        "price", "bid", "ask", "confidence_interval",
                        "publish_slot", "source_status",
                    )
                }
            )
            now = time.monotonic()
            previous = self._reference_state.get(reference_key)
            if (
                previous is not None
                and previous[0] == reference_fingerprint
                and now - previous[1]
                    < self.archive.settings.reference_unchanged_heartbeat_seconds
            ):
                self.reference_duplicates_suppressed += 1
                return None
            self._reference_state[reference_key] = (reference_fingerprint, now)
            try:
                await self._archive("reference_prices", data, priority=4)
            except ArchiveBackpressureError:
                LOGGER.warning("Reference-price archive degraded; hot copy retained")
            if self.postgres_pressure == "critical":
                await self._record_pressure_degradation(item, priority=4)
                return None
            compact = dict(data)
            compact["raw_data"] = {}
            return WriteItem(item.kind, compact)
        if item.kind in {"sports_feed_updates", "market_lifecycle_events", "trades"}:
            compact = dict(data)
            compact["raw_data"] = {}
            return WriteItem(item.kind, compact)
        return item

    async def _archive(
        self, stream: str, data: dict[str, Any], *, priority: int
    ) -> None:
        if self.archive is None:
            raise RuntimeError(f"archive writer is required for {stream}")
        await self.archive.put(ArchiveRecord.create(stream, data, priority=priority))

    def _retain_raw_ws(
        self, data: dict[str, Any], tier: CollectionTier
    ) -> bool:
        if self.archive is None:
            return False
        policy = self.archive.settings.raw_ws_policy
        if policy == "none":
            return False
        if policy == "all":
            return True
        known = {
            "book",
            "price_change",
            "last_trade_price",
            "tick_size_change",
            "new_market",
            "market_resolved",
            "best_bid_ask",
            "price",
            "equity_prices",
            "crypto_prices",
            "crypto_prices_chainlink",
            "crypto_prices_twap_thirty",
            "crypto_prices_twap_sixty",
            "sports",
        }
        message_type = str(data.get("message_type") or "unknown")
        channel = str(data.get("channel") or "")
        known_rtds_channels = {
            "rtds:crypto_prices",
            "rtds:crypto_prices_chainlink",
            "rtds:crypto_prices_twap_thirty",
            "rtds:crypto_prices_twap_sixty",
            "rtds:equity_prices",
            "rtds:comments",
        }
        known_transport_envelope = (
            channel in known_rtds_channels
            and message_type in {"update", "snapshot"}
        ) or (
            channel == "sports"
            and message_type not in {"error", "malformed_json", "unknown"}
        )
        is_error = (
            (message_type not in known and not known_transport_envelope)
            or message_type in {"error", "malformed_json", "parser_error", "unknown"}
        )
        if policy in {"errors", "errors_sample"}:
            if is_error:
                return True
            if policy == "errors":
                return False
            digest = content_hash(
                {
                    "channel": channel,
                    "message_type": message_type,
                    "payload": data.get("payload"),
                }
            )
            bucket = int(digest[:16], 16) / float(1 << 64)
            return bucket < float(self.archive.settings.raw_ws_valid_sample_rate)
        return policy == "full_l2" and tier is CollectionTier.FULL_L2

    async def _record_pressure_degradation(
        self, item: WriteItem, *, priority: int
    ) -> None:
        if self.archive is not None:
            await self.archive.database.record_archive_degradation(
                run_id=self.run_id,
                stream=item.kind,
                priority=priority,
                reason="storage_critical_optional_hot_write_shed",
                rows_affected=1,
                bytes_affected=0,
                details=dict(getattr(self, "storage_pressure_details", {})),
            )

    def set_storage_pressure(
        self, pressure: str, *, details: dict[str, Any] | None = None
    ) -> None:
        self.postgres_pressure = pressure
        self.storage_pressure_details = dict(details or {})

    def put_nowait(self, item: WriteItem) -> bool:
        try:
            self.queue.put_nowait(item)
            self._queued_at_monotonic[id(item)] = time.monotonic()
            self.queue_put_count += 1
            self.max_queue_depth = max(self.max_queue_depth, self.queue.qsize())
            return True
        except asyncio.QueueFull:
            self.queue_full_rejections += 1
            return False

    async def run(self) -> None:
        batch: list[WriteItem] = []
        while not self._stop.is_set() or not self.queue.empty() or batch:
            flush_event: asyncio.Event | None = None
            timeout = self.flush_interval_seconds if not batch else min(
                self.flush_interval_seconds, 0.25
            )
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                self._queued_at_monotonic.pop(id(item), None)
                if item.kind == "__flush__":
                    flush_event = item.data["event"]
                else:
                    batch.append(item)
            except TimeoutError:
                pass
            if batch and (
                len(batch) >= self.batch_size
                or self._stop.is_set()
                or self.queue.empty()
                or flush_event is not None
            ):
                flushing = batch
                batch = []
                try:
                    await self._flush_with_retry(flushing)
                finally:
                    for _ in flushing:
                        self.queue.task_done()
            if flush_event is not None:
                self.queue.task_done()
                flush_event.set()

    def metrics(self) -> dict[str, Any]:
        now = time.monotonic()
        oldest_queued = (
            min(self._queued_at_monotonic.values())
            if self._queued_at_monotonic
            else None
        )
        oldest_waiter = (
            min(self._queue_put_wait_started.values())
            if self._queue_put_wait_started
            else None
        )
        task_state = "stopped" if self._has_started else "not_started"
        task_error: str | None = None
        if self._task is not None:
            if self._task.cancelled():
                task_state = "cancelled"
            elif self._task.done():
                error = self._task.exception()
                task_state = "failed" if error is not None else "stopped"
                task_error = type(error).__name__ if error is not None else None
            else:
                task_state = "running"
        return {
            "queue_depth": self.queue.qsize(),
            "queue_max_rows": self.queue.maxsize,
            "max_queue_depth": self.max_queue_depth,
            "oldest_queued_seconds": (
                max(0.0, now - oldest_queued)
                if oldest_queued is not None
                else 0.0
            ),
            "queue_put_waiters": len(self._queue_put_wait_started),
            "oldest_queue_put_wait_seconds": (
                max(0.0, now - oldest_waiter)
                if oldest_waiter is not None
                else 0.0
            ),
            "queue_put_count": self.queue_put_count,
            "queue_put_blocked_count": self.queue_put_blocked_count,
            "queue_put_wait_seconds_total": round(
                self.queue_put_wait_seconds_total, 6
            ),
            "queue_put_wait_seconds_max": round(
                self.queue_put_wait_seconds_max, 6
            ),
            "queue_put_wait_seconds_last": round(
                self.queue_put_wait_seconds_last, 6
            ),
            "queue_full_rejections": self.queue_full_rejections,
            "route_count": self.route_count,
            "route_seconds_total": round(self.route_seconds_total, 6),
            "route_seconds_max": round(self.route_seconds_max, 6),
            "route_seconds_last": round(self.route_seconds_last, 6),
            "route_by_kind": {
                kind: {
                    "count": int(timing["count"]),
                    "seconds_total": round(float(timing["seconds_total"]), 6),
                    "seconds_max": round(float(timing["seconds_max"]), 6),
                    "seconds_last": round(float(timing["seconds_last"]), 6),
                }
                for kind, timing in sorted(self._route_by_kind.items())
            },
            "slow_route_warning_seconds": SLOW_ROUTE_SECONDS,
            "task_state": task_state,
            "task_error_type": task_error,
            "rows_written": self.rows_written,
            "failed_items": self.failed_items,
        }

    async def flush(self) -> None:
        """Wait for DB records preceding this FIFO boundary, not future traffic."""
        if self._task is not None and self._task.done():
            await self._task
        event = asyncio.Event()
        await self.queue.put(WriteItem("__flush__", {"event": event}))
        await event.wait()

    async def _flush_with_retry(self, batch: list[WriteItem]) -> None:
        delay = 0.5
        last_error: BaseException | None = None
        for attempt in range(1, 7):
            try:
                counts = await self.database.write_items(batch)
                self.rows_written += sum(counts.values())
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                if isinstance(
                    exc,
                    (
                        DataError,
                        IntegrityError,
                        ProgrammingError,
                        TypeError,
                        ValueError,
                    ),
                ):
                    LOGGER.error(
                        "Permanent database batch error; isolating bad rows",
                        extra={
                            "batch_size": len(batch),
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:1000],
                        },
                    )
                    break
                if attempt == 6:
                    LOGGER.exception(
                        "Database batch failed after retries; isolating bad rows",
                        extra={"batch_size": len(batch)},
                    )
                    break
                LOGGER.exception(
                    "Database batch failed; retrying",
                    extra={"batch_size": len(batch), "attempt": attempt, "delay": delay},
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15)
        assert last_error is not None
        await self._isolate_failed_batch(batch, last_error)

    async def _isolate_failed_batch(
        self, batch: list[WriteItem], batch_error: BaseException
    ) -> None:
        if len(batch) > 1:
            midpoint = len(batch) // 2
            for subset in (batch[:midpoint], batch[midpoint:]):
                try:
                    counts = await self.database.write_items(subset)
                    self.rows_written += sum(counts.values())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._isolate_failed_batch(subset, exc)
            return

        item = batch[0]
        try:
            await self.database.record_write_failure(
                item, batch_error, run_id=self.run_id
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "Unable to quarantine failed database row",
                extra={"item_kind": item.kind},
            )
            raise batch_error
        self.failed_items += 1
        LOGGER.error(
            "Database row quarantined after permanent write failure",
            extra={
                "item_kind": item.kind,
                "error_type": type(batch_error).__name__,
                "error": str(batch_error)[:1000],
            },
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self.archive is not None:
            await self.archive.stop()
