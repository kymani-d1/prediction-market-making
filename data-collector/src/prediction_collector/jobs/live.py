from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from prediction_collector.common.records import book_snapshot_item
from prediction_collector.common.retry import RetryPolicy
from prediction_collector.common.types import (
    LiveSelection,
    MarketCandidate,
    MarketExclusion,
)
from prediction_collector.common.utils import utc_now
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.polymarket.parser import parse_book
from prediction_collector.polymarket.rtds import PolymarketRtdsWebSocket
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.polymarket.sports import PolymarketSportsWebSocket
from prediction_collector.polymarket.websocket import PolymarketMarketWebSocket
from prediction_collector.tiering import CollectionTier, TierAssignment, TierManager
from prediction_collector.writer import BatchWriter


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class LiveCoverageState:
    candidates: list[MarketCandidate] = field(default_factory=list)
    selection: LiveSelection | None = None
    assignments: list[TierAssignment] = field(default_factory=list)
    confirmed_subscribed: int = 0

    def metrics(self) -> dict[str, int]:
        if self.selection is None:
            return {
                "discovered": 0,
                "active": 0,
                "tradable": 0,
                "subscribed": 0,
                "excluded": 0,
            }
        return {
            "discovered": self.selection.discovered,
            "active": self.selection.active,
            "tradable": self.selection.tradable,
            "subscribed": self.confirmed_subscribed,
            "excluded": len(self.selection.excluded),
        }


@dataclass(slots=True)
class MarketSocketShard:
    shard_id: int
    subscriptions: dict[str, str]
    task: asyncio.Task[None]
    planned_stop: asyncio.Event


class LiveCollector:
    def __init__(
        self,
        *,
        settings: Settings,
        database: Database,
        writer: BatchWriter,
        metrics: ThroughputMetrics,
        polymarket_service: PolymarketService,
        tier_manager: TierManager,
    ) -> None:
        self.settings = settings
        self.database = database
        self.writer = writer
        self.metrics = metrics
        self.polymarket_service = polymarket_service
        self.tier_manager = tier_manager
        self.coverage = LiveCoverageState()
        self._last_discovery: list[MarketCandidate] = []
        self._discovery_gaps: list[int] = []
        self.discovery_state = "pending"
        self._selection_lock = asyncio.Lock()
        self._discovery_retry = RetryPolicy(
            max_attempts=1,
            base_delay_seconds=1.0,
            max_delay_seconds=60.0,
            jitter_ratio=0.25,
        )
        self._economics_gaps: dict[str, list[int]] = {}
        self.stop = asyncio.Event()
        self.run_id: int | None = None
        self.market_shards: dict[int, MarketSocketShard] = {}
        self._next_market_shard_id = 1
        self.background_tasks: list[asyncio.Task[None]] = []
        self.reconciliation_task: asyncio.Task[None] | None = None
        self._task_failure: asyncio.Future[tuple[str, BaseException | None]] | None = None
        self.polymarket_ws = PolymarketMarketWebSocket(
            url=settings.polymarket_ws_url,
            writer=writer,
            database=database,
            metrics=metrics,
            tier_manager=tier_manager,
        )

    async def run(self) -> None:
        self.run_id = await self.database.start_run("live", "polymarket")
        self.writer.run_id = self.run_id
        if self.writer.archive is not None:
            self.writer.archive.run_id = self.run_id
        await self.writer.start()
        self._task_failure = asyncio.get_running_loop().create_future()
        if self.writer.task is not None:
            self._watch_task(self.writer.task)
        if self.writer.archive is not None and self.writer.archive.task is not None:
            self._watch_task(self.writer.archive.task)
        try:
            await self._start_background_tasks()
            stop_waiter = asyncio.create_task(self.stop.wait(), name="collector-stop-waiter")
            assert self._task_failure is not None
            done, _ = await asyncio.wait(
                [stop_waiter, self._task_failure],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._task_failure in done:
                task_name, error = self._task_failure.result()
                if error is not None:
                    raise RuntimeError(f"collector task failed: {task_name}") from error
                raise RuntimeError(f"collector task stopped unexpectedly: {task_name}")
            await self._shutdown_tasks()
            await self.writer.stop()
            archive_degraded = bool(
                self.writer.archive and self.writer.archive.degraded
            )
            await self.database.finish_run(
                self.run_id,
                status=(
                    "partial"
                    if self.writer.failed_items or archive_degraded
                    else "completed"
                ),
                records_processed=0,
                rows_written=self.writer.rows_written,
                coverage=self.coverage.metrics(),
            )
        except asyncio.CancelledError:
            self.stop.set()
            await self._shutdown_tasks()
            await self.writer.stop()
            await self.database.finish_run(
                self.run_id,
                status="cancelled",
                records_processed=0,
                rows_written=self.writer.rows_written,
                coverage=self.coverage.metrics(),
            )
            raise
        except Exception as exc:
            self.stop.set()
            await self._shutdown_tasks()
            try:
                await self.writer.stop()
            except Exception:
                LOGGER.exception("Storage writers also failed during shutdown")
            await self.database.finish_run(
                self.run_id,
                status="failed",
                records_processed=0,
                rows_written=self.writer.rows_written,
                error_summary=f"{type(exc).__name__}: {exc}",
                coverage=self.coverage.metrics(),
            )
            raise

    async def _start_background_tasks(self) -> None:
        if self.settings.polymarket_rtds_enabled:
            rtds = PolymarketRtdsWebSocket(
                url=self.settings.polymarket_rtds_url,
                writer=self.writer,
                database=self.database,
                metrics=self.metrics,
                store_raw=True,
                equity_symbols=self.settings.polymarket_equity_symbols,
                comments_enabled=self.settings.polymarket_comments_enabled,
            )
            self.background_tasks.append(
                self._create_watched_task(
                    rtds.run(run_id=self.run_id, stop=self.stop),
                    name="polymarket-rtds",
                )
            )
        if self.settings.polymarket_sports_enabled:
            sports = PolymarketSportsWebSocket(
                url=self.settings.polymarket_sports_ws_url,
                writer=self.writer,
                database=self.database,
                metrics=self.metrics,
                store_raw=True,
            )
            self.background_tasks.append(
                self._create_watched_task(
                    sports.run(run_id=self.run_id, stop=self.stop),
                    name="polymarket-sports",
                )
            )
        loops = (
            (self._discovery_loop(), "polymarket-market-discovery"),
            (self._economics_loop(), "economics-refresh"),
            (self._fee_rate_loop(), "polymarket-fee-rate-refresh"),
            (self._metrics_loop(), "throughput-metrics"),
            (self._storage_loop(), "storage-metrics"),
            (self._retention_loop(), "postgres-retention"),
            (self._tier_reevaluation_loop(), "tier-reevaluation"),
            (self._snapshot_loop(), "microstructure-observations"),
            (self._reconcile_loop(), "orderbook-reconciliation"),
        )
        self.background_tasks.extend(
            self._create_watched_task(coroutine, name=name)
            for coroutine, name in loops
        )

    async def _discovery_loop(self) -> None:
        attempt = 0
        while not self.stop.is_set():
            try:
                self.discovery_state = "discovering"

                async def on_page(page: list[MarketCandidate]) -> None:
                    await self._merge_discovery_page(page)

                discovered = await self.polymarket_service.discover_live(
                    reconcile_absent=False,
                    on_page=on_page,
                )
                self._last_discovery = discovered
                self.discovery_state = "ready"
                await self._apply_tiers(
                    discovered,
                    persist=True,
                    log_summary=True,
                )
                self._schedule_absent_reconciliation(discovered)
                await self._resolve_discovery_gaps()
                attempt = 0
                await _sleep(self.stop, self.settings.metadata_sync_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                self.discovery_state = "retrying"
                delay = self._discovery_retry.delay(min(attempt, 32))
                LOGGER.exception(
                    f"Polymarket live discovery failed: {type(exc).__name__}: {exc}",
                    extra={
                        "attempt": attempt,
                        "retry_delay_seconds": round(delay, 3),
                        "retained_partial_markets": len(self._last_discovery),
                    },
                )
                await self._record_discovery_gap(exc, attempt, delay)
                await _sleep(self.stop, delay)

    async def _merge_discovery_page(self, page: list[MarketCandidate]) -> None:
        existing = {market.external_id: market for market in self._last_discovery}
        existing.update({market.external_id: market for market in page})
        self._last_discovery = list(existing.values())
        await self._apply_tiers(self._last_discovery, persist=False, log_summary=False)

    async def _apply_tiers(
        self,
        candidates: list[MarketCandidate],
        *,
        persist: bool,
        log_summary: bool,
    ) -> None:
        async with self._selection_lock:
            assignments = self.tier_manager.evaluate(candidates)
            subscribed = [
                assignment.market
                for assignment in assignments
                if assignment.tier is not CollectionTier.METADATA_ONLY
            ]
            excluded = [
                MarketExclusion(
                    "polymarket",
                    assignment.market.external_id,
                    assignment.reasons[-1] if assignment.reasons else "metadata_only",
                )
                for assignment in assignments
                if assignment.tier is CollectionTier.METADATA_ONLY
            ]
            selection = LiveSelection(
                discovered=len(candidates),
                active=sum(market.active for market in candidates),
                tradable=sum(market.tradable for market in candidates),
                subscribed=subscribed,
                excluded=excluded,
            )
            self.coverage = LiveCoverageState(
                candidates=list(candidates),
                selection=selection,
                assignments=assignments,
                confirmed_subscribed=self.coverage.confirmed_subscribed,
            )
            if persist:
                await self.database.record_tier_assignments(assignments)
                confirmed = await self.database.active_subscribed_market_ids(self.run_id)
                reasons = {
                    ("polymarket", assignment.market.external_id): ",".join(
                        assignment.reasons
                    )
                    for assignment in assignments
                    if assignment.tier is CollectionTier.METADATA_ONLY
                }
                await self.database.record_live_selection(
                    self.run_id, candidates, confirmed, reasons
                )
            # Reconcile desired state against the actual running shards on
            # every pass. This is intentionally idempotent: if persistence or
            # shard creation failed once, an unchanged next discovery still
            # converges instead of assuming the previous desired set is live.
            await self._reconcile_market_shards(subscribed)
            if log_summary:
                counts = self.tier_manager.counts()
                bindings = sum(item.ceiling_binding for item in assignments)
                LOGGER.info(
                    "Tier assignment summary",
                    extra={
                        "discovered_markets": len(candidates),
                        "active_markets": selection.active,
                        "tradable_markets": selection.tradable,
                        "full_l2_markets": counts[CollectionTier.FULL_L2.value],
                        "sampled_markets": counts[CollectionTier.SAMPLED.value],
                        "metadata_only_markets": counts[CollectionTier.METADATA_ONLY.value],
                        "resource_ceiling_exclusions": bindings,
                        "discovery_state": self.discovery_state,
                    },
                )

    async def _reconcile_market_shards(
        self, selected: list[MarketCandidate]
    ) -> None:
        desired = {
            token: market.external_id
            for market in selected
            for token in market.outcome_token_ids
        }
        capacity = self.settings.polymarket_ws_subscription_chunk_size
        existing = sorted(self.market_shards.values(), key=lambda shard: shard.shard_id)
        proposals = {
            shard.shard_id: {
                identifier: desired[identifier]
                for identifier in shard.subscriptions
                if identifier in desired
            }
            for shard in existing
        }
        assigned = {
            identifier for proposal in proposals.values() for identifier in proposal
        }
        remaining = deque(
            identifier for identifier in sorted(desired) if identifier not in assigned
        )
        fill_candidates = [
            shard
            for shard in existing
            if proposals[shard.shard_id] != shard.subscriptions
            and len(proposals[shard.shard_id]) < capacity
        ]
        stable_tail = next(
            (
                shard
                for shard in reversed(existing)
                if len(proposals[shard.shard_id]) < capacity
                and shard not in fill_candidates
            ),
            None,
        )
        if stable_tail is not None:
            fill_candidates.append(stable_tail)
        for shard in fill_candidates:
            proposal = proposals[shard.shard_id]
            while remaining and len(proposal) < capacity:
                identifier = remaining.popleft()
                proposal[identifier] = desired[identifier]
        while remaining:
            shard_id = self._next_market_shard_id
            self._next_market_shard_id += 1
            identifiers = [
                remaining.popleft() for _ in range(min(capacity, len(remaining)))
            ]
            proposals[shard_id] = {
                identifier: desired[identifier] for identifier in identifiers
            }
        existing_by_id = {shard.shard_id: shard for shard in existing}
        for shard_id in sorted(set(existing_by_id) | set(proposals)):
            old = existing_by_id.get(shard_id)
            new = proposals.get(shard_id, {})
            if old is not None and old.subscriptions == new:
                continue
            await self._replace_market_shard(shard_id, old=old, new_subscriptions=new)

    async def _replace_market_shard(
        self,
        shard_id: int,
        *,
        old: MarketSocketShard | None,
        new_subscriptions: dict[str, str],
    ) -> None:
        recovery_gap_ids: list[int] = []
        if old is not None and new_subscriptions:
            recovery_gap_ids.append(
                await self.database.record_gap(
                    run_id=self.run_id,
                    connection_id=None,
                    exchange="polymarket",
                    channel="market",
                    market_external_id=None,
                    outcome_external_id=None,
                    gap_type="planned_subscription_refresh",
                    reconnect_reason="tier_or_token_subscription_changed",
                    details={
                        "shard_id": shard_id,
                        "old_identifiers": sorted(old.subscriptions),
                        "new_identifiers": sorted(new_subscriptions),
                    },
                )
            )
        if old is not None:
            old.planned_stop.set()
            old.task.cancel()
            await asyncio.gather(old.task, return_exceptions=True)
            self.market_shards.pop(shard_id, None)
        if not new_subscriptions:
            return
        planned_stop = asyncio.Event()
        task = self._create_watched_task(
            self.polymarket_ws.run(
                new_subscriptions,
                run_id=self.run_id,
                stop=self.stop,
                connection_label=f"shard-{shard_id}",
                planned_stop=planned_stop,
                recovery_gap_ids=tuple(recovery_gap_ids),
            ),
            name=f"polymarket-market-ws-{shard_id}",
        )
        self.market_shards[shard_id] = MarketSocketShard(
            shard_id, dict(new_subscriptions), task, planned_stop
        )

    def _schedule_absent_reconciliation(
        self, candidates: list[MarketCandidate]
    ) -> None:
        if self.reconciliation_task is not None and not self.reconciliation_task.done():
            LOGGER.info("Previous absent-market reconciliation is still running")
            return
        self.reconciliation_task = self._create_watched_task(
            self._reconcile_absent_markets(list(candidates)),
            name="polymarket-absent-market-reconciliation",
        )

    async def _reconcile_absent_markets(
        self, candidates: list[MarketCandidate]
    ) -> None:
        try:
            await self.polymarket_service.reconcile_absent_live(candidates)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("Absent-market state reconciliation failed")
            await self.database.record_gap(
                run_id=self.run_id,
                connection_id=None,
                exchange="polymarket",
                channel="rest:market_state_reconciliation",
                market_external_id=None,
                outcome_external_id=None,
                gap_type="state_reconciliation_batch_failed",
                reconnect_reason=f"{type(exc).__name__}: {exc}",
                details={"current_markets": len(candidates)},
            )

    async def _record_discovery_gap(
        self, exc: Exception, attempt: int, delay: float
    ) -> None:
        if self._discovery_gaps:
            return
        gap_id = await self.database.record_gap(
            run_id=self.run_id,
            connection_id=None,
            exchange="polymarket",
            channel="rest:market_discovery",
            market_external_id=None,
            outcome_external_id=None,
            gap_type="discovery_refresh_failed",
            reconnect_reason=f"{type(exc).__name__}: {exc}",
            details={
                "attempt": attempt,
                "retry_delay_seconds": round(delay, 3),
                "retained_partial_markets": len(self._last_discovery),
            },
        )
        self._discovery_gaps.append(gap_id)

    async def _resolve_discovery_gaps(self) -> None:
        unresolved: list[int] = []
        for gap_id in self._discovery_gaps:
            try:
                await self.database.resolve_gap(
                    gap_id, action="successful_complete_market_discovery"
                )
            except Exception:
                unresolved.append(gap_id)
                LOGGER.exception("Failed to resolve discovery gap", extra={"gap_id": gap_id})
        self._discovery_gaps = unresolved

    async def _economics_loop(self) -> None:
        while not self.stop.is_set():
            await self._sync_economics(
                channel="rest:economics_refresh",
                include_fee_rates=False,
                include_rewards=True,
            )
            await _sleep(self.stop, self.settings.economics_sync_interval_seconds)

    async def _fee_rate_loop(self) -> None:
        while not self.stop.is_set():
            await self._sync_economics(
                channel="rest:fee_rate_refresh",
                include_fee_rates=True,
                include_rewards=False,
                fee_rate_live_only=True,
            )
            await _sleep(
                self.stop, self.settings.polymarket_fee_rate_sync_interval_seconds
            )

    async def _sync_economics(self, *, channel: str, **options: bool) -> None:
        try:
            counts = await self.polymarket_service.sync_fees_and_incentives(**options)
            for gap_id in self._economics_gaps.pop(channel, []):
                await self.database.resolve_gap(gap_id, action="successful_economics_refresh")
            LOGGER.info(
                "Live economics refresh complete",
                extra={"channel": channel, "counts": counts},
            )
        except Exception as exc:
            LOGGER.exception("Live economics refresh failed", extra={"channel": channel})
            if not self._economics_gaps.get(channel):
                gap_id = await self.database.record_gap(
                    run_id=self.run_id,
                    connection_id=None,
                    exchange="polymarket",
                    channel=channel,
                    market_external_id=None,
                    outcome_external_id=None,
                    gap_type="economics_refresh_failed",
                    reconnect_reason=f"{type(exc).__name__}: {exc}",
                    details=options,
                )
                self._economics_gaps[channel] = [gap_id]

    async def _tier_reevaluation_loop(self) -> None:
        while not self.stop.is_set():
            await _sleep(self.stop, self.settings.tier_reevaluation_interval_seconds)
            if self.stop.is_set() or not self._last_discovery:
                continue
            await self._apply_tiers(
                self._last_discovery,
                persist=True,
                log_summary=True,
            )

    async def _snapshot_loop(self) -> None:
        interval = min(
            self.settings.full_l2_observation_interval_seconds,
            self.settings.sampled_snapshot_interval_seconds,
        )
        while not self.stop.is_set():
            await _sleep(self.stop, interval)
            if self.stop.is_set():
                return
            for item in self.polymarket_ws.market_snapshot_items(
                full_l2_interval_seconds=self.settings.full_l2_observation_interval_seconds,
                sampled_interval_seconds=self.settings.sampled_snapshot_interval_seconds,
            ):
                await self.writer.put(item)

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            await _sleep(self.stop, self.settings.orderbook_reconcile_interval_seconds)
            if self.stop.is_set():
                return
            for assignment in self.coverage.assignments:
                if assignment.tier is not CollectionTier.FULL_L2:
                    continue
                for token in assignment.market.outcome_token_ids:
                    try:
                        result = await self.polymarket_service.rest.orderbook(token)
                        raw = result.data if isinstance(result.data, dict) else {}
                        snapshot = parse_book(raw)
                        if snapshot:
                            item = book_snapshot_item(snapshot, reconciliation=True)
                            item.data["archive_only"] = True
                            await self.writer.put(item)
                    except Exception:
                        LOGGER.exception(
                            "Orderbook reconciliation failed",
                            extra={"market": assignment.market.external_id, "token": token},
                        )

    async def _metrics_loop(self) -> None:
        interval = self.settings.metrics_log_interval_seconds
        await self.database.database_row_write_deltas()
        while not self.stop.is_set():
            interval_start = utc_now()
            await _sleep(self.stop, interval)
            if self.stop.is_set():
                return
            snapshot = await self.metrics.snapshot_and_reset()
            server_deltas = await self.database.database_row_write_deltas()
            snapshot["database_rows_per_minute"] = {
                table: round(count * 60 / interval, 2)
                for table, count in server_deltas.items()
            }
            snapshot["database_rows_source"] = "pg_stat_user_tables"
            if self.writer.archive is not None:
                snapshot["archive"] = self.writer.archive.metrics()
            self.coverage.confirmed_subscribed = (
                await self.database.active_subscribed_market_count(self.run_id)
            )
            coverage = self.coverage.metrics()
            LOGGER.info(
                "Collector throughput",
                extra={
                    **snapshot,
                    **self.tier_manager.counts(),
                    "subscribed_markets": coverage["subscribed"],
                    "discovery_state": self.discovery_state,
                },
            )
            await self.database.record_metrics(
                run_id=self.run_id,
                interval_start=interval_start,
                interval_seconds=interval,
                snapshot=snapshot,
                coverage=coverage,
            )

    async def _storage_loop(self) -> None:
        while not self.stop.is_set():
            try:
                postgres = await self.database.storage_snapshot()
                archive = (
                    self.writer.archive.metrics() if self.writer.archive is not None else {}
                )
                size = int(postgres["postgres_database_bytes"])
                warning = int(self.settings.postgres_storage_warn_gb * Decimal(1024**3))
                critical = int(self.settings.postgres_storage_critical_gb * Decimal(1024**3))
                pressure = "critical" if size >= critical else "warning" if size >= warning else "normal"
                queue_rows = int(archive.get("queue_depth", 0))
                queue_bytes = int(archive.get("queue_bytes", 0))
                if (
                    queue_rows >= self.settings.archive_queue_critical_rows
                    or queue_bytes >= self.settings.archive_queue_critical_bytes
                ):
                    pressure = "critical"
                elif pressure == "normal" and (
                    queue_rows >= self.settings.archive_queue_warn_rows
                    or queue_bytes >= self.settings.archive_queue_warn_bytes
                ):
                    pressure = "warning"
                self.writer.postgres_pressure = pressure
                await self.database.record_storage_metrics(
                    run_id=self.run_id,
                    postgres=postgres,
                    archive=archive,
                    pressure_state=pressure,
                )
                log = LOGGER.warning if pressure != "normal" else LOGGER.info
                log(
                    "Storage throughput",
                    extra={
                        "pressure_state": pressure,
                        "postgres_database_bytes": size,
                        "major_table_bytes": postgres["major_table_bytes"],
                        "archive": archive,
                    },
                )
            except Exception:
                LOGGER.exception("Storage metrics collection failed")
            await _sleep(self.stop, self.settings.storage_metrics_interval_seconds)

    async def _retention_loop(self) -> None:
        while not self.stop.is_set():
            await _sleep(self.stop, self.settings.retention_interval_seconds)
            if self.stop.is_set():
                return
            deleted = await self.database.apply_retention()
            if any(deleted.values()):
                LOGGER.info("PostgreSQL hot retention complete", extra={"deleted": deleted})

    def _create_watched_task(self, coroutine: Any, *, name: str) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine, name=name)
        self._watch_task(task)
        return task

    def _watch_task(self, task: asyncio.Task[None]) -> None:
        def completed(done: asyncio.Task[None]) -> None:
            if done.cancelled() or self.stop.is_set():
                return
            if self._task_failure is not None and not self._task_failure.done():
                self._task_failure.set_result((done.get_name(), done.exception()))

        task.add_done_callback(completed)

    async def _stop_market_tasks(self) -> None:
        shards = list(self.market_shards.values())
        for shard in shards:
            shard.planned_stop.set()
            shard.task.cancel()
        if shards:
            await asyncio.gather(*(shard.task for shard in shards), return_exceptions=True)
        self.market_shards.clear()

    async def _shutdown_tasks(self) -> None:
        self.stop.set()
        await self._stop_market_tasks()
        if self.reconciliation_task is not None:
            self.reconciliation_task.cancel()
            await asyncio.gather(self.reconciliation_task, return_exceptions=True)
            self.reconciliation_task = None
        for task in self.background_tasks:
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


def _subscription_fingerprints(markets: Iterable[MarketCandidate]) -> set[tuple[Any, ...]]:
    return {
        (market.external_id, tuple(sorted(market.outcome_token_ids)))
        for market in markets
    }
