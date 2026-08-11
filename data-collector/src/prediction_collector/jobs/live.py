from __future__ import annotations

import asyncio
import logging
from collections import deque
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from prediction_collector.common.coverage import select_live_markets
from prediction_collector.common.records import book_snapshot_item
from prediction_collector.common.types import LiveSelection, MarketCandidate
from prediction_collector.common.utils import utc_now
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.kalshi.auth import KalshiSigner
from prediction_collector.kalshi.parser import parse_orderbook_snapshot
from prediction_collector.kalshi.service import KalshiService
from prediction_collector.kalshi.websocket import KalshiWebSocket
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.polymarket.parser import parse_book
from prediction_collector.polymarket.rtds import PolymarketRtdsWebSocket
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.polymarket.sports import PolymarketSportsWebSocket
from prediction_collector.polymarket.websocket import PolymarketMarketWebSocket
from prediction_collector.writer import BatchWriter


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class LiveCoverageState:
    candidates: list[MarketCandidate] = field(default_factory=list)
    selection: LiveSelection | None = None
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
    exchange: str
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
        polymarket_service: PolymarketService | None,
        kalshi_service: KalshiService | None,
    ) -> None:
        self.settings = settings
        self.database = database
        self.writer = writer
        self.metrics = metrics
        self.polymarket_service = polymarket_service
        self.kalshi_service = kalshi_service
        self.coverage = LiveCoverageState()
        self._last_discovery_by_exchange: dict[str, list[MarketCandidate]] = {}
        self._economics_gaps: dict[tuple[str, str], list[int]] = {}
        self.stop = asyncio.Event()
        self.run_id: int | None = None
        self.market_shards: dict[tuple[str, int], MarketSocketShard] = {}
        self._next_market_shard_id: dict[str, int] = {
            "polymarket": 1,
            "kalshi": 1,
        }
        self.background_tasks: list[asyncio.Task[None]] = []
        self.reconciliation_tasks: dict[str, asyncio.Task[None]] = {}
        self._task_failure: asyncio.Future[tuple[str, BaseException | None]] | None = None

        self.polymarket_ws = (
            PolymarketMarketWebSocket(
                url=settings.polymarket_ws_url,
                writer=writer,
                database=database,
                metrics=metrics,
                store_raw=settings.store_raw_ws,
            )
            if polymarket_service
            else None
        )
        self.kalshi_ws: KalshiWebSocket | None = None
        if (
            kalshi_service
            and settings.kalshi_websocket_configured
            and settings.kalshi_private_key_path is not None
            and settings.kalshi_private_key_path.is_file()
        ):
            assert settings.kalshi_api_key_id and settings.kalshi_private_key_path
            self.kalshi_ws = KalshiWebSocket(
                url=settings.kalshi_ws_url,
                signer=KalshiSigner(
                    settings.kalshi_api_key_id, settings.kalshi_private_key_path
                ),
                writer=writer,
                database=database,
                metrics=metrics,
                store_raw=settings.store_raw_ws,
            )

    async def run(self) -> None:
        self.run_id = await self.database.start_run("live", None)
        self.writer.run_id = self.run_id
        await self.writer.start()
        self._task_failure = asyncio.get_running_loop().create_future()
        if self.writer.task is not None:
            self._watch_task(self.writer.task)
        try:
            await self._refresh_selection(restart=False)
            await self._start_market_tasks()
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
            await self.database.finish_run(
                self.run_id,
                status="partial" if self.writer.failed_items else "completed",
                records_processed=0,
                rows_written=self.writer.rows_written,
                coverage=self.coverage.metrics(),
            )
        except asyncio.CancelledError:
            self.stop.set()
            await self._shutdown_tasks()
            await self.writer.stop()
            if self.run_id is not None:
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
                LOGGER.exception("Database writer also failed during collector shutdown")
            if self.run_id is not None:
                await self.database.finish_run(
                    self.run_id,
                    status="failed",
                    records_processed=0,
                    rows_written=self.writer.rows_written,
                    error_summary=f"{type(exc).__name__}: {exc}",
                    coverage=self.coverage.metrics(),
                )
            raise

    async def _discover(self) -> list[MarketCandidate]:
        candidates: list[MarketCandidate] = []
        missing_without_cache: list[str] = []
        if self.polymarket_service:
            try:
                discovered = await self.polymarket_service.discover_live(
                    reconcile_absent=False
                )
                self._last_discovery_by_exchange["polymarket"] = discovered
                candidates.extend(discovered)
                self._schedule_absent_reconciliation(
                    "polymarket", self.polymarket_service, discovered
                )
            except Exception as exc:
                LOGGER.exception("Polymarket live discovery failed")
                cached = self._last_discovery_by_exchange.get("polymarket")
                if cached is None:
                    missing_without_cache.append("polymarket")
                else:
                    candidates.extend(cached)
                await self.database.record_gap(
                    run_id=self.run_id,
                    connection_id=None,
                    exchange="polymarket",
                    channel="rest:market_discovery",
                    market_external_id=None,
                    outcome_external_id=None,
                    gap_type="discovery_refresh_failed",
                    reconnect_reason=f"{type(exc).__name__}: {exc}",
                    details={"retained_cached_markets": len(cached or [])},
                )
        if self.kalshi_service:
            try:
                discovered = await self.kalshi_service.discover_live(
                    reconcile_absent=False
                )
                self._last_discovery_by_exchange["kalshi"] = discovered
                candidates.extend(discovered)
                self._schedule_absent_reconciliation(
                    "kalshi", self.kalshi_service, discovered
                )
            except Exception as exc:
                LOGGER.exception("Kalshi live discovery failed")
                cached = self._last_discovery_by_exchange.get("kalshi")
                if cached is None:
                    missing_without_cache.append("kalshi")
                else:
                    candidates.extend(cached)
                await self.database.record_gap(
                    run_id=self.run_id,
                    connection_id=None,
                    exchange="kalshi",
                    channel="rest:market_discovery",
                    market_external_id=None,
                    outcome_external_id=None,
                    gap_type="discovery_refresh_failed",
                    reconnect_reason=f"{type(exc).__name__}: {exc}",
                    details={"retained_cached_markets": len(cached or [])},
                )
        if missing_without_cache:
            raise RuntimeError(
                "Initial live discovery failed for enabled exchange(s): "
                + ", ".join(missing_without_cache)
            )
        if not candidates:
            raise RuntimeError("Live discovery returned no markets from enabled exchanges")
        return candidates

    def _schedule_absent_reconciliation(
        self,
        exchange: str,
        service: PolymarketService | KalshiService,
        candidates: list[MarketCandidate],
    ) -> None:
        current = self.reconciliation_tasks.get(exchange)
        if current is not None and not current.done():
            LOGGER.info(
                "Previous absent-market reconciliation is still running",
                extra={"exchange": exchange, "current_markets": len(candidates)},
            )
            return
        task = asyncio.create_task(
            self._reconcile_absent_markets(exchange, service, list(candidates)),
            name=f"{exchange}-absent-market-reconciliation",
        )
        self.reconciliation_tasks[exchange] = task

    async def _reconcile_absent_markets(
        self,
        exchange: str,
        service: PolymarketService | KalshiService,
        candidates: list[MarketCandidate],
    ) -> None:
        try:
            await service.reconcile_absent_live(candidates)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception(
                "Absent-market state reconciliation failed",
                extra={"exchange": exchange},
            )
            await self.database.record_gap(
                run_id=self.run_id,
                connection_id=None,
                exchange=exchange,
                channel="rest:market_state_reconciliation",
                market_external_id=None,
                outcome_external_id=None,
                gap_type="state_reconciliation_batch_failed",
                reconnect_reason=f"{type(exc).__name__}: {exc}",
                details={"current_markets": len(candidates)},
            )

    async def _refresh_selection(self, *, restart: bool) -> None:
        candidates = await self._discover()
        selection = select_live_markets(
            candidates,
            max_markets=self.settings.max_live_markets,
            min_volume=self.settings.min_live_market_volume,
            min_liquidity=self.settings.min_live_market_liquidity,
            allowlist=self.settings.live_market_allowlist,
            blocklist=self.settings.live_market_blocklist,
            unavailable_exchanges=(
                {"kalshi": "credentials_missing"}
                if self.kalshi_service is not None and self.kalshi_ws is None
                else {}
            ),
        )
        previous = {
            (market.exchange, market.external_id)
            for market in (self.coverage.selection.subscribed if self.coverage.selection else [])
        }
        current = {(market.exchange, market.external_id) for market in selection.subscribed}
        previous_subscriptions = _subscription_fingerprints(
            self.coverage.selection.subscribed if self.coverage.selection else []
        )
        current_subscriptions = _subscription_fingerprints(selection.subscribed)
        reasons = {
            (item.exchange, item.external_id): item.reason for item in selection.excluded
        }
        self.coverage = LiveCoverageState(candidates=candidates, selection=selection)
        confirmed_subscriptions = await self.database.active_subscribed_market_ids(
            self.run_id
        )
        await self.database.record_live_selection(
            self.run_id, candidates, confirmed_subscriptions, reasons
        )
        LOGGER.info(
            "Live market coverage",
            extra={
                "discovered_markets": selection.discovered,
                "active_markets": selection.active,
                "tradable_markets": selection.tradable,
                "markets_selected_for_subscription": len(selection.subscribed),
                "excluded_markets": len(selection.excluded),
                "exclusion_reasons": selection.excluded_counts,
                "max_live_markets": self.settings.max_live_markets,
            },
        )
        for excluded in selection.excluded:
            LOGGER.debug(
                "Live market excluded",
                extra={
                    "exchange": excluded.exchange,
                    "market": excluded.external_id,
                    "reason": excluded.reason,
                },
            )
        if restart:
            if current_subscriptions != previous_subscriptions:
                LOGGER.info(
                    "Live market set changed; reconciling subscription shards",
                    extra={
                        "added": len(current - previous),
                        "removed": len(previous - current),
                        "subscription_definitions_changed": len(
                            current_subscriptions.symmetric_difference(
                                previous_subscriptions
                            )
                        ),
                    },
                )
            await self._reconcile_market_tasks()

    async def _start_market_tasks(self) -> None:
        await self._reconcile_market_tasks()

    async def _reconcile_market_tasks(self) -> None:
        assert self.coverage.selection is not None
        selected = self.coverage.selection.subscribed
        desired: dict[str, dict[str, str]] = {
            "polymarket": {},
            "kalshi": {},
        }
        if self.polymarket_ws:
            for market in selected:
                if market.exchange == "polymarket":
                    desired["polymarket"].update(
                        {token: market.external_id for token in market.outcome_token_ids}
                    )
        if self.kalshi_ws:
            desired["kalshi"] = {
                market.external_id: market.external_id
                for market in selected
                if market.exchange == "kalshi"
            }

        await self._reconcile_exchange_shards(
            "polymarket",
            desired["polymarket"],
            self.settings.polymarket_ws_subscription_chunk_size,
        )
        await self._reconcile_exchange_shards(
            "kalshi",
            desired["kalshi"],
            self.settings.kalshi_ws_subscription_chunk_size,
        )

    async def _reconcile_exchange_shards(
        self,
        exchange: str,
        desired: dict[str, str],
        capacity: int,
    ) -> None:
        existing = sorted(
            (
                shard
                for shard in self.market_shards.values()
                if shard.exchange == exchange
            ),
            key=lambda shard: shard.shard_id,
        )
        proposals: dict[int, dict[str, str]] = {
            shard.shard_id: {
                identifier: desired[identifier]
                for identifier in shard.subscriptions
                if identifier in desired
            }
            for shard in existing
        }
        assigned = {
            identifier
            for subscription in proposals.values()
            for identifier in subscription
        }
        remaining = deque(
            identifier for identifier in sorted(desired) if identifier not in assigned
        )

        # Fill already-changing shards first, then rotate at most one stable
        # tail shard for additions. This preserves stable connections across a
        # changing uncapped universe without creating one tiny shard per poll.
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
            shard_id = self._next_market_shard_id[exchange]
            self._next_market_shard_id[exchange] += 1
            identifiers = [remaining.popleft() for _ in range(min(capacity, len(remaining)))]
            proposals[shard_id] = {
                identifier: desired[identifier] for identifier in identifiers
            }

        existing_by_id = {shard.shard_id: shard for shard in existing}
        for shard_id in sorted(set(existing_by_id) | set(proposals)):
            old = existing_by_id.get(shard_id)
            new_subscriptions = proposals.get(shard_id, {})
            if old is not None and old.subscriptions == new_subscriptions:
                continue
            await self._replace_market_shard(
                exchange,
                shard_id,
                old=old,
                new_subscriptions=new_subscriptions,
            )

    async def _replace_market_shard(
        self,
        exchange: str,
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
                    exchange=exchange,
                    channel="market" if exchange == "polymarket" else "orderbook_delta",
                    market_external_id=None,
                    outcome_external_id=None,
                    gap_type="planned_subscription_refresh",
                    reconnect_reason="subscription_definition_changed",
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
            self.market_shards.pop((exchange, shard_id), None)
        if not new_subscriptions:
            return

        planned_stop = asyncio.Event()
        if exchange == "polymarket":
            assert self.polymarket_ws is not None
            coroutine = self.polymarket_ws.run(
                new_subscriptions,
                run_id=self.run_id,
                stop=self.stop,
                connection_label=f"shard-{shard_id}",
                planned_stop=planned_stop,
                recovery_gap_ids=tuple(recovery_gap_ids),
            )
        else:
            assert self.kalshi_ws is not None
            coroutine = self.kalshi_ws.run_market_chunk(
                sorted(new_subscriptions),
                run_id=self.run_id,
                stop=self.stop,
                connection_label=f"shard-{shard_id}",
                planned_stop=planned_stop,
                recovery_gap_ids=tuple(recovery_gap_ids),
            )
        task = self._create_watched_task(
            coroutine,
            name=f"{exchange}-market-ws-{shard_id}",
        )
        self.market_shards[(exchange, shard_id)] = MarketSocketShard(
            exchange=exchange,
            shard_id=shard_id,
            subscriptions=dict(new_subscriptions),
            task=task,
            planned_stop=planned_stop,
        )

    async def _start_background_tasks(self) -> None:
        if self.polymarket_service and self.settings.polymarket_rtds_enabled:
            rtds = PolymarketRtdsWebSocket(
                url=self.settings.polymarket_rtds_url,
                writer=self.writer,
                database=self.database,
                metrics=self.metrics,
                store_raw=self.settings.store_raw_ws,
                equity_symbols=self.settings.polymarket_equity_symbols,
                comments_enabled=self.settings.polymarket_comments_enabled,
            )
            self.background_tasks.append(
                self._create_watched_task(
                    rtds.run(run_id=self.run_id, stop=self.stop), name="polymarket-rtds"
                )
            )
        if self.polymarket_service and self.settings.polymarket_sports_enabled:
            sports = PolymarketSportsWebSocket(
                url=self.settings.polymarket_sports_ws_url,
                writer=self.writer,
                database=self.database,
                metrics=self.metrics,
                store_raw=self.settings.store_raw_ws,
            )
            self.background_tasks.append(
                self._create_watched_task(
                    sports.run(run_id=self.run_id, stop=self.stop),
                    name="polymarket-sports",
                )
            )
        if self.polymarket_service:
            self.background_tasks.append(
                self._create_watched_task(
                    self._polymarket_fee_rate_loop(),
                    name="polymarket-fee-rate-refresh",
                )
            )
        if self.kalshi_service and self.kalshi_ws is None:
            LOGGER.warning(
                "Kalshi WebSocket collection disabled: KALSHI_API_KEY_ID and "
                "KALSHI_PRIVATE_KEY_PATH are both required; public REST remains enabled"
            )
        if self.kalshi_ws:
            self.background_tasks.append(
                self._create_watched_task(
                    self.kalshi_ws.run_lifecycle(run_id=self.run_id, stop=self.stop),
                    name="kalshi-lifecycle-ws",
                )
            )
            if self.settings.kalshi_reference_feeds_enabled:
                for channel in ("cfbenchmarks_value", "pyth_value"):
                    self.background_tasks.append(
                        self._create_watched_task(
                            self.kalshi_ws.run_reference(
                                channel, run_id=self.run_id, stop=self.stop
                            ),
                            name=f"kalshi-{channel}",
                        )
                    )
        self.background_tasks.extend(
            [
                self._create_watched_task(self._discovery_loop(), name="market-discovery"),
                self._create_watched_task(
                    self._economics_loop(), name="economics-refresh"
                ),
                self._create_watched_task(self._metrics_loop(), name="throughput-metrics"),
                self._create_watched_task(self._snapshot_loop(), name="market-snapshots"),
                self._create_watched_task(self._reconcile_loop(), name="orderbook-reconciliation"),
            ]
        )

    def _create_watched_task(
        self, coroutine: Any, *, name: str
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine, name=name)
        self._watch_task(task)
        return task

    def _watch_task(self, task: asyncio.Task[None]) -> None:
        def completed(done: asyncio.Task[None]) -> None:
            # Market-subscription refresh intentionally cancels the old tasks.
            if done.cancelled() or self.stop.is_set():
                return
            failure = self._task_failure
            if failure is None or failure.done():
                return
            failure.set_result((done.get_name(), done.exception()))

        task.add_done_callback(completed)

    async def _discovery_loop(self) -> None:
        while not self.stop.is_set():
            await _sleep(self.stop, self.settings.metadata_sync_interval_seconds)
            if self.stop.is_set():
                return
            try:
                await self._refresh_selection(restart=True)
            except Exception:
                LOGGER.exception("Periodic live market discovery failed")

    async def _economics_loop(self) -> None:
        while not self.stop.is_set():
            await self._sync_economics_once()
            await _sleep(self.stop, self.settings.economics_sync_interval_seconds)

    async def _sync_economics_once(self) -> None:
        refreshes = []
        if self.polymarket_service:
            refreshes.append(
                self._sync_exchange_economics(
                    "polymarket",
                    self.polymarket_service,
                    include_fee_rates=False,
                    include_rewards=True,
                )
            )
        if self.kalshi_service:
            refreshes.append(
                self._sync_exchange_economics("kalshi", self.kalshi_service)
            )
        if refreshes:
            await asyncio.gather(*refreshes)

    async def _polymarket_fee_rate_loop(self) -> None:
        assert self.polymarket_service is not None
        while not self.stop.is_set():
            await self._sync_polymarket_fee_rates_once()
            await _sleep(
                self.stop,
                self.settings.polymarket_fee_rate_sync_interval_seconds,
            )

    async def _sync_polymarket_fee_rates_once(self) -> None:
        assert self.polymarket_service is not None
        await self._sync_exchange_economics(
            "polymarket",
            self.polymarket_service,
            include_fee_rates=True,
            include_rewards=False,
            fee_rate_live_only=True,
            channel="rest:fee_rate_refresh",
            gap_type="fee_rate_refresh_failed",
            interval_seconds=self.settings.polymarket_fee_rate_sync_interval_seconds,
        )

    async def _sync_exchange_economics(
        self,
        exchange: str,
        service: Any,
        *,
        include_fee_rates: bool | None = None,
        include_rewards: bool | None = None,
        fee_rate_live_only: bool | None = None,
        channel: str = "rest:economics_refresh",
        gap_type: str = "economics_refresh_failed",
        interval_seconds: int | None = None,
    ) -> None:
        try:
            options: dict[str, bool] = {}
            if include_fee_rates is not None:
                options["include_fee_rates"] = include_fee_rates
            if include_rewards is not None:
                options["include_rewards"] = include_rewards
            if fee_rate_live_only is not None:
                options["fee_rate_live_only"] = fee_rate_live_only
            counts = await service.sync_fees_and_incentives(**options)
            gap_key = (exchange, channel)
            unresolved = self._economics_gaps.pop(gap_key, [])
            still_open: list[int] = []
            for gap_id in unresolved:
                try:
                    await self.database.resolve_gap(
                        gap_id,
                        action="successful_economics_refresh",
                    )
                except Exception:
                    still_open.append(gap_id)
                    LOGGER.exception(
                        "Failed to resolve recovered economics data gap",
                        extra={
                            "exchange": exchange,
                            "channel": channel,
                            "gap_id": gap_id,
                        },
                    )
            if still_open:
                self._economics_gaps[gap_key] = still_open
            LOGGER.info(
                "Live economics refresh complete",
                extra={"exchange": exchange, "channel": channel, "counts": counts},
            )
        except Exception as exc:
            LOGGER.exception(
                "Live economics refresh failed",
                extra={"exchange": exchange, "channel": channel},
            )
            try:
                gap_id = await self.database.record_gap(
                    run_id=self.run_id,
                    connection_id=None,
                    exchange=exchange,
                    channel=channel,
                    market_external_id=None,
                    outcome_external_id=None,
                    gap_type=gap_type,
                    reconnect_reason=f"{type(exc).__name__}: {exc}",
                    details={
                        "interval_seconds": (
                            interval_seconds
                            or self.settings.economics_sync_interval_seconds
                        ),
                        "fee_rates_included": include_fee_rates,
                        "rewards_included": include_rewards,
                        "fee_rate_live_only": fee_rate_live_only,
                    },
                )
                self._economics_gaps.setdefault((exchange, channel), []).append(
                    gap_id
                )
            except Exception:
                # One exchange and its observability write must not prevent the
                # other exchange from refreshing or terminate the live loop.
                LOGGER.exception(
                    "Failed to persist live economics data gap",
                    extra={"exchange": exchange},
                )

    async def _metrics_loop(self) -> None:
        interval = self.settings.metrics_log_interval_seconds
        # Establish the server-side baseline before the first observation
        # window so direct and batch writes are both measured.
        await self.database.database_row_write_deltas()
        while not self.stop.is_set():
            interval_start = utc_now()
            await _sleep(self.stop, interval)
            if self.stop.is_set():
                return
            snapshot = await self.metrics.snapshot_and_reset()
            application_rates = dict(
                snapshot.get("database_rows_per_minute", {})
            )
            server_deltas = await self.database.database_row_write_deltas()
            scale = 60.0 / max(interval, 1)
            snapshot["application_accounted_database_rows_per_minute"] = (
                application_rates
            )
            snapshot["database_rows_per_minute"] = {
                table: round(count * scale, 2)
                for table, count in server_deltas.items()
            }
            snapshot["database_rows_source"] = "pg_stat_user_tables"
            self.coverage.confirmed_subscribed = (
                await self.database.active_subscribed_market_count(self.run_id)
            )
            coverage = self.coverage.metrics()
            LOGGER.info(
                "Collector throughput",
                extra={
                    **snapshot,
                    "active_markets": coverage["active"],
                    "tradable_markets": coverage["tradable"],
                    "subscribed_markets": coverage["subscribed"],
                    "excluded_markets": coverage["excluded"],
                },
            )
            await self.database.record_metrics(
                run_id=self.run_id,
                interval_start=interval_start,
                interval_seconds=interval,
                snapshot=snapshot,
                coverage=coverage,
            )

    async def _snapshot_loop(self) -> None:
        while not self.stop.is_set():
            await _sleep(self.stop, self.settings.market_snapshot_interval_seconds)
            if self.stop.is_set():
                return
            if self.polymarket_ws:
                for item in self.polymarket_ws.market_snapshot_items():
                    await self.writer.put(item)

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            await _sleep(self.stop, self.settings.orderbook_reconcile_interval_seconds)
            if self.stop.is_set():
                return
            await self._reconcile_once()

    async def _reconcile_once(self) -> None:
        """Archive independent REST snapshots without rewriting live WS state.

        REST fetches race with incremental WebSocket deltas and carry no shared
        ordering boundary.  They are useful audit/reconciliation observations,
        but applying them to an in-memory live book would silently discard any
        delta received while the request was in flight.
        """
        if self.coverage.selection is None:
            return
        for market in self.coverage.selection.subscribed:
            if self.stop.is_set():
                return
            try:
                if market.exchange == "polymarket" and self.polymarket_service:
                    for token in market.outcome_token_ids:
                        result = await self.polymarket_service.rest.orderbook(token)
                        raw = result.data if isinstance(result.data, dict) else {}
                        snapshot = parse_book(raw)
                        if snapshot:
                            await self.writer.put(
                                book_snapshot_item(snapshot, reconciliation=True)
                            )
                elif market.exchange == "kalshi" and self.kalshi_service:
                    result = await self.kalshi_service.rest.orderbook(market.external_id)
                    raw = (
                        result.data.get("orderbook", result.data)
                        if isinstance(result.data, dict)
                        else {}
                    )
                    envelope = {
                        "type": "orderbook_snapshot",
                        "msg": {"market_ticker": market.external_id, **raw},
                    }
                    snapshot = parse_orderbook_snapshot(envelope, use_yes_price=False)
                    if snapshot:
                        await self.writer.put(
                            book_snapshot_item(snapshot, reconciliation=True)
                        )
            except Exception:
                LOGGER.exception(
                    "Orderbook reconciliation failed",
                    extra={"exchange": market.exchange, "market": market.external_id},
                )

    async def _stop_market_tasks(self) -> None:
        shards = list(self.market_shards.values())
        for shard in shards:
            shard.task.cancel()
        if shards:
            await asyncio.gather(
                *(shard.task for shard in shards),
                return_exceptions=True,
            )
        self.market_shards.clear()

    async def _shutdown_tasks(self) -> None:
        self.stop.set()
        await self._stop_market_tasks()
        for task in self.reconciliation_tasks.values():
            task.cancel()
        if self.reconciliation_tasks:
            await asyncio.gather(
                *self.reconciliation_tasks.values(), return_exceptions=True
            )
        self.reconciliation_tasks.clear()
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
    """Describe the actual socket subscription, including mutable Poly token sets."""
    return {
        (
            market.exchange,
            market.external_id,
            tuple(sorted(market.outcome_token_ids))
            if market.exchange == "polymarket"
            else (),
        )
        for market in markets
    }
