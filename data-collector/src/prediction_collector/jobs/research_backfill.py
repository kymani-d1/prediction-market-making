from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Mapping

from prediction_collector.common.diagnostics import process_memory_snapshot
from prediction_collector.common.records import trade_item
from prediction_collector.common.types import ResearchMarket
from prediction_collector.common.utils import (
    as_decimal,
    parse_timestamp,
    request_parameters,
    utc_now,
)
from prediction_collector.config import Settings
from prediction_collector.polymarket.parser import parse_trade
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)
_TERMINAL = frozenset({"completed", "partial", "unavailable", "not_applicable"})


@dataclass(frozen=True, slots=True)
class ResearchBackfillResult:
    records_processed: int
    rows_written: int
    details: dict[str, object]
    status: str


async def run_polymarket_research_backfill(
    service: PolymarketService,
    writer: BatchWriter,
    settings: Settings,
    *,
    phase: str = "all",
    cohort_version: str | None = None,
    max_markets: int | None = None,
) -> ResearchBackfillResult:
    if phase not in {"all", "catalogue", "cohort", "data"}:
        raise ValueError(f"unsupported research backfill phase: {phase}")
    version = cohort_version or settings.research_backfill_cohort_version
    limit = (
        settings.research_backfill_max_markets
        if max_markets is None
        else max_markets
    )
    if not 1 <= limit <= settings.research_backfill_hard_max_markets:
        raise ValueError(
            "research cohort exceeds its configured safety bound "
            f"({limit} > {settings.research_backfill_hard_max_markets})"
        )

    details: dict[str, object] = {
        "mode": "bounded_research_dataset_bootstrap",
        "cohort_version": version,
        "requested_max_markets": limit,
        "memory_start": process_memory_snapshot(),
        "excluded_legacy_stages": [
            "comments",
            "holders",
            "current_order_books",
            "live_tier_assignments",
            "exhaustive_fee_lookups",
            "exhaustive_current_rewards",
        ],
    }
    await writer.start()
    try:
        horizon_start = utc_now() - timedelta(
            days=settings.research_backfill_catalogue_horizon_days
        )
        if phase in {"all", "catalogue"}:
            if settings.research_backfill_refresh_catalogue:
                details["catalogue"] = await service.sync_metadata(
                    include_closed=True,
                    closed_since=horizon_start,
                    checkpoint_prefix=f"research:{version}:",
                    archive_raw_rest=settings.research_backfill_archive_raw_rest,
                )
            else:
                details["catalogue"] = {
                    "status": "reused_existing_catalogue",
                    "refresh_configured": False,
                    "horizon_days": settings.research_backfill_catalogue_horizon_days,
                }
        if phase in {"all", "cohort"}:
            criteria = {
                "method": "stratified_deterministic_sample",
                "method_version": 2,
                "horizon_days": settings.research_backfill_catalogue_horizon_days,
                "dimensions": [
                    "category",
                    "volume_bucket",
                    "liquidity_bucket",
                    "duration_bucket",
                    "lifecycle_bucket",
                    "calendar_quarter",
                ],
                "volume_buckets_usdc": [0, 1_000, 100_000],
                "liquidity_buckets_usdc": [0, 1_000, 10_000],
                "duration_buckets_days": [1, 7, 30],
                "category_source": "event_metadata_then_question_taxonomy_v1",
                "category_balance": "round_robin_before_full_strata_depth",
                "requires_outcome_token": True,
                "active_markets_included": True,
            }
            details["cohort"] = dict(
                await service.database.create_or_get_research_cohort(
                    exchange="polymarket",
                    version=version,
                    seed=settings.research_backfill_seed,
                    max_markets=limit,
                    horizon_start=horizon_start,
                    criteria=criteria,
                )
            )
        if phase in {"all", "data"}:
            details["historical_data"] = await _collect_cohort_data(
                service,
                writer,
                settings,
                cohort_version=version,
            )
        await writer.queue.join()
        if writer.archive is not None:
            await writer.archive.join()
    finally:
        await writer.stop()

    if phase in {"all", "cohort", "data"}:
        details["cohort_status"] = dict(
            await service.database.research_cohort_status(
                exchange="polymarket", cohort_version=version
            )
        )
    details["memory_end"] = process_memory_snapshot()
    details["write_failures"] = writer.failed_items
    historical_data = details.get("historical_data")
    catalogue = details.get("catalogue")
    cohort = details.get("cohort")
    if isinstance(historical_data, Mapping):
        processed = int(historical_data.get("markets_attempted") or 0)
    elif isinstance(cohort, Mapping):
        processed = int(cohort.get("selected_count") or 0)
    elif isinstance(catalogue, Mapping):
        processed = sum(
            int(catalogue.get(key) or 0)
            for key in ("series", "events", "markets", "outcomes", "tags")
        )
    else:
        processed = 0
    cohort_status = details.get("cohort_status")
    partial = bool(
        writer.failed_items
        or (
            isinstance(cohort_status, Mapping)
            and (
                int(cohort_status.get("partial_markets") or 0) > 0
                or int(cohort_status.get("retryable_failed_markets") or 0) > 0
            )
        )
    )
    return ResearchBackfillResult(
        records_processed=processed,
        rows_written=writer.rows_written,
        details=details,
        status="partial" if partial else "completed",
    )


async def _collect_cohort_data(
    service: PolymarketService,
    writer: BatchWriter,
    settings: Settings,
    *,
    cohort_version: str,
) -> dict[str, Any]:
    concurrency = settings.research_backfill_request_concurrency
    batch: list[ResearchMarket] = []
    markets_seen = 0
    markets_skipped_complete = 0
    bounded_batches = 0

    async def run_batch(values: list[ResearchMarket]) -> None:
        nonlocal bounded_batches
        bounded_batches += 1
        async with asyncio.TaskGroup() as tasks:
            for market in values:
                tasks.create_task(
                    _collect_market(service, writer, settings, market),
                    name=f"research-market-{market.selection_rank}",
                )

    async for market in service.database.iter_research_markets(
        exchange="polymarket",
        cohort_version=cohort_version,
        batch_size=settings.research_backfill_candidate_batch_size,
    ):
        markets_seen += 1
        if all(
            value in _TERMINAL
            for value in (
                market.price_status,
                market.trade_status,
                market.resolution_status,
                market.economics_status,
            )
        ):
            markets_skipped_complete += 1
            continue
        batch.append(market)
        if len(batch) >= concurrency:
            await run_batch(batch)
            batch = []
            LOGGER.info(
                "Research backfill bounded batch complete",
                extra={
                    "cohort_version": cohort_version,
                    "markets_seen": markets_seen,
                    "request_concurrency_bound": concurrency,
                    "process_memory": process_memory_snapshot(),
                },
            )
    if batch:
        await run_batch(batch)
    return {
        "markets_seen": markets_seen,
        "markets_skipped_complete": markets_skipped_complete,
        "markets_attempted": markets_seen - markets_skipped_complete,
        "bounded_batches": bounded_batches,
        "request_concurrency_bound": concurrency,
        "candidate_batch_size": settings.research_backfill_candidate_batch_size,
        "price_fidelity_minutes": settings.research_backfill_price_fidelity_minutes,
        "raw_rest_archived": settings.research_backfill_archive_raw_rest,
    }


async def _collect_market(
    service: PolymarketService,
    writer: BatchWriter,
    settings: Settings,
    market: ResearchMarket,
) -> None:
    if market.price_status not in _TERMINAL:
        await _collect_prices(service, writer, settings, market)
    if market.trade_status not in _TERMINAL:
        await _collect_trades(service, writer, settings, market)
    if market.resolution_status not in _TERMINAL:
        await _record_resolution(service, market)
    if market.economics_status not in _TERMINAL:
        await _record_economics(service, market)


async def _collect_prices(
    service: PolymarketService,
    writer: BatchWriter,
    settings: Settings,
    market: ResearchMarket,
) -> None:
    token_ids = [outcome.token_id for outcome in market.outcomes]
    if not token_ids:
        await _set_phase(
            service,
            market,
            "price",
            "unavailable",
            provenance={"reason": "no_outcome_tokens"},
        )
        return
    if len(token_ids) > 20:
        raise RuntimeError(
            f"market {market.external_id} exceeds the 20-token batch API bound"
        )
    await _set_phase(service, market, "price", "in_progress")
    result: Any = None
    try:
        result = await service.rest.batch_price_history(
            token_ids,
            interval="max",
            fidelity_minutes=settings.research_backfill_price_fidelity_minutes,
        )
        if settings.research_backfill_archive_raw_rest:
            await service._raw_result(
                "clob",
                "/batch-prices-history",
                "research_price_history",
                result,
                market.external_id,
            )
        payload = result.data if isinstance(result.data, Mapping) else {}
        histories = payload.get("history")
        if not isinstance(histories, Mapping):
            raise RuntimeError("Polymarket batch price response omitted history map")
        record_count = 0
        token_counts: dict[str, int] = {}
        missing_tokens: list[str] = []
        interval_seconds = settings.research_backfill_price_fidelity_minutes * 60
        retrieved_at = utc_now()
        for token_id in token_ids:
            history = histories.get(token_id)
            if not isinstance(history, list):
                missing_tokens.append(token_id)
                continue
            token_count = 0
            for point in history:
                if not isinstance(point, Mapping):
                    continue
                timestamp = parse_timestamp(point.get("t"))
                price = as_decimal(point.get("p"))
                if timestamp is None or price is None:
                    continue
                await writer.put(
                    WriteItem(
                        "candlesticks",
                        {
                            "exchange": "polymarket",
                            "market_external_id": market.external_id,
                            "outcome_external_id": token_id,
                            "interval_seconds": interval_seconds,
                            "period_start": timestamp,
                            "period_end": timestamp
                            + timedelta(seconds=interval_seconds),
                            "open": price,
                            "high": price,
                            "low": price,
                            "close": price,
                            "bid_open": None,
                            "bid_high": None,
                            "bid_low": None,
                            "bid_close": None,
                            "ask_open": None,
                            "ask_high": None,
                            "ask_low": None,
                            "ask_close": None,
                            "volume": None,
                            "open_interest": None,
                            "source_timestamp": timestamp,
                            "retrieved_at": retrieved_at,
                            "raw_data": {
                                "source": "clob_batch_prices_history",
                                "fidelity_minutes": (
                                    settings.research_backfill_price_fidelity_minutes
                                ),
                            },
                        },
                    )
                )
                record_count += 1
                token_count += 1
            token_counts[token_id] = token_count
        await writer.queue.join()
        if writer.archive is not None:
            await writer.archive.join()
        if record_count == 0:
            status = "unavailable"
        elif missing_tokens or any(count == 0 for count in token_counts.values()):
            status = "partial"
        else:
            status = "completed"
        await _set_phase(
            service,
            market,
            "price",
            status,
            records=record_count,
            requests=1,
            partial_history=status == "partial",
            provenance={
                "endpoint": "/batch-prices-history",
                "interval": "max",
                "fidelity_minutes": (
                    settings.research_backfill_price_fidelity_minutes
                ),
                "token_counts": token_counts,
                "missing_tokens": missing_tokens,
                "http_status": result.status_code,
                "parameters": request_parameters(result.url),
                "retrieved_at": retrieved_at,
            },
        )
    except Exception as exc:
        await _set_phase(
            service,
            market,
            "price",
            "retryable_failed",
            requests=1,
            provenance={"endpoint": "/batch-prices-history"},
            error_summary=f"{type(exc).__name__}: {exc}",
        )
        raise


async def _collect_trades(
    service: PolymarketService,
    writer: BatchWriter,
    settings: Settings,
    market: ResearchMarket,
) -> None:
    await _set_phase(service, market, "trade", "in_progress")
    records = 0
    requests = 0
    pages_fetched = 0
    windows_completed = 0
    saturated_windows = 0
    end = int(utc_now().timestamp())
    documented_floor = end - (3 * 365 * 24 * 60 * 60)
    opened_epoch = int(market.open_time.timestamp()) if market.open_time else None
    start = max(documented_floor, opened_epoch or documented_floor)
    history_floor = bool(opened_epoch is not None and opened_epoch < documented_floor)
    try:
        if history_floor:
            await service.database.record_gap(
                run_id=writer.run_id,
                connection_id=None,
                exchange="polymarket",
                channel="rest:research-trades",
                market_external_id=market.external_id,
                outcome_external_id=None,
                gap_type="upstream_history_floor",
                details={
                    "market_open_epoch": opened_epoch,
                    "requested_start_epoch": start,
                    "documented_market_history_floor_years": 3,
                },
            )
        pending_windows: list[tuple[int, int]] = [(start, end)]
        while pending_windows:
            window_start, window_end = pending_windows.pop()
            pages: list[tuple[list[dict[str, Any]], Any]] = []
            async for items, result in service.rest.iter_trades(
                market=market.external_id,
                start=window_start,
                end=window_end,
                page_size=10_000,
                max_offset=10_000,
            ):
                pages.append((items, result))
                pages_fetched += 1
                if settings.research_backfill_archive_raw_rest:
                    await service._raw_page(
                        "data",
                        "/trades",
                        "research_trades",
                        items,
                        result,
                        external_key=(
                            f"{market.external_id}:{window_start}:{window_end}"
                        ),
                    )
            if not pages:
                requests += 1
            else:
                requests += len(pages)
                if len(pages) == 1 and len(pages[0][0]) == 10_000:
                    # The iterator issued the terminating offset=10000 request,
                    # which can legitimately return an empty page.
                    requests += 1
            saturated = bool(
                len(pages) >= 2 and len(pages[-1][0]) == 10_000
            )
            if saturated and window_start < window_end:
                midpoint = window_start + (window_end - window_start) // 2
                pending_windows.append((midpoint + 1, window_end))
                pending_windows.append((window_start, midpoint))
                continue
            windows_completed += 1
            if saturated:
                saturated_windows += 1
                await service.database.record_gap(
                    run_id=writer.run_id,
                    connection_id=None,
                    exchange="polymarket",
                    channel="rest:research-trades",
                    market_external_id=market.external_id,
                    outcome_external_id=None,
                    gap_type="rest_pagination_limit",
                    details={
                        "window_start_epoch": window_start,
                        "window_end_epoch": window_end,
                        "documented_max_offset": 10_000,
                    },
                )
            for items, _result in pages:
                received_at = utc_now()
                monotonic_ns = time.monotonic_ns()
                for raw in items:
                    trade = parse_trade(
                        raw,
                        received_at=received_at,
                        received_monotonic_ns=monotonic_ns,
                    )
                    if trade is None:
                        continue
                    await writer.put(trade_item(trade))
                    records += 1
        await writer.queue.join()
        if writer.archive is not None:
            await writer.archive.join()
        partial = history_floor or saturated_windows > 0
        await _set_phase(
            service,
            market,
            "trade",
            "partial" if partial else "completed",
            records=records,
            requests=requests,
            partial_history=partial,
            provenance={
                "endpoint": "/trades",
                "window_start_epoch": start,
                "window_end_epoch": end,
                "pages_fetched": pages_fetched,
                "windows_completed": windows_completed,
                "saturated_windows": saturated_windows,
                "upstream_history_floor": history_floor,
                "documented_max_offset": 10_000,
            },
        )
    except Exception as exc:
        await _set_phase(
            service,
            market,
            "trade",
            "retryable_failed",
            records=records,
            requests=requests,
            provenance={
                "endpoint": "/trades",
                "pages_fetched": pages_fetched,
                "windows_completed": windows_completed,
            },
            error_summary=f"{type(exc).__name__}: {exc}",
        )
        raise


async def _record_resolution(
    service: PolymarketService, market: ResearchMarket
) -> None:
    winner = next(
        (
            outcome
            for outcome in market.outcomes
            if outcome.last_price is not None and outcome.last_price == 1
        ),
        None,
    )
    closed = market.status.lower() in {"closed", "resolved", "settled", "finalized"}
    if market.active and not closed:
        status = "not_applicable"
        reason = "market_not_resolved"
    elif market.result is not None or winner is not None:
        status = "completed"
        reason = "normalized_result" if market.result is not None else "terminal_price"
    else:
        status = "unavailable"
        reason = "closed_market_without_recoverable_result"
    await _set_phase(
        service,
        market,
        "resolution",
        status,
        provenance={
            "reason": reason,
            "market_result": market.result,
            "settlement_time": market.settlement_time,
            "winner_token_id": winner.token_id if winner else None,
            "winner_name": winner.name if winner else None,
        },
    )


async def _record_economics(
    service: PolymarketService, market: ResearchMarket
) -> None:
    raw = market.raw_data
    fees_enabled = raw.get("feesEnabled")
    fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
    has_fee_evidence = (
        fees_enabled is False
        or isinstance(fee_schedule, Mapping)
        or market.fee_rate is not None
    )
    has_reward_evidence = any(
        raw.get(key) is not None
        for key in (
            "rewardsMinSize",
            "rewardsMaxSpread",
            "rewards_min_size",
            "rewards_max_spread",
        )
    )
    await _set_phase(
        service,
        market,
        "economics",
        "completed" if has_fee_evidence or has_reward_evidence else "unavailable",
        provenance={
            "source": "gamma_market_metadata",
            "historical_reconstruction": False,
            "fee_status": (
                "fee_free"
                if fees_enabled is False
                else "current_metadata"
                if isinstance(fee_schedule, Mapping) or market.fee_rate is not None
                else "unknown"
            ),
            "fees_enabled": fees_enabled,
            "fee_schedule": fee_schedule,
            "normalized_fee_rate": market.fee_rate,
            "maker_base_fee": raw.get("makerBaseFee"),
            "taker_base_fee": raw.get("takerBaseFee"),
            "reward_status": "current_metadata" if has_reward_evidence else "unknown",
            "rewards_min_size": (
                raw.get("rewardsMinSize") or raw.get("rewards_min_size")
            ),
            "rewards_max_spread": (
                raw.get("rewardsMaxSpread") or raw.get("rewards_max_spread")
            ),
            "network_requests": 0,
        },
    )


async def _set_phase(
    service: PolymarketService,
    market: ResearchMarket,
    phase: str,
    status: str,
    *,
    records: int = 0,
    requests: int = 0,
    partial_history: bool = False,
    provenance: Mapping[str, Any] | None = None,
    error_summary: str | None = None,
) -> None:
    await service.database.set_research_phase_status(
        cohort_id=market.cohort_id,
        market_id=market.market_id,
        phase=phase,
        status=status,
        records=records,
        requests=requests,
        partial_history=partial_history,
        provenance=provenance,
        error_summary=error_summary,
    )
