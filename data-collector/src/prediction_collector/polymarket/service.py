from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from prediction_collector.common.records import book_snapshot_item, trade_item
from prediction_collector.common.types import MarketCandidate
from prediction_collector.common.utils import (
    as_decimal,
    content_hash,
    first_present,
    parse_timestamp,
    request_parameters,
    utc_now,
)
from prediction_collector.database import Database, MetadataSyncDiagnostics
from prediction_collector.polymarket.parser import (
    normalise_event,
    normalise_market,
    normalise_series,
    parse_book,
    parse_market_candidate,
    parse_trade,
)
from prediction_collector.polymarket.rest import PolymarketRestClient
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)


class PolymarketService:
    def __init__(
        self,
        *,
        rest: PolymarketRestClient,
        database: Database,
        writer: BatchWriter,
        store_raw_rest: bool,
    ) -> None:
        self.rest = rest
        self.database = database
        self.writer = writer
        self.store_raw_rest = store_raw_rest

    async def sync_metadata(self, *, include_closed: bool = True) -> dict[str, int]:
        counts = {"series": 0, "events": 0, "markets": 0, "outcomes": 0, "tags": 0}
        diagnostics = MetadataSyncDiagnostics()
        try:
            async for items, result in self.rest.iter_series():
                await self._raw_page("gamma", "/series", "series", items, result)
                for raw in items:
                    await self.database.upsert_series(normalise_series(raw))
                    counts["series"] += 1
        except Exception:
            LOGGER.exception("Polymarket series sync failed; continuing metadata sync")

        try:
            async for items, result in self.rest.iter_tags():
                await self._raw_page("gamma", "/tags", "tags", items, result)
                for raw in items:
                    await self.database.upsert_tag("polymarket", raw)
                    counts["tags"] += 1
        except Exception:
            LOGGER.exception("Polymarket tag sync failed; continuing metadata sync")

        states = [False, True] if include_closed else [False]
        for closed in states:
            async for items, result, cursor in self.rest.iter_events(closed=closed):
                await self._raw_page(
                    "gamma", "/events/keyset", "events", items, result, external_key=cursor
                )
                for raw in items:
                    for series_raw in _as_dict_list(raw.get("series")):
                        await self.database.upsert_series(normalise_series(series_raw))
                    await self.database.upsert_event(normalise_event(raw))
                    counts["events"] += 1
                await self.database.checkpoint(
                    "polymarket",
                    "metadata_events",
                    checkpoint_key=f"closed={str(closed).lower()}",
                    cursor=cursor,
                    timestamp=utc_now(),
                    metadata={"records": counts["events"]},
                )

            async for items, result, cursor in self.rest.iter_markets(closed=closed):
                await self._raw_page(
                    "gamma", "/markets/keyset", "markets", items, result, external_key=cursor
                )
                for raw in items:
                    event_external_id: str | None = None
                    nested_events = _as_dict_list(raw.get("events"))
                    if nested_events:
                        event = normalise_event(nested_events[0])
                        event_external_id = event["external_id"]
                        await self.database.upsert_event(event)
                    market, outcomes = normalise_market(
                        raw, event_external_id=event_external_id
                    )
                    market["exchange_timestamp"] = result.response_timestamp
                    market["exchange_timestamp_is_transport"] = True
                    market["observed_at"] = (
                        getattr(result, "requested_at", None) or utc_now()
                    )
                    market_id = await self.database.upsert_market(
                        market, diagnostics=diagnostics
                    )
                    counts["markets"] += 1
                    for outcome in outcomes:
                        await self.database.upsert_outcome(market_id, outcome)
                        counts["outcomes"] += 1
                await self.database.checkpoint(
                    "polymarket",
                    "metadata_markets",
                    checkpoint_key=f"closed={str(closed).lower()}",
                    cursor=cursor,
                    timestamp=utc_now(),
                    metadata={"records": counts["markets"]},
                )
        LOGGER.info(
            "Polymarket metadata sync complete",
            extra={**counts, **diagnostics.as_log_fields()},
        )
        return counts

    async def discover_live(
        self, *, reconcile_absent: bool = True
    ) -> list[MarketCandidate]:
        candidates: list[MarketCandidate] = []
        diagnostics = MetadataSyncDiagnostics()
        async for items, result, cursor in self.rest.iter_markets(closed=False):
            await self._raw_page(
                "gamma", "/markets/keyset", "markets", items, result, external_key=cursor
            )
            for raw in items:
                candidate = parse_market_candidate(raw)
                candidates.append(candidate)
                market, outcomes = normalise_market(raw)
                market["exchange_timestamp"] = result.response_timestamp
                market["exchange_timestamp_is_transport"] = True
                market["observed_at"] = (
                    getattr(result, "requested_at", None) or utc_now()
                )
                market_id = await self.database.upsert_market(
                    market, diagnostics=diagnostics
                )
                for outcome in outcomes:
                    await self.database.upsert_outcome(market_id, outcome)
        if not candidates:
            raise RuntimeError("Polymarket complete open-market discovery returned zero markets")
        if reconcile_absent:
            await self.reconcile_absent_live(
                candidates,
                diagnostics=diagnostics,
                emit_summary=False,
            )
        LOGGER.info(
            "Polymarket live metadata sync complete",
            extra={"markets": len(candidates), **diagnostics.as_log_fields()},
        )
        return candidates

    async def reconcile_absent_live(
        self,
        candidates: list[MarketCandidate],
        *,
        diagnostics: MetadataSyncDiagnostics | None = None,
        emit_summary: bool = True,
    ) -> None:
        """Enrich markets absent from a complete open pass without blocking sockets."""
        diagnostics = diagnostics or MetadataSyncDiagnostics()
        absent = await self.database.absent_active_markets(
            exchange="polymarket",
            discovered_external_ids=(candidate.external_id for candidate in candidates),
        )
        received_at = utc_now()
        monotonic_ns = time.monotonic_ns()
        for absent_market in absent:
            external_id = str(absent_market["external_id"])
            new_status: str | None = None
            source_timestamp = None
            exchange_timestamp = None
            try:
                source_id = absent_market.get("source_id")
                if source_id is None:
                    raise RuntimeError("Gamma market id is unavailable")
                result = await self.rest.market(str(source_id))
                raw = result.data if isinstance(result.data, dict) else None
                if not isinstance(raw, dict):
                    raise RuntimeError("Gamma market-state response was not an object")
                await self._raw_result(
                    "gamma", f"/markets/{source_id}", "market_state_reconciliation", result, external_id
                )
                market, outcomes = normalise_market(raw)
                market["exchange_timestamp"] = result.response_timestamp
                market["exchange_timestamp_is_transport"] = True
                market["observed_at"] = (
                    getattr(result, "requested_at", None) or utc_now()
                )
                market_id = await self.database.upsert_market(
                    market, diagnostics=diagnostics
                )
                for outcome in outcomes:
                    await self.database.upsert_outcome(market_id, outcome)
                new_status = str(market.get("status") or "unknown")
                source_timestamp = market.get("source_timestamp")
                exchange_timestamp = result.response_timestamp
                payload = {
                    "type": "authoritative_state_reconciliation",
                    "condition_id": external_id,
                    "status": new_status,
                    "market": raw,
                }
                event_type = "state_reconciled_from_rest"
            except Exception as exc:
                payload = {
                    "type": "not_open_state_unresolved",
                    "condition_id": external_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }
                await self.database.apply_market_metadata_patch(
                    exchange="polymarket",
                    market_external_id=external_id,
                    updates={
                        "is_active": False,
                        "is_tradable": False,
                        "accepting_orders": False,
                    },
                    lifecycle_payload=payload,
                    source_timestamp=None,
                    exchange_timestamp=None,
                    observed_at=received_at,
                )
                await self.database.record_gap(
                    run_id=self.writer.run_id,
                    connection_id=None,
                    exchange="polymarket",
                    channel="rest:market_state_reconciliation",
                    market_external_id=external_id,
                    outcome_external_id=None,
                    gap_type="authoritative_state_fetch_failed",
                    reconnect_reason=payload["error"],
                )
                event_type = "not_open_state_unresolved"
            await self.writer.put(
                WriteItem(
                    "market_lifecycle_events",
                    {
                        "exchange": "polymarket",
                        "market_external_id": external_id,
                        "outcome_external_id": None,
                        "connection_id": None,
                        "external_event_id": None,
                        "dedup_hash": content_hash(payload),
                        "event_type": event_type,
                        "previous_status": absent_market.get("status"),
                        "new_status": new_status,
                        "source_timestamp": source_timestamp,
                        "exchange_timestamp": exchange_timestamp,
                        "source_timestamp_raw": None,
                        "exchange_timestamp_raw": None,
                        "received_at": received_at,
                        "received_monotonic_ns": monotonic_ns,
                        "sequence_number": None,
                        "details": payload,
                        "raw_data": payload,
                    },
                )
            )
        if emit_summary:
            LOGGER.info(
                "Polymarket market-state metadata reconciliation complete",
                extra={
                    "markets_reconciled": len(absent),
                    **diagnostics.as_log_fields(),
                },
            )

    async def backfill_trades(self) -> dict[str, int]:
        # The Data API rejects offsets above 10,000.  Its documented recovery
        # path is to page within start/end epoch-second windows, so saturated
        # windows are recursively bisected.  A one-second window that still
        # contains 20,000 records is retained as explicitly partial rather than
        # silently certified as complete.
        markets = await self.database.live_candidates("polymarket")
        parsed_count = 0
        saturated_markets = 0
        saturated_windows = 0
        history_floor_markets = 0
        markets_queried = 0
        pages_fetched = 0
        windows_completed = 0
        page_size = 10_000
        for market in markets:  # MAX_LIVE_MARKETS never enters this path
            markets_queried += 1
            market_saturated = False
            end = int(utc_now().timestamp())
            documented_floor = end - (3 * 365 * 24 * 60 * 60)
            opened_at = parse_timestamp(
                first_present(
                    market.raw_data,
                    "startDate",
                    "startTime",
                    "start_date",
                    "open_time",
                )
            )
            opened_epoch = int(opened_at.timestamp()) if opened_at else None
            start = max(documented_floor, opened_epoch or documented_floor)
            if opened_epoch is not None and opened_epoch < documented_floor:
                history_floor_markets += 1
                await self.database.record_gap(
                    run_id=self.writer.run_id,
                    connection_id=None,
                    exchange="polymarket",
                    channel="rest:trades",
                    market_external_id=market.external_id,
                    outcome_external_id=None,
                    gap_type="upstream_history_floor",
                    details={
                        "market_open_epoch": opened_epoch,
                        "requested_start_epoch": start,
                        "reason": (
                            "Polymarket documents an approximately three-year floor "
                            "for market-scoped trade queries"
                        ),
                    },
                )

            pending_windows: list[tuple[int, int]] = [(start, end)]
            market_pages = 0
            market_windows = 0
            while pending_windows:
                window_start, window_end = pending_windows.pop()
                pages: list[tuple[list[dict[str, Any]], Any]] = []
                async for items, result in self.rest.iter_trades(
                    market=market.external_id,
                    start=window_start,
                    end=window_end,
                    page_size=page_size,
                    max_offset=10_000,
                ):
                    pages.append((items, result))
                    market_pages += 1
                    pages_fetched += 1
                    await self._raw_page(
                        "data",
                        "/trades",
                        "trades",
                        items,
                        result,
                        external_key=(
                            f"{market.external_id}:{window_start}:{window_end}"
                        ),
                    )

                saturated = len(pages) >= 2 and len(pages[-1][0]) == page_size
                if saturated and window_start < window_end:
                    midpoint = window_start + (window_end - window_start) // 2
                    # LIFO: append the newer half first so the older half is
                    # processed first, while keeping the windows disjoint.
                    pending_windows.append((midpoint + 1, window_end))
                    pending_windows.append((window_start, midpoint))
                    continue

                market_windows += 1
                windows_completed += 1
                if saturated:
                    market_saturated = True
                    saturated_windows += 1
                    await self.database.record_gap(
                        run_id=self.writer.run_id,
                        connection_id=None,
                        exchange="polymarket",
                        channel="rest:trades",
                        market_external_id=market.external_id,
                        outcome_external_id=None,
                        gap_type="rest_pagination_limit",
                        details={
                            "documented_max_offset": 10_000,
                            "page_size": page_size,
                            "window_start_epoch": window_start,
                            "window_end_epoch": window_end,
                            "records_retrieved": sum(len(items) for items, _ in pages),
                        },
                    )
                    LOGGER.error(
                        "Polymarket one-second trade window exceeded the offset budget",
                        extra={
                            "market": market.external_id,
                            "window_start_epoch": window_start,
                            "window_end_epoch": window_end,
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
                        if trade:
                            await self.writer.put(trade_item(trade))
                            parsed_count += 1

            saturated_markets += int(market_saturated)
            await self.database.checkpoint(
                "polymarket",
                "trades",
                checkpoint_key=market.external_id,
                timestamp=utc_now(),
                last_external_id=market.external_id,
                metadata={
                    "records_processed_total": parsed_count,
                    "pages": market_pages,
                    "completed_windows": market_windows,
                    "saturated": market_saturated,
                },
            )
        return {
            "records": parsed_count,
            "markets_queried": markets_queried,
            "saturated_markets": saturated_markets,
            "saturated_windows": saturated_windows,
            "history_floor_markets": history_floor_markets,
            "pages_fetched": pages_fetched,
            "windows_completed": windows_completed,
        }

    async def sync_fees_and_incentives(
        self,
        *,
        include_fee_rates: bool = True,
        include_rewards: bool = True,
        fee_rate_live_only: bool = False,
    ) -> dict[str, int]:
        counts = {
            "markets_checked": 0,
            "fee_versions_inserted": 0,
            "reward_records_seen": 0,
            "reward_versions_inserted": 0,
            "errors": 0,
        }
        # Fee rates require one request per outcome token. Explicit backfills
        # retain that comprehensive behavior; periodic live refreshes can skip
        # it while still collecting both current reward variants below.
        markets = (
            await self.database.live_candidates("polymarket")
            if include_fee_rates
            else []
        )
        if fee_rate_live_only:
            markets = [market for market in markets if market.active and market.tradable]
        for market in markets:  # historical/live limits intentionally do not apply
            counts["markets_checked"] += 1
            observed_at = utc_now()
            outcome_fees: dict[str, Any] = {}
            fee_values: list[Decimal] = []
            for token_id in market.outcome_token_ids:
                try:
                    result = await self.rest.fee_rate(token_id)
                    await self._raw_result(
                        "clob", "/fee-rate", "fee_rate", result, token_id
                    )
                    payload = result.data if isinstance(result.data, dict) else {
                        "value": result.data
                    }
                    base_fee_bps = as_decimal(
                        first_present(payload, "base_fee", "baseFee", "fee_rate_bps")
                    )
                    if base_fee_bps is not None:
                        rate = base_fee_bps / Decimal("10000")
                        outcome_fees[token_id] = {
                            "raw": payload,
                            "base_fee_bps": str(base_fee_bps),
                            "normalized_fee_rate": str(rate),
                            "normalized_fee_rate_unit": "fraction",
                        }
                    else:
                        rate = as_decimal(first_present(payload, "fee_rate", "feeRate"))
                        outcome_fees[token_id] = {
                            "raw": payload,
                            "normalized_fee_rate": str(rate) if rate is not None else None,
                            "normalized_fee_rate_unit": "fraction",
                        }
                    if rate is not None:
                        fee_values.append(rate)
                except Exception as exc:
                    counts["errors"] += 1
                    LOGGER.exception(
                        "Polymarket fee-rate collection failed",
                        extra={"market": market.external_id, "token_id": token_id},
                    )
                    await self.database.record_gap(
                        run_id=self.writer.run_id,
                        connection_id=None,
                        exchange="polymarket",
                        channel="rest:fee-rate",
                        market_external_id=market.external_id,
                        outcome_external_id=token_id,
                        gap_type="rest_collection_failed",
                        reconnect_reason=f"{type(exc).__name__}: {exc}",
                    )
            if outcome_fees:
                inserted = await self.database.record_fee_configuration(
                    exchange="polymarket",
                    scope_type="market",
                    scope_external_id=market.external_id,
                    fee_type="clob_fee_rate",
                    effective_from=observed_at,
                    observed_at=observed_at,
                    fee_rate=(
                        fee_values[0]
                        if fee_values and all(value == fee_values[0] for value in fee_values)
                        else None
                    ),
                    currency="USDC",
                    configuration={"outcomes": outcome_fees},
                    version_current=True,
                )
                counts["fee_versions_inserted"] += int(inserted)

        if not include_rewards:
            LOGGER.info(
                "Polymarket fee and incentive sync complete",
                extra={
                    **counts,
                    "fee_rates_included": include_fee_rates,
                    "rewards_included": False,
                    "fee_rate_live_only": fee_rate_live_only,
                },
            )
            return counts

        for sponsored in (False, True):
            reward_variant = "sponsored" if sponsored else "standard"
            async for items, result, cursor in self.rest.iter_current_rewards(
                sponsored=sponsored
            ):
                await self._raw_page(
                    "clob",
                    "/rewards/markets/current",
                    f"liquidity_rewards_{reward_variant}",
                    items,
                    result,
                    external_key=f"{reward_variant}:{cursor or 'first'}",
                )
                observed_at = utc_now()
                for raw in items:
                    market_id = first_present(
                        raw, "condition_id", "conditionId", "market", "market_id"
                    )
                    if market_id is None:
                        counts["errors"] += 1
                        LOGGER.warning("Skipping Polymarket reward without market identity")
                        continue
                    counts["reward_records_seen"] += 1
                    minimum_size = as_decimal(raw.get("rewards_min_size"))
                    maximum_spread = as_decimal(raw.get("rewards_max_spread"))
                    schedules = _as_dict_list(raw.get("rewards_config"))
                    daily_summary = {
                        "rewards_daily_rate": raw.get("rewards_daily_rate"),
                        "sponsored_daily_rate": raw.get("sponsored_daily_rate"),
                        "native_daily_rate": raw.get("native_daily_rate"),
                        "total_daily_rate": raw.get("total_daily_rate"),
                        "sponsors_count": raw.get("sponsors_count"),
                    }

                    # Preserve each exchange-defined schedule as its own row so
                    # changes to one reward asset do not overwrite its peers.
                    for index, schedule in enumerate(schedules):
                        schedule_identity = str(
                            schedule.get("asset_address")
                            or schedule.get("id")
                            or index
                        )
                        inserted = await self.database.record_incentive_configuration(
                            exchange="polymarket",
                            scope_type="market",
                            scope_external_id=str(market_id),
                            incentive_type=(
                                f"liquidity_reward:{reward_variant}:{schedule_identity}"
                            ),
                            effective_from=(
                                parse_timestamp(schedule.get("start_date")) or observed_at
                            ),
                            effective_to=parse_timestamp(schedule.get("end_date")),
                            observed_at=observed_at,
                            reward_rate=as_decimal(schedule.get("rate_per_day")),
                            reward_amount=as_decimal(schedule.get("total_rewards")),
                            reward_currency="USDC",
                            minimum_size=minimum_size,
                            maximum_spread=maximum_spread,
                            configuration={
                                "variant": reward_variant,
                                "daily_summary": daily_summary,
                                "schedule": schedule,
                                "market_configuration": raw,
                            },
                        )
                        counts["reward_versions_inserted"] += int(inserted)

                    # The current aggregate has no exchange change timestamp;
                    # version it by observation time and retain all exact fields.
                    aggregate_rate = as_decimal(
                        first_present(
                            raw,
                            "total_daily_rate",
                            "rewards_daily_rate",
                            "native_daily_rate",
                            "sponsored_daily_rate",
                        )
                    )
                    inserted = await self.database.record_incentive_configuration(
                        exchange="polymarket",
                        scope_type="market",
                        scope_external_id=str(market_id),
                        incentive_type=f"liquidity_reward_summary:{reward_variant}",
                        effective_from=observed_at,
                        observed_at=observed_at,
                        reward_rate=aggregate_rate,
                        reward_currency="USDC/day",
                        minimum_size=minimum_size,
                        maximum_spread=maximum_spread,
                        configuration={
                            "variant": reward_variant,
                            "daily_summary": daily_summary,
                            "market_configuration": raw,
                        },
                        version_current=True,
                    )
                    counts["reward_versions_inserted"] += int(inserted)
        LOGGER.info(
            "Polymarket fee and incentive sync complete",
            extra={
                **counts,
                "fee_rates_included": include_fee_rates,
                "rewards_included": include_rewards,
                "fee_rate_live_only": fee_rate_live_only,
            },
        )
        return counts

    async def backfill_comments(self) -> dict[str, int]:
        count = 0
        errors = 0
        try:
            async for items, result in self.rest.iter_comments():
                await self._raw_page("gamma", "/comments", "comments", items, result)
                received_at = utc_now()
                monotonic_ns = time.monotonic_ns()
                for raw in items:
                    profile = raw.get("profile") if isinstance(raw.get("profile"), dict) else {}
                    comment_id = str(raw.get("id") or content_hash(raw))
                    await self.writer.put(
                        WriteItem(
                            "comments",
                            {
                                "exchange": "polymarket",
                                "external_comment_id": comment_id,
                                "dedup_hash": content_hash(
                                    {"id": comment_id, "createdAt": raw.get("createdAt")}
                                ),
                                "parent_entity_type": str(
                                    raw.get("parentEntityType") or ""
                                ).lower(),
                                "parent_entity_id": str(raw.get("parentEntityID") or ""),
                                "parent_external_comment_id": raw.get("parentCommentID"),
                                "public_identifier": first_present(
                                    raw, "userAddress", "replyAddress"
                                ),
                                "profile_name": first_present(profile, "name", "pseudonym"),
                                "body": str(raw.get("body") or ""),
                                "source_created_at": parse_timestamp(raw.get("createdAt")),
                                "source_updated_at": parse_timestamp(raw.get("updatedAt")),
                                "source_timestamp": parse_timestamp(raw.get("createdAt")),
                                "exchange_timestamp": None,
                                "received_at": received_at,
                                "received_monotonic_ns": monotonic_ns,
                                "raw_data": raw,
                            },
                        )
                    )
                    count += 1
        except Exception:
            errors += 1
            LOGGER.exception("Optional Polymarket comments backfill failed")
        return {"records": count, "errors": errors}

    async def backfill_market_data(self) -> dict[str, int]:
        counts = {"books": 0, "price_points": 0, "holders": 0}
        markets = await self.database.live_candidates("polymarket")
        for market in markets:  # deliberately all markets; live caps never enter this path
            for token_id in market.outcome_token_ids:
                if market.active and market.tradable:
                    try:
                        result = await self.rest.orderbook(token_id)
                        await self._raw_result("clob", "/book", "orderbook", result, token_id)
                        raw = result.data if isinstance(result.data, dict) else {}
                        snapshot = parse_book(raw)
                        if snapshot:
                            await self.writer.put(book_snapshot_item(snapshot))
                            counts["books"] += 1
                    except Exception:
                        LOGGER.exception(
                            "Polymarket orderbook snapshot failed",
                            extra={"token_id": token_id},
                        )
                try:
                    result = await self.rest.price_history(token_id, interval="max", fidelity_minutes=1)
                    await self._raw_result(
                        "clob", "/prices-history", "price_history", result, token_id
                    )
                    history = result.data.get("history", []) if isinstance(result.data, dict) else []
                    for point in history:
                        if not isinstance(point, dict):
                            continue
                        timestamp = parse_timestamp(point.get("t"))
                        price = as_decimal(point.get("p"))
                        if timestamp is None or price is None:
                            continue
                        await self.writer.put(
                            WriteItem(
                                "candlesticks",
                                {
                                    "exchange": "polymarket",
                                    "market_external_id": market.external_id,
                                    "outcome_external_id": token_id,
                                    "interval_seconds": 60,
                                    "period_start": timestamp,
                                    "period_end": timestamp + timedelta(seconds=60),
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
                                    "retrieved_at": utc_now(),
                                    "raw_data": point,
                                },
                            )
                        )
                        counts["price_points"] += 1
                except Exception:
                    LOGGER.exception(
                        "Polymarket price history failed", extra={"token_id": token_id}
                    )
            try:
                result = await self.rest.holders(market.external_id)
                await self._raw_result(
                    "data", "/holders", "holders", result, market.external_id
                )
                observed_at = utc_now()
                for token_group in result.data if isinstance(result.data, list) else []:
                    if not isinstance(token_group, dict):
                        continue
                    token_id = str(token_group.get("token") or "") or None
                    holders = token_group.get("holders") or []
                    for rank, holder in enumerate(holders, 1):
                        if not isinstance(holder, dict):
                            continue
                        identifier = first_present(
                            holder, "proxyWallet", "wallet", "address", "user"
                        )
                        amount = as_decimal(first_present(holder, "amount", "balance"))
                        if identifier is None or amount is None:
                            continue
                        await self.writer.put(
                            WriteItem(
                                "holder_snapshots",
                                {
                                    "exchange": "polymarket",
                                    "market_external_id": market.external_id,
                                    "outcome_external_id": token_id,
                                    "public_identifier": str(identifier),
                                    "profile_name": first_present(
                                        holder, "name", "pseudonym"
                                    ),
                                    "amount": amount,
                                    "rank": rank,
                                    "observed_at": observed_at,
                                    "source_timestamp": None,
                                    "received_at": observed_at,
                                    "raw_data": holder,
                                },
                            )
                        )
                        counts["holders"] += 1
            except Exception:
                LOGGER.exception(
                    "Optional Polymarket holder snapshot failed",
                    extra={"market": market.external_id},
                )
        return counts

    async def _raw_page(
        self,
        source: str,
        endpoint: str,
        entity_type: str,
        payload: Any,
        result: Any,
        external_key: str | None = None,
    ) -> None:
        if not self.store_raw_rest:
            return
        await self.database.store_raw_rest(
            exchange="polymarket",
            source=source,
            endpoint=endpoint,
            entity_type=entity_type,
            external_key=external_key,
            requested_at=result.requested_at,
            received_at=utc_now(),
            response_timestamp=result.response_timestamp,
            http_status=result.status_code,
            parameters=request_parameters(result.url),
            payload=payload,
        )

    async def _raw_result(
        self,
        source: str,
        endpoint: str,
        entity_type: str,
        result: Any,
        external_key: str | None,
    ) -> None:
        await self._raw_page(
            source, endpoint, entity_type, result.data, result, external_key=external_key
        )


def _as_dict_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []
