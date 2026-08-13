from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
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
from prediction_collector.kalshi.parser import (
    normalise_event,
    normalise_market,
    normalise_multivariate_collection,
    normalise_series,
    parse_market_candidate,
    parse_orderbook_snapshot,
    parse_trade,
)
from prediction_collector.kalshi.rest import KalshiRestClient
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)


class KalshiService:
    def __init__(
        self,
        *,
        rest: KalshiRestClient,
        database: Database,
        writer: BatchWriter,
        store_raw_rest: bool,
    ) -> None:
        self.rest = rest
        self.database = database
        self.writer = writer
        self.store_raw_rest = store_raw_rest
        self.event_series: dict[str, str] = {}
        self.series_tickers: set[str] = set()

    async def sync_metadata(self, *, include_historical: bool = True) -> dict[str, int]:
        counts = {
            "series": 0,
            "events": 0,
            "markets": 0,
            "outcomes": 0,
            "market_groups": 0,
        }
        diagnostics = MetadataSyncDiagnostics()
        async for items, result, cursor in self.rest.iter_series():
            await self._raw("/series", "series", items, result, cursor)
            for raw in items:
                series = normalise_series(raw)
                await self.database.upsert_series(series)
                if series["external_id"]:
                    self.series_tickers.add(str(series["external_id"]))
                counts["series"] += 1

        async for items, result, cursor in self.rest.iter_events():
            await self._raw("/events", "events", items, result, cursor)
            for raw in items:
                event = normalise_event(raw)
                if event.get("series_external_id"):
                    self.event_series[event["external_id"]] = event["series_external_id"]
                await self.database.upsert_event(event)
                counts["events"] += 1

        # Regular /events excludes multivariate events. Pull ordinary markets
        # first so an MVE market's selected legs can resolve to existing market
        # and outcome rows when its nested payload is processed below.
        iterators = [self.rest.iter_markets(mve_filter="exclude")]
        if include_historical:
            iterators.append(self.rest.iter_historical_markets())
        for iterator in iterators:
            async for items, result, cursor in iterator:
                historical = "/historical/" in result.url
                await self._raw(
                    "/historical/markets" if historical else "/markets",
                    "markets",
                    items,
                    result,
                    cursor,
                )
                for raw in items:
                    market, outcomes = normalise_market(raw)
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
                    "kalshi",
                    "historical_markets" if historical else "markets",
                    cursor=cursor,
                    timestamp=utc_now(),
                    metadata={"records": counts["markets"]},
                )

        async for items, result, cursor in self.rest.iter_multivariate_events(
            with_nested_markets=True
        ):
            await self._raw(
                "/events/multivariate",
                "multivariate_events",
                items,
                result,
                cursor,
            )
            for raw_event in items:
                event = normalise_event(raw_event)
                if event.get("series_external_id"):
                    self.event_series[event["external_id"]] = event["series_external_id"]
                    self.series_tickers.add(str(event["series_external_id"]))
                await self.database.upsert_event(event)
                counts["events"] += 1
                nested_markets = raw_event.get("markets")
                if not isinstance(nested_markets, list):
                    continue
                for raw_market in nested_markets:
                    if not isinstance(raw_market, dict):
                        continue
                    market, outcomes = normalise_market(raw_market)
                    # Some nested representations omit event_ticker even though
                    # the containing MVE event supplies the identity.
                    if not market.get("event_external_id"):
                        market["event_external_id"] = event["external_id"]
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
                "kalshi",
                "multivariate_events",
                cursor=cursor,
                timestamp=utc_now(),
                metadata={"records": counts["events"]},
            )

        # Collection objects contain constraints that are absent from generated
        # market payloads. Upsert them last so the authoritative collection raw
        # object remains on the shared market_groups row.
        async for items, result, cursor in self.rest.iter_multivariate_event_collections():
            await self._raw(
                "/multivariate_event_collections",
                "multivariate_event_collections",
                items,
                result,
                cursor,
            )
            for raw_collection in items:
                group = normalise_multivariate_collection(raw_collection)
                if not group["external_id"]:
                    LOGGER.warning(
                        "Skipping Kalshi multivariate collection without collection_ticker"
                    )
                    continue
                await self.database.upsert_market_group(group)
                counts["market_groups"] += 1
            await self.database.checkpoint(
                "kalshi",
                "multivariate_event_collections",
                cursor=cursor,
                timestamp=utc_now(),
                metadata={"records": counts["market_groups"]},
            )
        LOGGER.info(
            "Kalshi metadata sync complete",
            extra={**counts, **diagnostics.as_log_fields()},
        )
        return counts

    async def sync_fees_and_incentives(self) -> dict[str, int]:
        """Collect current and historical fee schedules plus incentive programs."""
        counts = {
            "series_discovered": 0,
            "series_fee_changes_seen": 0,
            "series_fee_changes_inserted": 0,
            "event_fee_changes_seen": 0,
            "event_fee_changes_inserted": 0,
            "incentive_programs_seen": 0,
            "incentive_programs_inserted": 0,
        }
        # A standalone live process performs market discovery rather than a
        # full metadata sync. Refresh the ticker set here so both initial and
        # newly created series receive fee-schedule collection.
        async for items, result, cursor in self.rest.iter_series():
            await self._raw("/series", "series", items, result, cursor)
            for raw in items:
                series = normalise_series(raw)
                if series["external_id"] not in self.series_tickers:
                    await self.database.upsert_series(series)
                    self.series_tickers.add(str(series["external_id"]))
                    counts["series_discovered"] += 1
        for ticker in sorted(self.series_tickers):
            async for items, result, cursor in self.rest.iter_series_fee_changes(ticker):
                await self._raw(
                    "/series/fee_changes", "series_fee_changes", items, result, ticker
                )
                observed_at = utc_now()
                for raw in items:
                    counts["series_fee_changes_seen"] += 1
                    scope_id = str(raw.get("series_ticker") or ticker)
                    effective = parse_timestamp(raw.get("scheduled_ts")) or observed_at
                    inserted = await self.database.record_fee_configuration(
                        exchange="kalshi",
                        scope_type="series",
                        scope_external_id=scope_id,
                        fee_type="series_fee_multiplier",
                        effective_from=effective,
                        observed_at=observed_at,
                        source_timestamp=parse_timestamp(raw.get("scheduled_ts")),
                        multiplier=as_decimal(raw.get("fee_multiplier")),
                        configuration=raw,
                    )
                    counts["series_fee_changes_inserted"] += int(inserted)

        async for items, result, cursor in self.rest.iter_event_fee_changes():
            await self._raw(
                "/events/fee_changes", "event_fee_changes", items, result, cursor
            )
            observed_at = utc_now()
            for raw in items:
                event_ticker = str(raw.get("event_ticker") or "")
                if not event_ticker:
                    LOGGER.warning("Skipping Kalshi fee change without event_ticker")
                    continue
                counts["event_fee_changes_seen"] += 1
                effective = parse_timestamp(raw.get("scheduled_ts")) or observed_at
                inserted = await self.database.record_fee_configuration(
                    exchange="kalshi",
                    scope_type="event",
                    scope_external_id=event_ticker,
                    fee_type="event_fee_override",
                    effective_from=effective,
                    observed_at=observed_at,
                    source_timestamp=parse_timestamp(raw.get("scheduled_ts")),
                    multiplier=as_decimal(raw.get("fee_multiplier_override")),
                    configuration=raw,
                    semantic_configuration={
                        "fee_type_override": raw.get("fee_type_override")
                    },
                )
                counts["event_fee_changes_inserted"] += int(inserted)

        async for items, result, cursor in self.rest.iter_incentive_programs():
            await self._raw(
                "/incentive_programs", "incentive_programs", items, result, cursor
            )
            observed_at = utc_now()
            for raw in items:
                scope_id = str(
                    first_present(raw, "market_ticker", "market_id", "id") or ""
                )
                if not scope_id:
                    LOGGER.warning("Skipping Kalshi incentive without market identity")
                    continue
                counts["incentive_programs_seen"] += 1
                discount_bps = as_decimal(raw.get("discount_factor_bps"))
                inserted = await self.database.record_incentive_configuration(
                    exchange="kalshi",
                    scope_type="market",
                    scope_external_id=scope_id,
                    incentive_type=str(
                        first_present(raw, "type", "incentive_type")
                        or "market_incentive"
                    ),
                    effective_from=parse_timestamp(raw.get("start_date")) or observed_at,
                    effective_to=parse_timestamp(raw.get("end_date")),
                    observed_at=observed_at,
                    reward_amount=as_decimal(raw.get("period_reward")),
                    reward_currency="USD",
                    minimum_size=as_decimal(raw.get("target_size_fp")),
                    multiplier=(
                        discount_bps / Decimal("10000")
                        if discount_bps is not None
                        else None
                    ),
                    configuration=raw,
                )
                counts["incentive_programs_inserted"] += int(inserted)
        LOGGER.info("Kalshi fee and incentive sync complete", extra=counts)
        return counts

    async def discover_live(
        self,
        *,
        reconcile_absent: bool = True,
        on_page: Callable[[list[MarketCandidate]], Awaitable[None]] | None = None,
    ) -> list[MarketCandidate]:
        candidates_by_id: dict[str, MarketCandidate] = {}
        diagnostics = MetadataSyncDiagnostics()
        # Ordinary markets are subscribed first. The much larger MVE relation
        # is consumed separately below so it cannot delay ordinary capture.
        async for items, result, cursor in self.rest.iter_markets(
            status="open", mve_filter="exclude"
        ):
            await self._raw("/markets", "markets", items, result, cursor)
            page_candidates: list[MarketCandidate] = []
            for raw in items:
                candidate = parse_market_candidate(raw)
                candidates_by_id[candidate.external_id] = candidate
                page_candidates.append(candidate)
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
            if on_page is not None and page_candidates:
                await on_page(page_candidates)

        async for items, result, cursor in self.rest.iter_multivariate_events(
            with_nested_markets=True
        ):
            await self._raw(
                "/events/multivariate", "multivariate_events", items, result, cursor
            )
            page_candidates = []
            for raw_event in items:
                event = normalise_event(raw_event)
                await self.database.upsert_event(event)
                nested_markets = raw_event.get("markets")
                if not isinstance(nested_markets, list):
                    continue
                for raw in nested_markets:
                    if not isinstance(raw, dict):
                        raise RuntimeError(
                            "Kalshi multivariate event contained malformed market rows"
                        )
                    candidate = parse_market_candidate(raw)
                    candidates_by_id[candidate.external_id] = candidate
                    page_candidates.append(candidate)
                    market, outcomes = normalise_market(raw)
                    if not market.get("event_external_id"):
                        market["event_external_id"] = event["external_id"]
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
            if on_page is not None and page_candidates:
                await on_page(page_candidates)
        candidates = list(candidates_by_id.values())
        if not candidates:
            raise RuntimeError("Kalshi complete open-market discovery returned zero markets")
        if reconcile_absent:
            await self.reconcile_absent_live(
                candidates,
                diagnostics=diagnostics,
                emit_summary=False,
            )
        LOGGER.info(
            "Kalshi live metadata sync complete",
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
            exchange="kalshi",
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
                try:
                    result = await self.rest.market(external_id)
                    endpoint = f"/markets/{external_id}"
                except Exception:
                    result = await self.rest.market(external_id, historical=True)
                    endpoint = f"/historical/markets/{external_id}"
                raw_payload = result.data
                raw = (
                    raw_payload.get("market", raw_payload)
                    if isinstance(raw_payload, dict)
                    else None
                )
                if not isinstance(raw, dict):
                    raise RuntimeError("Kalshi market-state response was not an object")
                await self._raw(endpoint, "market_state_reconciliation", raw, result, external_id)
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
                    "market_ticker": external_id,
                    "status": new_status,
                    "market": raw,
                }
                event_type = "state_reconciled_from_rest"
            except Exception as exc:
                # Absence from a complete all-open pass proves only that the
                # market is no longer subscribable. Preserve its last exact
                # status when the authoritative detail fetch fails.
                payload = {
                    "type": "not_open_state_unresolved",
                    "market_ticker": external_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }
                await self.database.apply_market_metadata_patch(
                    exchange="kalshi",
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
                    exchange="kalshi",
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
                        "exchange": "kalshi",
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
                "Kalshi market-state metadata reconciliation complete",
                extra={
                    "markets_reconciled": len(absent),
                    **diagnostics.as_log_fields(),
                },
            )

    async def backfill_trades(self) -> int:
        count = 0
        for historical, iterator in (
            (False, self.rest.iter_trades()),
            (True, self.rest.iter_historical_trades()),
        ):
            async for items, result, cursor in iterator:
                await self._raw(
                    "/historical/trades" if historical else "/markets/trades",
                    "trades",
                    items,
                    result,
                    cursor,
                )
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
                        count += 1
                await self.database.checkpoint(
                    "kalshi",
                    "historical_trades" if historical else "trades",
                    cursor=cursor,
                    timestamp=utc_now(),
                    metadata={"records": count},
                )
        return count

    async def backfill_market_data(self) -> dict[str, int]:
        counts = {"books": 0, "candles": 0, "errors": 0}
        historical_cutoff = None
        try:
            cutoff_result = await self.rest.historical_cutoff()
            cutoff_payload = (
                cutoff_result.data if isinstance(cutoff_result.data, dict) else {}
            )
            await self._raw(
                "/historical/cutoff",
                "historical_cutoff",
                cutoff_payload,
                cutoff_result,
                "global",
            )
            historical_cutoff = parse_timestamp(
                cutoff_payload.get("market_settled_ts")
            )
            if historical_cutoff is None:
                raise RuntimeError("historical cutoff omitted market_settled_ts")
        except Exception as exc:
            counts["errors"] += 1
            LOGGER.exception("Kalshi historical cutoff collection failed")
            await self.database.record_gap(
                run_id=self.writer.run_id,
                connection_id=None,
                exchange="kalshi",
                channel="rest:historical_cutoff",
                market_external_id=None,
                outcome_external_id=None,
                gap_type="rest_collection_failed",
                reconnect_reason=f"{type(exc).__name__}: {exc}",
            )
        markets = await self.database.live_candidates("kalshi")
        for market in markets:  # no MAX_LIVE_MARKETS here by design
            if market.active and market.tradable:
                try:
                    result = await self.rest.orderbook(market.external_id)
                    await self._raw(
                        f"/markets/{market.external_id}/orderbook",
                        "orderbook",
                        result.data,
                        result,
                        market.external_id,
                    )
                    raw = result.data.get("orderbook", result.data) if isinstance(result.data, dict) else {}
                    envelope = {
                        "type": "orderbook_snapshot",
                        "msg": {"market_ticker": market.external_id, **raw},
                    }
                    snapshot = parse_orderbook_snapshot(envelope, use_yes_price=False)
                    if snapshot:
                        await self.writer.put(book_snapshot_item(snapshot))
                        counts["books"] += 1
                except Exception:
                    LOGGER.exception(
                        "Kalshi current orderbook snapshot failed",
                        extra={"market": market.external_id},
                    )

            raw = market.raw_data
            start = parse_timestamp(raw.get("open_time"))
            end = parse_timestamp(
                first_present(raw, "settlement_ts", "close_time", "expiration_time")
            ) or utc_now()
            if start is None or end <= start:
                continue
            series_ticker = raw.get("series_ticker") or self.event_series.get(
                str(raw.get("event_ticker") or "")
            )
            settled_at = parse_timestamp(raw.get("settlement_ts"))
            archived = bool(
                historical_cutoff is not None
                and settled_at is not None
                and settled_at < historical_cutoff
            )
            # The archive boundary, not active/inactive state, selects the
            # endpoint. If the cutoff is unavailable or the exchange moves the
            # row between our cutoff read and request, try the alternate path.
            endpoint_modes = [archived]
            if not archived or series_ticker:
                endpoint_modes.append(not archived)
            attempted_modes: list[str] = []
            result = None
            last_error: Exception | None = None
            used_historical = archived
            for historical in dict.fromkeys(endpoint_modes):
                if not historical and not series_ticker:
                    continue
                attempted_modes.append("historical" if historical else "current")
                try:
                    result = await self.rest.candlesticks(
                        str(series_ticker or ""),
                        market.external_id,
                        start_ts=int(start.timestamp()),
                        end_ts=int(end.timestamp()),
                        period_interval=60,
                        historical=historical,
                    )
                    used_historical = historical
                    break
                except Exception as exc:
                    last_error = exc
            if result is None:
                counts["errors"] += 1
                LOGGER.error(
                    "Kalshi candlestick backfill failed",
                    extra={
                        "market": market.external_id,
                        "attempted_endpoints": attempted_modes,
                    },
                    exc_info=(
                        (type(last_error), last_error, last_error.__traceback__)
                        if last_error is not None
                        else None
                    ),
                )
                await self.database.record_gap(
                    run_id=self.writer.run_id,
                    connection_id=None,
                    exchange="kalshi",
                    channel="rest:candlesticks",
                    market_external_id=market.external_id,
                    outcome_external_id=f"{market.external_id}:yes",
                    gap_type="rest_collection_failed",
                    reconnect_reason=(
                        f"{type(last_error).__name__}: {last_error}"
                        if last_error is not None
                        else "no eligible candlestick endpoint"
                    ),
                    details={"attempted_endpoints": attempted_modes},
                )
                continue
            try:
                await self._raw(
                    (
                        f"/historical/markets/{market.external_id}/candlesticks"
                        if used_historical
                        else f"/series/{series_ticker}/markets/"
                        f"{market.external_id}/candlesticks"
                    ),
                    "candlesticks",
                    result.data,
                    result,
                    market.external_id,
                )
                candles = (
                    result.data.get("candlesticks", [])
                    if isinstance(result.data, dict)
                    else []
                )
                for raw_candle in candles:
                    if not isinstance(raw_candle, dict):
                        continue
                    period_start = parse_timestamp(
                        first_present(raw_candle, "end_period_ts", "start_period_ts")
                    )
                    if period_start is None:
                        continue
                    price = raw_candle.get("price") if isinstance(raw_candle.get("price"), dict) else {}
                    yes_bid = raw_candle.get("yes_bid") if isinstance(raw_candle.get("yes_bid"), dict) else {}
                    yes_ask = raw_candle.get("yes_ask") if isinstance(raw_candle.get("yes_ask"), dict) else {}
                    await self.writer.put(
                        WriteItem(
                            "candlesticks",
                            {
                                "exchange": "kalshi",
                                "market_external_id": market.external_id,
                                "outcome_external_id": f"{market.external_id}:yes",
                                "interval_seconds": 3600,
                                "period_start": period_start - timedelta(hours=1),
                                "period_end": period_start,
                                "open": _candle_value(price, "open"),
                                "high": _candle_value(price, "high"),
                                "low": _candle_value(price, "low"),
                                "close": _candle_value(price, "close"),
                                "bid_open": _candle_value(yes_bid, "open"),
                                "bid_high": _candle_value(yes_bid, "high"),
                                "bid_low": _candle_value(yes_bid, "low"),
                                "bid_close": _candle_value(yes_bid, "close"),
                                "ask_open": _candle_value(yes_ask, "open"),
                                "ask_high": _candle_value(yes_ask, "high"),
                                "ask_low": _candle_value(yes_ask, "low"),
                                "ask_close": _candle_value(yes_ask, "close"),
                                "volume": as_decimal(
                                    first_present(raw_candle, "volume_fp", "volume")
                                ),
                                "open_interest": as_decimal(
                                    first_present(
                                        raw_candle, "open_interest_fp", "open_interest"
                                    )
                                ),
                                "source_timestamp": period_start,
                                "retrieved_at": utc_now(),
                                "raw_data": raw_candle,
                            },
                        )
                    )
                    counts["candles"] += 1
            except Exception as exc:
                counts["errors"] += 1
                LOGGER.exception(
                    "Kalshi candlestick normalization/write failed",
                    extra={"market": market.external_id},
                )
                await self.database.record_gap(
                    run_id=self.writer.run_id,
                    connection_id=None,
                    exchange="kalshi",
                    channel="rest:candlesticks",
                    market_external_id=market.external_id,
                    outcome_external_id=f"{market.external_id}:yes",
                    gap_type="normalization_failed",
                    reconnect_reason=f"{type(exc).__name__}: {exc}",
                    details={
                        "endpoint": "historical" if used_historical else "current"
                    },
                )
        return counts

    async def _raw(
        self,
        endpoint: str,
        entity_type: str,
        payload: Any,
        result: Any,
        external_key: str | None,
    ) -> None:
        if not self.store_raw_rest:
            return
        await self.database.store_raw_rest(
            exchange="kalshi",
            source="trade-api-v2",
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


def _candle_value(raw: dict[str, Any], key: str) -> Any:
    return as_decimal(raw.get(f"{key}_dollars") or raw.get(key))
