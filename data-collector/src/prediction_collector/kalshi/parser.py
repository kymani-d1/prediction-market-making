from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from prediction_collector.common.orderbook import kalshi_yes_book
from prediction_collector.common.types import (
    MarketCandidate,
    ParsedBookSnapshot,
    ParsedBookUpdate,
    ParsedTrade,
)
from prediction_collector.common.utils import (
    as_decimal,
    as_int,
    canonical_json,
    first_present,
    parse_timestamp,
    trade_fingerprint,
    utc_now,
)


def dollars(raw: dict[str, Any], dollar_key: str, cent_key: str) -> Decimal | None:
    value = as_decimal(raw.get(dollar_key))
    if value is not None:
        return value
    cents = as_decimal(raw.get(cent_key))
    return cents / Decimal("100") if cents is not None else None


def normalise_series(raw: dict[str, Any]) -> dict[str, Any]:
    ticker = str(raw.get("ticker") or raw.get("series_ticker") or "")
    return {
        "exchange": "kalshi",
        "external_id": ticker,
        "ticker": ticker,
        "title": raw.get("title") or ticker,
        "category": raw.get("category"),
        "frequency": raw.get("frequency"),
        "raw_data": raw,
    }


def normalise_event(raw: dict[str, Any]) -> dict[str, Any]:
    ticker = str(raw.get("event_ticker") or raw.get("ticker") or "")
    return {
        "exchange": "kalshi",
        "external_id": ticker,
        "series_external_id": raw.get("series_ticker"),
        "ticker": ticker,
        "slug": None,
        "title": raw.get("title") or raw.get("sub_title") or ticker,
        "description": raw.get("description"),
        "category": raw.get("category"),
        "status": raw.get("status"),
        "start_time": parse_timestamp(first_present(raw, "start_date", "open_time")),
        "end_time": parse_timestamp(first_present(raw, "close_time", "end_date")),
        "created_time": parse_timestamp(first_present(raw, "created_time", "created_at")),
        "updated_time": parse_timestamp(first_present(raw, "updated_time", "updated_at")),
        "rules": _market_rules(raw),
        "resolution_source": _text_value(raw.get("settlement_sources")),
        "source_timestamp": parse_timestamp(first_present(raw, "updated_time", "updated_at")),
        "raw_data": raw,
    }


def normalise_multivariate_collection(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise Kalshi's collection object into the linked-market group model."""
    ticker = str(raw.get("collection_ticker") or "")
    associated_events = raw.get("associated_events")
    associated_event_tickers = raw.get("associated_event_tickers")
    return {
        "exchange": "kalshi",
        "external_id": f"mve:{ticker}" if ticker else "",
        "event_external_id": None,
        "group_type": "multivariate",
        "name": raw.get("title") or ticker,
        "description": raw.get("description") or raw.get("functional_description"),
        "status": raw.get("status"),
        "constraint_definition": {
            "collection_ticker": ticker,
            "series_ticker": raw.get("series_ticker"),
            "associated_events": (
                associated_events if isinstance(associated_events, list) else []
            ),
            "associated_event_tickers": (
                associated_event_tickers
                if isinstance(associated_event_tickers, list)
                else []
            ),
            "is_ordered": raw.get("is_ordered"),
            "is_single_market_per_event": raw.get("is_single_market_per_event"),
            "is_all_yes": raw.get("is_all_yes"),
            "size_min": raw.get("size_min"),
            "size_max": raw.get("size_max"),
            "functional_description": raw.get("functional_description"),
            "exchange_index": raw.get("exchange_index"),
        },
        "raw_data": raw,
    }


def parse_market_candidate(raw: dict[str, Any]) -> MarketCandidate:
    ticker = str(raw.get("ticker") or "")
    status = str(raw.get("status") or "").lower()
    active = status in {"open", "active"}
    # Provisional is a possible future-removal designation, not a trading halt.
    # Kalshi still exposes those contracts for trading while their status is
    # open/active, so excluding them would violate comprehensive live coverage.
    tradable = active
    return MarketCandidate(
        exchange="kalshi",
        external_id=ticker,
        ticker=ticker,
        status=status,
        active=active,
        tradable=tradable,
        volume=as_decimal(first_present(raw, "volume_fp", "volume")),
        volume_24h=as_decimal(first_present(raw, "volume_24h_fp", "volume_24h")),
        # Kalshi deprecated both liquidity fields in February 2026 and now
        # documents them as always returning zero. None means unavailable; a
        # synthetic zero would corrupt ranking and threshold decisions.
        liquidity=None,
        outcome_token_ids=(),
        raw_data=raw,
    )


def normalise_market(raw: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    candidate = parse_market_candidate(raw)
    market = {
        "exchange": "kalshi",
        "external_id": candidate.external_id,
        "event_external_id": raw.get("event_ticker"),
        "ticker": candidate.ticker,
        "slug": None,
        "condition_id": None,
        "question": raw.get("title") or candidate.external_id,
        "subtitle": raw.get("subtitle") or raw.get("sub_title"),
        "description": raw.get("description"),
        "rules": _market_rules(raw),
        "resolution_source": _text_value(raw.get("settlement_sources")),
        "source_timestamp": parse_timestamp(first_present(raw, "updated_time", "updated_at")),
        "status": candidate.status,
        "market_type": raw.get("market_type") or "binary",
        "is_active": candidate.active,
        "is_tradable": candidate.tradable,
        "open_time": parse_timestamp(raw.get("open_time")),
        "close_time": parse_timestamp(raw.get("close_time")),
        "settlement_time": parse_timestamp(raw.get("settlement_ts")),
        "result": raw.get("result"),
        "settlement_value": as_decimal(
            first_present(raw, "settlement_value_dollars", "settlement_value")
        ),
        "volume": candidate.volume,
        "volume_24h": candidate.volume_24h,
        "open_interest": as_decimal(first_present(raw, "open_interest_fp", "open_interest")),
        "liquidity": candidate.liquidity,
        "tick_size": None,
        "price_level_structure": {
            "type": raw.get("price_level_structure"),
            "ranges": raw.get("price_ranges"),
        },
        "structural_metadata": {
            key: raw.get(key)
            for key in (
                "floor_strike",
                "cap_strike",
                "strike_type",
                "functional_strike",
                "custom_strike",
                "exchange_index",
                "fractional_trading_enabled",
                "expected_expiration_time",
                "latest_expiration_time",
                "expiration_time",
            )
            if key in raw
        },
        "fee_rate": as_decimal(first_present(raw, "fee_rate", "fees")),
        "enable_order_book": True,
        "accepting_orders": candidate.tradable,
        "negative_risk": False,
        "raw_data": raw,
    }
    outcomes = [
        {
            "exchange": "kalshi",
            "external_id": f"{candidate.external_id}:yes",
            "token_id": None,
            "name": raw.get("yes_sub_title") or "Yes",
            "outcome_index": 0,
            "last_price": dollars(raw, "last_price_dollars", "last_price"),
            "raw_data": {"side": "yes", "market_ticker": candidate.external_id},
        },
        {
            "exchange": "kalshi",
            "external_id": f"{candidate.external_id}:no",
            "token_id": None,
            "name": raw.get("no_sub_title") or "No",
            "outcome_index": 1,
            "last_price": (
                Decimal("1") - dollars(raw, "last_price_dollars", "last_price")
                if dollars(raw, "last_price_dollars", "last_price") is not None
                else None
            ),
            "raw_data": {"side": "no", "market_ticker": candidate.external_id},
        },
    ]
    return market, outcomes


def parse_trade(
    raw: dict[str, Any],
    *,
    received_at: datetime | None = None,
    received_monotonic_ns: int | None = None,
) -> ParsedTrade | None:
    ticker = first_present(raw, "ticker", "market_ticker")
    price = dollars(raw, "yes_price_dollars", "yes_price")
    if price is None:
        price = dollars(raw, "price_dollars", "price")
    size = as_decimal(first_present(raw, "count_fp", "count", "size"))
    executed = parse_timestamp(
        first_present(raw, "ts_ms", "created_time", "ts", "timestamp")
    )
    if ticker is None or price is None or size is None or executed is None:
        return None
    trade_id = first_present(raw, "trade_id", "id")
    side = first_present(
        raw, "taker_outcome_side", "taker_book_side", "taker_side", "side"
    )
    received = received_at or utc_now()
    monotonic = received_monotonic_ns or time.monotonic_ns()
    dedup = trade_fingerprint(
        exchange="kalshi",
        market_external_id=str(ticker),
        outcome_external_id=f"{ticker}:yes",
        executed_at=executed,
        price=price,
        size=size,
        side=str(side) if side is not None else None,
        external_trade_id=str(trade_id) if trade_id else None,
    )
    exchange_timestamp = parse_timestamp(
        first_present(raw, "ts_ms", "created_time", "ts", "timestamp")
    )
    return ParsedTrade(
        exchange="kalshi",
        market_external_id=str(ticker),
        outcome_external_id=f"{ticker}:yes",
        external_trade_id=str(trade_id) if trade_id else None,
        executed_at=executed,
        received_at=received,
        received_monotonic_ns=monotonic,
        price=price,
        size=size,
        side=str(side).lower() if side is not None else None,
        transaction_hash=None,
        dedup_hash=dedup,
        source_timestamp=None,
        exchange_timestamp=exchange_timestamp,
        source_timestamp_raw=None,
        exchange_timestamp_raw=(
            str(first_present(raw, "ts_ms", "created_time", "ts", "timestamp"))
            if first_present(raw, "ts_ms", "created_time", "ts", "timestamp")
            is not None
            else None
        ),
        raw_data=raw,
    )


def _dollar_levels(raw: dict[str, Any], prefix: str) -> list[Any]:
    for dollar_key in (f"{prefix}_dollars_fp", f"{prefix}_dollars"):
        if isinstance(raw.get(dollar_key), list):
            return raw[dollar_key]
    values = raw.get(prefix) or []
    converted: list[list[str]] = []
    if isinstance(values, list):
        for level in values:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                price = as_decimal(level[0])
                if price is not None:
                    converted.append([str(price / Decimal("100")), str(level[1])])
    return converted


def parse_orderbook_snapshot(
    envelope: dict[str, Any],
    *,
    received_at: datetime | None = None,
    received_monotonic_ns: int | None = None,
    use_yes_price: bool = False,
) -> ParsedBookSnapshot | None:
    msg = envelope.get("msg") if isinstance(envelope.get("msg"), dict) else envelope
    ticker = first_present(msg, "market_ticker", "ticker")
    if ticker is None:
        return None
    yes_raw = _dollar_levels(msg, "yes")
    no_raw = _dollar_levels(msg, "no")
    if use_yes_price:
        bids = _sorted_levels(yes_raw, reverse=True)
        asks = _sorted_levels(no_raw, reverse=False)
    else:
        bids, asks = kalshi_yes_book(yes_raw, no_raw)
    timestamp = parse_timestamp(first_present(msg, "ts", "timestamp"))
    return ParsedBookSnapshot(
        exchange="kalshi",
        market_external_id=str(ticker),
        outcome_external_id=f"{ticker}:yes",
        source_timestamp=None,
        exchange_timestamp=timestamp,
        source_timestamp_raw=None,
        exchange_timestamp_raw=(
            str(first_present(msg, "ts", "timestamp"))
            if first_present(msg, "ts", "timestamp") is not None
            else None
        ),
        received_at=received_at or utc_now(),
        received_monotonic_ns=received_monotonic_ns or time.monotonic_ns(),
        sequence_number=as_int(envelope.get("seq") or msg.get("seq")),
        book_hash=None,
        bids=bids,
        asks=asks,
        best_bid=as_decimal(bids[0][0]) if bids else None,
        best_ask=as_decimal(asks[0][0]) if asks else None,
        raw_data=envelope,
    )


def parse_orderbook_delta(
    envelope: dict[str, Any],
    *,
    received_at: datetime | None = None,
    received_monotonic_ns: int | None = None,
    use_yes_price: bool = False,
) -> ParsedBookUpdate | None:
    msg = envelope.get("msg") if isinstance(envelope.get("msg"), dict) else envelope
    ticker = first_present(msg, "market_ticker", "ticker")
    side = str(msg.get("side") or "").lower()
    price = dollars(msg, "price_dollars", "price")
    delta = as_decimal(first_present(msg, "delta_fp", "delta"))
    if ticker is None or side not in {"yes", "no"} or price is None or delta is None:
        return None
    # Standardized book is from YES's perspective. A NO bid is a YES ask.
    standard_side = "bid" if side == "yes" else "ask"
    standard_price = price if side == "yes" or use_yes_price else Decimal("1") - price
    return ParsedBookUpdate(
        exchange="kalshi",
        market_external_id=str(ticker),
        outcome_external_id=f"{ticker}:yes",
        source_timestamp=None,
        exchange_timestamp=parse_timestamp(first_present(msg, "ts_ms", "ts", "timestamp")),
        source_timestamp_raw=None,
        exchange_timestamp_raw=(
            str(first_present(msg, "ts_ms", "ts", "timestamp"))
            if first_present(msg, "ts_ms", "ts", "timestamp") is not None
            else None
        ),
        received_at=received_at or utc_now(),
        received_monotonic_ns=received_monotonic_ns or time.monotonic_ns(),
        sequence_number=as_int(envelope.get("seq") or msg.get("seq")),
        book_hash=None,
        side=standard_side,
        price=standard_price,
        size=None,
        size_delta=delta,
        operation="delta",
        raw_data=envelope,
    )


def _sorted_levels(values: list[Any], *, reverse: bool) -> list[list[str]]:
    levels: list[tuple[Decimal, Decimal]] = []
    for value in values:
        if not isinstance(value, (list, tuple)) or len(value) < 2:
            continue
        price = as_decimal(value[0])
        size = as_decimal(value[1])
        if price is not None and size is not None and size > 0:
            levels.append((price, size))
    levels.sort(key=lambda item: item[0], reverse=reverse)
    return [[str(price), str(size)] for price, size in levels]


def _text_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    return str(value)


def _market_rules(raw: dict[str, Any]) -> str | None:
    parts = [
        str(value).strip()
        for value in (raw.get("rules_primary"), raw.get("rules_secondary"))
        if value is not None and str(value).strip()
    ]
    return "\n\n".join(parts) or None
