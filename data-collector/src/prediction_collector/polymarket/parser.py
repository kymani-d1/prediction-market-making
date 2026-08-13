from __future__ import annotations

import json
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from prediction_collector.common.types import (
    MarketCandidate,
    ParsedBookSnapshot,
    ParsedBookUpdate,
    ParsedTrade,
)
from prediction_collector.common.utils import (
    as_decimal,
    as_int,
    first_present,
    parse_timestamp,
    trade_fingerprint,
    utc_now,
)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in {"true", "1", "yes"}
    return default


def market_status(raw: dict[str, Any]) -> str:
    if _bool(raw.get("resolved")) or raw.get("resolution") or raw.get("result"):
        return "resolved"
    if _bool(raw.get("closed")):
        return "closed"
    if _bool(raw.get("archived")):
        return "archived"
    if _bool(raw.get("active")) and _bool(raw.get("acceptingOrders"), True):
        return "active"
    return str(raw.get("status") or "inactive").lower()


def parse_market_candidate(raw: dict[str, Any]) -> MarketCandidate:
    # conditionId is the cross-surface CTF/CLOB identifier. Gamma's row id is
    # retained as an alias and in raw_data.
    external_id = str(first_present(raw, "conditionId", "condition_id", "id") or "")
    token_ids = tuple(str(value) for value in _json_list(raw.get("clobTokenIds")) if value)
    active = _bool(raw.get("active")) and not _bool(raw.get("closed"))
    tradable = (
        active
        and _bool(raw.get("enableOrderBook"), bool(token_ids))
        and _bool(raw.get("acceptingOrders"), False)
        and bool(token_ids)
    )
    return MarketCandidate(
        exchange="polymarket",
        external_id=external_id,
        ticker=str(raw.get("slug")) if raw.get("slug") else None,
        status=market_status(raw),
        active=active,
        tradable=tradable,
        volume=as_decimal(first_present(raw, "volumeNum", "volume")),
        volume_24h=as_decimal(first_present(raw, "volume24hr", "volume_24hr")),
        liquidity=as_decimal(first_present(raw, "liquidityNum", "liquidity")),
        outcome_token_ids=token_ids,
        aliases=tuple(
            str(value)
            for value in (
                first_present(raw, "conditionId", "condition_id"),
                raw.get("id"),
                raw.get("slug"),
            )
            if value
        ),
        raw_data=raw,
    )


def normalise_event(raw: dict[str, Any]) -> dict[str, Any]:
    start_time = parse_timestamp(first_present(raw, "startDate", "startTime"))
    end_time = parse_timestamp(first_present(raw, "endDate", "endTime"))
    if start_time is not None and end_time is not None and end_time < start_time:
        end_time = None
    return {
        "exchange": "polymarket",
        "external_id": str(raw.get("id") or raw.get("slug") or ""),
        "series_external_id": _nested_id(raw.get("series")),
        "ticker": raw.get("ticker"),
        "slug": raw.get("slug"),
        "title": raw.get("title") or raw.get("question") or raw.get("slug") or "",
        "description": raw.get("description"),
        "category": raw.get("category"),
        "status": (
            "closed"
            if _bool(raw.get("closed"))
            else "active"
            if _bool(raw.get("active"))
            else "inactive"
        ),
        "start_time": start_time,
        "end_time": end_time,
        "created_time": parse_timestamp(
            first_present(raw, "creationDate", "createdAt", "created_at")
        ),
        "updated_time": parse_timestamp(first_present(raw, "updatedAt", "updated_at")),
        "rules": raw.get("rules"),
        "resolution_source": first_present(raw, "resolutionSource", "resolution_source"),
        "source_timestamp": parse_timestamp(first_present(raw, "updatedAt", "updated_at")),
        "raw_data": raw,
    }


def _nested_id(value: Any) -> str | None:
    if isinstance(value, list) and value:
        value = value[0]
    if isinstance(value, dict):
        nested = first_present(value, "id", "ticker", "slug")
        return str(nested) if nested is not None else None
    if value is not None and value != "":
        return str(value)
    return None


def normalise_market(
    raw: dict[str, Any], *, event_external_id: str | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    candidate = parse_market_candidate(raw)
    open_time = parse_timestamp(first_present(raw, "startDate", "startTime"))
    close_time = parse_timestamp(first_present(raw, "endDate", "endTime"))
    # Gamma occasionally carries a stale event end date on a newly opened
    # market. Preserve it in raw_data, but do not normalize an impossible
    # close-before-open interval that violates the lifecycle contract.
    if open_time is not None and close_time is not None and close_time < open_time:
        close_time = None
    market = {
        "exchange": "polymarket",
        "external_id": candidate.external_id,
        "event_external_id": event_external_id or _nested_id(raw.get("events")),
        "ticker": raw.get("marketMakerAddress"),
        "slug": raw.get("slug"),
        "condition_id": first_present(raw, "conditionId", "condition_id"),
        "question": raw.get("question") or raw.get("title") or candidate.external_id,
        "subtitle": raw.get("groupItemTitle"),
        "description": raw.get("description"),
        "rules": raw.get("rules"),
        "resolution_source": first_present(raw, "resolutionSource", "resolution_source"),
        "source_timestamp": parse_timestamp(first_present(raw, "updatedAt", "updated_at")),
        "status": candidate.status,
        "market_type": raw.get("marketType") or "binary",
        "is_active": candidate.active,
        "is_tradable": candidate.tradable,
        "open_time": open_time,
        "close_time": close_time,
        "settlement_time": parse_timestamp(
            first_present(raw, "settlementTime", "settledAt", "settled_at")
        ),
        "result": first_present(raw, "resolution", "result", "winningOutcome"),
        "settlement_value": as_decimal(
            first_present(raw, "settlementValue", "settlement_value")
        ),
        "volume": candidate.volume,
        "volume_24h": candidate.volume_24h,
        "open_interest": as_decimal(first_present(raw, "openInterest", "open_interest")),
        "liquidity": candidate.liquidity,
        "tick_size": as_decimal(first_present(raw, "orderPriceMinTickSize", "minimumTickSize")),
        "fee_rate": as_decimal(first_present(raw, "fee", "feeRate", "takerBaseFee")),
        "enable_order_book": _bool(raw.get("enableOrderBook")),
        "accepting_orders": _bool(raw.get("acceptingOrders")),
        "negative_risk": _bool(first_present(raw, "negRisk", "negativeRisk")),
        "raw_data": raw,
    }
    names = [str(value) for value in _json_list(raw.get("outcomes"))]
    token_ids = [str(value) for value in _json_list(raw.get("clobTokenIds"))]
    prices = _json_list(raw.get("outcomePrices"))
    outcome_count = max(len(names), len(token_ids))
    outcomes: list[dict[str, Any]] = []
    for index in range(outcome_count):
        token_id = token_ids[index] if index < len(token_ids) else None
        name = names[index] if index < len(names) else f"Outcome {index}"
        outcomes.append(
            {
                "exchange": "polymarket",
                # Keep identity stable before and after a CLOB token is
                # assigned; token_id remains separately queryable and unique.
                "external_id": f"{candidate.external_id}:outcome:{index}",
                "token_id": token_id,
                "name": name,
                "outcome_index": index,
                "last_price": as_decimal(prices[index]) if index < len(prices) else None,
                "raw_data": {
                    "name": name,
                    "token_id": token_id,
                    "outcome_index": index,
                    "price": prices[index] if index < len(prices) else None,
                },
            }
        )
    return market, outcomes


def normalise_series(raw: dict[str, Any]) -> dict[str, Any]:
    external_id = str(first_present(raw, "id", "ticker", "slug") or "")
    return {
        "exchange": "polymarket",
        "external_id": external_id,
        "ticker": raw.get("ticker") or raw.get("slug"),
        "title": raw.get("title") or raw.get("name") or external_id,
        "category": raw.get("category"),
        "frequency": raw.get("recurrence") or raw.get("frequency"),
        "raw_data": raw,
    }


def parse_trade(
    raw: dict[str, Any],
    *,
    received_at: datetime | None = None,
    received_monotonic_ns: int | None = None,
) -> ParsedTrade | None:
    received = received_at or utc_now()
    monotonic = received_monotonic_ns or time.monotonic_ns()
    market = first_present(raw, "conditionId", "condition_id", "market", "market_id")
    outcome = first_present(raw, "asset", "asset_id", "token_id")
    price = as_decimal(first_present(raw, "price", "last_trade_price"))
    size = as_decimal(first_present(raw, "size", "amount", "quantity"))
    executed = parse_timestamp(first_present(raw, "timestamp", "created_at", "createdAt"))
    if market is None or price is None or size is None or executed is None:
        return None
    external_trade_id = first_present(raw, "id", "trade_id")
    transaction_hash = first_present(raw, "transactionHash", "transaction_hash")
    side = first_present(raw, "side", "taker_side")
    dedup = trade_fingerprint(
        exchange="polymarket",
        market_external_id=str(market),
        outcome_external_id=str(outcome) if outcome is not None else None,
        executed_at=executed,
        price=price,
        size=size,
        side=str(side) if side is not None else None,
        transaction_hash=str(transaction_hash) if transaction_hash else None,
        external_trade_id=str(external_trade_id) if external_trade_id else None,
    )
    exchange_timestamp = parse_timestamp(first_present(raw, "timestamp", "exchange_timestamp"))
    return ParsedTrade(
        exchange="polymarket",
        market_external_id=str(market),
        outcome_external_id=str(outcome) if outcome is not None else None,
        external_trade_id=str(external_trade_id) if external_trade_id else None,
        executed_at=executed,
        received_at=received,
        received_monotonic_ns=monotonic,
        price=price,
        size=size,
        side=str(side).lower() if side is not None else None,
        transaction_hash=str(transaction_hash) if transaction_hash else None,
        dedup_hash=dedup,
        source_timestamp=None,
        exchange_timestamp=exchange_timestamp,
        source_timestamp_raw=None,
        exchange_timestamp_raw=(
            str(first_present(raw, "timestamp", "exchange_timestamp"))
            if first_present(raw, "timestamp", "exchange_timestamp") is not None
            else None
        ),
        raw_data=raw,
    )


def parse_book(
    raw: dict[str, Any],
    *,
    received_at: datetime | None = None,
    received_monotonic_ns: int | None = None,
) -> ParsedBookSnapshot | None:
    market = first_present(raw, "market", "condition_id", "conditionId")
    asset = first_present(raw, "asset_id", "asset", "token_id")
    bids_raw = raw.get("bids")
    asks_raw = raw.get("asks")
    if bids_raw is None:
        bids_raw = []
    if asks_raw is None:
        asks_raw = []
    if market is None or not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
        return None
    bids = _normalise_levels(bids_raw, reverse=True)
    asks = _normalise_levels(asks_raw, reverse=False)
    timestamp = parse_timestamp(first_present(raw, "timestamp", "exchange_timestamp"))
    return ParsedBookSnapshot(
        exchange="polymarket",
        market_external_id=str(market),
        outcome_external_id=str(asset) if asset is not None else None,
        source_timestamp=None,
        exchange_timestamp=timestamp,
        source_timestamp_raw=None,
        exchange_timestamp_raw=(
            str(first_present(raw, "timestamp", "exchange_timestamp"))
            if first_present(raw, "timestamp", "exchange_timestamp") is not None
            else None
        ),
        received_at=received_at or utc_now(),
        received_monotonic_ns=received_monotonic_ns or time.monotonic_ns(),
        sequence_number=as_int(first_present(raw, "sequence", "seq")),
        book_hash=str(raw.get("hash")) if raw.get("hash") is not None else None,
        bids=bids,
        asks=asks,
        best_bid=as_decimal(bids[0][0]) if bids else None,
        best_ask=as_decimal(asks[0][0]) if asks else None,
        raw_data=raw,
    )


def _normalise_levels(raw_levels: list[Any], *, reverse: bool) -> list[list[str]]:
    levels: list[tuple[Decimal, Decimal]] = []
    for raw in raw_levels:
        if isinstance(raw, dict):
            price = as_decimal(raw.get("price"))
            size = as_decimal(raw.get("size"))
        elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
            price, size = as_decimal(raw[0]), as_decimal(raw[1])
        else:
            continue
        if price is not None and size is not None:
            levels.append((price, size))
    levels.sort(key=lambda item: item[0], reverse=reverse)
    return [[str(price), str(size)] for price, size in levels]


def parse_price_changes(
    raw: dict[str, Any],
    *,
    received_at: datetime | None = None,
    received_monotonic_ns: int | None = None,
) -> list[ParsedBookUpdate]:
    received = received_at or utc_now()
    monotonic = received_monotonic_ns or time.monotonic_ns()
    market = first_present(raw, "market", "condition_id", "conditionId")
    timestamp = parse_timestamp(first_present(raw, "timestamp", "exchange_timestamp"))
    changes = raw.get("price_changes") or raw.get("changes") or []
    if not isinstance(changes, list):
        return []
    parsed: list[ParsedBookUpdate] = []
    for change in changes:
        if not isinstance(change, dict):
            continue
        price = as_decimal(change.get("price"))
        size = as_decimal(change.get("size"))
        side = change.get("side")
        asset = first_present(change, "asset_id", "asset", "token_id")
        if market is None or price is None or size is None or side is None:
            continue
        parsed.append(
            ParsedBookUpdate(
                exchange="polymarket",
                market_external_id=str(market),
                outcome_external_id=str(asset) if asset is not None else None,
                source_timestamp=None,
                exchange_timestamp=timestamp,
                source_timestamp_raw=None,
                exchange_timestamp_raw=(
                    str(first_present(raw, "timestamp", "exchange_timestamp"))
                    if first_present(raw, "timestamp", "exchange_timestamp") is not None
                    else None
                ),
                received_at=received,
                received_monotonic_ns=monotonic,
                sequence_number=as_int(first_present(change, "sequence", "seq")),
                book_hash=(
                    str(change.get("hash") or raw.get("hash"))
                    if change.get("hash") or raw.get("hash")
                    else None
                ),
                side=str(side).lower(),
                price=price,
                size=size,
                size_delta=None,
                operation="set" if size > 0 else "delete",
                raw_data={"envelope": raw, "change": change},
            )
        )
    return parsed
