from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from prediction_collector.polymarket.parser import (
    normalise_event,
    normalise_market,
    parse_book,
    parse_market_candidate,
    parse_price_changes,
    parse_trade,
)


FixtureLoader = Callable[[str], dict[str, Any]]
RECEIVED_AT = datetime(2026, 8, 11, 14, 0, 2, tzinfo=UTC)


def test_market_candidate_decodes_gamma_json_arrays_and_exact_metrics(
    load_fixture: FixtureLoader,
) -> None:
    raw = load_fixture("polymarket_market.json")

    candidate = parse_market_candidate(raw)

    assert candidate.exchange == "polymarket"
    assert candidate.external_id == raw["conditionId"]
    assert candidate.source_id == str(raw["id"])
    assert str(raw["id"]) in candidate.selectors
    assert candidate.ticker == "nvda-above-240-on-january-30-2026"
    assert candidate.active is True
    assert candidate.tradable is True
    assert candidate.volume == Decimal("1234.5")
    assert candidate.volume_24h == Decimal("56.75")
    assert candidate.liquidity == Decimal("789.125")
    assert candidate.outcome_token_ids == (
        "76043073756653678226373981964075571318267289248134717369284518995922789326425",
        "31690934263385727664202099278545688007799199447969475608906331829650099442770",
    )


def test_market_without_explicit_accepting_orders_is_not_trade_ready(
    load_fixture: FixtureLoader,
) -> None:
    raw = load_fixture("polymarket_market.json")
    raw.pop("acceptingOrders")

    candidate = parse_market_candidate(raw)

    assert candidate.active is True
    assert candidate.tradable is False


def test_normalised_market_preserves_condition_event_and_outcome_index_mapping(
    load_fixture: FixtureLoader,
) -> None:
    market, outcomes = normalise_market(load_fixture("polymarket_market.json"))

    assert market["external_id"] == market["condition_id"]
    assert market["condition_id"].startswith("0x311d")
    assert market["event_external_id"] == "125819"
    assert market["tick_size"] == Decimal("0.01")
    assert [outcome["name"] for outcome in outcomes] == ["Yes", "No"]
    assert [outcome["outcome_index"] for outcome in outcomes] == [0, 1]
    assert [outcome["external_id"] for outcome in outcomes] == [
        f"{market['external_id']}:outcome:0",
        f"{market['external_id']}:outcome:1",
    ]
    assert [outcome["last_price"] for outcome in outcomes] == [
        Decimal("0.4200"),
        Decimal("0.5800"),
    ]


def test_normalised_market_does_not_emit_close_before_open(
    load_fixture: FixtureLoader,
) -> None:
    raw = load_fixture("polymarket_market.json")
    raw["startDate"] = "2026-07-01T00:00:00Z"
    raw["endDate"] = "2025-12-31T12:00:00Z"

    market, _ = normalise_market(raw)

    assert market["open_time"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert market["close_time"] is None
    assert market["raw_data"]["endDate"] == "2025-12-31T12:00:00Z"


def test_normalised_event_does_not_emit_end_before_start() -> None:
    raw = {
        "id": "EVENT-A",
        "title": "Event",
        "active": True,
        "startDate": "2026-07-01T00:00:00Z",
        "endDate": "2025-12-31T12:00:00Z",
    }

    event = normalise_event(raw)

    assert event["start_time"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert event["end_time"] is None
    assert event["raw_data"]["endDate"] == "2025-12-31T12:00:00Z"


def test_book_parser_sorts_levels_and_preserves_hash_and_receive_clock(
    load_fixture: FixtureLoader,
) -> None:
    parsed = parse_book(
        load_fixture("polymarket_ws_book.json"),
        received_at=RECEIVED_AT,
        received_monotonic_ns=123456789,
    )

    assert parsed is not None
    assert parsed.market_external_id.startswith("0x311d")
    assert parsed.bids == [
        ["0.42", "3.250"],
        ["0.41", "2"],
        ["0.40", "8.125"],
    ]
    assert parsed.asks == [
        ["0.44", "7.500"],
        ["0.45", "1.25"],
        ["0.46", "5"],
    ]
    assert parsed.best_bid == Decimal("0.42")
    assert parsed.best_ask == Decimal("0.44")
    assert parsed.book_hash == "0xbookhash"
    assert parsed.sequence_number is None
    assert parsed.exchange_timestamp == datetime.fromtimestamp(1757908892.351, tz=UTC)
    assert parsed.received_at is RECEIVED_AT
    assert parsed.received_monotonic_ns == 123456789


def test_price_change_parser_handles_multiple_absolute_updates_and_deletion(
    load_fixture: FixtureLoader,
) -> None:
    updates = parse_price_changes(
        load_fixture("polymarket_ws_price_change.json"),
        received_at=RECEIVED_AT,
        received_monotonic_ns=987654321,
    )

    assert len(updates) == 2
    assert updates[0].side == "buy"
    assert updates[0].price == Decimal("0.42")
    assert updates[0].size == Decimal("9.75")
    assert updates[0].size_delta is None
    assert updates[0].operation == "set"
    assert updates[0].book_hash == "causing-order-hash"
    assert updates[1].price == Decimal("0.40")
    assert updates[1].size == Decimal("0")
    assert updates[1].operation == "delete"
    assert all(update.sequence_number is None for update in updates)


def test_last_trade_parser_preserves_decimal_timestamp_side_and_identity(
    load_fixture: FixtureLoader,
) -> None:
    parsed = parse_trade(
        load_fixture("polymarket_trade.json"),
        received_at=RECEIVED_AT,
        received_monotonic_ns=99,
    )

    assert parsed is not None
    assert parsed.exchange == "polymarket"
    assert parsed.price == Decimal("0.4560")
    assert parsed.size == Decimal("219.217767")
    assert parsed.side == "buy"
    assert parsed.transaction_hash == "0xtransaction"
    assert parsed.executed_at == datetime.fromtimestamp(1750428146.322, tz=UTC)
    assert parsed.exchange_timestamp == parsed.executed_at
    assert parsed.received_at is RECEIVED_AT
    assert len(parsed.dedup_hash) == 64


def test_unknown_ws_shapes_are_not_misclassified_as_trade_or_price_change(
    load_fixture: FixtureLoader,
) -> None:
    unknown = load_fixture("unknown_ws_message.json")

    assert unknown["event_type"] == "future_message_type"
    assert parse_price_changes(unknown) == []
    assert parse_trade(unknown) is None
    assert unknown["future_field"] == {"must_be_preserved": True}


def test_incomplete_trade_is_rejected() -> None:
    assert parse_trade({"market": "m", "price": "0.5"}) is None


def test_non_list_book_sides_are_rejected() -> None:
    assert parse_book({"market": "m", "bids": {}, "asks": []}) is None


def test_non_list_price_changes_are_ignored() -> None:
    assert parse_price_changes({"market": "m", "price_changes": {}}) == []
