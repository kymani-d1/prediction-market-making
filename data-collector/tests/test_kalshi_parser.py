from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from prediction_collector.kalshi.parser import (
    normalise_market,
    normalise_multivariate_collection,
    parse_market_candidate,
    parse_orderbook_delta,
    parse_orderbook_snapshot,
    parse_trade,
)


FixtureLoader = Callable[[str], dict[str, Any]]
RECEIVED_AT = datetime(2026, 8, 11, 14, 0, 2, tzinfo=UTC)


def test_market_candidate_prefers_fixed_point_fields_without_float_loss(
    load_fixture: FixtureLoader,
) -> None:
    candidate = parse_market_candidate(load_fixture("kalshi_market.json"))

    assert candidate.exchange == "kalshi"
    assert candidate.external_id == "KXBTC-26AUG11-T100000"
    assert candidate.active is True
    assert candidate.tradable is True
    assert candidate.volume == Decimal("123.4500")
    assert candidate.volume_24h == Decimal("56.7500")
    # Kalshi deprecated the REST liquidity fields and documents them as
    # returning zero, so they cannot be treated as a live ranking metric.
    assert candidate.liquidity is None


def test_provisional_active_market_remains_tradable(load_fixture: FixtureLoader) -> None:
    raw = load_fixture("kalshi_market.json")
    raw["is_provisional"] = True

    candidate = parse_market_candidate(raw)

    assert candidate.active is True
    assert candidate.tradable is True


def test_normalised_binary_outcomes_are_exact_complements(
    load_fixture: FixtureLoader,
) -> None:
    market, outcomes = normalise_market(load_fixture("kalshi_market.json"))

    assert market["liquidity"] is None
    assert market["open_interest"] == Decimal("40.25")
    assert market["price_level_structure"]["type"] == "linear_cent"
    assert outcomes[0]["last_price"] == Decimal("0.4200")
    assert outcomes[1]["last_price"] == Decimal("0.5800")
    assert [outcome["external_id"] for outcome in outcomes] == [
        "KXBTC-26AUG11-T100000:yes",
        "KXBTC-26AUG11-T100000:no",
    ]


def test_multivariate_collection_constraints_are_normalised_and_raw_is_retained() -> None:
    raw = {
        "collection_ticker": "KXMVE-COLLECTION",
        "series_ticker": "KXMVE",
        "title": "Two selected outcomes",
        "functional_description": "Both selected legs resolve yes",
        "associated_event_tickers": ["EVENT-A", "EVENT-B"],
        "associated_events": [
            {"ticker": "EVENT-A", "is_yes_only": False},
            {"ticker": "EVENT-B", "is_yes_only": True},
        ],
        "is_ordered": False,
        "is_single_market_per_event": True,
        "is_all_yes": False,
        "size_min": 2,
        "size_max": 2,
        "exchange_index": "mve-index",
    }

    group = normalise_multivariate_collection(raw)

    assert group["external_id"] == "mve:KXMVE-COLLECTION"
    assert group["group_type"] == "multivariate"
    assert group["description"] == "Both selected legs resolve yes"
    assert group["constraint_definition"]["associated_event_tickers"] == [
        "EVENT-A",
        "EVENT-B",
    ]
    assert group["constraint_definition"]["is_single_market_per_event"] is True
    assert group["raw_data"] is raw


def test_snapshot_converts_no_bids_to_yes_asks_and_preserves_sequence(
    load_fixture: FixtureLoader,
) -> None:
    parsed = parse_orderbook_snapshot(
        load_fixture("kalshi_orderbook_snapshot.json"),
        received_at=RECEIVED_AT,
        received_monotonic_ns=123,
    )

    assert parsed is not None
    assert parsed.sequence_number == 41
    assert parsed.bids == [["0.42", "3.250"], ["0.40", "8.125"]]
    assert parsed.asks == [["0.55", "2.50"], ["0.60", "4"]]
    assert parsed.best_bid == Decimal("0.42")
    assert parsed.best_ask == Decimal("0.55")
    assert parsed.exchange_timestamp == datetime(2026, 8, 11, 14, 0, 1, 230000, tzinfo=UTC)
    assert parsed.received_at is RECEIVED_AT


def test_no_delta_becomes_yes_ask_at_complement_price(
    load_fixture: FixtureLoader,
) -> None:
    parsed = parse_orderbook_delta(
        load_fixture("kalshi_orderbook_delta_no.json"),
        received_at=RECEIVED_AT,
        received_monotonic_ns=456,
    )

    assert parsed is not None
    assert parsed.sequence_number == 42
    assert parsed.side == "ask"
    assert parsed.price == Decimal("0.55")
    assert parsed.size is None
    assert parsed.size_delta == Decimal("-1.500")
    assert parsed.operation == "delta"
    assert parsed.exchange_timestamp == datetime.fromtimestamp(1786456801.240, tz=UTC)


def test_yes_delta_remains_yes_bid_and_cent_price_is_converted() -> None:
    parsed = parse_orderbook_delta(
        {
            "seq": 7,
            "msg": {
                "market_ticker": "KXTEST",
                "side": "yes",
                "price": 42,
                "delta": 3,
            },
        }
    )

    assert parsed is not None
    assert parsed.side == "bid"
    assert parsed.price == Decimal("0.42")
    assert parsed.size_delta == Decimal("3")


def test_trade_prefers_dollar_fixed_point_price_and_preserves_trade_id(
    load_fixture: FixtureLoader,
) -> None:
    parsed = parse_trade(
        load_fixture("kalshi_trade.json"),
        received_at=RECEIVED_AT,
        received_monotonic_ns=789,
    )

    assert parsed is not None
    assert parsed.external_trade_id == "f0f2397a-ephemeral-fixture"
    assert parsed.price == Decimal("0.4200")
    assert parsed.size == Decimal("12.500")
    assert parsed.side == "yes"
    assert parsed.executed_at == datetime.fromtimestamp(1786456801.230, tz=UTC)
    assert parsed.received_at is RECEIVED_AT
    assert parsed.received_monotonic_ns == 789
    assert len(parsed.dedup_hash) == 64


def test_invalid_snapshot_delta_and_trade_are_ignored() -> None:
    assert parse_orderbook_snapshot({"seq": 1, "msg": {"yes": [], "no": []}}) is None
    assert parse_orderbook_delta(
        {"msg": {"market_ticker": "KX", "side": "maybe", "price": 50, "delta": 1}}
    ) is None
    assert parse_trade({"ticker": "KX", "yes_price": 50}) is None
