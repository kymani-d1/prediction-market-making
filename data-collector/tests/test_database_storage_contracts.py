from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from prediction_collector.database import (
    _market_metadata_digest,
    _write_query,
)


NOW = datetime(2026, 8, 13, tzinfo=UTC)


def test_metadata_digest_ignores_transport_and_volatile_market_metrics() -> None:
    base = {
        "ticker": "MARKET",
        "question": "Will it happen?",
        "status": "active",
        "is_active": True,
        "is_tradable": True,
        "volume": Decimal("1"),
        "volume_24h": Decimal("2"),
        "liquidity": Decimal("3"),
        "observed_at": NOW,
        "exchange_timestamp": NOW,
        "raw_data": {
            "clobTokenIds": ["yes", "no"],
            "bestBid": "0.4",
            "volume": "1",
        },
    }
    changed_transport = {
        **base,
        "volume": Decimal("999"),
        "observed_at": datetime(2026, 8, 14, tzinfo=UTC),
        "raw_data": {**base["raw_data"], "bestBid": "0.9", "volume": "999"},
    }
    assert _market_metadata_digest(base) == _market_metadata_digest(changed_transport)
    assert _market_metadata_digest(base) != _market_metadata_digest(
        {**base, "rules": "New resolution rule"}
    )


def test_raw_rest_provenance_sql_has_no_payload_column() -> None:
    # High-volume REST payloads are routed to ArchiveWriter; no write-query
    # branch exists that can append them to PostgreSQL.
    try:
        _write_query("raw_rest_payloads", {})
    except ValueError as error:
        assert "unsupported write item kind" in str(error)
    else:
        raise AssertionError("raw REST payload unexpectedly has a PostgreSQL batch path")


def test_current_book_update_sql_is_upsert_not_append_history() -> None:
    query, _ = _write_query(
        "current_orderbook_updates",
        {
            "exchange": "polymarket",
            "market_external_id": "market",
            "outcome_external_id": "token",
            "connection_id": 1,
            "source_timestamp": NOW,
            "exchange_timestamp": NOW,
            "received_at": NOW,
            "received_monotonic_ns": 1,
            "sequence_number": None,
            "book_hash": None,
            "best_bid": Decimal("0.4"),
            "best_ask": Decimal("0.6"),
            "midpoint": Decimal("0.5"),
            "spread": Decimal("0.2"),
            "bid_depth": Decimal("10"),
            "ask_depth": Decimal("12"),
            "level_count": 2,
            "side": "buy",
            "price": Decimal("0.4"),
            "size": Decimal("10"),
        },
    )
    assert "INSERT INTO current_orderbooks" in query
    assert "ON CONFLICT (outcome_id) DO UPDATE" in query
    assert "current_orderbook_levels" in query
    assert "orderbook_updates" not in query


def test_current_book_snapshot_deduplicates_exchange_price_levels() -> None:
    query, _ = _write_query(
        "current_orderbook_snapshots",
        {
            "exchange": "polymarket",
            "market_external_id": "market",
            "outcome_external_id": "token",
            "connection_id": 1,
            "source_timestamp": NOW,
            "exchange_timestamp": NOW,
            "received_at": NOW,
            "received_monotonic_ns": 1,
            "sequence_number": None,
            "book_hash": "hash",
            "best_bid": Decimal("0.4"),
            "best_ask": Decimal("0.6"),
            "midpoint": Decimal("0.5"),
            "spread": Decimal("0.2"),
            "bid_depth": Decimal("10"),
            "ask_depth": Decimal("12"),
            "level_count": 2,
            "bids": [["0.4", "5"], ["0.4", "10"]],
            "asks": [["0.6", "12"]],
        },
    )
    assert "DISTINCT ON (side_name, price::NUMERIC)" in query
    assert "WITH ORDINALITY" in query
