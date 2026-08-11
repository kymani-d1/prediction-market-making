from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.kalshi.websocket import (
    KalshiWebSocket,
    _kalshi_lifecycle_updates,
)
from prediction_collector.polymarket.rtds import PolymarketRtdsWebSocket
from prediction_collector.polymarket.websocket import _polymarket_lifecycle_updates
from prediction_collector.writer import WriteItem


class CapturingWriter:
    def __init__(self) -> None:
        self.items: list[WriteItem] = []

    async def put(self, item: WriteItem) -> None:
        self.items.append(item)


class EventDatabase:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def upsert_event(self, value: dict[str, Any]) -> int:
        self.events.append(value)
        return 1


def test_kalshi_settlement_keeps_categorical_result_and_numeric_value_separate() -> None:
    updates = _kalshi_lifecycle_updates(
        "settled",
        {
            "result": "yes",
            "settlement_value_dollars": "0.6250",
            "settled_ts": 1_800_000_000,
        },
    )

    assert updates["status"] == "finalized"
    assert updates["result"] == "yes"
    assert updates["settlement_value"] == Decimal("0.6250")
    assert updates["settlement_time"] is not None


def test_kalshi_floor_strike_only_lifecycle_is_normalized_as_metadata() -> None:
    updates = _kalshi_lifecycle_updates(
        "metadata_updated",
        {"floor_strike": "102500.0000"},
    )

    assert updates == {
        "structural_metadata": {"floor_strike": "102500.0000"}
    }


def test_polymarket_new_market_does_not_invent_trade_readiness() -> None:
    assert _polymarket_lifecycle_updates("new_market", {}) == {}
    assert _polymarket_lifecycle_updates("new_market", {"active": True}) == {
        "status": "active",
        "is_active": True,
    }


@pytest.mark.asyncio
async def test_rtds_prefers_full_accuracy_reference_value() -> None:
    writer = CapturingWriter()
    socket = PolymarketRtdsWebSocket(
        url="wss://example.invalid",
        writer=writer,  # type: ignore[arg-type]
        database=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        store_raw=False,
        equity_symbols=frozenset(),
        comments_enabled=False,
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)

    await socket._price_item(
        "equity_prices",
        "pyth",
        "AAPL",
        {
            "timestamp": 1_800_000_000,
            "value": "189.42",
            "full_accuracy_value": "189.42170000",
        },
        {},
        1,
        now,
        now,
        123,
    )

    assert writer.items[0].data["price"] == Decimal("189.42170000")


@pytest.mark.asyncio
async def test_chainlink_twap_scales_e18_and_keeps_window_feed_identity() -> None:
    writer = CapturingWriter()
    socket = PolymarketRtdsWebSocket(
        url="wss://example.invalid",
        writer=writer,  # type: ignore[arg-type]
        database=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        store_raw=False,
        equity_symbols=frozenset(),
        comments_enabled=False,
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    raw_exact = "65000500000000000000000"

    await socket._handle(
        {
            "topic": "crypto_prices_twap_thirty",
            "type": "update",
            "timestamp": 1_800_000_001,
            "payload": {
                "symbol": "BTC/USD",
                "timestamp": 1_800_000_000,
                "value": "65000.50",
                "full_accuracy_value": raw_exact,
                "window_s": 30,
            },
        },
        connection_id=1,
        received_at=now,
        monotonic_ns=124,
    )

    assert writer.items[0].data["provider"] == "chainlink_twap_30s"
    assert writer.items[0].data["price"] == Decimal("65000.5")
    assert writer.items[0].data["raw_data"]["point"]["full_accuracy_value"] == raw_exact


@pytest.mark.asyncio
async def test_rtds_snapshot_and_live_delivery_share_measurement_identity() -> None:
    writer = CapturingWriter()
    socket = PolymarketRtdsWebSocket(
        url="wss://example.invalid",
        writer=writer,  # type: ignore[arg-type]
        database=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        store_raw=False,
        equity_symbols=frozenset(),
        comments_enabled=False,
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    measurement = {
        "symbol": "AAPL",
        "timestamp": 1_800_000_000,
        "full_accuracy_value": "189.42170000",
    }

    for payload in (
        {"symbol": "AAPL", "data": [{"symbol": "AAPL", "timestamp": 1_799_999_999, "value": "189"}, measurement]},
        {"symbol": "AAPL", "data": [measurement]},
        measurement,
    ):
        await socket._handle(
            {"topic": "equity_prices", "type": "update", "payload": payload},
            connection_id=1,
            received_at=now,
            monotonic_ns=125,
        )

    repeated = [
        item
        for item in writer.items
        if item.data["price"] == Decimal("189.42170000")
    ]
    assert len(repeated) == 3
    assert len({item.data["external_update_id"] for item in repeated}) == 1
    assert [item.data["source_status"] for item in repeated] == [
        "snapshot",
        "snapshot",
        "live",
    ]


@pytest.mark.asyncio
async def test_kalshi_event_lifecycle_without_market_is_persisted() -> None:
    writer = CapturingWriter()
    database = EventDatabase()
    socket = KalshiWebSocket(
        url="wss://example.invalid",
        signer=object(),  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        store_raw=False,
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)

    await socket._handle(
        {
            "type": "event_lifecycle",
            "sid": 5,
            "msg": {
                "event_ticker": "EVENT-1",
                "series_ticker": "SERIES",
                "title": "Example event",
            },
        },
        connection_id=1,
        received_at=now,
        monotonic_ns=123,
        open_gaps={},
    )

    assert database.events[0]["external_id"] == "EVENT-1"
    assert writer.items[0].kind == "event_lifecycle_events"
    assert writer.items[0].data["event_external_id"] == "EVENT-1"
