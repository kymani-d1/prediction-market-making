from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from prediction_collector.polymarket.rtds import PolymarketRtdsWebSocket
from prediction_collector.polymarket.websocket import _polymarket_lifecycle_updates
from prediction_collector.writer import WriteItem


class CapturingWriter:
    def __init__(self) -> None:
        self.items: list[WriteItem] = []

    async def put(self, item: WriteItem) -> None:
        self.items.append(item)


def socket(writer: CapturingWriter) -> PolymarketRtdsWebSocket:
    return PolymarketRtdsWebSocket(
        url="wss://example.invalid",
        writer=writer,  # type: ignore[arg-type]
        database=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        store_raw=False,
        equity_symbols=frozenset(),
        comments_enabled=False,
    )


def test_new_market_lifecycle_does_not_invent_trade_readiness() -> None:
    assert _polymarket_lifecycle_updates("new_market", {}) == {}
    assert _polymarket_lifecycle_updates("new_market", {"active": True}) == {
        "status": "active",
        "is_active": True,
    }


@pytest.mark.asyncio
async def test_rtds_prefers_full_accuracy_reference_value() -> None:
    writer = CapturingWriter()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    await socket(writer)._price_item(
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
    now = datetime(2026, 1, 1, tzinfo=UTC)
    raw_exact = "65000500000000000000000"
    await socket(writer)._handle(
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
async def test_snapshot_and_live_delivery_share_semantic_measurement_id() -> None:
    writer = CapturingWriter()
    value = socket(writer)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    measurement = {
        "symbol": "AAPL",
        "timestamp": 1_800_000_000,
        "full_accuracy_value": "189.42170000",
    }
    for payload in (
        {"symbol": "AAPL", "data": [measurement]},
        {"symbol": "AAPL", "data": [measurement]},
        measurement,
    ):
        await value._handle(
            {"topic": "equity_prices", "type": "update", "payload": payload},
            connection_id=1,
            received_at=now,
            monotonic_ns=125,
        )
    assert len({item.data["external_update_id"] for item in writer.items}) == 1
    assert [item.data["source_status"] for item in writer.items] == [
        "snapshot",
        "snapshot",
        "live",
    ]
