from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.jobs.live import LiveCollector
from prediction_collector.polymarket.rtds import (
    PolymarketRtdsWebSocket,
    RtdsApplicationSilenceError,
    _receive_rtds_application_frame,
)
from prediction_collector.polymarket.websocket import (
    PolymarketMarketWebSocket,
    _fully_initialized_markets,
    _polymarket_lifecycle_updates,
)
from prediction_collector.writer import WriteItem


class CapturingWriter:
    def __init__(self) -> None:
        self.items: list[WriteItem] = []

    async def put(self, item: WriteItem) -> None:
        self.items.append(item)


class SocketDatabase:
    def __init__(self) -> None:
        self.close_reasons: list[str] = []

    async def create_connection(self, **_: Any) -> int:
        return 1

    async def update_connection_stats(self, *_: Any, **__: Any) -> None:
        return None

    async def close_connection(self, *_: Any, **values: Any) -> None:
        self.close_reasons.append(values["reason"])

    async def record_gap(self, **_: Any) -> int:
        raise AssertionError("planned shard stop must not record an unknown gap")


class SocketMetrics:
    async def message(self, *_: Any, **__: Any) -> None:
        return None


def socket(writer: CapturingWriter) -> PolymarketRtdsWebSocket:
    return PolymarketRtdsWebSocket(
        url="wss://example.invalid",
        writer=writer,  # type: ignore[arg-type]
        database=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        store_raw=False,
        equity_symbols=frozenset(),
        comments_enabled=False,
        application_silence_timeout_seconds=600,
    )


def test_new_market_lifecycle_does_not_invent_trade_readiness() -> None:
    assert _polymarket_lifecycle_updates("new_market", {}) == {}
    assert _polymarket_lifecycle_updates("new_market", {"active": True}) == {
        "status": "active",
        "is_active": True,
    }


def test_partial_initial_dump_confirms_only_complete_markets() -> None:
    mapping = {
        "a-yes": "market-a",
        "a-no": "market-a",
        "b-yes": "market-b",
        "b-no": "market-b",
    }
    assert _fully_initialized_markets(
        mapping, {"a-yes", "a-no", "b-yes"}
    ) == {"market-a"}


@pytest.mark.asyncio
async def test_rtds_heartbeat_pongs_cannot_mask_application_silence() -> None:
    class HeartbeatOnlySocket:
        async def recv(self) -> str:
            await asyncio.sleep(0)
            return "PONG"

    with pytest.raises(
        RtdsApplicationSilenceError,
        match="no RTDS application message",
    ):
        await asyncio.wait_for(
            _receive_rtds_application_frame(
                HeartbeatOnlySocket(), timeout_seconds=0.01
            ),
            timeout=0.5,
        )


@pytest.mark.asyncio
async def test_rtds_application_frame_satisfies_liveness() -> None:
    class DataSocket:
        def __init__(self) -> None:
            self.frames = iter(("PONG", '{"topic":"crypto_prices"}'))

        async def recv(self) -> str:
            return next(self.frames)

    assert await _receive_rtds_application_frame(
        DataSocket(), timeout_seconds=1
    ) == '{"topic":"crypto_prices"}'


@pytest.mark.asyncio
async def test_market_socket_planned_stop_survives_swallowed_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receive_started = asyncio.Event()
    context_entries = 0

    class WebSocket:
        async def send(self, _: str) -> None:
            return None

        async def recv(self) -> str:
            receive_started.set()
            await asyncio.Future()
            raise AssertionError("unreachable")

    class CancellationSwallowingConnection:
        async def __aenter__(self) -> WebSocket:
            nonlocal context_entries
            context_entries += 1
            return WebSocket()

        async def __aexit__(
            self,
            error_type: type[BaseException] | None,
            _: BaseException | None,
            __: object,
        ) -> bool:
            # Reproduce the production race: the planned refresh cancelled the
            # shard while the transport was unwinding a remote disconnect, and
            # the transport context consumed that cancellation.
            return error_type is asyncio.CancelledError

    monkeypatch.setattr(
        "prediction_collector.polymarket.websocket.connect",
        lambda *_args, **_kwargs: CancellationSwallowingConnection(),
    )
    database = SocketDatabase()
    market_socket = PolymarketMarketWebSocket(
        url="wss://example.invalid",
        writer=CapturingWriter(),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        metrics=SocketMetrics(),  # type: ignore[arg-type]
        tier_manager=object(),  # type: ignore[arg-type]
    )
    stop = asyncio.Event()
    planned_stop = asyncio.Event()
    collector = LiveCollector.__new__(LiveCollector)
    collector.stop = stop
    collector._task_failure = asyncio.get_running_loop().create_future()
    task = collector._create_watched_task(
        market_socket.run(
            {"token": "market"},
            run_id=1,
            stop=stop,
            connection_label="shard-1",
            planned_stop=planned_stop,
        ),
        name="polymarket-market-ws-1",
        expected_stop=planned_stop,
    )
    await asyncio.wait_for(receive_started.wait(), timeout=1)

    planned_stop.set()
    task.cancel()
    await asyncio.wait_for(task, timeout=1)
    await asyncio.sleep(0)

    assert context_entries == 1
    assert database.close_reasons == ["planned_subscription_refresh"]
    assert not collector._task_failure.done()


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
async def test_chainlink_spot_uses_documented_decimal_value_not_e18_auxiliary() -> None:
    writer = CapturingWriter()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    await socket(writer)._price_item(
        "crypto_prices_chainlink",
        "chainlink_spot",
        "ZEC/USD",
        {
            "timestamp": 1_800_000_000,
            "value": "486.41403713256",
            "full_accuracy_value": "486414037132560000000",
        },
        {},
        1,
        now,
        now,
        123,
    )
    assert writer.items[0].data["price"] == Decimal("486.41403713256")


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
