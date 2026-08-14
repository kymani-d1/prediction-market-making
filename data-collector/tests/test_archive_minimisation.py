from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
import pyarrow as pa

from prediction_collector.archive import (
    compact_archive_row,
    decimal_components,
    decimal_from_components,
    stable_archive_key,
    STREAM_SCHEMAS,
)
from prediction_collector.archive_replay import replay_book
from prediction_collector.common.orderbook import OrderBook
from prediction_collector.common.utils import utc_now
from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.polymarket.websocket import PolymarketMarketWebSocket
from prediction_collector.tiering import CollectionTier
from prediction_collector.writer import BatchWriter, WriteItem


NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


class FakeArchive:
    def __init__(self) -> None:
        self.records: list[Any] = []
        self.settings = SimpleNamespace(
            raw_ws_policy="errors_sample",
            raw_ws_valid_sample_rate=Decimal("0.001"),
            reference_unchanged_heartbeat_seconds=300,
        )

    async def put(self, record: Any) -> None:
        self.records.append(record)


class FakeTierManager:
    def __init__(self, tier: CollectionTier) -> None:
        self.tier = tier

    def tier_for(self, _market: str | None) -> CollectionTier:
        return self.tier

    def activity_for(self, _market: str) -> tuple[int, Decimal, int]:
        return 0, Decimal("0"), 0


def route_writer(tier: CollectionTier) -> tuple[BatchWriter, FakeArchive, FakeTierManager]:
    archive = FakeArchive()
    manager = FakeTierManager(tier)
    writer = BatchWriter.__new__(BatchWriter)
    writer.archive = archive
    writer.tier_manager = manager
    writer.postgres_pressure = "normal"
    writer._reference_state = {}
    writer.reference_duplicates_suppressed = 0
    return writer, archive, manager


def test_fixed_point_archive_encoding_is_exact_and_restart_stable() -> None:
    values = ["0", "0.123456789012345678", "1000000.00000001", "-0.0004", "1E+3"]
    for value in values:
        mantissa, scale = decimal_components(value)
        assert decimal_from_components(mantissa, scale) == Decimal(value)
    assert stable_archive_key("market", "condition-a") == stable_archive_key(
        "market", "condition-a"
    )
    assert stable_archive_key("market", "condition-a") != stable_archive_key(
        "token", "condition-a"
    )


def test_reference_decimal256_preserves_real_twap_precision() -> None:
    expected = Decimal("606.82118946874785792")
    row = compact_archive_row(
        "reference_prices",
        {
            "provider": "chainlink_twap_30s",
            "external_instrument_id": "bnb/usd",
            "price": expected,
            "received_at": NOW,
        },
    )
    table = pa.Table.from_pylist([row], schema=STREAM_SCHEMAS["reference_prices"])
    assert table["price"][0].as_py() == expected


def test_full_l2_snapshot_delta_replay_ignores_unordered_rest_reconciliation() -> None:
    snapshot = compact_archive_row(
        "orderbook_snapshots",
        {
            "market_external_id": "m",
            "outcome_external_id": "t",
            "received_at": NOW,
            "received_monotonic_ns": 10,
            "snapshot_type": "initial",
            "bids": [["0.10", "5.00"]],
            "asks": [["0.20", "7.00"]],
            "book_hash": "initial-hash",
        },
    )
    update_bid = compact_archive_row(
        "orderbook_updates",
        {
            "market_external_id": "m",
            "outcome_external_id": "t",
            "received_at": NOW + timedelta(microseconds=1),
            "received_monotonic_ns": 20,
            "side": "buy",
            "price": "0.15",
            "size": "6.2500",
            "operation": "set",
        },
    )
    delete_ask = compact_archive_row(
        "orderbook_updates",
        {
            "market_external_id": "m",
            "outcome_external_id": "t",
            "received_at": NOW + timedelta(microseconds=2),
            "received_monotonic_ns": 30,
            "side": "sell",
            "price": "0.20",
            "size": "0",
            "operation": "set",
        },
    )
    unordered_rest = compact_archive_row(
        "orderbook_snapshots",
        {
            "market_external_id": "m",
            "outcome_external_id": "t",
            "received_at": NOW + timedelta(microseconds=3),
            "received_monotonic_ns": 40,
            "snapshot_type": "reconciliation",
            "is_reconciliation": True,
            "bids": [["0.01", "1"]],
            "asks": [["0.99", "1"]],
        },
    )
    book = replay_book([snapshot, unordered_rest], [update_bid, delete_ask])
    assert book.best_bid == Decimal("0.15")
    assert book.bids[Decimal("0.15")] == Decimal("6.2500")
    assert Decimal("0.20") not in book.asks


@pytest.mark.asyncio
async def test_writer_archives_only_minimal_permanent_tier_representations() -> None:
    writer, archive, manager = route_writer(CollectionTier.FULL_L2)
    observation = WriteItem(
        "microstructure_observations",
        {"market_external_id": "m", "observed_at": NOW, "best_bid": "0.4"},
    )
    routed = await writer._route(observation)
    assert routed is not None
    assert archive.records == []  # FULL_L2 observations are replay-derived hot rows.

    manager.tier = CollectionTier.SAMPLED
    await writer._route(observation)
    assert [record.stream for record in archive.records] == [
        "microstructure_observations"
    ]

    manager.tier = CollectionTier.METADATA_ONLY
    closing = WriteItem(
        "orderbook_snapshots",
        {
            "market_external_id": "m",
            "outcome_external_id": "t",
            "snapshot_type": "closing",
            "archive_only": True,
            "received_at": NOW,
            "bids": [],
            "asks": [],
        },
    )
    assert await writer._route(closing) is None
    assert archive.records[-1].stream == "orderbook_snapshots"


def test_raw_websocket_policy_keeps_errors_and_about_point_one_percent() -> None:
    writer, _archive, _manager = route_writer(CollectionTier.FULL_L2)
    assert writer._retain_raw_ws(
        {"channel": "market", "message_type": "malformed_json", "payload": "{"},
        CollectionTier.FULL_L2,
    )
    retained = sum(
        writer._retain_raw_ws(
            {
                "channel": "market",
                "message_type": "book",
                "payload": {"sample": index},
            },
            CollectionTier.FULL_L2,
        )
        for index in range(100_000)
    )
    assert 60 <= retained <= 140


def test_sampled_observations_are_change_driven_with_sparse_heartbeat() -> None:
    manager = FakeTierManager(CollectionTier.SAMPLED)
    socket = PolymarketMarketWebSocket(
        url="wss://example.invalid",
        writer=None,  # type: ignore[arg-type]
        database=None,  # type: ignore[arg-type]
        metrics=None,  # type: ignore[arg-type]
        tier_manager=manager,  # type: ignore[arg-type]
    )
    book = OrderBook()
    book.reset([["0.4", "10"]], [["0.6", "12"]])
    socket.books["t"] = book
    socket.last_market_for_asset["t"] = "m"

    first = socket.market_snapshot_items(
        full_l2_interval_seconds=5,
        sampled_interval_seconds=30,
        sampled_heartbeat_seconds=900,
    )
    assert first[0].data["observation_kind"] == "change"
    assert socket.market_snapshot_items(
        full_l2_interval_seconds=5,
        sampled_interval_seconds=30,
        sampled_heartbeat_seconds=900,
    ) == []

    socket._last_observation_at["t"] = utc_now() - timedelta(seconds=901)
    heartbeat = socket.market_snapshot_items(
        full_l2_interval_seconds=5,
        sampled_interval_seconds=30,
        sampled_heartbeat_seconds=900,
    )
    assert heartbeat[0].data["observation_kind"] == "heartbeat"

    socket._last_observation_at["t"] = utc_now() - timedelta(seconds=31)
    book.apply_absolute("buy", Decimal("0.45"), Decimal("3"))
    changed = socket.market_snapshot_items(
        full_l2_interval_seconds=5,
        sampled_interval_seconds=30,
        sampled_heartbeat_seconds=900,
    )
    assert changed[0].data["observation_kind"] == "change"


def test_crossed_replay_intermediate_is_not_emitted_as_observation() -> None:
    manager = FakeTierManager(CollectionTier.SAMPLED)
    socket = PolymarketMarketWebSocket(
        url="wss://example.invalid",
        writer=None,  # type: ignore[arg-type]
        database=None,  # type: ignore[arg-type]
        metrics=None,  # type: ignore[arg-type]
        tier_manager=manager,  # type: ignore[arg-type]
    )
    book = OrderBook()
    book.reset([["0.55", "8"]], [["0.54", "15"]])
    socket.books["t"] = book
    socket.last_market_for_asset["t"] = "m"

    assert socket.market_snapshot_items(
        full_l2_interval_seconds=5,
        sampled_interval_seconds=30,
        sampled_heartbeat_seconds=900,
    ) == []


@pytest.mark.asyncio
async def test_closed_market_retention_requires_archive_finalization_and_grace() -> None:
    class Cursor:
        def __init__(self, rowcount: int) -> None:
            self.rowcount = rowcount

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        async def execute(self, query: str, params: tuple[Any, ...]) -> Cursor:
            self.calls.append((query, params))
            return Cursor(len(self.calls))

    class Context:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        async def __aenter__(self) -> Connection:
            return self.connection

        async def __aexit__(self, *_: Any) -> None:
            return None

    class Pool:
        def __init__(self, connection: Connection) -> None:
            self.connection_value = connection

        def connection(self) -> Context:
            return Context(self.connection_value)

    connection = Connection()
    database = Database(Settings(closed_market_hot_state_grace_hours=36))
    database.pool = Pool(connection)  # type: ignore[assignment]

    deleted = await database.apply_retention()

    assert deleted == {
        "reference_price_updates": 1,
        "microstructure_observations": 2,
        "closed_market_current_orderbooks": 3,
        "closed_market_tiers": 4,
    }
    eviction_sql, eviction_params = connection.calls[2]
    assert "JOIN market_archive_finalizations" in eviction_sql
    assert "NOT market.is_active OR NOT market.is_tradable" in eviction_sql
    assert eviction_params == ("36 hours",)
    tier_sql, tier_params = connection.calls[3]
    assert "market_archive_finalizations" in tier_sql
    assert "NOT EXISTS" in tier_sql
    assert tier_params == ("36 hours",)
