from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.common.http import _http_date_timestamp
from prediction_collector.common.types import LiveSelection, MarketCandidate
from prediction_collector.database import (
    Database,
    _fee_configuration_digest,
    _market_metadata_digest,
    _preserve_newer_market_state,
)
from prediction_collector.jobs.live import (
    LiveCollector,
    LiveCoverageState,
    _subscription_fingerprints,
)
from prediction_collector.kalshi.service import KalshiService
from prediction_collector.kalshi.websocket import KalshiWebSocket, _recovery_complete


def _candidate(*, tokens: tuple[str, ...]) -> MarketCandidate:
    return MarketCandidate(
        exchange="polymarket",
        external_id="CONDITION-A",
        ticker=None,
        status="active",
        active=True,
        tradable=True,
        outcome_token_ids=tokens,
    )


def test_subscription_fingerprint_detects_token_set_changes() -> None:
    before = _subscription_fingerprints([_candidate(tokens=("YES-A", "NO-A"))])
    reordered = _subscription_fingerprints([_candidate(tokens=("NO-A", "YES-A"))])
    augmented = _subscription_fingerprints(
        [_candidate(tokens=("YES-A", "NO-A", "NEW-OUTCOME"))]
    )

    assert before == reordered
    assert before != augmented


def test_stale_rest_metadata_cannot_regress_newer_lifecycle_state() -> None:
    lifecycle_time = datetime(2026, 8, 11, 14, 0, 2, tzinfo=UTC)
    rest_time = datetime(2026, 8, 11, 14, 0, 1, tzinfo=UTC)
    current = {
        "status": "resolved",
        "is_active": False,
        "is_tradable": False,
        "accepting_orders": False,
        "question": "Current question",
        "subtitle": None,
        "description": "Current description",
        "rules": "Current rules",
        "open_time": None,
        "close_time": lifecycle_time,
        "settlement_time": lifecycle_time,
        "result": "yes",
        "settlement_value": Decimal("1"),
        "tick_size": Decimal("0.01"),
        "fee_rate": Decimal("0.003"),
        "price_level_structure": {"type": "linear"},
        "metadata_source_timestamp": None,
        "metadata_exchange_timestamp": lifecycle_time,
        "metadata_observation_timestamp": lifecycle_time,
        "metadata_exchange_timestamp_is_transport": False,
        "metadata_resolution_source": "official",
        "raw_data": {"_latest_lifecycle_event": {"type": "market_resolved"}},
    }
    stale_rest = {
        "status": "active",
        "is_active": True,
        "is_tradable": True,
        "accepting_orders": True,
        "question": "Stale question",
        "source_timestamp": rest_time,
        "exchange_timestamp": datetime(2026, 8, 11, 14, 0, 3, tzinfo=UTC),
        "raw_data": {"active": True},
    }

    merged, was_stale = _preserve_newer_market_state(stale_rest, current)

    assert was_stale is True
    assert merged["status"] == "resolved"
    assert merged["is_active"] is False
    assert merged["is_tradable"] is False
    assert merged["result"] == "yes"
    assert merged["source_timestamp"] is None
    assert merged["exchange_timestamp"] == lifecycle_time
    assert merged["raw_data"]["active"] is True
    assert merged["raw_data"]["_latest_lifecycle_event"] == {
        "type": "market_resolved"
    }


def test_no_timestamp_lifecycle_uses_local_observation_watermark() -> None:
    request_started = datetime(2026, 8, 11, 14, 0, 1, tzinfo=UTC)
    lifecycle_received = datetime(2026, 8, 11, 14, 0, 2, tzinfo=UTC)
    http_date = datetime(2026, 8, 11, 14, 0, 3, tzinfo=UTC)
    current = {
        "status": "finalized",
        "is_active": False,
        "is_tradable": False,
        "accepting_orders": False,
        "metadata_source_timestamp": None,
        "metadata_exchange_timestamp": None,
        "metadata_exchange_timestamp_is_transport": False,
        "metadata_observation_timestamp": lifecycle_received,
        "raw_data": {},
    }
    in_flight_rest = {
        "status": "active",
        "is_active": True,
        "is_tradable": True,
        "accepting_orders": True,
        "source_timestamp": None,
        "exchange_timestamp": http_date,
        "exchange_timestamp_is_transport": True,
        "observed_at": request_started,
        "raw_data": {},
    }

    merged, was_stale = _preserve_newer_market_state(in_flight_rest, current)

    assert was_stale is True
    assert merged["status"] == "finalized"
    assert merged["is_active"] is False


def test_metadata_digest_ignores_market_metrics_but_versions_contract_changes() -> None:
    base = {
        "status": "active",
        "rules": "Rule A",
        "tick_size": Decimal("0.01"),
        "volume": Decimal("10"),
        "volume_24h": Decimal("2"),
        "liquidity": Decimal("5"),
        "raw_data": {
            "volume": "10",
            "volume24hr": "2",
            "bestBid": "0.40",
            "lastTradePrice": "0.42",
            "clobTokenIds": ["YES", "NO"],
        },
    }
    metric_change = {
        **base,
        "volume": Decimal("50"),
        "volume_24h": Decimal("9"),
        "liquidity": Decimal("12"),
        "raw_data": {
            **base["raw_data"],
            "volume": "50",
            "volume24hr": "9",
            "bestBid": "0.48",
            "lastTradePrice": "0.49",
        },
    }

    assert _market_metadata_digest(base) == _market_metadata_digest(metric_change)
    assert _market_metadata_digest(base) != _market_metadata_digest(
        {**base, "status": "resolved"}
    )
    assert _market_metadata_digest(base) != _market_metadata_digest(
        {**base, "rules": "Rule B"}
    )
    assert _market_metadata_digest(base) != _market_metadata_digest(
        {**base, "tick_size": Decimal("0.001")}
    )
    assert _market_metadata_digest(base) != _market_metadata_digest(
        {**base, "structural_metadata": {"floor_strike": "102500"}}
    )


def test_kalshi_rest_and_ws_fee_wrappers_share_semantic_digest() -> None:
    common = {
        "semantic_configuration": {"fee_type_override": "quadratic"},
        "maker_rate": None,
        "taker_rate": None,
        "fee_rate": None,
        "multiplier": Decimal("1.25"),
        "fixed_fee": None,
        "currency": "USD",
    }
    rest_digest = _fee_configuration_digest(
        configuration={
            "id": "REST-ROW",
            "event_ticker": "EVENT-A",
            "fee_type_override": "quadratic",
            "fee_multiplier_override": 1.25,
            "scheduled_ts": 1_800_000_000,
        },
        **common,
    )
    ws_digest = _fee_configuration_digest(
        configuration={
            "type": "market_lifecycle_v2",
            "msg": {
                "event_ticker": "EVENT-A",
                "fee_type_override": "quadratic",
                "fee_multiplier_override": 1.25,
            },
        },
        **common,
    )

    assert rest_digest == ws_digest


class _HistoryConnection:
    def __init__(self, current: dict[str, Any] | None) -> None:
        self.current = current
        self.executions: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, parameters: tuple[Any, ...]) -> _Cursor:
        compact = " ".join(sql.split())
        self.executions.append((compact, parameters))
        if compact.startswith("SELECT id, content_hash"):
            return _Cursor(self.current)
        return _Cursor(None)


@pytest.mark.asyncio
async def test_history_insert_binds_non_null_observation_separately_from_source() -> None:
    database = Database.__new__(Database)
    connection = _HistoryConnection(None)
    observed_at = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)
    await database._record_market_metadata_history(
        connection,
        market_id=9,
        value={
            "exchange": "kalshi",
            "status": "active",
            "is_active": True,
            "is_tradable": True,
            "source_timestamp": None,
            "exchange_timestamp": observed_at,
            "exchange_timestamp_is_transport": True,
            "observed_at": observed_at,
            "raw_data": {},
        },
    )

    insert = next(
        item for item in connection.executions if item[0].startswith("INSERT INTO")
    )
    assert "observation_timestamp, source_timestamp, exchange_timestamp" in insert[0]
    assert insert[1][3] == observed_at
    assert insert[1][4] is None
    assert insert[1][5] == observed_at
    assert insert[1][6] is True


@pytest.mark.asyncio
async def test_identical_history_observation_advances_ordering_watermarks() -> None:
    database = Database.__new__(Database)
    observed_at = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)
    value = {
        "exchange": "polymarket",
        "status": "active",
        "observed_at": observed_at,
        "source_timestamp": observed_at,
        "exchange_timestamp": None,
        "raw_data": {},
    }
    connection = _HistoryConnection(
        {"id": 17, "content_hash": _market_metadata_digest(value)}
    )

    await database._record_market_metadata_history(
        connection,
        market_id=9,
        value=value,
    )

    update = next(
        item
        for item in connection.executions
        if item[0].startswith("UPDATE market_metadata_history SET")
    )
    assert "observation_timestamp = GREATEST" in update[0]
    assert "source_timestamp = CASE" in update[0]
    assert update[1][0] == observed_at
    assert update[1][1:4] == (observed_at, observed_at, observed_at)


def test_http_date_header_is_parsed_as_utc() -> None:
    assert _http_date_timestamp("Tue, 11 Aug 2026 12:00:00 GMT") == datetime(
        2026, 8, 11, 12, 0, tzinfo=UTC
    )


class _Cursor:
    def __init__(self, row: dict[str, Any] | None = None, rowcount: int = 1) -> None:
        self.row = row
        self.rowcount = rowcount

    async def fetchone(self) -> dict[str, Any] | None:
        return self.row


class _EventConnection:
    def __init__(self) -> None:
        self.sql = ""
        self.parameters: tuple[Any, ...] = ()

    async def execute(self, sql: str, parameters: tuple[Any, ...]) -> _Cursor:
        self.sql = " ".join(sql.split())
        self.parameters = parameters
        return _Cursor({"id": 7})


class _ConnectionContext(AbstractAsyncContextManager[_EventConnection]):
    def __init__(self, connection: _EventConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _EventConnection:
        return self.connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _Pool:
    def __init__(self, connection: _EventConnection) -> None:
        self._connection = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


class _Metrics:
    async def rows(self, table: str, count: int = 1) -> None:
        assert table == "events"


@pytest.mark.asyncio
async def test_sparse_event_upsert_preserves_existing_status_on_conflict() -> None:
    connection = _EventConnection()
    database = Database.__new__(Database)
    database.pool = _Pool(connection)  # type: ignore[assignment]
    database.metrics = _Metrics()  # type: ignore[assignment]

    await database.upsert_event(
        {
            "exchange": "kalshi",
            "external_id": "EVENT-A",
            "title": "Created event",
            "status": None,
            "raw_data": {"event_type": "created"},
        }
    )

    assert connection.parameters[9] == "unknown"
    assert connection.parameters[-1] is False
    assert "status = CASE WHEN %s THEN EXCLUDED.status ELSE events.status END" in (
        connection.sql
    )


class _Writer:
    def __init__(self) -> None:
        self.items: list[Any] = []

    async def put(self, item: Any) -> None:
        self.items.append(item)


class _GapDatabase:
    def __init__(self) -> None:
        self.resolved: list[tuple[int, str]] = []

    async def resolve_gap(
        self, gap_id: int, *, action: str, recovery_snapshot_id: int | None = None
    ) -> None:
        self.resolved.append((gap_id, action))


@pytest.mark.asyncio
async def test_reconnect_snapshot_resolves_all_persisted_sequence_gaps() -> None:
    socket = KalshiWebSocket.__new__(KalshiWebSocket)
    socket.books = {}
    socket.writer = _Writer()  # type: ignore[assignment]
    socket.database = _GapDatabase()  # type: ignore[assignment]
    socket._open_sequence_gaps = {"MARKET-A": [11, 12]}

    await socket._handle(
        {
            "type": "orderbook_snapshot",
            "seq": 50,
            "msg": {
                "market_ticker": "MARKET-A",
                "yes_dollars_fp": [["0.40", "8"]],
                "no_dollars_fp": [["0.45", "2"]],
            },
        },
        connection_id=5,
        received_at=datetime(2026, 8, 11, 14, 0, tzinfo=UTC),
        monotonic_ns=123,
        open_gaps=socket._open_sequence_gaps,
    )

    assert socket._open_sequence_gaps == {}
    assert socket.database.resolved == [
        (11, "websocket_snapshot_reset"),
        (12, "websocket_snapshot_reset"),
    ]
    assert socket.books["MARKET-A"].valid is True


@pytest.mark.asyncio
async def test_archived_out_of_order_delta_does_not_mutate_live_book() -> None:
    socket = KalshiWebSocket.__new__(KalshiWebSocket)
    socket.books = {}
    socket.writer = _Writer()  # type: ignore[assignment]
    socket.database = _GapDatabase()  # type: ignore[assignment]
    now = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)
    await socket._handle(
        {
            "type": "orderbook_snapshot",
            "seq": 50,
            "msg": {
                "market_ticker": "MARKET-A",
                "yes_dollars_fp": [["0.40", "8"]],
                "no_dollars_fp": [],
            },
        },
        connection_id=5,
        received_at=now,
        monotonic_ns=100,
        open_gaps={},
    )
    await socket._handle(
        {
            "type": "orderbook_delta",
            "seq": 51,
            "msg": {
                "market_ticker": "MARKET-A",
                "side": "yes",
                "price_dollars": "0.40",
                "delta_fp": "2",
            },
        },
        connection_id=5,
        received_at=now,
        monotonic_ns=101,
        open_gaps={},
    )
    assert socket.books["MARKET-A"].bids[Decimal("0.40")] == Decimal("10")
    assert socket.books["MARKET-A"].sequence == 51

    await socket._handle(
        {
            "type": "orderbook_delta",
            "seq": 51,
            "msg": {
                "market_ticker": "MARKET-A",
                "side": "yes",
                "price_dollars": "0.40",
                "delta_fp": "5",
            },
        },
        connection_id=5,
        received_at=now,
        monotonic_ns=102,
        open_gaps={},
        apply_orderbook_message=False,
    )

    assert socket.books["MARKET-A"].bids[Decimal("0.40")] == Decimal("10")
    assert socket.books["MARKET-A"].sequence == 51
    assert socket.writer.items[-1].data["event_type"] == (
        "out_of_order_delta_archived"
    )


@pytest.mark.asyncio
async def test_out_of_order_snapshot_is_archived_without_reset_or_gap_resolution() -> None:
    socket = KalshiWebSocket.__new__(KalshiWebSocket)
    socket.books = {}
    socket.writer = _Writer()  # type: ignore[assignment]
    socket.database = _GapDatabase()  # type: ignore[assignment]
    now = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)
    open_gaps = {"MARKET-A": [21]}
    await socket._handle(
        {
            "type": "orderbook_snapshot",
            "seq": 10,
            "msg": {
                "market_ticker": "MARKET-A",
                "yes_dollars_fp": [["0.40", "8"]],
                "no_dollars_fp": [],
            },
        },
        connection_id=5,
        received_at=now,
        monotonic_ns=200,
        open_gaps={},
    )
    await socket._handle(
        {
            "type": "orderbook_delta",
            "seq": 11,
            "msg": {
                "market_ticker": "MARKET-A",
                "side": "yes",
                "price_dollars": "0.40",
                "delta_fp": "2",
            },
        },
        connection_id=5,
        received_at=now,
        monotonic_ns=201,
        open_gaps={},
    )

    await socket._handle(
        {
            "type": "orderbook_snapshot",
            "seq": 10,
            "msg": {
                "market_ticker": "MARKET-A",
                "yes_dollars_fp": [["0.40", "1"]],
                "no_dollars_fp": [],
            },
        },
        connection_id=5,
        received_at=now,
        monotonic_ns=202,
        open_gaps=open_gaps,
        apply_orderbook_message=False,
    )

    assert socket.books["MARKET-A"].bids[Decimal("0.40")] == Decimal("10")
    assert socket.books["MARKET-A"].sequence == 11
    assert open_gaps == {"MARKET-A": [21]}
    assert socket.database.resolved == []
    assert socket.writer.items[-1].data["snapshot_type"] == (
        "out_of_order_archived"
    )


def test_kalshi_shard_recovery_requires_every_market_snapshot() -> None:
    options = {
        "tickers": ["A", "B"],
        "channels": ["orderbook_delta", "trade", "ticker"],
        "subscription_confirmed": True,
    }
    assert not _recovery_complete(
        **options,
        snapshots_seen=set(),
        message_type="trade",
    )
    assert not _recovery_complete(
        **options,
        snapshots_seen={"A"},
        message_type="orderbook_snapshot",
    )
    assert _recovery_complete(
        **options,
        snapshots_seen={"A", "B"},
        message_type="orderbook_snapshot",
    )


class _Result:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class _PolyRest:
    async def orderbook(self, token: str) -> _Result:
        return _Result(
            {
                "market": "CONDITION-A",
                "asset_id": token,
                "bids": [["0.40", "10"]],
                "asks": [["0.60", "10"]],
                "hash": "rest-hash",
            }
        )


@pytest.mark.asyncio
async def test_rest_reconciliation_archives_without_mutating_live_book() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.stop = asyncio.Event()
    collector.coverage = LiveCoverageState(
        selection=LiveSelection(
            discovered=1,
            active=1,
            tradable=1,
            subscribed=[_candidate(tokens=("YES-A",))],
            excluded=[],
        )
    )
    collector.polymarket_service = type("Service", (), {"rest": _PolyRest()})()
    collector.kalshi_service = None
    collector.writer = _Writer()  # type: ignore[assignment]
    live_book_marker = object()
    collector.polymarket_ws = type(
        "Socket", (), {"books": {"YES-A": live_book_marker}}
    )()

    await collector._reconcile_once()

    assert len(collector.writer.items) == 1
    assert collector.writer.items[0].data["is_reconciliation"] is True
    assert collector.polymarket_ws.books["YES-A"] is live_book_marker


class _KalshiCandleRest:
    def __init__(self) -> None:
        self.modes: list[bool] = []

    async def historical_cutoff(self) -> _Result:
        return _Result({"market_settled_ts": 1_775_000_000})

    async def candlesticks(self, *args: Any, historical: bool, **kwargs: Any) -> _Result:
        self.modes.append(historical)
        return _Result({"candlesticks": []})


class _KalshiCandleDatabase:
    def __init__(self, market: MarketCandidate) -> None:
        self.market = market
        self.gaps: list[dict[str, Any]] = []

    async def live_candidates(self, exchange: str) -> list[MarketCandidate]:
        assert exchange == "kalshi"
        return [self.market]

    async def record_gap(self, **value: Any) -> int:
        self.gaps.append(value)
        return len(self.gaps)


class _RunWriter(_Writer):
    run_id = 51


@pytest.mark.asyncio
async def test_inactive_post_cutoff_kalshi_market_uses_current_candle_endpoint() -> None:
    # Settled at 1_776_000_000, after the archive cutoff at 1_775_000_000.
    # Inactive does not mean archived.
    market = MarketCandidate(
        exchange="kalshi",
        external_id="KX-RECENT-CLOSED",
        ticker="KX-RECENT-CLOSED",
        status="closed",
        active=False,
        tradable=False,
        raw_data={
            "series_ticker": "KX-SERIES",
            "open_time": 1_775_500_000,
            "settlement_ts": 1_776_000_000,
        },
    )
    rest = _KalshiCandleRest()
    database = _KalshiCandleDatabase(market)
    service = KalshiService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=_RunWriter(),  # type: ignore[arg-type]
        store_raw_rest=False,
    )

    counts = await service.backfill_market_data()

    assert rest.modes == [False]
    assert counts == {"books": 0, "candles": 0, "errors": 0}
    assert database.gaps == []


class _ShardDatabase:
    def __init__(self) -> None:
        self.gaps: list[dict[str, Any]] = []

    async def record_gap(self, **value: Any) -> int:
        self.gaps.append(value)
        return len(self.gaps)


class _ShardPolySocket:
    def __init__(self) -> None:
        self.starts: list[dict[str, Any]] = []

    async def run(self, subscriptions: dict[str, str], **options: Any) -> None:
        self.starts.append(
            {"subscriptions": dict(subscriptions), "options": dict(options)}
        )
        await asyncio.Future()


@pytest.mark.asyncio
async def test_market_change_rotates_only_affected_subscription_shard() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.market_shards = {}
    collector._next_market_shard_id = {"polymarket": 1, "kalshi": 1}
    collector.database = _ShardDatabase()  # type: ignore[assignment]
    collector.polymarket_ws = _ShardPolySocket()  # type: ignore[assignment]
    collector.kalshi_ws = None
    collector.run_id = 77
    collector.stop = asyncio.Event()
    collector._task_failure = None

    await collector._reconcile_exchange_shards(
        "polymarket",
        {"A": "M1", "B": "M1", "C": "M2", "D": "M2"},
        2,
    )
    await asyncio.sleep(0)
    first_task = collector.market_shards[("polymarket", 1)].task
    stable_task = collector.market_shards[("polymarket", 2)].task

    await collector._reconcile_exchange_shards(
        "polymarket",
        {"A": "M1-RELINKED", "B": "M1", "C": "M2", "D": "M2"},
        2,
    )
    await asyncio.sleep(0)

    assert first_task.cancelled()
    assert collector.market_shards[("polymarket", 1)].task is not first_task
    assert collector.market_shards[("polymarket", 2)].task is stable_task
    assert not stable_task.done()
    assert len(collector.database.gaps) == 1
    assert collector.database.gaps[0]["gap_type"] == "planned_subscription_refresh"
    replacement = collector.polymarket_ws.starts[-1]
    assert replacement["subscriptions"]["A"] == "M1-RELINKED"
    assert replacement["options"]["recovery_gap_ids"] == (1,)

    collector.stop.set()
    await collector._stop_market_tasks()
