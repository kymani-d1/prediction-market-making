from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from prediction_collector.common.retry import RetryPolicy
from prediction_collector.common.types import MarketCandidate
from prediction_collector.config import Settings
from prediction_collector.jobs.live import LiveCollector


def candidate(exchange: str, external_id: str) -> MarketCandidate:
    return MarketCandidate(
        exchange=exchange,
        external_id=external_id,
        ticker=external_id,
        status="active",
        active=True,
        tradable=True,
        outcome_token_ids=(f"{external_id}-YES", f"{external_id}-NO"),
    )


class GapDatabase:
    def __init__(self) -> None:
        self.gaps: list[dict[str, Any]] = []
        self.resolved: list[tuple[int, str]] = []

    async def record_gap(self, **value: Any) -> int:
        self.gaps.append(value)
        return len(self.gaps)

    async def resolve_gap(self, gap_id: int, *, action: str) -> None:
        self.resolved.append((gap_id, action))


def discovery_collector(database: GapDatabase) -> LiveCollector:
    collector = LiveCollector.__new__(LiveCollector)
    collector.settings = Settings(
        metadata_sync_interval_seconds=300,
        max_live_markets=0,
    )
    collector.database = database  # type: ignore[assignment]
    collector.stop = asyncio.Event()
    collector.run_id = 91
    collector._last_discovery_by_exchange = {}
    collector._discovery_gaps = {}
    collector.discovery_state = {"polymarket": "pending", "kalshi": "pending"}
    collector._discovery_retry_policy = RetryPolicy(
        max_attempts=1,
        base_delay_seconds=0.01,
        max_delay_seconds=0.01,
        jitter_ratio=0,
    )
    collector._schedule_absent_reconciliation = lambda *args: None  # type: ignore[method-assign]
    return collector


class RecoveringService:
    def __init__(self, market: MarketCandidate) -> None:
        self.market = market
        self.calls = 0

    async def discover_live(self, *, reconcile_absent: bool, on_page: Any) -> list[MarketCandidate]:
        assert reconcile_absent is False
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("HTTP 503 GET https://gamma.test/events/keyset")
        await on_page([self.market])
        return [self.market]

    async def reconcile_absent_live(self, candidates: list[MarketCandidate]) -> None:
        return None


@pytest.mark.asyncio
async def test_initial_discovery_failure_retries_and_resolves_gap() -> None:
    database = GapDatabase()
    collector = discovery_collector(database)
    applied: list[tuple[list[str], bool]] = []

    async def apply(markets: list[MarketCandidate], **options: Any) -> None:
        applied.append(([market.external_id for market in markets], options["persist_decisions"]))
        if options["persist_decisions"]:
            collector.stop.set()

    collector._apply_selection = apply  # type: ignore[method-assign]
    service = RecoveringService(candidate("polymarket", "POLY-A"))

    await collector._exchange_discovery_loop("polymarket", service)  # type: ignore[arg-type]

    assert service.calls == 2
    assert database.gaps[0]["reconnect_reason"].startswith("RuntimeError: HTTP 503")
    assert database.resolved == [(1, "successful_complete_market_discovery")]
    assert collector.discovery_state["polymarket"] == "ready"
    assert applied == [(["POLY-A"], False), (["POLY-A"], True)]


class AlwaysFailingService:
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop
        self.call_times: list[float] = []

    async def discover_live(self, **_: Any) -> list[MarketCandidate]:
        self.call_times.append(time.monotonic())
        if len(self.call_times) == 4:
            self.stop.set()
        raise RuntimeError("temporary upstream outage")


@pytest.mark.asyncio
async def test_discovery_retry_is_bounded_and_does_not_busy_loop() -> None:
    collector = discovery_collector(GapDatabase())
    collector._apply_selection = lambda *args, **kwargs: None  # type: ignore[method-assign]
    service = AlwaysFailingService(collector.stop)

    await collector._exchange_discovery_loop("polymarket", service)  # type: ignore[arg-type]

    intervals = [
        later - earlier
        for earlier, later in zip(service.call_times, service.call_times[1:])
    ]
    assert len(service.call_times) == 4
    assert all(interval >= 0.008 for interval in intervals)
    assert all(interval < 0.1 for interval in intervals)


class HangingService:
    async def discover_live(self, **_: Any) -> list[MarketCandidate]:
        await asyncio.Future()


@pytest.mark.asyncio
async def test_exchange_discovery_isolation_starts_successful_exchange() -> None:
    collector = discovery_collector(GapDatabase())
    ready = asyncio.Event()

    async def apply(markets: list[MarketCandidate], **options: Any) -> None:
        if options["persist_decisions"] and any(
            market.exchange == "polymarket" for market in markets
        ):
            ready.set()

    collector._apply_selection = apply  # type: ignore[method-assign]
    polymarket = RecoveringService(candidate("polymarket", "POLY-A"))
    polymarket.calls = 1  # Succeed on its first call.

    poly_task = asyncio.create_task(
        collector._exchange_discovery_loop("polymarket", polymarket)  # type: ignore[arg-type]
    )
    kalshi_task = asyncio.create_task(
        collector._exchange_discovery_loop("kalshi", HangingService())  # type: ignore[arg-type]
    )
    await asyncio.wait_for(ready.wait(), timeout=1)

    assert collector.discovery_state["polymarket"] == "ready"
    assert collector.discovery_state["kalshi"] == "discovering"
    collector.stop.set()
    poly_task.cancel()
    kalshi_task.cancel()
    await asyncio.gather(poly_task, kalshi_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unrestricted_incremental_discovery_retains_every_page() -> None:
    collector = discovery_collector(GapDatabase())
    observed: list[list[str]] = []

    async def apply(markets: list[MarketCandidate], **_: Any) -> None:
        observed.append(sorted(market.external_id for market in markets))

    collector._apply_selection = apply  # type: ignore[method-assign]
    await collector._merge_discovery_page(
        "polymarket", [candidate("polymarket", "A"), candidate("polymarket", "B")]
    )
    await collector._merge_discovery_page(
        "polymarket", [candidate("polymarket", "C")]
    )

    assert observed[-1] == ["A", "B", "C"]


class RunDatabase:
    def __init__(self) -> None:
        self.finished: list[dict[str, Any]] = []

    async def start_run(self, *_: Any) -> int:
        return 1

    async def finish_run(self, run_id: int, **value: Any) -> None:
        self.finished.append({"run_id": run_id, **value})


class RunWriter:
    task = None
    failed_items = 0
    rows_written = 0
    run_id: int | None = None

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


@pytest.mark.asyncio
async def test_run_starts_background_feeds_without_initial_discovery_barrier() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.database = RunDatabase()  # type: ignore[assignment]
    collector.writer = RunWriter()  # type: ignore[assignment]
    collector.stop = asyncio.Event()
    collector.run_id = None
    collector.coverage = type("Coverage", (), {"metrics": lambda self: {}})()
    collector._task_failure = None
    started = asyncio.Event()

    async def start_background() -> None:
        started.set()
        collector.stop.set()

    collector._start_background_tasks = start_background  # type: ignore[method-assign]
    collector._shutdown_tasks = lambda: asyncio.sleep(0)  # type: ignore[method-assign]
    collector._refresh_selection = lambda **kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("run must not perform synchronous initial discovery")
    )
    collector._start_market_tasks = lambda: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("market tasks require discovered candidates")
    )

    await collector.run()

    assert started.is_set()
    assert collector.database.finished[0]["status"] == "completed"
