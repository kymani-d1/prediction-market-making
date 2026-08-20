from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.common.types import MarketCandidate
from prediction_collector.jobs.live import (
    LiveCollector,
    _confirmed_current_subscriptions,
)
from prediction_collector.tiering import TierManager


def candidate(external_id: str) -> MarketCandidate:
    return MarketCandidate(
        exchange="polymarket",
        external_id=external_id,
        ticker=external_id,
        status="active",
        active=True,
        tradable=True,
        accepting_orders=True,
        enable_order_book=True,
        liquidity=Decimal("100"),
        outcome_token_ids=(f"{external_id}-YES", f"{external_id}-NO"),
    )


def tier_manager() -> TierManager:
    return TierManager(
        full_l2_max_markets=0,
        sampled_max_markets=0,
        full_l2_min_score=Decimal("1000"),
        full_l2_min_liquidity=Decimal("1000"),
        full_l2_min_recent_trades=2,
        full_l2_min_book_updates=100,
    )


def capped_tier_manager() -> TierManager:
    return TierManager(
        full_l2_max_markets=1,
        sampled_max_markets=1,
        full_l2_min_score=Decimal("0"),
        full_l2_min_liquidity=Decimal("0"),
        full_l2_min_recent_trades=0,
        full_l2_min_book_updates=0,
        sampled_promotion_score=Decimal("0"),
    )


@pytest.mark.asyncio
async def test_incremental_discovery_retains_every_page_and_subscribes_before_completion() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector._last_discovery = []
    collector._selection_lock = asyncio.Lock()
    collector.tier_manager = tier_manager()
    collector.run_id = 1
    collector.coverage = type(
        "Coverage",
        (),
        {"selection": None, "confirmed_subscribed": 0},
    )()
    observed: list[list[str]] = []

    async def apply(candidates: list[MarketCandidate], **_: Any) -> None:
        observed.append(sorted(item.external_id for item in candidates))

    collector._persist_and_apply_tiers = apply  # type: ignore[method-assign]
    await collector._merge_discovery_page([candidate("A"), candidate("B")])
    await collector._merge_discovery_page([candidate("C")])
    assert observed == [["A", "B"], ["A", "B", "C"]]


@pytest.mark.asyncio
async def test_incremental_discovery_stops_rotating_shards_after_caps_are_full() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector._last_discovery = []
    collector._selection_lock = asyncio.Lock()
    collector.tier_manager = capped_tier_manager()
    collector.run_id = 1
    collector.coverage = type(
        "Coverage",
        (),
        {"selection": None, "confirmed_subscribed": 0},
    )()
    observed: list[list[str]] = []

    async def apply(candidates: list[MarketCandidate], **_: Any) -> None:
        observed.append(sorted(item.external_id for item in candidates))
        collector.tier_manager.evaluate(candidates)

    collector._persist_and_apply_tiers = apply  # type: ignore[method-assign]
    await collector._merge_discovery_page([candidate("A"), candidate("B")])
    await collector._merge_discovery_page([candidate("C")])
    assert sorted(item.external_id for item in collector._last_discovery) == [
        "A", "B", "C"
    ]
    assert observed == [["A", "B"]]


def test_confirmed_subscriptions_are_intersected_with_current_desired_tiers() -> None:
    selected = [candidate("still-selected")]
    assert _confirmed_current_subscriptions(
        selected,
        {
            ("polymarket", "still-selected"),
            ("polymarket", "demoted"),
        },
    ) == {("polymarket", "still-selected")}


@pytest.mark.asyncio
async def test_one_shot_absent_reconciliation_may_complete_normally() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.reconciliation_task = None
    collector._task_failure = asyncio.get_running_loop().create_future()

    class Service:
        async def reconcile_absent_live(self, _: list[MarketCandidate]) -> None:
            return None

    collector.polymarket_service = Service()  # type: ignore[assignment]
    collector._schedule_absent_reconciliation([candidate("A")])
    assert collector.reconciliation_task is not None
    await collector.reconciliation_task
    assert not collector._task_failure.done()


class RunDatabase:
    def __init__(self) -> None:
        self.finished: list[dict[str, Any]] = []

    async def start_run(self, *_: Any) -> int:
        return 1

    async def finish_run(self, run_id: int, **value: Any) -> None:
        self.finished.append({"run_id": run_id, **value})


class RunWriter:
    task = None
    archive = None
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
    await collector.run()
    assert started.is_set()
    assert collector.database.finished[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_supervised_task_normal_return_is_reported_as_unexpected(
    caplog: pytest.LogCaptureFixture,
) -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.stop = asyncio.Event()
    collector._task_failure = asyncio.get_running_loop().create_future()
    caplog.set_level("ERROR", logger="prediction_collector.jobs.live")

    task = collector._create_watched_task(
        asyncio.sleep(0), name="unexpected-return"
    )
    await task
    await asyncio.sleep(0)

    assert collector._task_failure.done()
    assert collector._task_failure.result() == ("unexpected-return", None)
    assert "Live collector supervised task exited" in caplog.text


def test_discovery_diagnostics_are_bounded_stage_aggregates() -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector._discovery_cycle = 3
    collector._discovery_page_count = 25
    collector._last_discovery = [candidate("A")]
    collector.discovery_state = "discovering"
    collector._discovery_stage_timings = {}

    collector._record_discovery_stage("crawl", 1.25)
    collector._record_discovery_stage("crawl", 0.75)

    diagnostics = collector._discovery_diagnostics()
    assert diagnostics["discovery_cycle"] == 3
    assert diagnostics["discovery_pages"] == 25
    assert diagnostics["retained_candidates"] == 1
    assert diagnostics["discovery_stages"]["crawl"] == {
        "count": 2,
        "seconds_total": 2.0,
        "seconds_max": 1.25,
        "seconds_last": 0.75,
    }
    assert "pid" in diagnostics["process_memory"]


@pytest.mark.asyncio
async def test_large_tier_evaluation_is_dispatched_off_the_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.tier_manager = tier_manager()
    dispatched: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []

    async def to_thread(
        function: Any, *args: Any, **kwargs: Any
    ) -> Any:
        dispatched.append((function, args, kwargs))
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", to_thread)

    assignments = await collector._evaluate_tiers_off_loop([candidate("A")])

    assert dispatched == [
        (
            collector.tier_manager.evaluate,
            ([candidate("A")],),
            {"retain_metadata_assignments": False},
        )
    ]
    assert [assignment.market.external_id for assignment in assignments] == ["A"]


@pytest.mark.asyncio
async def test_discovery_persists_and_applies_one_shared_tier_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector._selection_lock = asyncio.Lock()
    collector._discovery_stage_timings = {}
    collector.discovery_state = "discovering"
    collector.run_id = 1
    collector.coverage = type("Coverage", (), {"confirmed_subscribed": 0})()
    collector.tier_manager = capped_tier_manager()
    original_evaluate = collector.tier_manager.evaluate
    evaluations = 0

    def evaluate(*args: Any, **kwargs: Any) -> Any:
        nonlocal evaluations
        evaluations += 1
        return original_evaluate(*args, **kwargs)

    monkeypatch.setattr(collector.tier_manager, "evaluate", evaluate)
    persisted: list[str] = []

    class Service:
        async def persist_live_candidates(
            self, values: list[MarketCandidate]
        ) -> None:
            persisted.extend(item.external_id for item in values)

    collector.polymarket_service = Service()  # type: ignore[assignment]
    collector._reconcile_market_shards = (  # type: ignore[method-assign]
        lambda _: asyncio.sleep(0)
    )

    await collector._persist_and_apply_tiers(
        [candidate("A"), candidate("B"), candidate("C")],
        persist=False,
        log_summary=False,
        record_stages=True,
    )

    assert evaluations == 1
    assert sorted(persisted) == ["A", "B"]
    assert set(collector._discovery_stage_timings) == {
        "evaluate_tiers",
        "persist_selected",
        "apply_tiers",
    }
