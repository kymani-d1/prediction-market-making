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
    collector._persist_selected_candidates = (  # type: ignore[method-assign]
        lambda _: asyncio.sleep(0)
    )
    observed: list[list[str]] = []

    async def apply(candidates: list[MarketCandidate], **_: Any) -> None:
        observed.append(sorted(item.external_id for item in candidates))

    collector._apply_tiers = apply  # type: ignore[method-assign]
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
    collector._persist_selected_candidates = (  # type: ignore[method-assign]
        lambda _: asyncio.sleep(0)
    )
    observed: list[list[str]] = []

    async def apply(candidates: list[MarketCandidate], **_: Any) -> None:
        observed.append(sorted(item.external_id for item in candidates))
        collector.tier_manager.evaluate(candidates)

    collector._apply_tiers = apply  # type: ignore[method-assign]
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
