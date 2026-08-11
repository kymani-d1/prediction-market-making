from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from prediction_collector.config import Settings
from prediction_collector.common.types import MarketCandidate
from prediction_collector.jobs.backfill import run_polymarket_backfill
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.kalshi.service import KalshiService
from prediction_collector.polymarket.service import PolymarketService


@dataclass
class FakeResult:
    data: Any


class RewardsOnlyRest:
    def __init__(self) -> None:
        self.fee_rate_calls: list[str] = []
        self.reward_variants: list[bool] = []

    async def fee_rate(self, token_id: str) -> FakeResult:
        self.fee_rate_calls.append(token_id)
        raise AssertionError("live reward refresh must not request per-token fee rates")

    async def iter_current_rewards(self, *, sponsored: bool):  # type: ignore[no-untyped-def]
        self.reward_variants.append(sponsored)
        if not sponsored:
            yield (
                [
                    {
                        "condition_id": "CONDITION-A",
                        "rewards_min_size": "10",
                        "rewards_max_spread": "0.03",
                        "total_daily_rate": "25",
                        "rewards_config": [],
                    }
                ],
                FakeResult({}),
                None,
            )


class RewardsOnlyDatabase:
    def __init__(self) -> None:
        self.live_candidate_calls = 0
        self.incentives: list[dict[str, Any]] = []

    async def live_candidates(self, exchange: str):  # type: ignore[no-untyped-def]
        self.live_candidate_calls += 1
        raise AssertionError("fee-rate-disabled refresh must not scan every market")

    async def record_incentive_configuration(self, **kwargs: Any) -> bool:
        self.incentives.append(kwargs)
        return True


class FakeWriter:
    run_id = 41


@pytest.mark.asyncio
async def test_polymarket_live_economics_skips_fee_rate_storm_but_refreshes_rewards() -> None:
    rest = RewardsOnlyRest()
    database = RewardsOnlyDatabase()
    service = PolymarketService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=FakeWriter(),  # type: ignore[arg-type]
        store_raw_rest=False,
    )

    counts = await service.sync_fees_and_incentives(include_fee_rates=False)

    assert database.live_candidate_calls == 0
    assert rest.fee_rate_calls == []
    assert rest.reward_variants == [False, True]
    assert counts == {
        "markets_checked": 0,
        "fee_versions_inserted": 0,
        "reward_records_seen": 1,
        "reward_versions_inserted": 1,
        "errors": 0,
    }
    assert len(database.incentives) == 1
    assert database.incentives[0]["incentive_type"] == (
        "liquidity_reward_summary:standard"
    )


class FeeRateRest:
    def __init__(self) -> None:
        self.tokens: list[str] = []

    async def fee_rate(self, token_id: str) -> FakeResult:
        self.tokens.append(token_id)
        return FakeResult({"base_fee": 100})


class FeeRateDatabase:
    def __init__(self) -> None:
        self.fees: list[dict[str, Any]] = []

    async def live_candidates(self, exchange: str) -> list[MarketCandidate]:
        assert exchange == "polymarket"
        return [
            MarketCandidate(
                exchange="polymarket",
                external_id="LIVE",
                ticker=None,
                status="active",
                active=True,
                tradable=True,
                outcome_token_ids=("LIVE-YES", "LIVE-NO"),
            ),
            MarketCandidate(
                exchange="polymarket",
                external_id="CLOSED",
                ticker=None,
                status="closed",
                active=False,
                tradable=False,
                outcome_token_ids=("CLOSED-YES",),
            ),
            MarketCandidate(
                exchange="polymarket",
                external_id="PAUSED",
                ticker=None,
                status="active",
                active=True,
                tradable=False,
                outcome_token_ids=("PAUSED-YES",),
            ),
        ]

    async def record_fee_configuration(self, **kwargs: Any) -> bool:
        self.fees.append(kwargs)
        return True


@pytest.mark.asyncio
async def test_slow_fee_refresh_scans_all_live_tokens_without_refreshing_rewards() -> None:
    rest = FeeRateRest()
    database = FeeRateDatabase()
    service = PolymarketService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=FakeWriter(),  # type: ignore[arg-type]
        store_raw_rest=False,
    )

    counts = await service.sync_fees_and_incentives(
        include_fee_rates=True,
        include_rewards=False,
        fee_rate_live_only=True,
    )

    assert rest.tokens == ["LIVE-YES", "LIVE-NO"]
    assert counts == {
        "markets_checked": 1,
        "fee_versions_inserted": 1,
        "reward_records_seen": 0,
        "reward_versions_inserted": 0,
        "errors": 0,
    }
    assert len(database.fees) == 1
    assert database.fees[0]["scope_external_id"] == "LIVE"


class KalshiEconomicsRest:
    def __init__(self) -> None:
        self.series_calls = 0
        self.fee_series: list[str] = []

    async def iter_series(self):  # type: ignore[no-untyped-def]
        self.series_calls += 1
        yield ([{"ticker": "SERIES-A", "title": "Series A"}], FakeResult({}), None)

    async def iter_series_fee_changes(self, ticker: str):  # type: ignore[no-untyped-def]
        self.fee_series.append(ticker)
        if False:
            yield None

    async def iter_event_fee_changes(self):  # type: ignore[no-untyped-def]
        if False:
            yield None

    async def iter_incentive_programs(self):  # type: ignore[no-untyped-def]
        if False:
            yield None


class KalshiEconomicsDatabase:
    def __init__(self) -> None:
        self.series: list[dict[str, Any]] = []

    async def upsert_series(self, value: dict[str, Any]) -> int:
        self.series.append(value)
        return len(self.series)


@pytest.mark.asyncio
async def test_live_kalshi_economics_refreshes_series_and_upserts_new_ones_once() -> None:
    rest = KalshiEconomicsRest()
    database = KalshiEconomicsDatabase()
    service = KalshiService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=FakeWriter(),  # type: ignore[arg-type]
        store_raw_rest=False,
    )

    first = await service.sync_fees_and_incentives()
    second = await service.sync_fees_and_incentives()

    assert first["series_discovered"] == 1
    assert second["series_discovered"] == 0
    assert rest.series_calls == 2
    assert rest.fee_series == ["SERIES-A", "SERIES-A"]
    assert database.series[0]["external_id"] == "SERIES-A"


class JoinedQueue:
    async def join(self) -> None:
        return None


class BackfillWriter:
    def __init__(self) -> None:
        self.queue = JoinedQueue()
        self.failed_items = 0
        self.rows_written = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


class BackfillService:
    def __init__(self) -> None:
        self.include_fee_rates: list[bool] = []

    async def sync_metadata(self, *, include_closed: bool) -> dict[str, int]:
        assert include_closed is True
        return {}

    async def sync_fees_and_incentives(
        self,
        *,
        include_fee_rates: bool,
        include_rewards: bool,
        fee_rate_live_only: bool,
    ) -> dict[str, int]:
        assert include_rewards is True
        assert fee_rate_live_only is False
        self.include_fee_rates.append(include_fee_rates)
        return {"errors": 0}

    async def backfill_trades(self) -> dict[str, int]:
        return {}

    async def backfill_comments(self) -> dict[str, int]:
        return {}

    async def backfill_market_data(self) -> dict[str, int]:
        return {}


@pytest.mark.asyncio
async def test_explicit_polymarket_backfill_keeps_comprehensive_fee_rates() -> None:
    service = BackfillService()

    result = await run_polymarket_backfill(
        service,  # type: ignore[arg-type]
        BackfillWriter(),  # type: ignore[arg-type]
    )

    assert service.include_fee_rates == [True]
    assert result.status == "completed"


class FailingPolymarketEconomics:
    def __init__(self) -> None:
        self.options: list[dict[str, bool]] = []

    async def sync_fees_and_incentives(self, **options: bool):
        self.options.append(options)
        raise RuntimeError("polymarket economics unavailable")


class SuccessfulKalshiEconomics:
    def __init__(self) -> None:
        self.calls = 0

    async def sync_fees_and_incentives(self) -> dict[str, int]:
        self.calls += 1
        return {"updated": 2}


class SuccessfulPolymarketEconomics:
    def __init__(self) -> None:
        self.options: list[dict[str, bool]] = []

    async def sync_fees_and_incentives(self, **options: bool) -> dict[str, int]:
        self.options.append(options)
        return {"updated": 2}


class GapDatabase:
    def __init__(self) -> None:
        self.gaps: list[dict[str, Any]] = []
        self.resolved: list[tuple[int, str]] = []

    async def record_gap(self, **kwargs: Any) -> int:
        self.gaps.append(kwargs)
        return len(self.gaps)

    async def resolve_gap(self, gap_id: int, *, action: str) -> None:
        self.resolved.append((gap_id, action))


def collector_settings() -> Settings:
    return Settings(
        polymarket_rtds_enabled=False,
        polymarket_sports_enabled=False,
        economics_sync_interval_seconds=600,
    )


@pytest.mark.asyncio
async def test_live_economics_failure_is_isolated_and_recorded_as_data_gap() -> None:
    polymarket = FailingPolymarketEconomics()
    kalshi = SuccessfulKalshiEconomics()
    database = GapDatabase()
    collector = LiveCollector(
        settings=collector_settings(),
        database=database,  # type: ignore[arg-type]
        writer=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        polymarket_service=polymarket,  # type: ignore[arg-type]
        kalshi_service=kalshi,  # type: ignore[arg-type]
    )
    collector.run_id = 91

    await collector._sync_economics_once()

    assert polymarket.options == [
        {"include_fee_rates": False, "include_rewards": True}
    ]
    assert kalshi.calls == 1
    assert len(database.gaps) == 1
    gap = database.gaps[0]
    assert gap["run_id"] == 91
    assert gap["exchange"] == "polymarket"
    assert gap["channel"] == "rest:economics_refresh"
    assert gap["gap_type"] == "economics_refresh_failed"
    assert gap["details"] == {
        "interval_seconds": 600,
        "fee_rates_included": False,
        "rewards_included": True,
        "fee_rate_live_only": None,
    }
    assert "RuntimeError: polymarket economics unavailable" in gap["reconnect_reason"]


class RecoveringPolymarketEconomics:
    def __init__(self) -> None:
        self.calls = 0

    async def sync_fees_and_incentives(self, **options: bool) -> dict[str, int]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary economics outage")
        return {"updated": 1}


@pytest.mark.asyncio
async def test_successful_economics_refresh_resolves_prior_failure_gap() -> None:
    service = RecoveringPolymarketEconomics()
    database = GapDatabase()
    collector = LiveCollector(
        settings=collector_settings(),
        database=database,  # type: ignore[arg-type]
        writer=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        polymarket_service=service,  # type: ignore[arg-type]
        kalshi_service=None,
    )
    collector.run_id = 92

    await collector._sync_economics_once()
    await collector._sync_economics_once()

    assert len(database.gaps) == 1
    assert database.resolved == [(1, "successful_economics_refresh")]
    assert collector._economics_gaps == {}


@pytest.mark.asyncio
async def test_slow_live_fee_refresh_uses_fee_only_live_candidate_mode() -> None:
    polymarket = SuccessfulPolymarketEconomics()
    collector = LiveCollector(
        settings=collector_settings(),
        database=GapDatabase(),  # type: ignore[arg-type]
        writer=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        polymarket_service=polymarket,  # type: ignore[arg-type]
        kalshi_service=None,
    )

    await collector._sync_polymarket_fee_rates_once()

    assert polymarket.options == [
        {
            "include_fee_rates": True,
            "include_rewards": False,
            "fee_rate_live_only": True,
        }
    ]


@pytest.mark.asyncio
async def test_slow_live_fee_refresh_failure_has_distinct_data_gap() -> None:
    database = GapDatabase()
    collector = LiveCollector(
        settings=collector_settings(),
        database=database,  # type: ignore[arg-type]
        writer=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        polymarket_service=FailingPolymarketEconomics(),  # type: ignore[arg-type]
        kalshi_service=None,
    )

    await collector._sync_polymarket_fee_rates_once()

    assert len(database.gaps) == 1
    gap = database.gaps[0]
    assert gap["channel"] == "rest:fee_rate_refresh"
    assert gap["gap_type"] == "fee_rate_refresh_failed"
    assert gap["details"] == {
        "interval_seconds": 21_600,
        "fee_rates_included": True,
        "rewards_included": False,
        "fee_rate_live_only": True,
    }


@pytest.mark.asyncio
async def test_economics_loop_is_registered_through_supervised_task_factory() -> None:
    collector = LiveCollector(
        settings=collector_settings(),
        database=GapDatabase(),  # type: ignore[arg-type]
        writer=object(),  # type: ignore[arg-type]
        metrics=object(),  # type: ignore[arg-type]
        polymarket_service=FailingPolymarketEconomics(),  # type: ignore[arg-type]
        kalshi_service=None,
    )
    task_names: list[str] = []

    def capture(coroutine: Any, *, name: str) -> object:
        task_names.append(name)
        coroutine.close()
        return object()

    collector._create_watched_task = capture  # type: ignore[method-assign]

    await collector._start_background_tasks()

    assert "economics-refresh" in task_names
    assert "polymarket-fee-rate-refresh" in task_names
