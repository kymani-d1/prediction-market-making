from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.common.types import ResearchMarket, ResearchOutcome
from prediction_collector.config import ConfigurationError, Settings
from prediction_collector.database import Database, research_selection_key
from prediction_collector.jobs.research_backfill import (
    _collect_trades,
    _record_economics,
    run_polymarket_research_backfill,
)
from prediction_collector.main import _writer
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import WriteItem


NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)


@dataclass
class FakeResult:
    data: Any
    requested_at: datetime = NOW
    response_timestamp: datetime = NOW
    received_at: datetime = NOW
    status_code: int = 200
    url: str = "https://clob.test/resource"


class FakeQueue:
    def __init__(self) -> None:
        self.joins = 0

    async def join(self) -> None:
        self.joins += 1


class ResearchWriter:
    def __init__(self) -> None:
        self.run_id = 101
        self.queue = FakeQueue()
        self.archive = None
        self.failed_items = 0
        self.rows_written = 0
        self.items: list[WriteItem] = []
        self.started = 0
        self.stopped = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def put(self, item: WriteItem) -> None:
        self.items.append(item)


def research_market(
    rank: int = 1,
    *,
    statuses: tuple[str, str, str, str] = (
        "not_started",
        "not_started",
        "not_started",
        "not_started",
    ),
    open_time: datetime | None = NOW - timedelta(days=30),
) -> ResearchMarket:
    external_id = f"0x{rank:064x}"
    return ResearchMarket(
        cohort_id=7,
        cohort_version="pilot-v1",
        selection_rank=rank,
        market_id=rank,
        external_id=external_id,
        question=f"Market {rank}?",
        category="politics" if rank % 2 else "sports",
        status="resolved",
        active=False,
        open_time=open_time,
        close_time=NOW - timedelta(days=1),
        settlement_time=NOW - timedelta(days=1),
        result=None,
        volume=Decimal("1000"),
        liquidity=Decimal("100"),
        fee_rate=None,
        raw_data={
            "feesEnabled": False,
            "rewardsMinSize": "10",
            "rewardsMaxSpread": "0.03",
        },
        outcomes=(
            ResearchOutcome(
                external_id=f"{external_id}:outcome:0",
                token_id=f"token-{rank}-yes",
                name="Yes",
                outcome_index=0,
                last_price=Decimal("1"),
            ),
            ResearchOutcome(
                external_id=f"{external_id}:outcome:1",
                token_id=f"token-{rank}-no",
                name="No",
                outcome_index=1,
                last_price=Decimal("0"),
            ),
        ),
        price_status=statuses[0],
        trade_status=statuses[1],
        resolution_status=statuses[2],
        economics_status=statuses[3],
    )


class ResearchDatabase:
    def __init__(self, markets: list[ResearchMarket]) -> None:
        self.markets = markets
        self.cohort_calls: list[dict[str, Any]] = []
        self.phase_updates: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.iteration_started = 0

    async def create_or_get_research_cohort(self, **kwargs: Any) -> dict[str, Any]:
        self.cohort_calls.append(kwargs)
        return {
            "id": 7,
            "version": kwargs["version"],
            "selected_count": min(len(self.markets), kwargs["max_markets"]),
            "max_markets": kwargs["max_markets"],
            "status": "ready",
        }

    async def iter_research_markets(self, **kwargs: Any):  # type: ignore[no-untyped-def]
        assert kwargs["exchange"] == "polymarket"
        self.iteration_started += 1
        for market in self.markets:
            yield market

    async def set_research_phase_status(self, **kwargs: Any) -> None:
        self.phase_updates.append(kwargs)

    async def research_cohort_status(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "id": 7,
            "version": kwargs["cohort_version"],
            "selected_count": len(self.markets),
            "completed_markets": len(self.markets),
            "partial_markets": 0,
            "retryable_failed_markets": 0,
            "price_records": 4 * len(self.markets),
            "trade_records": len(self.markets),
            "request_count": 2 * len(self.markets),
        }

    async def record_gap(self, **kwargs: Any) -> None:
        self.gaps.append(kwargs)

    async def live_candidates(self, *_: Any, **__: Any) -> None:
        raise AssertionError("research backfill must not materialize the full universe")

    async def record_tier_assignments(self, *_: Any, **__: Any) -> None:
        raise AssertionError("research backfill must not assign live tiers")


class ResearchRest:
    def __init__(self) -> None:
        self.price_calls: list[list[str]] = []
        self.trade_calls: list[str] = []
        self.forbidden_calls: list[str] = []
        self.active_price_calls = 0
        self.max_active_price_calls = 0
        self.fail_prices = False

    async def batch_price_history(
        self, token_ids: list[str], **kwargs: Any
    ) -> FakeResult:
        self.price_calls.append(list(token_ids))
        self.active_price_calls += 1
        self.max_active_price_calls = max(
            self.max_active_price_calls, self.active_price_calls
        )
        await asyncio.sleep(0.005)
        self.active_price_calls -= 1
        if self.fail_prices:
            raise RuntimeError("transient price failure")
        return FakeResult(
            {
                "history": {
                    token_id: [{"t": int(NOW.timestamp()), "p": "0.5"}]
                    for token_id in token_ids
                }
            },
            url="https://clob.test/batch-prices-history",
        )

    async def iter_trades(self, *, market: str, **kwargs: Any):  # type: ignore[no-untyped-def]
        self.trade_calls.append(market)
        yield (
            [
                {
                    "conditionId": market,
                    "asset": "token-traded",
                    "price": 0.45,
                    "size": 10,
                    "side": "BUY",
                    "timestamp": int(NOW.timestamp()),
                    "transactionHash": f"tx-{market}",
                }
            ],
            FakeResult([], url="https://data.test/trades"),
        )

    async def iter_comments(self, **_: Any) -> None:
        self.forbidden_calls.append("comments")
        raise AssertionError("comments are excluded")

    async def holders(self, *_: Any, **__: Any) -> None:
        self.forbidden_calls.append("holders")
        raise AssertionError("holders are excluded")

    async def orderbook(self, *_: Any, **__: Any) -> None:
        self.forbidden_calls.append("orderbook")
        raise AssertionError("current books are excluded")

    async def fee_rate(self, *_: Any, **__: Any) -> None:
        self.forbidden_calls.append("fee_rate")
        raise AssertionError("universal token fee lookups are excluded")


def service_for(
    markets: list[ResearchMarket],
) -> tuple[PolymarketService, ResearchDatabase, ResearchRest, ResearchWriter]:
    database = ResearchDatabase(markets)
    rest = ResearchRest()
    writer = ResearchWriter()
    service = PolymarketService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )
    return service, database, rest, writer


def test_research_defaults_and_absolute_bounds() -> None:
    value = Settings.from_env({}, load_dotenv_file=False)
    assert value.research_backfill_max_markets == 2_500
    assert value.research_backfill_hard_max_markets == 5_000
    assert value.research_backfill_catalogue_horizon_days == 730
    assert value.research_backfill_price_fidelity_minutes == 60
    assert value.research_backfill_archive_raw_rest is False

    with pytest.raises(ConfigurationError, match="cannot exceed 5000"):
        Settings.from_env(
            {"RESEARCH_BACKFILL_HARD_MAX_MARKETS": "5001"},
            load_dotenv_file=False,
        )


def test_research_writer_does_not_start_archive_when_raw_rest_is_disabled() -> None:
    settings = Settings()
    writer = _writer(
        Database(settings), settings, None, archive_enabled=False
    )
    assert writer.archive is None
    with pytest.raises(ConfigurationError, match="cannot exceed"):
        Settings.from_env(
            {
                "RESEARCH_BACKFILL_MAX_MARKETS": "100",
                "RESEARCH_BACKFILL_HARD_MAX_MARKETS": "50",
            },
            load_dotenv_file=False,
        )


def test_cohort_selection_key_is_stable_and_seeded() -> None:
    first = research_selection_key("seed-a", "market-1")
    assert first == research_selection_key("seed-a", "market-1")
    assert first != research_selection_key("seed-b", "market-1")
    assert first != research_selection_key("seed-a", "market-2")


@pytest.mark.asyncio
async def test_database_rejects_cohort_above_absolute_ceiling_before_querying() -> None:
    database = object.__new__(Database)
    with pytest.raises(ValueError, match="between 1 and 5000"):
        await database.create_or_get_research_cohort(
            exchange="polymarket",
            version="too-large",
            seed="seed",
            max_markets=5_001,
            horizon_start=NOW,
            criteria={},
        )


@pytest.mark.asyncio
async def test_research_path_only_fetches_cohort_prices_and_trades() -> None:
    market = research_market()
    service, database, rest, writer = service_for([market])
    settings = replace(
        Settings(),
        research_backfill_cohort_version="pilot-v1",
        research_backfill_max_markets=1,
        research_backfill_request_concurrency=1,
        research_backfill_candidate_batch_size=1,
    )

    result = await run_polymarket_research_backfill(
        service, writer, settings, phase="all"
    )

    assert result.status == "completed"
    assert rest.price_calls == [["token-1-yes", "token-1-no"]]
    assert rest.trade_calls == [market.external_id]
    assert rest.forbidden_calls == []
    assert database.iteration_started == 1
    assert database.cohort_calls[0]["max_markets"] == 1
    assert database.cohort_calls[0]["criteria"]["dimensions"] == [
        "category",
        "volume_bucket",
        "liquidity_bucket",
        "duration_bucket",
        "lifecycle_bucket",
        "calendar_quarter",
    ]
    assert {item.kind for item in writer.items} == {"candlesticks", "trades"}
    assert {
        item.data["outcome_external_id"]
        for item in writer.items
        if item.kind == "candlesticks"
    } == {"token-1-yes", "token-1-no"}
    assert not any(item.kind == "raw_rest_payloads" for item in writer.items)
    terminal = {
        (update["phase"], update["status"])
        for update in database.phase_updates
        if update["status"] != "in_progress"
    }
    assert terminal == {
        ("price", "completed"),
        ("trade", "completed"),
        ("resolution", "completed"),
        ("economics", "completed"),
    }


@pytest.mark.asyncio
async def test_completed_market_is_not_refetched_after_restart() -> None:
    complete = research_market(
        statuses=("completed", "completed", "completed", "completed")
    )
    service, database, rest, writer = service_for([complete])
    settings = replace(
        Settings(),
        research_backfill_cohort_version="pilot-v1",
        research_backfill_max_markets=1,
        research_backfill_request_concurrency=1,
        research_backfill_candidate_batch_size=1,
    )

    result = await run_polymarket_research_backfill(
        service, writer, settings, phase="data"
    )

    assert result.details["historical_data"]["markets_skipped_complete"] == 1  # type: ignore[index]
    assert rest.price_calls == []
    assert rest.trade_calls == []
    assert database.phase_updates == []


@pytest.mark.asyncio
async def test_transient_failure_does_not_advance_price_checkpoint() -> None:
    market = research_market()
    service, database, rest, writer = service_for([market])
    rest.fail_prices = True
    settings = replace(
        Settings(),
        research_backfill_cohort_version="pilot-v1",
        research_backfill_request_concurrency=1,
        research_backfill_candidate_batch_size=1,
    )

    with pytest.raises(ExceptionGroup):
        await run_polymarket_research_backfill(
            service, writer, settings, phase="data"
        )

    price_updates = [
        update for update in database.phase_updates if update["phase"] == "price"
    ]
    assert [update["status"] for update in price_updates] == [
        "in_progress",
        "retryable_failed",
    ]
    assert rest.trade_calls == []
    assert writer.stopped == 1


@pytest.mark.asyncio
async def test_request_concurrency_is_bounded_and_streamed() -> None:
    markets = [research_market(rank) for rank in range(1, 8)]
    service, database, rest, writer = service_for(markets)
    settings = replace(
        Settings(),
        research_backfill_cohort_version="pilot-v1",
        research_backfill_max_markets=7,
        research_backfill_request_concurrency=3,
        research_backfill_candidate_batch_size=4,
    )

    await run_polymarket_research_backfill(
        service, writer, settings, phase="data"
    )

    assert 1 < rest.max_active_price_calls <= 3
    assert len(rest.price_calls) == 7
    assert database.iteration_started == 1


@pytest.mark.asyncio
async def test_trade_history_floor_is_explicitly_partial() -> None:
    old_market = research_market(open_time=NOW - timedelta(days=4 * 365))
    service, database, _rest, writer = service_for([old_market])
    settings = replace(Settings(), research_backfill_archive_raw_rest=False)

    await _collect_trades(service, writer, settings, old_market)

    terminal = database.phase_updates[-1]
    assert terminal["phase"] == "trade"
    assert terminal["status"] == "partial"
    assert terminal["partial_history"] is True
    assert terminal["provenance"]["upstream_history_floor"] is True
    assert database.gaps[0]["gap_type"] == "upstream_history_floor"


@pytest.mark.asyncio
async def test_normalized_fee_metadata_needs_no_token_fee_request() -> None:
    market = replace(
        research_market(),
        raw_data={},
        fee_rate=Decimal("0.02"),
    )
    service, database, rest, _writer_instance = service_for([market])

    await _record_economics(service, market)

    terminal = database.phase_updates[-1]
    assert terminal["status"] == "completed"
    assert terminal["provenance"]["fee_status"] == "current_metadata"
    assert terminal["provenance"]["normalized_fee_rate"] == Decimal("0.02")
    assert terminal["provenance"]["network_requests"] == 0
    assert rest.forbidden_calls == []
