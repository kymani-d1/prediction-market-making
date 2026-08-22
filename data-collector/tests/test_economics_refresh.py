from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from prediction_collector.common.types import MarketCandidate
from prediction_collector.database import _fee_configuration_digest
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import WriteItem


NOW = datetime(2026, 8, 13, tzinfo=UTC)


@dataclass
class FakeResult:
    data: Any
    requested_at: datetime = NOW
    response_timestamp: datetime = NOW
    status_code: int = 200
    url: str = "https://example.invalid/resource"


class CapturingWriter:
    run_id = 41

    def __init__(self) -> None:
        self.items: list[WriteItem] = []

    async def put(self, item: WriteItem) -> None:
        self.items.append(item)


class RewardsRest:
    def __init__(self) -> None:
        self.fee_rate_calls: list[str] = []
        self.reward_variants: list[bool] = []

    async def fee_rate(self, token_id: str) -> FakeResult:
        self.fee_rate_calls.append(token_id)
        raise AssertionError("reward refresh must not request per-token fees")

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
                        "rewards_config": [
                            {
                                "id": "schedule-a",
                                "start_date": "2026-08-01T00:00:00Z",
                                "end_date": "2026-09-01T00:00:00Z",
                                "rate_per_day": "20",
                                "total_rewards": "620",
                            }
                        ],
                    }
                ],
                FakeResult({}),
                None,
            )


class EconomicsDatabase:
    def __init__(self) -> None:
        self.incentives: list[dict[str, Any]] = []
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
                accepting_orders=True,
                enable_order_book=True,
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
        ]

    async def collection_tier_market_ids(self, tiers: tuple[str, ...]) -> set[str]:
        assert tiers == ("full_l2", "sampled")
        return {"LIVE"}

    async def record_incentive_configuration(self, **kwargs: Any) -> bool:
        self.incentives.append(kwargs)
        return True

    async def record_fee_configuration(self, **kwargs: Any) -> bool:
        self.fees.append(kwargs)
        return True


@pytest.mark.asyncio
async def test_reward_refresh_skips_per_token_fee_storm_and_archives_raw_page() -> None:
    rest = RewardsRest()
    database = EconomicsDatabase()
    writer = CapturingWriter()
    service = PolymarketService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )
    counts = await service.sync_fees_and_incentives(include_fee_rates=False)
    assert rest.fee_rate_calls == []
    assert rest.reward_variants == [False, True]
    assert counts["reward_versions_inserted"] == 2
    assert {item.kind for item in writer.items} == {"raw_rest_payloads"}
    assert {item["incentive_type"] for item in database.incentives} == {
        "liquidity_reward:standard:schedule-a",
        "liquidity_reward_summary:standard",
    }


class FeeRest:
    def __init__(self) -> None:
        self.tokens: list[str] = []

    async def fee_rate(self, token_id: str) -> FakeResult:
        self.tokens.append(token_id)
        return FakeResult({"base_fee": 100})


@pytest.mark.asyncio
async def test_fee_rate_is_normalized_from_basis_points_and_live_only() -> None:
    rest = FeeRest()
    database = EconomicsDatabase()
    writer = CapturingWriter()
    service = PolymarketService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )
    counts = await service.sync_fees_and_incentives(
        include_fee_rates=True,
        include_rewards=False,
        fee_rate_live_only=True,
    )
    assert rest.tokens == ["LIVE-YES", "LIVE-NO"]
    assert counts["markets_checked"] == 1
    assert database.fees[0]["fee_rate"] == Decimal("0.01")
    assert len([item for item in writer.items if item.kind == "raw_rest_payloads"]) == 2


class StreamingEconomicsDatabase(EconomicsDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.gaps: list[dict[str, Any]] = []

    async def live_candidates(self, exchange: str) -> list[MarketCandidate]:
        raise AssertionError("the production fee path must not materialize all markets")

    async def iter_live_candidates(self, exchange: str):  # type: ignore[no-untyped-def]
        assert exchange == "polymarket"
        yield MarketCandidate(
            exchange="polymarket",
            external_id="HISTORIC",
            ticker=None,
            status="closed",
            active=False,
            tradable=False,
            outcome_token_ids=("MISSING-YES", "MISSING-NO"),
        )

    async def record_gap(self, **kwargs: Any) -> None:
        self.gaps.append(kwargs)


class MissingFeeRest:
    def __init__(self) -> None:
        self.tokens: list[str] = []

    async def fee_rate(self, token_id: str) -> FakeResult:
        self.tokens.append(token_id)
        request = httpx.Request(
            "GET", f"https://clob.polymarket.com/fee-rate?token_id={token_id}"
        )
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError(
            "fee configuration not found", request=request, response=response
        )


@pytest.mark.asyncio
async def test_fee_rate_404_is_persisted_as_expected_token_absence() -> None:
    rest = MissingFeeRest()
    database = StreamingEconomicsDatabase()
    writer = CapturingWriter()
    service = PolymarketService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    counts = await service.sync_fees_and_incentives(
        include_fee_rates=True,
        include_rewards=False,
        fee_rate_live_only=False,
    )

    assert rest.tokens == ["MISSING-YES", "MISSING-NO"]
    assert counts == {
        "markets_checked": 1,
        "fee_versions_inserted": 1,
        "fee_rates_unavailable": 2,
        "reward_records_seen": 0,
        "reward_versions_inserted": 0,
        "errors": 0,
    }
    assert database.gaps == []
    assert database.fees[0]["configuration"] == {
        "outcomes": {
            "MISSING-YES": {
                "available": False,
                "http_status": 404,
                "reason": "clob_token_not_found",
            },
            "MISSING-NO": {
                "available": False,
                "http_status": 404,
                "reason": "clob_token_not_found",
            },
        }
    }
    assert writer.items == []


def test_transport_wrappers_do_not_change_semantic_fee_digest() -> None:
    semantic = {"fee_type": "market", "multiplier": "1.0"}
    first = _fee_configuration_digest(
        configuration={"rest": semantic},
        semantic_configuration=semantic,
        maker_rate=None,
        taker_rate=None,
        fee_rate=None,
        multiplier="1.0",
        fixed_fee=None,
        currency="USDC",
    )
    second = _fee_configuration_digest(
        configuration={"websocket": {"msg": semantic, "sid": 7}},
        semantic_configuration=semantic,
        maker_rate=None,
        taker_rate=None,
        fee_rate=None,
        multiplier="1.0",
        fixed_fee=None,
        currency="USDC",
    )
    assert first == second
