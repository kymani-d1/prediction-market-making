from __future__ import annotations

from decimal import Decimal

import pytest

from prediction_collector.common.coverage import select_live_markets
from prediction_collector.common.types import MarketCandidate


def candidate(
    external_id: str,
    *,
    exchange: str = "polymarket",
    ticker: str | None = None,
    active: bool = True,
    tradable: bool = True,
    volume: str | None = "0",
    volume_24h: str | None = "0",
    liquidity: str | None = "0",
    tokens: tuple[str, ...] = (),
) -> MarketCandidate:
    return MarketCandidate(
        exchange=exchange,
        external_id=external_id,
        ticker=ticker,
        status="active" if active else "closed",
        active=active,
        tradable=tradable,
        volume=Decimal(volume) if volume is not None else None,
        volume_24h=Decimal(volume_24h) if volume_24h is not None else None,
        liquidity=Decimal(liquidity) if liquidity is not None else None,
        outcome_token_ids=tokens,
    )


def test_zero_limits_subscribe_every_active_tradable_market() -> None:
    markets = [
        candidate("one"),
        candidate("two", exchange="kalshi"),
        candidate("inactive", active=False, tradable=False),
        candidate("paused", active=True, tradable=False),
    ]

    result = select_live_markets(markets)

    assert result.discovered == 4
    assert result.active == 3
    assert result.tradable == 2
    assert [(m.exchange, m.external_id) for m in result.subscribed] == [
        ("kalshi", "two"),
        ("polymarket", "one"),
    ]
    assert result.excluded_counts == {"inactive": 1, "not_tradable": 1}


def test_unavailable_exchange_is_excluded_before_cap() -> None:
    markets = [
        candidate("kalshi-market", exchange="kalshi", liquidity="100"),
        candidate("poly-market", exchange="polymarket", liquidity="1"),
    ]

    result = select_live_markets(
        markets,
        max_markets=1,
        unavailable_exchanges={"kalshi": "credentials_missing"},
    )

    assert [market.external_id for market in result.subscribed] == ["poly-market"]
    assert {(item.external_id, item.reason) for item in result.excluded} == {
        ("kalshi-market", "credentials_missing")
    }


def test_thresholds_treat_missing_metrics_as_zero_and_record_exact_reason() -> None:
    result = select_live_markets(
        [
            candidate("eligible", volume="100", liquidity="50"),
            candidate("low-volume", volume="99.99", liquidity="100"),
            candidate("low-liquidity", volume="100", liquidity="49.99"),
            candidate("missing", volume=None, liquidity=None),
        ],
        min_volume=Decimal("100"),
        min_liquidity=Decimal("50"),
    )

    assert [m.external_id for m in result.subscribed] == ["eligible"]
    assert result.excluded_counts == {
        "below_min_volume": 2,
        "below_min_liquidity": 1,
    }


def test_blocklist_wins_and_allowlist_matches_all_documented_selectors() -> None:
    first = candidate("condition-1", ticker="slug-1", tokens=("yes-1", "no-1"))
    second = candidate("condition-2", ticker="slug-2", tokens=("yes-2", "no-2"))
    third = candidate("KALSHI-TICKER", exchange="kalshi", ticker="KALSHI-TICKER")

    result = select_live_markets(
        [first, second, third],
        allowlist=frozenset({"polymarket:yes-1", "slug-2", "kalshi:KALSHI-TICKER"}),
        blocklist=frozenset({"condition-2"}),
    )

    assert [(m.exchange, m.external_id) for m in result.subscribed] == [
        ("kalshi", "KALSHI-TICKER"),
        ("polymarket", "condition-1"),
    ]
    assert result.excluded_counts == {"blocklist": 1}


def test_cap_ranking_is_deterministic_and_applied_after_filtering() -> None:
    markets = [
        candidate("z", liquidity="100", volume_24h="9", volume="999"),
        candidate("b", liquidity="100", volume_24h="10", volume="10"),
        candidate("a", liquidity="100", volume_24h="10", volume="10"),
        candidate("higher-liquidity", liquidity="101", volume_24h="0", volume="10"),
        candidate("filtered", liquidity="1000", volume="1"),
    ]

    forward = select_live_markets(
        markets,
        max_markets=3,
        min_volume=Decimal("10"),
    )
    reverse = select_live_markets(
        reversed(markets),
        max_markets=3,
        min_volume=Decimal("10"),
    )

    expected = ["higher-liquidity", "a", "b"]
    assert [m.external_id for m in forward.subscribed] == expected
    assert [m.external_id for m in reverse.subscribed] == expected
    assert forward.excluded_counts == {
        "below_min_volume": 1,
        "max_live_markets_cap": 1,
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_markets": -1},
        {"min_volume": Decimal("-1")},
        {"min_liquidity": Decimal("-1")},
    ],
)
def test_negative_live_limits_are_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        select_live_markets([], **kwargs)  # type: ignore[arg-type]
