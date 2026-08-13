from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from prediction_collector.common.types import MarketCandidate
from prediction_collector.tiering import CollectionTier, TierManager


NOW = datetime(2026, 8, 13, tzinfo=UTC)


def market(
    external_id: str,
    *,
    liquidity: str = "0",
    volume_24h: str = "0",
    spread: str | None = None,
    rewards: bool = False,
    active: bool = True,
    closed: bool = False,
    archived: bool = False,
    accepting: bool = True,
    book: bool = True,
) -> MarketCandidate:
    return MarketCandidate(
        exchange="polymarket",
        external_id=external_id,
        ticker=None,
        status="active" if active else "closed",
        active=active,
        tradable=active and not closed and not archived and accepting and book,
        closed=closed,
        archived=archived,
        accepting_orders=accepting,
        enable_order_book=book,
        volume=Decimal(volume_24h),
        volume_24h=Decimal(volume_24h),
        liquidity=Decimal(liquidity),
        has_maker_rewards=rewards,
        spread=Decimal(spread) if spread is not None else None,
        close_time=NOW + timedelta(days=5),
        outcome_token_ids=(f"{external_id}-yes", f"{external_id}-no"),
    )


def manager(**overrides: object) -> TierManager:
    options = {
        "full_l2_max_markets": 1,
        "sampled_max_markets": 2,
        "full_l2_min_score": Decimal("55"),
        "full_l2_min_liquidity": Decimal("1000"),
        "full_l2_min_recent_trades": 2,
        "full_l2_min_book_updates": 3,
        "activity_window_seconds": 900,
    }
    options.update(overrides)
    return TierManager(**options)  # type: ignore[arg-type]


def test_three_tiers_and_deterministic_resource_ceiling() -> None:
    value = manager()
    markets = [
        market("reward-wide", liquidity="10", spread="0.10", rewards=True),
        market("high-liquidity", liquidity="100000", volume_24h="10000"),
        market("long-tail", liquidity="1"),
        market("inactive", active=False, accepting=False, book=False),
    ]
    first = value.evaluate(markets, observed_at=NOW)
    second = value.evaluate(reversed(markets), observed_at=NOW)
    assert [(item.market.external_id, item.tier) for item in first] == [
        (item.market.external_id, item.tier) for item in second
    ]
    tiers = {item.market.external_id: item for item in first}
    assert tiers["high-liquidity"].tier is CollectionTier.FULL_L2
    assert tiers["reward-wide"].tier is CollectionTier.SAMPLED
    assert tiers["long-tail"].tier is CollectionTier.SAMPLED
    assert tiers["inactive"].tier is CollectionTier.METADATA_ONLY
    assert tiers["reward-wide"].ceiling_binding
    assert "maker_rewards" in tiers["reward-wide"].reasons


def test_activity_promotes_then_ages_out_and_demotes() -> None:
    value = manager(
        full_l2_max_markets=0,
        sampled_max_markets=0,
        full_l2_min_score=Decimal("20"),
    )
    candidate = market("moving", liquidity="1")
    assert value.evaluate([candidate], observed_at=NOW)[0].tier is CollectionTier.SAMPLED
    value.record_book_update("moving", observed_at=NOW)
    value.record_book_update("moving", observed_at=NOW)
    value.record_book_update("moving", observed_at=NOW)
    promoted = value.evaluate([candidate], observed_at=NOW)
    assert promoted[0].tier is CollectionTier.FULL_L2
    demoted = value.evaluate([candidate], observed_at=NOW + timedelta(minutes=16))
    assert demoted[0].tier is CollectionTier.SAMPLED


def test_allowlist_forces_full_l2_and_blocklist_wins() -> None:
    allowed = manager(allowlist=frozenset({"forced"}))
    assignment = allowed.evaluate([market("forced")], observed_at=NOW)[0]
    assert assignment.tier is CollectionTier.FULL_L2
    assert "forced_allowlist" in assignment.reasons

    blocked = manager(
        allowlist=frozenset({"forced"}), blocklist=frozenset({"forced"})
    )
    assignment = blocked.evaluate([market("forced")], observed_at=NOW)[0]
    assert assignment.tier is CollectionTier.METADATA_ONLY
    assert "forced_blocklist" in assignment.reasons


def test_closed_archived_and_non_clob_markets_never_consume_subscription_capacity() -> None:
    value = manager(full_l2_max_markets=0, sampled_max_markets=0)
    candidates = [
        market("closed", closed=True),
        market("archived", archived=True),
        market("not-accepting", accepting=False),
        market("book-disabled", book=False),
    ]
    assignments = value.evaluate(candidates, observed_at=NOW)
    assert {item.tier for item in assignments} == {CollectionTier.METADATA_ONLY}
    assert value.subscribed_markets() == []
