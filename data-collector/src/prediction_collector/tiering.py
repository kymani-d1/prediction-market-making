from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Iterable

from prediction_collector.common.types import MarketCandidate
from prediction_collector.common.utils import utc_now


class CollectionTier(StrEnum):
    FULL_L2 = "full_l2"
    SAMPLED = "sampled"
    METADATA_ONLY = "metadata_only"


@dataclass(frozen=True, slots=True)
class TierAssignment:
    market: MarketCandidate
    tier: CollectionTier
    score: Decimal
    reasons: tuple[str, ...]
    ceiling_binding: bool = False


@dataclass(slots=True)
class _Activity:
    trades: deque[tuple[datetime, Decimal]]
    updates: deque[datetime]


class TierManager:
    """Deterministic, auditable market-tier policy with rolling activity signals."""

    def __init__(
        self,
        *,
        full_l2_max_markets: int,
        sampled_max_markets: int,
        full_l2_min_score: Decimal,
        full_l2_min_liquidity: Decimal,
        full_l2_min_recent_trades: int,
        full_l2_min_book_updates: int,
        allowlist: frozenset[str] = frozenset(),
        blocklist: frozenset[str] = frozenset(),
        activity_window_seconds: int = 900,
    ) -> None:
        self.full_l2_max_markets = full_l2_max_markets
        self.sampled_max_markets = sampled_max_markets
        self.full_l2_min_score = full_l2_min_score
        self.full_l2_min_liquidity = full_l2_min_liquidity
        self.full_l2_min_recent_trades = full_l2_min_recent_trades
        self.full_l2_min_book_updates = full_l2_min_book_updates
        self.allowlist = allowlist
        self.blocklist = blocklist
        self.activity_window = timedelta(seconds=activity_window_seconds)
        self.assignments: dict[str, TierAssignment] = {}
        self._activity: dict[str, _Activity] = defaultdict(
            lambda: _Activity(deque(), deque())
        )

    def record_trade(
        self, market_external_id: str, size: Decimal, *, observed_at: datetime | None = None
    ) -> None:
        now = observed_at or utc_now()
        activity = self._activity[market_external_id]
        activity.trades.append((now, size))
        self._trim(activity, now)

    def record_book_update(
        self, market_external_id: str, *, observed_at: datetime | None = None
    ) -> None:
        now = observed_at or utc_now()
        activity = self._activity[market_external_id]
        activity.updates.append(now)
        self._trim(activity, now)

    def evaluate(
        self, markets: Iterable[MarketCandidate], *, observed_at: datetime | None = None
    ) -> list[TierAssignment]:
        now = observed_at or utc_now()
        scored: list[tuple[MarketCandidate, Decimal, tuple[str, ...], bool]] = []
        metadata_only: list[TierAssignment] = []
        for market in markets:
            activity = self._activity[market.external_id]
            self._trim(activity, now)
            selectors = market.selectors
            forced = bool(selectors & self.allowlist)
            blocked = bool(selectors & self.blocklist)
            if not self._expensive_collection_eligible(market) or blocked:
                reasons = self._ineligible_reasons(market)
                if blocked:
                    reasons.append("forced_blocklist")
                metadata_only.append(
                    TierAssignment(
                        market,
                        CollectionTier.METADATA_ONLY,
                        Decimal("0"),
                        tuple(reasons or ["not_trade_ready"]),
                    )
                )
                continue
            score, reasons = self._score(market, activity, now)
            if forced:
                score += Decimal("10000")
                reasons.insert(0, "forced_allowlist")
            qualifies = forced or (
                score >= self.full_l2_min_score
                and (
                    (market.liquidity or Decimal("0")) >= self.full_l2_min_liquidity
                    or len(activity.trades) >= self.full_l2_min_recent_trades
                    or len(activity.updates) >= self.full_l2_min_book_updates
                    or market.has_maker_rewards
                )
            )
            scored.append((market, score, tuple(reasons), qualifies))

        scored.sort(key=lambda item: (-item[1], item[0].external_id))
        full_candidates = [item for item in scored if item[3]]
        full_ids = {
            item[0].external_id
            for item in (
                full_candidates[: self.full_l2_max_markets]
                if self.full_l2_max_markets
                else full_candidates
            )
        }
        full_cap_binding = bool(
            self.full_l2_max_markets
            and len(full_candidates) > self.full_l2_max_markets
        )
        remaining = [item for item in scored if item[0].external_id not in full_ids]
        sampled_items = (
            remaining[: self.sampled_max_markets]
            if self.sampled_max_markets
            else remaining
        )
        sampled_ids = {item[0].external_id for item in sampled_items}
        sampled_cap_binding = bool(
            self.sampled_max_markets and len(remaining) > self.sampled_max_markets
        )

        assignments = list(metadata_only)
        for market, score, reasons, qualifies in scored:
            if market.external_id in full_ids:
                tier = CollectionTier.FULL_L2
                final_reasons = reasons or ("full_l2_score",)
                binding = False
            elif market.external_id in sampled_ids:
                tier = CollectionTier.SAMPLED
                final_reasons = reasons or ("trade_ready_long_tail",)
                binding = full_cap_binding and qualifies
                if binding:
                    final_reasons = (*final_reasons, "full_l2_resource_ceiling")
            else:
                tier = CollectionTier.METADATA_ONLY
                final_reasons = (*reasons, "sampled_resource_ceiling")
                binding = sampled_cap_binding
            assignments.append(
                TierAssignment(market, tier, score, final_reasons, binding)
            )
        assignments.sort(key=lambda item: item.market.external_id)
        self.assignments = {item.market.external_id: item for item in assignments}
        return assignments

    def tier_for(self, market_external_id: str | None) -> CollectionTier:
        if not market_external_id:
            return CollectionTier.METADATA_ONLY
        assignment = self.assignments.get(market_external_id)
        return assignment.tier if assignment else CollectionTier.METADATA_ONLY

    def subscribed_markets(self) -> list[MarketCandidate]:
        return [
            item.market
            for item in self.assignments.values()
            if item.tier is not CollectionTier.METADATA_ONLY
        ]

    def counts(self) -> dict[str, int]:
        counts = {tier.value: 0 for tier in CollectionTier}
        for assignment in self.assignments.values():
            counts[assignment.tier.value] += 1
        return counts

    def activity_for(self, market_external_id: str) -> tuple[int, Decimal, int]:
        activity = self._activity[market_external_id]
        now = utc_now()
        self._trim(activity, now)
        return (
            len(activity.trades),
            sum((size for _, size in activity.trades), Decimal("0")),
            len(activity.updates),
        )

    def _trim(self, activity: _Activity, now: datetime) -> None:
        cutoff = now - self.activity_window
        while activity.trades and activity.trades[0][0] < cutoff:
            activity.trades.popleft()
        while activity.updates and activity.updates[0] < cutoff:
            activity.updates.popleft()

    @staticmethod
    def _expensive_collection_eligible(market: MarketCandidate) -> bool:
        return bool(
            market.active
            and market.tradable
            and not market.closed
            and not market.archived
            and market.accepting_orders
            and market.enable_order_book
            and market.outcome_token_ids
        )

    @staticmethod
    def _ineligible_reasons(market: MarketCandidate) -> list[str]:
        reasons: list[str] = []
        if not market.active:
            reasons.append("inactive")
        if market.closed:
            reasons.append("closed")
        if market.archived:
            reasons.append("archived")
        if not market.accepting_orders:
            reasons.append("not_accepting_orders")
        if not market.enable_order_book:
            reasons.append("order_book_disabled")
        if not market.outcome_token_ids:
            reasons.append("no_clob_tokens")
        return reasons

    @staticmethod
    def _score(
        market: MarketCandidate, activity: _Activity, now: datetime
    ) -> tuple[Decimal, list[str]]:
        liquidity = float(market.liquidity or 0)
        volume_24h = float(market.volume_24h or 0)
        score = Decimal(str(round(math.log10(1 + liquidity) * 10, 6)))
        score += Decimal(str(round(math.log10(1 + volume_24h) * 8, 6)))
        reasons: list[str] = []
        if liquidity > 0:
            reasons.append("liquidity")
        if volume_24h > 0:
            reasons.append("recent_volume")
        trade_count = len(activity.trades)
        update_count = len(activity.updates)
        if trade_count:
            score += Decimal(min(trade_count * 4, 40))
            reasons.append("recent_trades")
        if update_count:
            score += Decimal(str(round(min(math.log1p(update_count) * 6, 30), 6)))
            reasons.append("recent_book_activity")
        if market.has_maker_rewards:
            score += Decimal("30")
            reasons.append("maker_rewards")
        spread = market.spread or Decimal("0")
        if Decimal("0.02") <= spread <= Decimal("0.25"):
            score += Decimal("20")
            reasons.append("wide_actionable_spread")
        if market.close_time is not None:
            seconds = (market.close_time - now).total_seconds()
            if 0 < seconds <= 30 * 86400:
                score += Decimal("10")
                reasons.append("near_resolution")
        return score, reasons
