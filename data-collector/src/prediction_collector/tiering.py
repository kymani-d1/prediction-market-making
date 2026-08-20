from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from heapq import nsmallest
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
        sampled_promotion_score: Decimal = Decimal("20"),
        sampled_demotion_score: Decimal = Decimal("12"),
        full_l2_demotion_score: Decimal = Decimal("45"),
        min_dwell_seconds: int = 1800,
        full_l2_research_reserve: int = 0,
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
        self.sampled_promotion_score = sampled_promotion_score
        self.sampled_demotion_score = sampled_demotion_score
        self.full_l2_demotion_score = full_l2_demotion_score
        self.min_dwell = timedelta(seconds=min_dwell_seconds)
        self.full_l2_research_reserve = full_l2_research_reserve
        self.allowlist = allowlist
        self.blocklist = blocklist
        self.activity_window = timedelta(seconds=activity_window_seconds)
        self.assignments: dict[str, TierAssignment] = {}
        self._tier_since: dict[str, datetime] = {}
        self._persisted_tiers: dict[str, CollectionTier] = {}
        self._activity: dict[str, _Activity] = defaultdict(
            lambda: _Activity(deque(), deque())
        )
        self._counts = {tier.value: 0 for tier in CollectionTier}
        self.exclusion_counts: dict[str, int] = {}
        self.ceiling_exclusions = 0

    def record_trade(
        self, market_external_id: str, size: Decimal, *, observed_at: datetime | None = None
    ) -> None:
        now = observed_at or utc_now()
        activity = self._activity[market_external_id]
        activity.trades.append((now, size))
        self._trim(activity, now)

    def seed_previous_tiers(
        self, values: Iterable[tuple[str, str, datetime]]
    ) -> None:
        for external_id, tier, assigned_at in values:
            self._persisted_tiers[external_id] = CollectionTier(tier)
            self._tier_since[external_id] = assigned_at

    def record_book_update(
        self, market_external_id: str, *, observed_at: datetime | None = None
    ) -> None:
        now = observed_at or utc_now()
        activity = self._activity[market_external_id]
        activity.updates.append(now)
        self._trim(activity, now)

    def evaluate(
        self,
        markets: Iterable[MarketCandidate],
        *,
        observed_at: datetime | None = None,
        retain_metadata_assignments: bool = True,
    ) -> list[TierAssignment]:
        now = observed_at or utc_now()
        scored: list[
            tuple[MarketCandidate, Decimal, tuple[str, ...], bool, bool, bool]
        ] = []
        metadata_only: list[TierAssignment] = []
        evaluated_count = 0
        for market in markets:
            evaluated_count += 1
            activity = self._activity.get(market.external_id)
            if activity is not None:
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
            previous = self.assignments.get(market.external_id)
            previous_tier = (
                previous.tier if previous is not None
                else self._persisted_tiers.get(market.external_id)
            )
            tier_since = self._tier_since.get(market.external_id, now)
            in_dwell = now - tier_since < self.min_dwell
            externally_observable = bool(
                (market.liquidity or Decimal("0")) >= self.full_l2_min_liquidity
                or (market.volume_24h or Decimal("0")) > 0
                or market.has_maker_rewards
            )
            activity_observable = bool(
                activity is not None
                and (
                    len(activity.trades) >= self.full_l2_min_recent_trades
                    or len(activity.updates) >= self.full_l2_min_book_updates
                )
            )
            retained_full = bool(
                previous_tier is CollectionTier.FULL_L2
                and (in_dwell or score >= self.full_l2_demotion_score)
            )
            qualifies = forced or retained_full or (
                score >= self.full_l2_min_score
                and (externally_observable or activity_observable)
            )
            interesting = bool(
                market.has_maker_rewards
                or "wide_actionable_spread" in reasons
                or "near_resolution" in reasons
            )
            retain_sampled = bool(
                previous_tier is CollectionTier.SAMPLED
                and (in_dwell or score >= self.sampled_demotion_score)
            )
            sampled_eligible = bool(
                qualifies
                or retain_sampled
                or score >= self.sampled_promotion_score
            )
            scored.append(
                (market, score, tuple(reasons), qualifies, interesting, sampled_eligible)
            )

        score_order = lambda item: (-item[1], item[0].external_id)
        full_candidates = [item for item in scored if item[3]]
        research: list[
            tuple[MarketCandidate, Decimal, tuple[str, ...], bool, bool, bool]
        ] = []
        if self.full_l2_max_markets:
            reserve = min(self.full_l2_research_reserve, self.full_l2_max_markets)
            # The reserve is deliberately exploratory: it may admit a
            # metadata-visible maker-reward, wide-spread, or near-resolution
            # market that has not yet cleared the ordinary FULL_L2 threshold.
            # Selection remains deterministic without sorting the entire live
            # universe: only the bounded winners need ordering.
            research = nsmallest(
                reserve,
                (item for item in scored if item[4]),
                key=score_order,
            )
            research_ids = {item[0].external_id for item in research}
            general_slots = self.full_l2_max_markets - len(research)
            general = nsmallest(
                general_slots,
                (
                    item
                    for item in full_candidates
                    if item[0].external_id not in research_ids
                ),
                key=score_order,
            )
            selected_full = research + general
        else:
            selected_full = full_candidates
        full_ids = {item[0].external_id for item in selected_full}
        full_cap_binding = bool(
            self.full_l2_max_markets
            and len(
                {
                    item[0].external_id
                    for item in (*full_candidates, *research)
                }
            ) > self.full_l2_max_markets
        )
        remaining = [
            item for item in scored
            if item[0].external_id not in full_ids and item[5]
        ]
        sampled_items = (
            nsmallest(
                self.sampled_max_markets,
                remaining,
                key=score_order,
            )
            if self.sampled_max_markets
            else remaining
        )
        sampled_ids = {item[0].external_id for item in sampled_items}
        sampled_cap_binding = bool(
            self.sampled_max_markets and len(remaining) > self.sampled_max_markets
        )

        assignments = list(metadata_only)
        exclusion_counts: dict[str, int] = {}
        for item in metadata_only:
            reason = item.reasons[-1] if item.reasons else "not_trade_ready"
            exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1
        ceiling_exclusions = 0
        previous_assignments = self.assignments
        for market, score, reasons, qualifies, interesting, sampled_eligible in scored:
            if market.external_id in full_ids:
                tier = CollectionTier.FULL_L2
                final_reasons = reasons or ("full_l2_score",)
                if interesting and self.full_l2_research_reserve:
                    final_reasons = (*final_reasons, "research_bucket")
                binding = False
            elif market.external_id in sampled_ids:
                tier = CollectionTier.SAMPLED
                final_reasons = reasons or ("trade_ready_long_tail",)
                binding = full_cap_binding and qualifies
                if binding:
                    final_reasons = (*final_reasons, "full_l2_resource_ceiling")
            else:
                tier = CollectionTier.METADATA_ONLY
                if sampled_eligible:
                    final_reasons = (*reasons, "sampled_resource_ceiling")
                    binding = sampled_cap_binding
                else:
                    final_reasons = (*reasons, "below_sampled_promotion_threshold")
                    binding = False
            if tier is CollectionTier.METADATA_ONLY:
                reason = final_reasons[-1]
                exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1
                ceiling_exclusions += int(binding)
            previous = previous_assignments.get(market.external_id)
            previous_tier = (
                previous.tier if previous is not None
                else self._persisted_tiers.get(market.external_id)
            )
            # Live collection needs full objects only for subscribed markets
            # and for the small set of tracked markets being demoted. The
            # complete count/reason distribution is retained separately.
            if (
                retain_metadata_assignments
                or tier is not CollectionTier.METADATA_ONLY
                or previous_tier in {
                    CollectionTier.FULL_L2,
                    CollectionTier.SAMPLED,
                }
            ):
                assignments.append(
                    TierAssignment(market, tier, score, final_reasons, binding)
                )
        assignments.sort(key=lambda item: item.market.external_id)
        self.assignments = {
            item.market.external_id: item
            for item in assignments
            if item.tier is not CollectionTier.METADATA_ONLY
        }
        for item in assignments:
            previous = previous_assignments.get(item.market.external_id)
            previous_tier = (
                previous.tier if previous is not None
                else self._persisted_tiers.get(item.market.external_id)
            )
            if previous_tier is not item.tier:
                self._tier_since[item.market.external_id] = now
            if item.tier is not CollectionTier.METADATA_ONLY or previous_tier is not None:
                self._persisted_tiers[item.market.external_id] = item.tier
        self._counts = {
            CollectionTier.FULL_L2.value: len(full_ids),
            CollectionTier.SAMPLED.value: len(sampled_ids),
            CollectionTier.METADATA_ONLY.value: (
                evaluated_count - len(full_ids) - len(sampled_ids)
            ),
        }
        self.exclusion_counts = exclusion_counts
        self.ceiling_exclusions = ceiling_exclusions
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
        return dict(self._counts)

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
        market: MarketCandidate, activity: _Activity | None, now: datetime
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
        trade_count = len(activity.trades) if activity is not None else 0
        update_count = len(activity.updates) if activity is not None else 0
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
