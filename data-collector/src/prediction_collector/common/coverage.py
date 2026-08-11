from __future__ import annotations

from decimal import Decimal
from collections.abc import Iterable, Mapping

from prediction_collector.common.types import (
    LiveSelection,
    MarketCandidate,
    MarketExclusion,
)


def _metric(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def _rank_key(market: MarketCandidate) -> tuple[Decimal, Decimal, Decimal, str, str]:
    # Deterministic: liquidity, then 24h volume, then total volume descending;
    # exchange/external identifier ascending for a stable tie break.
    return (
        -_metric(market.liquidity),
        -_metric(market.volume_24h),
        -_metric(market.volume),
        market.exchange,
        market.external_id,
    )


def select_live_markets(
    markets: Iterable[MarketCandidate],
    *,
    max_markets: int = 0,
    min_volume: Decimal = Decimal("0"),
    min_liquidity: Decimal = Decimal("0"),
    allowlist: frozenset[str] = frozenset(),
    blocklist: frozenset[str] = frozenset(),
    unavailable_exchanges: Mapping[str, str] | None = None,
) -> LiveSelection:
    if max_markets < 0 or min_volume < 0 or min_liquidity < 0:
        raise ValueError("live market limits cannot be negative")

    discovered_markets = list(markets)
    unavailable_exchanges = unavailable_exchanges or {}
    active_count = sum(m.active for m in discovered_markets)
    tradable_count = sum(m.tradable for m in discovered_markets)
    selected: list[MarketCandidate] = []
    excluded: list[MarketExclusion] = []

    for market in discovered_markets:
        reason: str | None = None
        if not market.active:
            reason = "inactive"
        elif not market.tradable:
            reason = "not_tradable"
        elif market.exchange in unavailable_exchanges:
            reason = unavailable_exchanges[market.exchange]
        elif market.selectors & blocklist:
            reason = "blocklist"
        elif allowlist and not market.selectors & allowlist:
            reason = "not_in_allowlist"
        elif min_volume and _metric(market.volume) < min_volume:
            reason = "below_min_volume"
        elif min_liquidity and _metric(market.liquidity) < min_liquidity:
            reason = "below_min_liquidity"

        if reason:
            excluded.append(MarketExclusion(market.exchange, market.external_id, reason))
        else:
            selected.append(market)

    selected.sort(key=_rank_key)
    if max_markets and len(selected) > max_markets:
        for market in selected[max_markets:]:
            excluded.append(
                MarketExclusion(market.exchange, market.external_id, "max_live_markets_cap")
            )
        selected = selected[:max_markets]

    return LiveSelection(
        discovered=len(discovered_markets),
        active=active_count,
        tradable=tradable_count,
        subscribed=selected,
        excluded=excluded,
    )
