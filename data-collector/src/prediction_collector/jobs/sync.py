from __future__ import annotations

from prediction_collector.kalshi.service import KalshiService
from prediction_collector.polymarket.service import PolymarketService


async def sync_enabled_metadata(
    polymarket: PolymarketService | None,
    kalshi: KalshiService | None,
    *,
    include_historical: bool = False,
) -> dict[str, object]:
    result: dict[str, object] = {}
    if polymarket:
        result["polymarket"] = await polymarket.sync_metadata(
            include_closed=include_historical
        )
    if kalshi:
        result["kalshi"] = await kalshi.sync_metadata(
            include_historical=include_historical
        )
    return result

