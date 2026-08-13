from __future__ import annotations

from prediction_collector.polymarket.service import PolymarketService


async def sync_metadata(
    polymarket: PolymarketService,
    *,
    include_historical: bool = False,
) -> dict[str, object]:
    return {
        "polymarket": await polymarket.sync_metadata(
            include_closed=include_historical
        )
    }
