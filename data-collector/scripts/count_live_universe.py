"""Read-only count of the complete public Polymarket live-event cursor."""

from __future__ import annotations

import argparse
import asyncio
import json
import selectors
import sys
import time
from pathlib import Path

from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.config import Settings
from prediction_collector.polymarket.rest import PolymarketRestClient
from prediction_collector.polymarket.parser import parse_market_candidate


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path)
    return parser.parse_args()


async def run(snapshot: Path | None = None) -> dict[str, int | float | str]:
    settings = Settings.from_env()
    pages = events = markets = trade_ready_markets = 0
    started = time.perf_counter()
    snapshot_handle = None
    if snapshot is not None:
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot_handle = snapshot.open("w", encoding="utf-8")
    async with AsyncHttpClient(
        concurrency=min(settings.http_concurrency, 4),
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
    ) as http:
        client = PolymarketRestClient(
            http,
            gamma_url=settings.polymarket_gamma_url,
            data_url=settings.polymarket_data_url,
            clob_url=settings.polymarket_clob_url,
        )
        async for items, _, _ in client.iter_live_events():
            pages += 1
            for event in items:
                if not bool(event.get("active")) or bool(event.get("closed")):
                    continue
                events += 1
                nested = event.get("markets")
                if not isinstance(nested, list):
                    continue
                for market in nested:
                    if not isinstance(market, dict):
                        continue
                    markets += 1
                    candidate = parse_market_candidate(market)
                    if candidate.active and candidate.external_id and snapshot_handle:
                        candidate.source_id = (
                            str(market["id"])
                            if market.get("id") is not None
                            else None
                        )
                        candidate.event_external_id = (
                            str(event["id"])
                            if event.get("id") is not None
                            else None
                        )
                        snapshot_handle.write(
                            json.dumps(
                                {
                                    "exchange": candidate.exchange,
                                    "external_id": candidate.external_id,
                                    "ticker": candidate.ticker,
                                    "status": candidate.status,
                                    "active": candidate.active,
                                    "tradable": candidate.tradable,
                                    "closed": candidate.closed,
                                    "archived": candidate.archived,
                                    "accepting_orders": candidate.accepting_orders,
                                    "enable_order_book": candidate.enable_order_book,
                                    "has_maker_rewards": candidate.has_maker_rewards,
                                    "spread": candidate.spread,
                                    "close_time": candidate.close_time,
                                    "volume": candidate.volume,
                                    "volume_24h": candidate.volume_24h,
                                    "liquidity": candidate.liquidity,
                                    "outcome_token_ids": candidate.outcome_token_ids,
                                    "aliases": candidate.aliases,
                                    "source_id": candidate.source_id,
                                    "event_external_id": candidate.event_external_id,
                                },
                                default=str,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
                    if (
                        bool(market.get("active"))
                        and not bool(market.get("closed"))
                        and bool(market.get("acceptingOrders"))
                        and bool(market.get("enableOrderBook"))
                        and market.get("clobTokenIds")
                    ):
                        trade_ready_markets += 1
    if snapshot_handle is not None:
        snapshot_handle.close()
    result: dict[str, int | float | str] = {
        "pages": pages,
        "active_events": events,
        "nested_markets": markets,
        "trade_ready_markets": trade_ready_markets,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    if snapshot is not None:
        result["snapshot"] = str(snapshot)
    return result


def main() -> None:
    args = arguments()
    loop_factory = (
        (lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        if sys.platform == "win32"
        else None
    )
    print(
        json.dumps(
            asyncio.run(run(args.snapshot), loop_factory=loop_factory), indent=2
        )
    )


if __name__ == "__main__":
    main()
