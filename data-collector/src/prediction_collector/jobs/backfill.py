from __future__ import annotations

import logging
from dataclasses import dataclass

from prediction_collector.kalshi.service import KalshiService
from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import BatchWriter


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BackfillResult:
    records_processed: int
    rows_written: int
    details: dict[str, object]
    status: str


async def run_polymarket_backfill(
    service: PolymarketService, writer: BatchWriter
) -> BackfillResult:
    await writer.start()
    details: dict[str, object] = {}
    try:
        details["metadata"] = await service.sync_metadata(include_closed=True)
        details["fees_incentives"] = await service.sync_fees_and_incentives(
            include_fee_rates=True,
            include_rewards=True,
            fee_rate_live_only=False,
        )
        details["trades"] = await service.backfill_trades()
        details["comments"] = await service.backfill_comments()
        details["market_data"] = await service.backfill_market_data()
        await writer.queue.join()
    finally:
        await writer.stop()
    details["write_failures"] = writer.failed_items
    processed = _sum_numbers(details)
    trade_details = details.get("trades")
    incomplete_trade_history = bool(
        isinstance(trade_details, dict)
        and (
            trade_details.get("saturated_markets", 0)
            or trade_details.get("history_floor_markets", 0)
        )
    )
    fee_details = details.get("fees_incentives")
    incomplete_fee_history = bool(
        isinstance(fee_details, dict) and fee_details.get("errors", 0)
    )
    comment_details = details.get("comments")
    incomplete_comments = bool(
        isinstance(comment_details, dict) and comment_details.get("errors", 0)
    )
    return BackfillResult(
        processed,
        writer.rows_written,
        details,
        (
            "partial"
            if (
                writer.failed_items
                or incomplete_trade_history
                or incomplete_fee_history
                or incomplete_comments
            )
            else "completed"
        ),
    )


async def run_kalshi_backfill(
    service: KalshiService, writer: BatchWriter
) -> BackfillResult:
    await writer.start()
    details: dict[str, object] = {}
    try:
        details["metadata"] = await service.sync_metadata(include_historical=True)
        details["fees_incentives"] = await service.sync_fees_and_incentives()
        details["trades"] = await service.backfill_trades()
        details["market_data"] = await service.backfill_market_data()
        await writer.queue.join()
    finally:
        await writer.stop()
    details["write_failures"] = writer.failed_items
    processed = _sum_numbers(details)
    incomplete_sections = any(
        isinstance(details.get(section), dict)
        and bool(details[section].get("errors", 0))  # type: ignore[union-attr]
        for section in ("fees_incentives", "market_data")
    )
    return BackfillResult(
        processed,
        writer.rows_written,
        details,
        "partial" if writer.failed_items or incomplete_sections else "completed",
    )


def _sum_numbers(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        return sum(_sum_numbers(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return sum(_sum_numbers(item) for item in value)
    return 0
