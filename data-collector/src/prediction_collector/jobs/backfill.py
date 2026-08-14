from __future__ import annotations

from dataclasses import dataclass

from prediction_collector.polymarket.service import PolymarketService
from prediction_collector.writer import BatchWriter


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
        if writer.tier_manager is not None:
            assignments = writer.tier_manager.evaluate(
                await service.database.live_candidates("polymarket")
            )
            await service.database.record_tier_assignments(assignments)
            details["tiers"] = writer.tier_manager.counts()
        details["fees_incentives"] = await service.sync_fees_and_incentives(
            include_fee_rates=True,
            include_rewards=True,
            fee_rate_live_only=False,
        )
        details["trades"] = await service.backfill_trades()
        details["comments"] = await service.backfill_comments()
        details["market_data"] = await service.backfill_market_data()
        await writer.queue.join()
        if writer.archive is not None:
            await writer.archive.queue.join()
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
    incomplete_economics = bool(
        isinstance(details.get("fees_incentives"), dict)
        and details["fees_incentives"].get("errors", 0)  # type: ignore[union-attr]
    )
    incomplete_comments = bool(
        isinstance(details.get("comments"), dict)
        and details["comments"].get("errors", 0)  # type: ignore[union-attr]
    )
    incomplete_metadata = bool(
        isinstance(details.get("metadata"), dict)
        and details["metadata"].get(  # type: ignore[union-attr]
            "malformed_markets_skipped", 0
        )
    )
    archive_degraded = bool(writer.archive and writer.archive.degraded)
    return BackfillResult(
        processed,
        writer.rows_written,
        details,
        (
            "partial"
            if (
                writer.failed_items
                or archive_degraded
                or incomplete_trade_history
                or incomplete_economics
                or incomplete_comments
                or incomplete_metadata
            )
            else "completed"
        ),
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
