from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from websockets.asyncio.client import connect

from prediction_collector.common.records import raw_ws_item
from prediction_collector.common.utils import content_hash, parse_timestamp, utc_now
from prediction_collector.database import Database
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)


class PolymarketSportsWebSocket:
    def __init__(
        self,
        *,
        url: str,
        writer: BatchWriter,
        database: Database,
        metrics: ThroughputMetrics,
        store_raw: bool,
    ) -> None:
        self.url = url
        self.writer = writer
        self.database = database
        self.metrics = metrics
        self.store_raw = store_raw

    async def run(self, *, run_id: int | None, stop: asyncio.Event) -> None:
        attempt = 0
        unresolved_gaps: list[int] = []
        while not stop.is_set():
            connection_id: int | None = None
            messages = 0
            dropped = 0
            first_message_at: Any | None = None
            last_message_at: Any | None = None
            reason: str | None = None
            try:
                async with connect(
                    self.url,
                    ping_interval=None,
                    open_timeout=30,
                    close_timeout=10,
                    max_queue=2048,
                ) as websocket:
                    connection_id = await self.database.create_connection(
                        run_id=run_id,
                        exchange="polymarket",
                        channel="sports",
                        endpoint=self.url,
                        reconnect_attempt=attempt,
                        subscribed_market_count=0,
                        metadata={"coverage": "all_active_sports_events"},
                    )
                    LOGGER.info("Polymarket sports WebSocket connected")
                    async for frame in websocket:
                        received_at = utc_now()
                        if first_message_at is None:
                            first_message_at = received_at
                        last_message_at = received_at
                        monotonic_ns = time.monotonic_ns()
                        if isinstance(frame, bytes):
                            frame = frame.decode("utf-8", errors="replace")
                        if frame == "ping":
                            await websocket.send("pong")
                            continue
                        messages += 1
                        await self.metrics.message("polymarket_sports")
                        try:
                            raw = json.loads(frame)
                        except json.JSONDecodeError:
                            dropped += 1
                            LOGGER.warning("Malformed Polymarket sports JSON frame")
                            continue
                        if not isinstance(raw, dict):
                            dropped += 1
                            continue
                        for gap_id in unresolved_gaps:
                            await self.database.resolve_gap(
                                gap_id, action="first_message_after_reconnect"
                            )
                        unresolved_gaps.clear()
                        external_id = str(raw.get("gameId") or raw.get("slug") or "unknown")
                        source_timestamp = parse_timestamp(
                            raw.get("last_update") or raw.get("finishedAt")
                        )
                        if self.store_raw:
                            await self.writer.put(
                                raw_ws_item(
                                    exchange="polymarket",
                                    channel="sports",
                                    connection_id=connection_id,
                                    market_external_id=None,
                                    outcome_external_id=external_id,
                                    message_type="sport_result",
                                    source_timestamp=source_timestamp,
                                    exchange_timestamp=None,
                                    received_at=received_at,
                                    received_monotonic_ns=monotonic_ns,
                                    sequence_number=None,
                                    book_hash=None,
                                    payload=raw,
                                    source_timestamp_raw=(
                                        str(raw.get("last_update") or raw.get("finishedAt"))
                                        if raw.get("last_update") is not None
                                        or raw.get("finishedAt") is not None
                                        else None
                                    ),
                                )
                            )
                        home_score, away_score = _score(raw.get("score"))
                        await self.writer.put(
                            WriteItem(
                                "sports_feed_updates",
                                {
                                    "delivery_exchange": "polymarket",
                                    "provider": "polymarket_sports",
                                    "external_event_id": external_id,
                                    "external_update_id": content_hash(raw),
                                    "connection_id": connection_id,
                                    "update_type": "sport_result",
                                    "status": raw.get("status"),
                                    "period": raw.get("period"),
                                    "clock": raw.get("elapsed"),
                                    "home_score": home_score,
                                    "away_score": away_score,
                                    "source_timestamp": source_timestamp,
                                    "exchange_timestamp": None,
                                    "source_timestamp_raw": (
                                        str(raw.get("last_update") or raw.get("finishedAt"))
                                        if raw.get("last_update") is not None
                                        or raw.get("finishedAt") is not None
                                        else None
                                    ),
                                    "exchange_timestamp_raw": None,
                                    "received_at": received_at,
                                    "received_monotonic_ns": monotonic_ns,
                                    "sequence_number": None,
                                    "state": raw,
                                    "raw_data": raw,
                                },
                            )
                        )
                if stop.is_set():
                    attempt = 0
                else:
                    attempt += 1
                    reason = "connection_closed"
            except asyncio.CancelledError:
                reason = "task_cancelled"
                raise
            except Exception as exc:
                attempt += 1
                reason = f"{type(exc).__name__}: {exc}"
                LOGGER.warning(
                    "Polymarket sports WebSocket disconnected",
                    extra={"attempt": attempt, "error": reason},
                )
            finally:
                if connection_id is not None:
                    await self.database.update_connection_stats(
                        connection_id,
                        messages_received=messages,
                        messages_dropped=dropped,
                        first_message_at=first_message_at,
                        last_message_at=last_message_at,
                    )
                    await self.database.close_connection(
                        connection_id,
                        reason=reason or "connection_closed",
                        failed=reason not in {None, "task_cancelled"},
                    )
                    if not stop.is_set():
                        unresolved_gaps.append(
                            await self.database.record_gap(
                                run_id=run_id,
                                connection_id=connection_id,
                                exchange="polymarket",
                                channel="sports",
                                market_external_id=None,
                                outcome_external_id=None,
                                gap_type="connection_unknown_gap",
                                reconnect_reason=reason or "connection_closed",
                            )
                        )
            if not stop.is_set():
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=min(30.0, 0.5 * (2 ** min(attempt, 6)))
                    )
                except TimeoutError:
                    pass


def _score(value: Any) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(value, str) or "-" not in value:
        return None, None
    left, right = value.split("-", 1)
    try:
        return Decimal(left.strip()), Decimal(right.strip())
    except InvalidOperation:
        return None, None
