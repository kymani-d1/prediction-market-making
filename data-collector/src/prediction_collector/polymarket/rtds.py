from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from decimal import Decimal
from typing import Any

from websockets.asyncio.client import connect

from prediction_collector.common.records import raw_ws_item
from prediction_collector.common.utils import (
    as_decimal,
    content_hash,
    first_present,
    parse_timestamp,
    utc_now,
)
from prediction_collector.database import Database
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)


class PolymarketRtdsWebSocket:
    def __init__(
        self,
        *,
        url: str,
        writer: BatchWriter,
        database: Database,
        metrics: ThroughputMetrics,
        store_raw: bool,
        equity_symbols: frozenset[str],
        comments_enabled: bool,
    ) -> None:
        self.url = url
        self.writer = writer
        self.database = database
        self.metrics = metrics
        self.store_raw = store_raw
        self.equity_symbols = equity_symbols
        self.comments_enabled = comments_enabled

    def subscriptions(self) -> list[dict[str, str]]:
        subscriptions: list[dict[str, str]] = [
            {"topic": "crypto_prices", "type": "update"},
            {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""},
            {"topic": "crypto_prices_twap_thirty", "type": "update", "filters": ""},
            {"topic": "crypto_prices_twap_sixty", "type": "update", "filters": ""},
        ]
        subscriptions.extend(
            {
                "topic": "equity_prices",
                "type": "*",
                "filters": json.dumps({"symbol": symbol}, separators=(",", ":")),
            }
            for symbol in sorted(self.equity_symbols)
        )
        if self.comments_enabled:
            subscriptions.extend(
                {"topic": "comments", "type": message_type}
                for message_type in (
                    "comment_created",
                    "comment_removed",
                    "reaction_created",
                    "reaction_removed",
                )
            )
        return subscriptions

    async def run(self, *, run_id: int | None, stop: asyncio.Event) -> None:
        reconnect_attempt = 0
        unresolved_gaps: dict[str, list[int]] = {}
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
                    subscriptions = self.subscriptions()
                    connection_id = await self.database.create_connection(
                        run_id=run_id,
                        exchange="polymarket",
                        channel="rtds",
                        endpoint=self.url,
                        reconnect_attempt=reconnect_attempt,
                        subscribed_market_count=0,
                        metadata={"subscriptions": len(subscriptions)},
                    )
                    await websocket.send(
                        json.dumps({"action": "subscribe", "subscriptions": subscriptions})
                    )
                    LOGGER.info(
                        "Polymarket RTDS subscribed",
                        extra={"subscriptions": len(subscriptions)},
                    )
                    heartbeat = asyncio.create_task(
                        self._heartbeat(websocket, stop), name="polymarket-rtds-heartbeat"
                    )
                    try:
                        async for frame in websocket:
                            received_at = utc_now()
                            if first_message_at is None:
                                first_message_at = received_at
                            last_message_at = received_at
                            monotonic_ns = time.monotonic_ns()
                            if isinstance(frame, bytes):
                                frame = frame.decode("utf-8", errors="replace")
                            if frame == "PONG":
                                continue
                            messages += 1
                            await self.metrics.message("polymarket_rtds")
                            try:
                                decoded = json.loads(frame)
                            except json.JSONDecodeError:
                                dropped += 1
                                LOGGER.warning("Malformed Polymarket RTDS JSON frame")
                                continue
                            for envelope in decoded if isinstance(decoded, list) else [decoded]:
                                if isinstance(envelope, dict):
                                    topic = str(envelope.get("topic") or "unknown")
                                    for gap_id in unresolved_gaps.pop(topic, []):
                                        await self.database.resolve_gap(
                                            gap_id,
                                            action="first_message_after_reconnect",
                                        )
                                    await self._handle(
                                        envelope,
                                        connection_id=connection_id,
                                        received_at=received_at,
                                        monotonic_ns=monotonic_ns,
                                    )
                    finally:
                        heartbeat.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await heartbeat
                if stop.is_set():
                    reconnect_attempt = 0
                else:
                    reconnect_attempt += 1
                    reason = "connection_closed"
            except asyncio.CancelledError:
                reason = "task_cancelled"
                raise
            except Exception as exc:
                reconnect_attempt += 1
                reason = f"{type(exc).__name__}: {exc}"
                LOGGER.warning(
                    "Polymarket RTDS disconnected",
                    extra={"attempt": reconnect_attempt, "error": reason},
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
                        for topic in sorted(
                            {
                                str(subscription["topic"])
                                for subscription in self.subscriptions()
                            }
                        ):
                            gap_id = await self.database.record_gap(
                                run_id=run_id,
                                connection_id=connection_id,
                                exchange="polymarket",
                                channel=f"rtds:{topic}",
                                market_external_id=None,
                                outcome_external_id=None,
                                gap_type="connection_unknown_gap",
                                reconnect_reason=reason or "connection_closed",
                            )
                            unresolved_gaps.setdefault(topic, []).append(gap_id)
            if not stop.is_set():
                await _sleep(stop, min(30.0, 0.5 * (2 ** min(reconnect_attempt, 6))))

    async def _heartbeat(self, websocket: Any, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _sleep(stop, 5)
            if not stop.is_set():
                await websocket.send("PING")

    async def _handle(
        self,
        envelope: dict[str, Any],
        *,
        connection_id: int,
        received_at: Any,
        monotonic_ns: int,
    ) -> None:
        topic = str(envelope.get("topic") or "unknown")
        message_type = str(envelope.get("type") or "unknown")
        payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
        publisher_timestamp = parse_timestamp(envelope.get("timestamp"))
        source_timestamp = parse_timestamp(payload.get("timestamp"))
        symbol = first_present(payload, "symbol", "ticker", "feed_id")
        if self.store_raw:
            await self.writer.put(
                raw_ws_item(
                    exchange="polymarket",
                    channel=f"rtds:{topic}",
                    connection_id=connection_id,
                    market_external_id=None,
                    outcome_external_id=str(symbol) if symbol is not None else None,
                    message_type=message_type,
                    source_timestamp=source_timestamp,
                    exchange_timestamp=publisher_timestamp,
                    received_at=received_at,
                    received_monotonic_ns=monotonic_ns,
                    sequence_number=None,
                    book_hash=None,
                    payload=envelope,
                    source_timestamp_raw=(
                        str(payload.get("timestamp"))
                        if payload.get("timestamp") is not None
                        else None
                    ),
                    exchange_timestamp_raw=(
                        str(envelope.get("timestamp"))
                        if envelope.get("timestamp") is not None
                        else None
                    ),
                )
            )

        if topic in {
            "crypto_prices",
            "crypto_prices_chainlink",
            "crypto_prices_twap_thirty",
            "crypto_prices_twap_sixty",
            "equity_prices",
        }:
            provider = {
                "crypto_prices": "binance",
                "crypto_prices_chainlink": "chainlink_spot",
                "crypto_prices_twap_thirty": "chainlink_twap_30s",
                "crypto_prices_twap_sixty": "chainlink_twap_60s",
                "equity_prices": "pyth",
            }[topic]
            if isinstance(payload.get("data"), list):
                for point in payload["data"]:
                    if isinstance(point, dict):
                        await self._price_item(
                            topic,
                            provider,
                            str(symbol or point.get("symbol") or "unknown"),
                            point,
                            envelope,
                            connection_id,
                            publisher_timestamp,
                            received_at,
                            monotonic_ns,
                            delivery_mode="snapshot",
                        )
            else:
                await self._price_item(
                    topic,
                    provider,
                    str(symbol or "unknown"),
                    payload,
                    envelope,
                    connection_id,
                    publisher_timestamp,
                    received_at,
                    monotonic_ns,
                )
            return

        if topic == "comments" and message_type == "comment_created":
            profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
            comment_id = str(payload.get("id") or content_hash(payload))
            parent_type = str(payload.get("parentEntityType") or "").lower()
            await self.writer.put(
                WriteItem(
                    "comments",
                    {
                        "exchange": "polymarket",
                        "external_comment_id": comment_id,
                        "dedup_hash": content_hash(
                            {"id": comment_id, "created_at": payload.get("createdAt")}
                        ),
                        "parent_entity_type": parent_type,
                        "parent_entity_id": str(payload.get("parentEntityID") or ""),
                        "parent_external_comment_id": payload.get("parentCommentID"),
                        "public_identifier": first_present(
                            payload, "userAddress", "replyAddress"
                        ),
                        "profile_name": first_present(profile, "name", "pseudonym"),
                        "body": str(payload.get("body") or ""),
                        "source_created_at": parse_timestamp(payload.get("createdAt")),
                        "source_updated_at": parse_timestamp(payload.get("updatedAt")),
                        "source_timestamp": parse_timestamp(payload.get("createdAt")),
                        "exchange_timestamp": publisher_timestamp,
                        "received_at": received_at,
                        "received_monotonic_ns": monotonic_ns,
                        "raw_data": envelope,
                    },
                )
            )

    async def _price_item(
        self,
        topic: str,
        provider: str,
        symbol: str,
        point: dict[str, Any],
        envelope: dict[str, Any],
        connection_id: int,
        publisher_timestamp: Any,
        received_at: Any,
        monotonic_ns: int,
        delivery_mode: str = "live",
    ) -> None:
        # Accuracy semantics differ by topic. Equity exact values are decimal;
        # Chainlink TWAP exact values are signed E18 integers; the documented
        # Chainlink spot contract exposes ``value`` as the quote-currency
        # decimal and does not define full_accuracy_value.
        exact_value = as_decimal(point.get("full_accuracy_value"))
        display_value = as_decimal(point.get("value"))
        if exact_value is not None and topic in {
            "crypto_prices_twap_thirty",
            "crypto_prices_twap_sixty",
        }:
            # Chainlink TWAP full_accuracy_value is a signed E18 integer,
            # unlike the already-decimal exact values on the equity feed.
            price = exact_value / Decimal("1000000000000000000")
        elif exact_value is not None and topic == "equity_prices":
            price = exact_value
        else:
            price = display_value
        if price is None:
            return
        source_timestamp = parse_timestamp(point.get("timestamp"))
        external_update_id = content_hash(
            {
                "topic": topic,
                "symbol": symbol,
                "timestamp": point.get("timestamp"),
                "value": str(point.get("full_accuracy_value") or point.get("value")),
            }
        )
        await self.writer.put(
            WriteItem(
                "reference_price_updates",
                {
                    "delivery_exchange": "polymarket",
                    "provider": provider,
                    "external_instrument_id": symbol.lower(),
                    "external_update_id": external_update_id,
                    "connection_id": connection_id,
                    "source_timestamp": source_timestamp,
                    "exchange_timestamp": publisher_timestamp,
                    "source_timestamp_raw": (
                        str(point.get("timestamp"))
                        if point.get("timestamp") is not None
                        else None
                    ),
                    "exchange_timestamp_raw": (
                        str(envelope.get("timestamp"))
                        if envelope.get("timestamp") is not None
                        else None
                    ),
                    "received_at": received_at,
                    "received_monotonic_ns": monotonic_ns,
                    "sequence_number": None,
                    "price": price,
                    "bid": None,
                    "ask": None,
                    "confidence_interval": as_decimal(point.get("confidence")),
                    "publish_slot": point.get("publish_slot"),
                    "source_status": (
                        f"{delivery_mode}_carried_forward"
                        if point.get("is_carried_forward")
                        else delivery_mode
                    ),
                    "raw_data": {"envelope": envelope, "point": point},
                },
            )
        )


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass
