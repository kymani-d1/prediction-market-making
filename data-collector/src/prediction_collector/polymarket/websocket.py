from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from websockets.asyncio.client import connect

from prediction_collector.common.orderbook import OrderBook
from prediction_collector.common.records import (
    book_snapshot_item,
    book_update_item,
    raw_ws_item,
    trade_item,
)
from prediction_collector.common.utils import (
    as_decimal,
    content_hash,
    first_present,
    parse_timestamp,
    utc_now,
)
from prediction_collector.database import Database
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.polymarket.parser import (
    parse_book,
    parse_price_changes,
    parse_trade,
)
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)


class PolymarketMarketWebSocket:
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
        self.books: dict[str, OrderBook] = {}
        self.last_market_for_asset: dict[str, str] = {}
        self._unknown_types: set[str] = set()

    async def run(
        self,
        asset_to_market: Mapping[str, str],
        *,
        run_id: int | None,
        stop: asyncio.Event,
        connection_label: str,
        planned_stop: asyncio.Event | None = None,
        recovery_gap_ids: tuple[int, ...] = (),
    ) -> None:
        assets = list(dict.fromkeys(asset_to_market))
        if not assets:
            return
        reconnect_attempt = 0
        unresolved_connection_gaps: list[int] = list(recovery_gap_ids)
        while not stop.is_set():
            connection_id: int | None = None
            messages = 0
            dropped = 0
            first_message_at: Any | None = None
            last_message_at: Any | None = None
            disconnect_reason: str | None = None
            subscription_confirmed = False
            initial_assets_seen: set[str] = set()
            try:
                async with connect(
                    self.url,
                    ping_interval=None,
                    close_timeout=10,
                    open_timeout=30,
                    max_queue=2048,
                ) as websocket:
                    connection_id = await self.database.create_connection(
                        run_id=run_id,
                        exchange="polymarket",
                        channel="market",
                        endpoint=self.url,
                        reconnect_attempt=reconnect_attempt,
                        subscribed_market_count=len(set(asset_to_market.values())),
                        subscribed_market_external_ids=set(asset_to_market.values()),
                        pending_subscription=True,
                        metadata={
                            "label": connection_label,
                            "token_count": len(assets),
                            "custom_feature_enabled": True,
                            "initial_dump": True,
                            "level": 2,
                        },
                    )
                    await websocket.send(
                        json.dumps(
                            {
                                "assets_ids": assets,
                                "type": "market",
                                "custom_feature_enabled": True,
                                "initial_dump": True,
                                "level": 2,
                            }
                        )
                    )
                    LOGGER.info(
                        "Polymarket WebSocket subscription sent",
                        extra={
                            "connection": connection_label,
                            "markets": len(set(asset_to_market.values())),
                            "tokens": len(assets),
                        },
                    )
                    heartbeat = asyncio.create_task(
                        self._heartbeat(websocket, stop),
                        name=f"polymarket-heartbeat-{connection_label}",
                    )
                    try:
                        while not stop.is_set():
                            if subscription_confirmed:
                                frame = await websocket.recv()
                            else:
                                frame = await asyncio.wait_for(
                                    websocket.recv(), timeout=30
                                )
                            received_at = utc_now()
                            if first_message_at is None:
                                first_message_at = received_at
                            last_message_at = received_at
                            received_monotonic_ns = time.monotonic_ns()
                            if isinstance(frame, bytes):
                                frame = frame.decode("utf-8", errors="replace")
                            if frame == "PONG":
                                continue
                            try:
                                decoded = json.loads(frame)
                            except json.JSONDecodeError:
                                messages += 1
                                dropped += 1
                                await self.metrics.message("polymarket")
                                LOGGER.warning("Malformed Polymarket WebSocket JSON frame")
                                continue
                            envelopes = decoded if isinstance(decoded, list) else [decoded]
                            messages += len(envelopes)
                            await self.metrics.message("polymarket", len(envelopes))
                            for item in envelopes:
                                if (
                                    isinstance(item, dict)
                                    and str(item.get("event_type") or item.get("type"))
                                    == "book"
                                ):
                                    initial_asset = first_present(
                                        item, "asset_id", "asset", "token_id"
                                    )
                                    if initial_asset is not None:
                                        initial_assets_seen.add(str(initial_asset))
                            if (
                                not subscription_confirmed
                                and set(assets).issubset(initial_assets_seen)
                            ):
                                await self.database.confirm_subscription(
                                    connection_id,
                                    exchange="polymarket",
                                    channel="market",
                                    market_external_ids=set(asset_to_market.values()),
                                    acknowledgement={"event_type": "initial_book_dump"},
                                )
                                for gap_id in unresolved_connection_gaps:
                                    await self.database.resolve_gap(
                                        gap_id,
                                        action="initial_book_dump_after_reconnect",
                                    )
                                unresolved_connection_gaps.clear()
                                subscription_confirmed = True
                                LOGGER.info(
                                    "Polymarket WebSocket subscription confirmed",
                                    extra={
                                        "connection": connection_label,
                                        "markets": len(set(asset_to_market.values())),
                                        "tokens": len(assets),
                                    },
                                )
                            for envelope in envelopes:
                                if isinstance(envelope, dict):
                                    event_type = str(
                                        envelope.get("event_type")
                                        or envelope.get("type")
                                        or "unknown"
                                    )
                                    if event_type == "error":
                                        raise RuntimeError(
                                            f"Polymarket subscription error: {envelope}"
                                        )
                                    await self._handle(
                                        envelope,
                                        connection_id=connection_id,
                                        received_at=received_at,
                                        received_monotonic_ns=received_monotonic_ns,
                                    )
                    finally:
                        heartbeat.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await heartbeat
                reconnect_attempt = 0
            except asyncio.CancelledError:
                disconnect_reason = (
                    "planned_subscription_refresh"
                    if planned_stop is not None and planned_stop.is_set()
                    else "task_cancelled"
                )
                raise
            except Exception as exc:
                disconnect_reason = f"{type(exc).__name__}: {exc}"
                reconnect_attempt += 1
                LOGGER.warning(
                    "Polymarket WebSocket disconnected",
                    extra={
                        "connection": connection_label,
                        "attempt": reconnect_attempt,
                        "error": disconnect_reason,
                    },
                )
            finally:
                if connection_id is not None:
                    if not stop.is_set():
                        for asset_id in assets:
                            self.books.setdefault(asset_id, OrderBook()).valid = False
                    await self.database.update_connection_stats(
                        connection_id,
                        messages_received=messages,
                        messages_dropped=dropped,
                        first_message_at=first_message_at,
                        last_message_at=last_message_at,
                    )
                    await self.database.close_connection(
                        connection_id,
                        reason=disconnect_reason or "connection_closed",
                        failed=disconnect_reason
                        not in {
                            None,
                            "task_cancelled",
                            "planned_subscription_refresh",
                        },
                    )
                    if (
                        not stop.is_set()
                        and not (
                            planned_stop is not None and planned_stop.is_set()
                        )
                    ):
                        gap_id = await self.database.record_gap(
                            run_id=run_id,
                            connection_id=connection_id,
                            exchange="polymarket",
                            channel="market",
                            market_external_id=None,
                            outcome_external_id=None,
                            gap_type="connection_unknown_gap",
                            reconnect_reason=disconnect_reason or "connection_closed",
                            details={"token_count": len(assets)},
                        )
                        unresolved_connection_gaps.append(gap_id)
            if not stop.is_set():
                delay = min(30.0, 0.5 * (2 ** min(reconnect_attempt, 6)))
                await _stop_aware_sleep(stop, delay)

    async def _heartbeat(self, websocket: Any, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _stop_aware_sleep(stop, 10)
            if not stop.is_set():
                await websocket.send("PING")

    async def _handle(
        self,
        raw: dict[str, Any],
        *,
        connection_id: int,
        received_at: Any,
        received_monotonic_ns: int,
    ) -> None:
        event_type = str(raw.get("event_type") or raw.get("type") or "unknown")
        market = first_present(raw, "market", "condition_id", "conditionId")
        asset = first_present(raw, "asset_id", "asset", "token_id")
        if asset is not None and market is not None:
            self.last_market_for_asset[str(asset)] = str(market)
        exchange_timestamp = parse_timestamp(raw.get("timestamp"))
        if self.store_raw:
            await self.writer.put(
                raw_ws_item(
                    exchange="polymarket",
                    channel="market",
                    connection_id=connection_id,
                    market_external_id=str(market) if market is not None else None,
                    outcome_external_id=str(asset) if asset is not None else None,
                    message_type=event_type,
                    source_timestamp=None,
                    exchange_timestamp=exchange_timestamp,
                    received_at=received_at,
                    received_monotonic_ns=received_monotonic_ns,
                    sequence_number=None,
                    book_hash=str(raw.get("hash")) if raw.get("hash") else None,
                    payload=raw,
                    exchange_timestamp_raw=(
                        str(raw.get("timestamp"))
                        if raw.get("timestamp") is not None
                        else None
                    ),
                )
            )

        if event_type == "book":
            parsed = parse_book(
                raw,
                received_at=received_at,
                received_monotonic_ns=received_monotonic_ns,
            )
            if parsed:
                book = self.books.setdefault(parsed.outcome_external_id or "", OrderBook())
                book.reset(
                    parsed.bids,
                    parsed.asks,
                    book_hash=parsed.book_hash,
                    sequence=parsed.sequence_number,
                )
                await self.writer.put(book_snapshot_item(parsed, connection_id))
            return

        if event_type == "price_change":
            for update in parse_price_changes(
                raw,
                received_at=received_at,
                received_monotonic_ns=received_monotonic_ns,
            ):
                # The nested price_change hash identifies the causing order; it
                # is not the full-book hash and must not replace book.hash.
                update = replace(update, book_hash=None)
                book = self.books.setdefault(update.outcome_external_id or "", OrderBook())
                if book.valid and update.size is not None:
                    book.apply_absolute(update.side, update.price, update.size)
                await self.writer.put(book_update_item(update, connection_id))
            return

        if event_type == "last_trade_price":
            parsed_trade = parse_trade(
                raw,
                received_at=received_at,
                received_monotonic_ns=received_monotonic_ns,
            )
            if parsed_trade:
                await self.writer.put(trade_item(parsed_trade, connection_id))
            return

        if event_type in {
            "tick_size_change",
            "new_market",
            "market_resolved",
            "best_bid_ask",
        }:
            if event_type != "best_bid_ask" and market is not None:
                await self.writer.put(
                    WriteItem(
                        "market_lifecycle_events",
                        {
                            "exchange": "polymarket",
                            "market_external_id": str(market),
                            "outcome_external_id": str(asset) if asset is not None else None,
                            "connection_id": connection_id,
                            "external_event_id": None,
                            "dedup_hash": content_hash(
                                {"event_type": event_type, "payload": raw}
                            ),
                            "event_type": event_type,
                            "previous_status": None,
                            "new_status": "resolved" if event_type == "market_resolved" else None,
                            "source_timestamp": None,
                            "exchange_timestamp": exchange_timestamp,
                            "source_timestamp_raw": None,
                            "exchange_timestamp_raw": (
                                str(raw.get("timestamp"))
                                if raw.get("timestamp") is not None
                                else None
                            ),
                            "received_at": received_at,
                            "received_monotonic_ns": received_monotonic_ns,
                            "sequence_number": None,
                            "details": {
                                "old_tick_size": raw.get("old_tick_size"),
                                "new_tick_size": raw.get("new_tick_size"),
                                "winning_asset_id": raw.get("winning_asset_id"),
                                "winning_outcome": raw.get("winning_outcome"),
                            },
                            "raw_data": raw,
                        },
                    )
                )
                updates = _polymarket_lifecycle_updates(event_type, raw)
                if updates:
                    updated = await self.database.apply_market_metadata_patch(
                        exchange="polymarket",
                        market_external_id=str(market),
                        updates=updates,
                        lifecycle_payload=raw,
                        source_timestamp=None,
                        exchange_timestamp=exchange_timestamp,
                        observed_at=received_at,
                    )
                    if not updated:
                        LOGGER.info(
                            "Polymarket lifecycle arrived before market metadata",
                            extra={"market": str(market), "event_type": event_type},
                        )
            return

        if event_type not in self._unknown_types:
            self._unknown_types.add(event_type)
            LOGGER.warning(
                "Unknown Polymarket WebSocket message type",
                extra={"message_type": event_type},
            )

    def market_snapshot_items(self, connection_id: int | None = None) -> list[WriteItem]:
        now = utc_now()
        monotonic = time.monotonic_ns()
        items: list[WriteItem] = []
        for asset_id, book in self.books.items():
            if not book.valid:
                continue
            market = self.last_market_for_asset.get(asset_id)
            if not market:
                continue
            items.append(
                WriteItem(
                    "market_snapshots",
                    {
                        "exchange": "polymarket",
                        "market_external_id": market,
                        "outcome_external_id": asset_id,
                        "connection_id": connection_id,
                        "observed_at": now,
                        "source_timestamp": None,
                        "exchange_timestamp": None,
                        "received_at": now,
                        "received_monotonic_ns": monotonic,
                        "sequence_number": book.sequence,
                        "book_hash": book.book_hash,
                        "best_bid": book.best_bid,
                        "best_ask": book.best_ask,
                        "midpoint": book.midpoint,
                        "spread": book.spread,
                        "last_trade_price": None,
                        "bid_depth": book.bid_depth,
                        "ask_depth": book.ask_depth,
                        "volume": None,
                        "open_interest": None,
                        "liquidity": None,
                        "raw_data": {},
                    },
                )
            )
        return items


async def _stop_aware_sleep(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


def _polymarket_lifecycle_updates(
    event_type: str, raw: dict[str, Any]
) -> dict[str, Any]:
    if event_type == "tick_size_change":
        tick_size = as_decimal(raw.get("new_tick_size"))
        return {"tick_size": tick_size} if tick_size is not None else {}
    if event_type == "new_market":
        if raw.get("active") is True:
            return {"status": "active", "is_active": True}
        if raw.get("active") is False:
            return {"status": "created", "is_active": False}
        return {}
    if event_type == "market_resolved":
        updates: dict[str, Any] = {
            "status": "resolved",
            "is_active": False,
            "is_tradable": False,
            "accepting_orders": False,
        }
        result = first_present(raw, "winning_outcome", "winning_asset_id", "result")
        if result is not None:
            updates["result"] = result
        # Resolution is not settlement; do not fabricate settlement_time from
        # this message's exchange timestamp.
        return updates
    return {}
