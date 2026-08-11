from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Sequence
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
    as_int,
    content_hash,
    first_present,
    parse_timestamp,
    utc_now,
)
from prediction_collector.database import Database
from prediction_collector.kalshi.auth import KalshiSigner
from prediction_collector.kalshi.parser import (
    normalise_event,
    parse_orderbook_delta,
    parse_orderbook_snapshot,
    parse_trade,
)
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.writer import BatchWriter, WriteItem


LOGGER = logging.getLogger(__name__)


class KalshiWebSocket:
    def __init__(
        self,
        *,
        url: str,
        signer: KalshiSigner,
        writer: BatchWriter,
        database: Database,
        metrics: ThroughputMetrics,
        store_raw: bool,
    ) -> None:
        self.url = url
        self.signer = signer
        self.writer = writer
        self.database = database
        self.metrics = metrics
        self.store_raw = store_raw
        self.books: dict[str, OrderBook] = {}
        self._open_sequence_gaps: dict[str, list[int]] = {}
        self._unknown_types: set[str] = set()

    async def run_market_chunk(
        self,
        tickers: Sequence[str],
        *,
        run_id: int | None,
        stop: asyncio.Event,
        connection_label: str,
        planned_stop: asyncio.Event | None = None,
        recovery_gap_ids: tuple[int, ...] = (),
    ) -> None:
        unique_tickers = list(dict.fromkeys(tickers))
        if not unique_tickers:
            return
        await self._run(
            channels=["orderbook_delta", "trade", "ticker"],
            tickers=unique_tickers,
            run_id=run_id,
            stop=stop,
            connection_label=connection_label,
            global_stream=False,
            planned_stop=planned_stop,
            recovery_gap_ids=recovery_gap_ids,
        )

    async def run_lifecycle(
        self, *, run_id: int | None, stop: asyncio.Event
    ) -> None:
        await self._run(
            channels=["market_lifecycle_v2", "multivariate_market_lifecycle"],
            tickers=[],
            run_id=run_id,
            stop=stop,
            connection_label="lifecycle",
            global_stream=True,
        )

    async def run_reference(
        self,
        channel: str,
        *,
        run_id: int | None,
        stop: asyncio.Event,
    ) -> None:
        if channel == "cfbenchmarks_value":
            extra = {"index_ids": ["all"]}
        elif channel == "pyth_value":
            extra = {"underlying_tickers": ["all"]}
        else:
            raise ValueError(f"unsupported Kalshi reference channel {channel!r}")
        await self._run(
            channels=[channel],
            tickers=[],
            run_id=run_id,
            stop=stop,
            connection_label=channel,
            global_stream=True,
            extra_subscription_params=extra,
        )

    async def _run(
        self,
        *,
        channels: list[str],
        tickers: list[str],
        run_id: int | None,
        stop: asyncio.Event,
        connection_label: str,
        global_stream: bool,
        extra_subscription_params: dict[str, Any] | None = None,
        planned_stop: asyncio.Event | None = None,
        recovery_gap_ids: tuple[int, ...] = (),
    ) -> None:
        attempt = 0
        unresolved_connection_gaps: list[int] = list(recovery_gap_ids)
        # A reconnect is itself a valid recovery path for an order-book gap.
        # Keep these IDs across socket attempts so the first fresh snapshot can
        # close every sequence gap that preceded the disconnect.
        open_gaps = self._open_sequence_gaps
        while not stop.is_set():
            connection_id: int | None = None
            messages = 0
            dropped = 0
            first_message_at: Any | None = None
            last_message_at: Any | None = None
            first_sequence: int | None = None
            last_sequence: int | None = None
            reason: str | None = None
            sid_sequences: dict[int, int] = {}
            sid_channels: dict[int, str] = {}
            sid_markets: dict[int, set[str]] = {}
            subscription_confirmed = False
            acknowledged_channels: set[str] = set()
            recovery_snapshots_seen: set[str] = set()
            try:
                headers = self.signer.headers("GET", "/trade-api/ws/v2")
                async with connect(
                    self.url,
                    additional_headers=headers,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=30,
                    close_timeout=10,
                    max_queue=2048,
                ) as websocket:
                    connection_id = await self.database.create_connection(
                        run_id=run_id,
                        exchange="kalshi",
                        channel="+".join(channels),
                        endpoint=self.url,
                        reconnect_attempt=attempt,
                        subscribed_market_count=len(tickers),
                        subscribed_market_external_ids=tickers,
                        pending_subscription=True,
                        metadata={
                            "label": connection_label,
                            "channels": channels,
                            "use_yes_price": True,
                            "global": global_stream,
                        },
                    )
                    params: dict[str, Any] = {"channels": channels}
                    params.update(extra_subscription_params or {})
                    if "orderbook_delta" in channels:
                        params["use_yes_price"] = True
                    if "ticker" in channels:
                        params["skip_ticker_ack"] = True
                    if tickers:
                        params["market_tickers"] = tickers
                    await websocket.send(
                        json.dumps({"id": 1, "cmd": "subscribe", "params": params})
                    )
                    LOGGER.info(
                        "Kalshi WebSocket subscription sent",
                        extra={
                            "connection": connection_label,
                            "channels": channels,
                            "markets": len(tickers),
                        },
                    )
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
                        monotonic_ns = time.monotonic_ns()
                        if isinstance(frame, bytes):
                            frame = frame.decode("utf-8", errors="replace")
                        messages += 1
                        await self.metrics.message("kalshi")
                        try:
                            envelope = json.loads(frame)
                        except json.JSONDecodeError:
                            dropped += 1
                            LOGGER.warning("Malformed Kalshi WebSocket JSON frame")
                            continue
                        if not isinstance(envelope, dict):
                            dropped += 1
                            continue
                        msg_type = str(envelope.get("type") or "unknown")
                        msg = envelope.get("msg") if isinstance(envelope.get("msg"), dict) else {}
                        sid = as_int(envelope.get("sid"))
                        if sid is None:
                            sid = as_int(msg.get("sid"))
                        sequence = as_int(envelope.get("seq"))
                        ticker = first_present(msg, "market_ticker", "ticker")
                        apply_orderbook_message = True

                        if msg_type == "subscribed" and sid is not None:
                            subscribed_channel = str(msg.get("channel") or "unknown")
                            sid_channels[sid] = subscribed_channel
                            acknowledged_channels.add(subscribed_channel)
                            if subscribed_channel == "orderbook_delta":
                                sid_markets[sid] = set(tickers)
                            if (
                                not subscription_confirmed
                                and set(channels).issubset(acknowledged_channels)
                            ):
                                await self.database.confirm_subscription(
                                    connection_id,
                                    exchange="kalshi",
                                    channel="+".join(channels),
                                    market_external_ids=tickers,
                                    acknowledgement=envelope,
                                )
                                subscription_confirmed = True
                                LOGGER.info(
                                    "Kalshi WebSocket subscription confirmed",
                                    extra={
                                        "connection": connection_label,
                                        "channels": channels,
                                        "acknowledged_channels": sorted(
                                            acknowledged_channels
                                        ),
                                        "markets": len(tickers),
                                    },
                                )
                        if sequence is not None:
                            first_sequence = (
                                sequence
                                if first_sequence is None
                                else min(first_sequence, sequence)
                            )
                            last_sequence = (
                                sequence
                                if last_sequence is None
                                else max(last_sequence, sequence)
                            )
                            if sid is not None and sid in sid_sequences:
                                expected = sid_sequences[sid] + 1
                                if sequence != expected:
                                    channel = sid_channels.get(sid, msg_type)
                                    forward_gap = sequence > expected
                                    if (
                                        not forward_gap
                                        and channel == "orderbook_delta"
                                    ):
                                        apply_orderbook_message = False
                                    affected_markets = (
                                        sorted(sid_markets.get(sid, set()))
                                        if channel == "orderbook_delta" and forward_gap
                                        else [str(ticker)] if ticker else []
                                    )
                                    gap_targets: list[str | None] = (
                                        affected_markets if affected_markets else [None]
                                    )
                                    for affected_market in gap_targets:
                                        gap_id = await self.database.record_gap(
                                            run_id=run_id,
                                            connection_id=connection_id,
                                            exchange="kalshi",
                                            channel=channel,
                                            market_external_id=affected_market,
                                            outcome_external_id=(
                                                f"{affected_market}:yes"
                                                if affected_market
                                                else None
                                            ),
                                            gap_type=(
                                                "sequence_gap"
                                                if forward_gap
                                                else "out_of_order_sequence"
                                            ),
                                            last_sequence=sid_sequences[sid],
                                            expected_sequence=expected,
                                            actual_sequence=sequence,
                                            details={"sid": sid},
                                        )
                                        if affected_market and forward_gap:
                                            self.books.setdefault(
                                                affected_market, OrderBook()
                                            ).valid = False
                                            open_gaps.setdefault(
                                                affected_market, []
                                            ).append(gap_id)
                                    LOGGER.error(
                                        "Kalshi WebSocket sequence discontinuity",
                                        extra={
                                            "channel": channel,
                                            "market": ticker,
                                            "expected": expected,
                                            "actual": sequence,
                                        },
                                    )
                                    if affected_markets and channel == "orderbook_delta":
                                        await websocket.send(
                                            json.dumps(
                                                {
                                                    "id": int(time.time() * 1000) % 2_000_000_000,
                                                    "cmd": "update_subscription",
                                                    "params": {
                                                        "sids": [sid],
                                                        "market_tickers": affected_markets,
                                                        "action": "get_snapshot",
                                                    },
                                                }
                                            )
                                        )
                            if (
                                sid is not None
                                and (sid not in sid_sequences or sequence > sid_sequences[sid])
                            ):
                                sid_sequences[sid] = sequence

                        if self.store_raw:
                            await self.writer.put(
                                raw_ws_item(
                                    exchange="kalshi",
                                    channel=sid_channels.get(sid or -1, msg_type),
                                    connection_id=connection_id,
                                    market_external_id=str(ticker) if ticker else None,
                                    outcome_external_id=f"{ticker}:yes" if ticker else None,
                                    message_type=msg_type,
                                    source_timestamp=parse_timestamp(
                                        first_present(
                                            msg, "source_ts_ms", "source_timestamp"
                                        )
                                    ),
                                    exchange_timestamp=parse_timestamp(
                                        first_present(msg, "ts_ms", "time", "ts")
                                    ),
                                    received_at=received_at,
                                    received_monotonic_ns=monotonic_ns,
                                    sequence_number=sequence,
                                    book_hash=None,
                                    payload=envelope,
                                    source_timestamp_raw=(
                                        str(
                                            first_present(
                                                msg,
                                                "source_ts_ms",
                                                "source_timestamp",
                                            )
                                        )
                                        if first_present(
                                            msg,
                                            "source_ts_ms",
                                            "source_timestamp",
                                        )
                                        is not None
                                        else None
                                    ),
                                    exchange_timestamp_raw=(
                                        str(first_present(msg, "ts_ms", "time", "ts"))
                                        if first_present(msg, "ts_ms", "time", "ts")
                                        is not None
                                        else None
                                    ),
                                )
                            )
                        if msg_type == "error":
                            raise RuntimeError(f"Kalshi subscription error: {msg}")
                        await self._handle(
                            envelope,
                            connection_id=connection_id,
                            received_at=received_at,
                            monotonic_ns=monotonic_ns,
                            open_gaps=open_gaps,
                            apply_orderbook_message=apply_orderbook_message,
                        )
                        if (
                            msg_type == "orderbook_snapshot"
                            and ticker
                            and apply_orderbook_message
                        ):
                            recovery_snapshots_seen.add(str(ticker))
                        ordered_market_stream = bool(tickers and "orderbook_delta" in channels)
                        if _recovery_complete(
                            tickers=tickers,
                            channels=channels,
                            subscription_confirmed=subscription_confirmed,
                            snapshots_seen=recovery_snapshots_seen,
                            message_type=msg_type,
                        ):
                            for gap_id in unresolved_connection_gaps:
                                await self.database.resolve_gap(
                                    gap_id,
                                    action=(
                                        "all_market_snapshots_after_reconnect"
                                        if ordered_market_stream
                                        else "first_data_message_after_reconnect"
                                    ),
                                )
                            unresolved_connection_gaps.clear()
                if stop.is_set():
                    attempt = 0
                else:
                    attempt += 1
                    reason = "connection_closed"
            except asyncio.CancelledError:
                reason = (
                    "planned_subscription_refresh"
                    if planned_stop is not None and planned_stop.is_set()
                    else "task_cancelled"
                )
                raise
            except Exception as exc:
                attempt += 1
                reason = f"{type(exc).__name__}: {exc}"
                LOGGER.warning(
                    "Kalshi WebSocket disconnected",
                    extra={
                        "connection": connection_label,
                        "attempt": attempt,
                        "error": reason,
                    },
                )
            finally:
                if connection_id is not None:
                    if not stop.is_set():
                        for market_ticker in tickers:
                            self.books.setdefault(market_ticker, OrderBook()).valid = False
                    await self.database.update_connection_stats(
                        connection_id,
                        messages_received=messages,
                        messages_dropped=dropped,
                        first_sequence=first_sequence,
                        last_sequence=last_sequence,
                        first_message_at=first_message_at,
                        last_message_at=last_message_at,
                    )
                    await self.database.close_connection(
                        connection_id,
                        reason=reason or "connection_closed",
                        failed=reason
                        not in {
                            None,
                            "task_cancelled",
                            "planned_subscription_refresh",
                        },
                    )
                    if (
                        not stop.is_set()
                        and reason
                        and not (
                            planned_stop is not None and planned_stop.is_set()
                        )
                    ):
                        gap_id = await self.database.record_gap(
                            run_id=run_id,
                            connection_id=connection_id,
                            exchange="kalshi",
                            channel="+".join(channels),
                            market_external_id=None,
                            outcome_external_id=None,
                            gap_type="connection_unknown_gap",
                            reconnect_reason=reason,
                        )
                        unresolved_connection_gaps.append(gap_id)
            if not stop.is_set():
                await _sleep(stop, min(30.0, 0.5 * (2 ** min(attempt, 6))))

    async def _handle(
        self,
        envelope: dict[str, Any],
        *,
        connection_id: int,
        received_at: Any,
        monotonic_ns: int,
        open_gaps: dict[str, list[int]],
        apply_orderbook_message: bool = True,
    ) -> None:
        msg_type = str(envelope.get("type") or "unknown")
        msg = envelope.get("msg") if isinstance(envelope.get("msg"), dict) else {}
        ticker = str(first_present(msg, "market_ticker", "ticker") or "")
        if msg_type == "orderbook_snapshot":
            snapshot = parse_orderbook_snapshot(
                envelope,
                received_at=received_at,
                received_monotonic_ns=monotonic_ns,
                use_yes_price=True,
            )
            if snapshot:
                item = book_snapshot_item(snapshot, connection_id)
                if apply_orderbook_message:
                    book = self.books.setdefault(snapshot.market_external_id, OrderBook())
                    book.reset(
                        snapshot.bids,
                        snapshot.asks,
                        sequence=snapshot.sequence_number,
                    )
                    gap_ids = open_gaps.pop(snapshot.market_external_id, [])
                    for gap_id in gap_ids:
                        await self.database.resolve_gap(
                            gap_id, action="websocket_snapshot_reset"
                        )
                else:
                    item.data["snapshot_type"] = "out_of_order_archived"
                await self.writer.put(item)
            return
        if msg_type == "orderbook_delta":
            update = parse_orderbook_delta(
                envelope,
                received_at=received_at,
                received_monotonic_ns=monotonic_ns,
                use_yes_price=True,
            )
            if update:
                book = self.books.setdefault(update.market_external_id, OrderBook())
                if (
                    apply_orderbook_message
                    and book.valid
                    and update.size_delta is not None
                ):
                    book.apply_delta(
                        update.side,
                        update.price,
                        update.size_delta,
                        sequence=update.sequence_number,
                    )
                item = book_update_item(update, connection_id)
                if not apply_orderbook_message:
                    item.data["event_type"] = "out_of_order_delta_archived"
                await self.writer.put(item)
            return
        if msg_type == "trade":
            trade = parse_trade(
                msg,
                received_at=received_at,
                received_monotonic_ns=monotonic_ns,
            )
            if trade:
                await self.writer.put(trade_item(trade, connection_id))
            return
        if msg_type == "ticker" and ticker:
            best_bid = as_decimal(msg.get("yes_bid_dollars"))
            best_ask = as_decimal(msg.get("yes_ask_dollars"))
            await self.writer.put(
                WriteItem(
                    "market_snapshots",
                    {
                        "exchange": "kalshi",
                        "market_external_id": ticker,
                        "outcome_external_id": f"{ticker}:yes",
                        "connection_id": connection_id,
                        "observed_at": received_at,
                        "source_timestamp": None,
                        "exchange_timestamp": parse_timestamp(
                            first_present(msg, "ts_ms", "time", "ts")
                        ),
                        "received_at": received_at,
                        "received_monotonic_ns": monotonic_ns,
                        "sequence_number": as_int(envelope.get("seq")),
                        "book_hash": None,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "midpoint": (
                            (best_bid + best_ask) / 2
                            if best_bid is not None and best_ask is not None
                            else None
                        ),
                        "spread": (
                            best_ask - best_bid
                            if best_bid is not None and best_ask is not None
                            else None
                        ),
                        "last_trade_price": as_decimal(msg.get("price_dollars")),
                        "bid_depth": as_decimal(msg.get("yes_bid_size_fp")),
                        "ask_depth": as_decimal(msg.get("yes_ask_size_fp")),
                        "volume": as_decimal(msg.get("volume_fp")),
                        "open_interest": as_decimal(msg.get("open_interest_fp")),
                        "liquidity": None,
                        "raw_data": envelope,
                    },
                )
            )
            return
        if msg_type == "pyth_value":
            underlying = str(msg.get("underlying_ticker") or "unknown")
            source_timestamp = parse_timestamp(msg.get("source_ts_ms"))
            exchange_timestamp = parse_timestamp(msg.get("received_at"))
            price = as_decimal(msg.get("value_usd"))
            if price is not None:
                await self.writer.put(
                    WriteItem(
                        "reference_price_updates",
                        {
                            "delivery_exchange": "kalshi",
                            "provider": "pyth",
                            "external_instrument_id": underlying,
                            "external_update_id": content_hash(
                                {
                                    "underlying": underlying,
                                    "source_ts_ms": msg.get("source_ts_ms"),
                                    "value_usd": msg.get("value_usd"),
                                }
                            ),
                            "connection_id": connection_id,
                            "source_timestamp": source_timestamp,
                            "exchange_timestamp": exchange_timestamp,
                            "source_timestamp_raw": (
                                str(msg.get("source_ts_ms"))
                                if msg.get("source_ts_ms") is not None
                                else None
                            ),
                            "exchange_timestamp_raw": (
                                str(msg.get("received_at"))
                                if msg.get("received_at") is not None
                                else None
                            ),
                            "received_at": received_at,
                            "received_monotonic_ns": monotonic_ns,
                            "sequence_number": as_int(envelope.get("seq")),
                            "price": price,
                            "bid": None,
                            "ask": None,
                            "confidence_interval": None,
                            "publish_slot": None,
                            "source_status": "live",
                            "raw_data": envelope,
                        },
                    )
                )
            return
        if msg_type == "cfbenchmarks_value":
            index_id = str(msg.get("index_id") or "unknown")
            upstream: dict[str, Any] = {}
            if isinstance(msg.get("data"), str):
                try:
                    decoded = json.loads(msg["data"])
                    if isinstance(decoded, dict):
                        upstream = decoded
                except json.JSONDecodeError:
                    pass
            elif isinstance(msg.get("data"), dict):
                upstream = msg["data"]
            value = as_decimal(first_present(upstream, "value", "price"))
            # avg_60s_data is a trailing-window statistic, not a point-in-time
            # reference price.  Substituting it would corrupt lead/lag studies.
            if value is None and "cfbenchmarks_missing_spot_value" not in self._unknown_types:
                self._unknown_types.add("cfbenchmarks_missing_spot_value")
                LOGGER.warning(
                    "Kalshi CF Benchmarks message lacked a decodable spot value",
                    extra={"index_id": index_id},
                )
            if value is not None:
                source_timestamp = parse_timestamp(
                    first_present(upstream, "time", "timestamp", "ts")
                )
                exchange_timestamp = parse_timestamp(msg.get("received_at"))
                await self.writer.put(
                    WriteItem(
                        "reference_price_updates",
                        {
                            "delivery_exchange": "kalshi",
                            "provider": "cfbenchmarks",
                            "external_instrument_id": index_id,
                            "external_update_id": content_hash(
                                {
                                    "index_id": index_id,
                                    "source_time": first_present(
                                        upstream, "time", "timestamp", "ts"
                                    ),
                                    "value": str(value),
                                }
                            ),
                            "connection_id": connection_id,
                            "source_timestamp": source_timestamp,
                            "exchange_timestamp": exchange_timestamp,
                            "source_timestamp_raw": (
                                str(first_present(upstream, "time", "timestamp", "ts"))
                                if first_present(upstream, "time", "timestamp", "ts")
                                is not None
                                else None
                            ),
                            "exchange_timestamp_raw": (
                                str(msg.get("received_at"))
                                if msg.get("received_at") is not None
                                else None
                            ),
                            "received_at": received_at,
                            "received_monotonic_ns": monotonic_ns,
                            "sequence_number": as_int(envelope.get("seq")),
                            "price": value,
                            "bid": None,
                            "ask": None,
                            "confidence_interval": None,
                            "publish_slot": None,
                            "source_status": "live",
                            "raw_data": envelope,
                        },
                    )
                )
            return
        lifecycle_event_type = str(msg.get("event_type") or msg_type)
        if msg_type == "event_lifecycle":
            event_ticker = str(msg.get("event_ticker") or "")
            if not event_ticker:
                LOGGER.warning("Kalshi event_lifecycle missing event_ticker")
                return
            await self.database.upsert_event(normalise_event(msg))
            source_raw = first_present(msg, "created_ts", "ts")
            exchange_raw = first_present(msg, "ts_ms", "ts")
            await self.writer.put(
                WriteItem(
                    "event_lifecycle_events",
                    {
                        "exchange": "kalshi",
                        "event_external_id": event_ticker,
                        "connection_id": connection_id,
                        "external_update_id": (
                            str(msg.get("id")) if msg.get("id") is not None else None
                        ),
                        "dedup_hash": content_hash(envelope),
                        "event_type": "created",
                        "source_timestamp": parse_timestamp(source_raw),
                        "exchange_timestamp": parse_timestamp(exchange_raw),
                        "source_timestamp_raw": (
                            str(source_raw) if source_raw is not None else None
                        ),
                        "exchange_timestamp_raw": (
                            str(exchange_raw) if exchange_raw is not None else None
                        ),
                        "received_at": received_at,
                        "received_monotonic_ns": monotonic_ns,
                        "sequence_number": as_int(envelope.get("seq")),
                        "details": msg,
                        "raw_data": envelope,
                    },
                )
            )
            return
        if lifecycle_event_type == "event_fee_update":
            event_ticker = str(msg.get("event_ticker") or "")
            if event_ticker:
                source_timestamp = parse_timestamp(
                    first_present(msg, "scheduled_ts", "ts_ms", "ts")
                )
                await self.database.record_fee_configuration(
                    exchange="kalshi",
                    scope_type="event",
                    scope_external_id=event_ticker,
                    fee_type="event_fee_override",
                    effective_from=source_timestamp or received_at,
                    observed_at=received_at,
                    source_timestamp=source_timestamp,
                    multiplier=as_decimal(msg.get("fee_multiplier_override")),
                    configuration=envelope,
                    semantic_configuration={
                        "fee_type_override": msg.get("fee_type_override")
                    },
                    version_current=True,
                )
            else:
                LOGGER.warning("Kalshi event_fee_update missing event_ticker")
            return
        lifecycle_types = {
            "market_lifecycle_v2",
            "multivariate_market_lifecycle",
            "market_created",
            "market_activated",
            "market_deactivated",
            "market_close_date_updated",
            "market_determined",
            "market_settled",
            "metadata_updated",
            "price_level_structure_updated",
            "fractional_trading_updated",
        }
        if msg_type in lifecycle_types or msg.get("event_type"):
            event_type = str(msg.get("event_type") or msg_type)
            market_ticker = first_present(msg, "market_ticker", "ticker")
            if market_ticker:
                await self.writer.put(
                    WriteItem(
                        "market_lifecycle_events",
                        {
                            "exchange": "kalshi",
                            "market_external_id": str(market_ticker),
                            "outcome_external_id": None,
                            "connection_id": connection_id,
                            "external_event_id": str(msg.get("id")) if msg.get("id") else None,
                            "dedup_hash": content_hash(envelope),
                            "event_type": event_type,
                            "previous_status": msg.get("previous_status"),
                            "new_status": msg.get("status") or msg.get("new_status"),
                            "source_timestamp": parse_timestamp(msg.get("ts")),
                            "exchange_timestamp": parse_timestamp(
                                first_present(msg, "ts_ms", "ts")
                            ),
                            "source_timestamp_raw": (
                                str(msg.get("ts")) if msg.get("ts") is not None else None
                            ),
                            "exchange_timestamp_raw": (
                                str(first_present(msg, "ts_ms", "ts"))
                                if first_present(msg, "ts_ms", "ts") is not None
                                else None
                            ),
                            "received_at": received_at,
                            "received_monotonic_ns": monotonic_ns,
                            "sequence_number": as_int(envelope.get("seq")),
                            "details": msg,
                            "raw_data": envelope,
                        },
                    )
                )
                updates = _kalshi_lifecycle_updates(event_type, msg)
                if updates:
                    updated = await self.database.apply_market_metadata_patch(
                        exchange="kalshi",
                        market_external_id=str(market_ticker),
                        updates=updates,
                        lifecycle_payload=envelope,
                        source_timestamp=parse_timestamp(msg.get("ts")),
                        exchange_timestamp=parse_timestamp(
                            first_present(msg, "ts_ms", "ts")
                        ),
                        observed_at=received_at,
                    )
                    if not updated:
                        LOGGER.info(
                            "Kalshi lifecycle arrived before market metadata",
                            extra={
                                "market": str(market_ticker),
                                "event_type": event_type,
                            },
                        )
            return
        if msg_type in {
            "subscribed",
            "ok",
            "error",
            "cfbenchmarks_value_indexlist",
            "pyth_value_underlying_list",
        }:
            if msg_type == "error":
                LOGGER.error("Kalshi WebSocket protocol error", extra={"error": msg})
            return
        if msg_type not in self._unknown_types:
            self._unknown_types.add(msg_type)
            LOGGER.warning(
                "Unknown Kalshi WebSocket message type",
                extra={"message_type": msg_type},
            )


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


def _kalshi_lifecycle_updates(
    event_type: str, msg: dict[str, Any]
) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    if event_type == "activated":
        updates.update(
            status="active",
            is_active=True,
            is_tradable=True,
            accepting_orders=True,
        )
    elif event_type == "deactivated":
        updates.update(
            status="inactive",
            is_active=False,
            is_tradable=False,
            accepting_orders=False,
        )
    elif event_type == "determined":
        updates.update(
            status="determined",
            is_active=False,
            is_tradable=False,
            accepting_orders=False,
        )
        if msg.get("result") is not None:
            updates["result"] = msg["result"]
        settlement_value = as_decimal(
            first_present(msg, "settlement_value_dollars", "settlement_value")
        )
        if settlement_value is not None:
            updates["settlement_value"] = settlement_value
    elif event_type == "settled":
        updates.update(
            status="finalized",
            is_active=False,
            is_tradable=False,
            accepting_orders=False,
        )
        settled_at = parse_timestamp(msg.get("settled_ts"))
        if settled_at is not None:
            updates["settlement_time"] = settled_at
        if msg.get("result") is not None:
            updates["result"] = msg["result"]
        settlement_value = as_decimal(
            first_present(msg, "settlement_value_dollars", "settlement_value")
        )
        if settlement_value is not None:
            updates["settlement_value"] = settlement_value

    if event_type == "close_date_updated":
        close_time = parse_timestamp(first_present(msg, "close_ts", "close_time"))
        if close_time is not None:
            updates["close_time"] = close_time
    elif event_type == "created":
        open_time = parse_timestamp(first_present(msg, "open_ts", "open_time"))
        close_time = parse_timestamp(first_present(msg, "close_ts", "close_time"))
        if open_time is not None:
            updates["open_time"] = open_time
        if close_time is not None:
            updates["close_time"] = close_time

    metadata = (
        msg.get("additional_metadata")
        if isinstance(msg.get("additional_metadata"), dict)
        else {}
    )
    title = first_present(metadata, "title", "name")
    if title is not None:
        updates["question"] = str(title)
    subtitle = first_present(metadata, "subtitle", "yes_sub_title")
    if subtitle is not None:
        updates["subtitle"] = str(subtitle)
    rules = [
        str(value)
        for value in (
            metadata.get("rules_primary"),
            metadata.get("rules_secondary"),
        )
        if value
    ]
    if rules:
        updates["rules"] = "\n\n".join(rules)

    structure = msg.get("price_level_structure")
    if structure is None:
        structure = metadata.get("price_level_structure")
    ranges = msg.get("price_ranges")
    if ranges is None:
        ranges = metadata.get("price_ranges")
    if structure is not None or ranges is not None:
        updates["price_level_structure"] = {"type": structure, "ranges": ranges}
    structural_metadata: dict[str, Any] = {}
    for key in (
        "floor_strike",
        "cap_strike",
        "strike_type",
        "functional_strike",
        "custom_strike",
        "exchange_index",
        "fractional_trading_enabled",
        "expected_expiration_time",
        "latest_expiration_time",
        "expiration_time",
    ):
        if key in msg:
            structural_metadata[key] = msg[key]
        elif key in metadata:
            structural_metadata[key] = metadata[key]
    if structural_metadata:
        updates["structural_metadata"] = structural_metadata
    return updates


def _recovery_complete(
    *,
    tickers: list[str],
    channels: list[str],
    subscription_confirmed: bool,
    snapshots_seen: set[str],
    message_type: str,
) -> bool:
    ordered_market_stream = bool(tickers and "orderbook_delta" in channels)
    if ordered_market_stream:
        return subscription_confirmed and set(tickers).issubset(snapshots_seen)
    return message_type not in {
        "subscribed",
        "ok",
        "error",
        "cfbenchmarks_value_indexlist",
        "pyth_value_underlying_list",
    }
