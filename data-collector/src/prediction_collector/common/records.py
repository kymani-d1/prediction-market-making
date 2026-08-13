from __future__ import annotations

from typing import Any

from prediction_collector.common.types import (
    ParsedBookSnapshot,
    ParsedBookUpdate,
    ParsedTrade,
)
from prediction_collector.common.orderbook import OrderBook
from prediction_collector.common.utils import as_decimal
from prediction_collector.writer import WriteItem


def trade_item(trade: ParsedTrade, connection_id: int | None = None) -> WriteItem:
    return WriteItem(
        "trades",
        {
            "exchange": trade.exchange,
            "market_external_id": trade.market_external_id,
            "outcome_external_id": trade.outcome_external_id,
            "external_trade_id": trade.external_trade_id,
            "dedup_hash": trade.dedup_hash,
            "connection_id": connection_id,
            "executed_at": trade.executed_at,
            "source_timestamp": trade.source_timestamp,
            "exchange_timestamp": trade.exchange_timestamp,
            "source_timestamp_raw": trade.source_timestamp_raw,
            "exchange_timestamp_raw": trade.exchange_timestamp_raw,
            "received_at": trade.received_at,
            "received_monotonic_ns": trade.received_monotonic_ns,
            "sequence_number": None,
            "price": trade.price,
            "size": trade.size,
            "side": trade.side,
            "transaction_hash": trade.transaction_hash,
            "raw_data": trade.raw_data,
        },
    )


def book_snapshot_item(
    snapshot: ParsedBookSnapshot,
    connection_id: int | None = None,
    *,
    reconciliation: bool = False,
) -> WriteItem:
    bid_depth = sum(
        (as_decimal(level[1]) or 0 for level in snapshot.bids if len(level) >= 2),
        0,
    )
    ask_depth = sum(
        (as_decimal(level[1]) or 0 for level in snapshot.asks if len(level) >= 2),
        0,
    )
    midpoint = (
        (snapshot.best_bid + snapshot.best_ask) / 2
        if snapshot.best_bid is not None and snapshot.best_ask is not None
        else None
    )
    spread = (
        snapshot.best_ask - snapshot.best_bid
        if snapshot.best_bid is not None and snapshot.best_ask is not None
        else None
    )
    return WriteItem(
        "orderbook_snapshots",
        {
            "exchange": snapshot.exchange,
            "market_external_id": snapshot.market_external_id,
            "outcome_external_id": snapshot.outcome_external_id,
            "connection_id": connection_id,
            "snapshot_type": "reconciliation" if reconciliation else "exchange",
            "source_timestamp": snapshot.source_timestamp,
            "exchange_timestamp": snapshot.exchange_timestamp,
            "source_timestamp_raw": snapshot.source_timestamp_raw,
            "exchange_timestamp_raw": snapshot.exchange_timestamp_raw,
            "received_at": snapshot.received_at,
            "received_monotonic_ns": snapshot.received_monotonic_ns,
            "sequence_number": snapshot.sequence_number,
            "book_hash": snapshot.book_hash,
            "bids": snapshot.bids,
            "asks": snapshot.asks,
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "midpoint": midpoint,
            "spread": spread,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "level_count": len(snapshot.bids) + len(snapshot.asks),
            "is_reconciliation": reconciliation,
            "raw_data": snapshot.raw_data,
        },
    )


def book_update_item(
    update: ParsedBookUpdate,
    connection_id: int | None = None,
    *,
    book: OrderBook | None = None,
) -> WriteItem:
    return WriteItem(
        "orderbook_updates",
        {
            "exchange": update.exchange,
            "market_external_id": update.market_external_id,
            "outcome_external_id": update.outcome_external_id,
            "connection_id": connection_id,
            "source_timestamp": update.source_timestamp,
            "exchange_timestamp": update.exchange_timestamp,
            "source_timestamp_raw": update.source_timestamp_raw,
            "exchange_timestamp_raw": update.exchange_timestamp_raw,
            "received_at": update.received_at,
            "received_monotonic_ns": update.received_monotonic_ns,
            "sequence_number": update.sequence_number,
            "book_hash": update.book_hash,
            "side": update.side,
            "price": update.price,
            "size": update.size,
            "size_delta": update.size_delta,
            "operation": update.operation,
            "event_type": "price_change" if update.exchange == "polymarket" else "orderbook_delta",
            "best_bid": book.best_bid if book else None,
            "best_ask": book.best_ask if book else None,
            "midpoint": book.midpoint if book else None,
            "spread": book.spread if book else None,
            "bid_depth": book.bid_depth if book else 0,
            "ask_depth": book.ask_depth if book else 0,
            "level_count": len(book.bids) + len(book.asks) if book else 0,
            "raw_data": update.raw_data,
        },
    )


def raw_ws_item(
    *,
    exchange: str,
    channel: str,
    connection_id: int | None,
    market_external_id: str | None,
    outcome_external_id: str | None,
    message_type: str | None,
    source_timestamp: Any,
    exchange_timestamp: Any,
    received_at: Any,
    received_monotonic_ns: int,
    sequence_number: int | None,
    book_hash: str | None,
    payload: Any,
    source_timestamp_raw: str | None = None,
    exchange_timestamp_raw: str | None = None,
) -> WriteItem:
    return WriteItem(
        "raw_ws_messages",
        {
            "exchange": exchange,
            "channel": channel,
            "connection_id": connection_id,
            "market_external_id": market_external_id,
            "outcome_external_id": outcome_external_id,
            "message_type": message_type,
            "source_timestamp": source_timestamp,
            "exchange_timestamp": exchange_timestamp,
            "source_timestamp_raw": source_timestamp_raw,
            "exchange_timestamp_raw": exchange_timestamp_raw,
            "received_at": received_at,
            "received_monotonic_ns": received_monotonic_ns,
            "sequence_number": sequence_number,
            "book_hash": book_hash,
            "payload": payload,
        },
    )
