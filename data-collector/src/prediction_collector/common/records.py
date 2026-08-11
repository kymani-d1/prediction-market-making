from __future__ import annotations

from typing import Any

from prediction_collector.common.types import (
    ParsedBookSnapshot,
    ParsedBookUpdate,
    ParsedTrade,
)
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
            "is_reconciliation": reconciliation,
            "raw_data": snapshot.raw_data,
        },
    )


def book_update_item(
    update: ParsedBookUpdate, connection_id: int | None = None
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
