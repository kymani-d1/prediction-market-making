from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any

from prediction_collector.archive import decimal_from_components
from prediction_collector.common.orderbook import OrderBook


def replay_book(
    snapshots: Iterable[Mapping[str, Any]],
    updates: Iterable[Mapping[str, Any]],
    *,
    token_key: int | None = None,
) -> OrderBook:
    """Reconstruct one token book from compact v2 snapshots and deltas."""
    events: list[tuple[int, int, int, Mapping[str, Any]]] = []
    for row in snapshots:
        # REST reconciliation snapshots are immutable quality evidence, not
        # ordered state mutations. They can race with the live delta stream and
        # therefore must never be applied by deterministic replay.
        if row.get("is_reconciliation"):
            continue
        if token_key is None or int(row["token_key"]) == token_key:
            events.append(
                (
                    int(row["received_ts_ns"]),
                    int(row.get("received_monotonic_ns") or 0),
                    0,
                    row,
                )
            )
    for row in updates:
        if token_key is None or int(row["token_key"]) == token_key:
            events.append(
                (
                    int(row["received_ts_ns"]),
                    int(row.get("received_monotonic_ns") or 0),
                    1,
                    row,
                )
            )
    events.sort(key=lambda value: (value[0], value[1], value[2]))
    book = OrderBook()
    for _timestamp, _monotonic, event_kind, row in events:
        if event_kind == 0:
            bids: list[list[str]] = []
            asks: list[list[str]] = []
            for level in row.get("levels") or []:
                price = decimal_from_components(
                    level.get("price_mantissa"), level.get("price_scale")
                )
                size = decimal_from_components(
                    level.get("size_mantissa"), level.get("size_scale")
                )
                if price is None or size is None:
                    continue
                target = bids if int(level["side"]) == 1 else asks
                target.append([format(price, "f"), format(size, "f")])
            book.reset(bids, asks, book_hash=row.get("book_hash"))
            continue
        if not book.valid:
            raise ValueError("delta precedes a replay snapshot")
        price = decimal_from_components(row.get("price_mantissa"), row.get("price_scale"))
        size = decimal_from_components(row.get("size_mantissa"), row.get("size_scale"))
        if price is None or size is None:
            raise ValueError("book delta is missing exact price/size components")
        side = "buy" if int(row["side"]) == 1 else "sell"
        action = int(row["action"])
        if action == 4:
            book.apply_delta(side, price, size)
        else:
            book.apply_absolute(side, price, Decimal("0") if action == 2 else size)
    return book


def table_rows(table: Any) -> list[dict[str, Any]]:
    """Convert a PyArrow table to replay mappings without exposing Arrow details."""
    return [dict(row) for row in table.to_pylist()]
