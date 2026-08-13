from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable

from prediction_collector.common.utils import as_decimal


def _levels(values: Iterable[Any]) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for value in values:
        if isinstance(value, dict):
            price = as_decimal(value.get("price") or value.get("price_dollars"))
            size = as_decimal(
                value.get("size")
                if value.get("size") is not None
                else value.get("quantity") or value.get("count")
            )
        elif isinstance(value, (list, tuple)) and len(value) >= 2:
            price, size = as_decimal(value[0]), as_decimal(value[1])
        else:
            continue
        if price is not None and size is not None and size > 0:
            result[price] = size
    return result


@dataclass(slots=True)
class OrderBook:
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    sequence: int | None = None
    book_hash: str | None = None
    valid: bool = False

    def reset(
        self,
        bids: Iterable[Any],
        asks: Iterable[Any],
        *,
        sequence: int | None = None,
        book_hash: str | None = None,
    ) -> None:
        self.bids = _levels(bids)
        self.asks = _levels(asks)
        self.sequence = sequence
        self.book_hash = book_hash
        self.valid = True

    def apply_absolute(
        self,
        side: str,
        price: Decimal,
        size: Decimal,
        *,
        sequence: int | None = None,
        book_hash: str | None = None,
    ) -> None:
        levels = self._side(side)
        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        if sequence is not None:
            self.sequence = sequence
        # A snapshot hash identifies that exact full state. Any mutation that
        # does not carry a replacement full-book hash invalidates it.
        self.book_hash = book_hash

    def apply_delta(
        self,
        side: str,
        price: Decimal,
        delta: Decimal,
        *,
        sequence: int | None = None,
    ) -> None:
        levels = self._side(side)
        new_size = levels.get(price, Decimal("0")) + delta
        if new_size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = new_size
        if sequence is not None:
            self.sequence = sequence
        self.book_hash = None

    def _side(self, side: str) -> dict[Decimal, Decimal]:
        normalized = side.lower()
        if normalized in {"buy", "bid", "bids", "yes"}:
            return self.bids
        if normalized in {"sell", "ask", "asks", "no"}:
            return self.asks
        raise ValueError(f"unknown book side {side!r}")

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    @property
    def midpoint(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal("2")

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def bid_depth(self) -> Decimal:
        return sum(self.bids.values(), Decimal("0"))

    @property
    def ask_depth(self) -> Decimal:
        return sum(self.asks.values(), Decimal("0"))

    @property
    def bid_depth_top(self) -> Decimal:
        return self.bids.get(self.best_bid, Decimal("0")) if self.best_bid is not None else Decimal("0")

    @property
    def ask_depth_top(self) -> Decimal:
        return self.asks.get(self.best_ask, Decimal("0")) if self.best_ask is not None else Decimal("0")

    @property
    def bid_depth_1pct(self) -> Decimal:
        midpoint = self.midpoint
        if midpoint is None:
            return Decimal("0")
        floor = midpoint * Decimal("0.99")
        return sum((size for price, size in self.bids.items() if price >= floor), Decimal("0"))

    @property
    def ask_depth_1pct(self) -> Decimal:
        midpoint = self.midpoint
        if midpoint is None:
            return Decimal("0")
        ceiling = midpoint * Decimal("1.01")
        return sum((size for price, size in self.asks.items() if price <= ceiling), Decimal("0"))

    @property
    def imbalance(self) -> Decimal | None:
        total = self.bid_depth + self.ask_depth
        if total <= 0:
            return None
        return (self.bid_depth - self.ask_depth) / total

    def serialise_bids(self) -> list[list[str]]:
        return [[str(price), str(self.bids[price])] for price in sorted(self.bids, reverse=True)]

    def serialise_asks(self) -> list[list[str]]:
        return [[str(price), str(self.asks[price])] for price in sorted(self.asks)]
