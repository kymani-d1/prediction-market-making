from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


JsonObject = dict[str, Any]


@dataclass(slots=True)
class MarketCandidate:
    exchange: str
    external_id: str
    ticker: str | None
    status: str | None
    active: bool
    tradable: bool
    volume: Decimal | None = None
    volume_24h: Decimal | None = None
    liquidity: Decimal | None = None
    outcome_token_ids: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    raw_data: JsonObject = field(default_factory=dict)

    @property
    def selectors(self) -> frozenset[str]:
        values = {self.external_id, f"{self.exchange}:{self.external_id}"}
        if self.ticker:
            values.update({self.ticker, f"{self.exchange}:{self.ticker}"})
        values.update(self.outcome_token_ids)
        values.update(f"{self.exchange}:{token}" for token in self.outcome_token_ids)
        values.update(self.aliases)
        values.update(f"{self.exchange}:{alias}" for alias in self.aliases)
        return frozenset(values)


@dataclass(frozen=True, slots=True)
class MarketExclusion:
    exchange: str
    external_id: str
    reason: str


@dataclass(slots=True)
class LiveSelection:
    discovered: int
    active: int
    tradable: int
    subscribed: list[MarketCandidate]
    excluded: list[MarketExclusion]

    @property
    def excluded_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in self.excluded:
            counts[item.reason] = counts.get(item.reason, 0) + 1
        return counts


@dataclass(frozen=True, slots=True)
class ParsedTrade:
    exchange: str
    market_external_id: str
    outcome_external_id: str | None
    external_trade_id: str | None
    executed_at: datetime
    received_at: datetime
    received_monotonic_ns: int
    price: Decimal
    size: Decimal
    side: str | None
    transaction_hash: str | None
    dedup_hash: str
    source_timestamp: datetime | None
    exchange_timestamp: datetime | None
    source_timestamp_raw: str | None
    exchange_timestamp_raw: str | None
    raw_data: JsonObject


@dataclass(frozen=True, slots=True)
class ParsedBookSnapshot:
    exchange: str
    market_external_id: str
    outcome_external_id: str | None
    source_timestamp: datetime | None
    exchange_timestamp: datetime | None
    source_timestamp_raw: str | None
    exchange_timestamp_raw: str | None
    received_at: datetime
    received_monotonic_ns: int
    sequence_number: int | None
    book_hash: str | None
    bids: list[list[str]]
    asks: list[list[str]]
    best_bid: Decimal | None
    best_ask: Decimal | None
    raw_data: JsonObject


@dataclass(frozen=True, slots=True)
class ParsedBookUpdate:
    exchange: str
    market_external_id: str
    outcome_external_id: str | None
    source_timestamp: datetime | None
    exchange_timestamp: datetime | None
    source_timestamp_raw: str | None
    exchange_timestamp_raw: str | None
    received_at: datetime
    received_monotonic_ns: int
    sequence_number: int | None
    book_hash: str | None
    side: str
    price: Decimal
    size: Decimal | None
    size_delta: Decimal | None
    operation: str
    raw_data: JsonObject
