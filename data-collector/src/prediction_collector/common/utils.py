from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Iterator, TypeVar
from urllib.parse import parse_qs, urlsplit


T = TypeVar("T")


def utc_now() -> datetime:
    return datetime.now(UTC)


def as_decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default
    return result if result.is_finite() else default


def as_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "" or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float, Decimal)):
        numeric = Decimal(str(value))
        absolute = abs(numeric)
        if absolute >= Decimal("1000000000000000000"):
            numeric /= Decimal("1000000000")
        elif absolute >= Decimal("1000000000000000"):
            numeric /= Decimal("1000000")
        elif absolute >= Decimal("1000000000000"):
            numeric /= Decimal("1000")
        return datetime.fromtimestamp(float(numeric), tz=UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        return parse_timestamp(Decimal(text))
    except InvalidOperation:
        pass
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def request_parameters(url: str) -> dict[str, Any]:
    return {
        key: values[0] if len(values) == 1 else values
        for key, values in parse_qs(urlsplit(url).query, keep_blank_values=True).items()
    }


def trade_fingerprint(
    *,
    exchange: str,
    market_external_id: str,
    outcome_external_id: str | None,
    executed_at: datetime | str | int | float | None,
    price: Decimal | str | int | float,
    size: Decimal | str | int | float,
    side: str | None,
    transaction_hash: str | None = None,
    external_trade_id: str | None = None,
) -> str:
    stable = {
        "exchange": exchange.lower(),
        "market": market_external_id,
        "outcome": outcome_external_id,
        "executed_at": (
            parse_timestamp(executed_at).isoformat()
            if parse_timestamp(executed_at) is not None
            else str(executed_at)
        ),
        "price": str(Decimal(str(price)).normalize()),
        "size": str(Decimal(str(size)).normalize()),
        "side": side.lower() if side else None,
        "transaction_hash": transaction_hash,
        "external_trade_id": external_trade_id,
    }
    return content_hash(stable)


def chunks(values: Iterable[T], size: int) -> Iterator[list[T]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    batch: list[T] = []
    for value in values:
        batch.append(value)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None
