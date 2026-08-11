from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from prediction_collector.common.utils import canonical_json, content_hash, trade_fingerprint


def fingerprint(**overrides: object) -> str:
    values: dict[str, object] = {
        "exchange": "Polymarket",
        "market_external_id": "0xmarket",
        "outcome_external_id": "token",
        "executed_at": "2026-08-11T14:00:01.230Z",
        "price": "0.4200",
        "size": "12.5000",
        "side": "BUY",
        "transaction_hash": "0xtransaction",
        "external_trade_id": None,
    }
    values.update(overrides)
    return trade_fingerprint(**values)  # type: ignore[arg-type]


def test_equivalent_decimal_timestamp_and_case_representations_have_same_fingerprint() -> None:
    first = fingerprint()
    second = fingerprint(
        exchange="polymarket",
        executed_at=datetime(2026, 8, 11, 14, 0, 1, 230000, tzinfo=UTC),
        price=Decimal("0.42"),
        size=Decimal("12.5"),
        side="buy",
    )

    assert first == second
    assert len(first) == 64


def test_identity_fields_change_fingerprint() -> None:
    baseline = fingerprint()

    assert fingerprint(market_external_id="other") != baseline
    assert fingerprint(outcome_external_id="other-token") != baseline
    assert fingerprint(price="0.43") != baseline
    assert fingerprint(size="12.6") != baseline
    assert fingerprint(side="SELL") != baseline
    assert fingerprint(transaction_hash="0xother") != baseline
    assert fingerprint(external_trade_id="trade-id") != baseline


def test_canonical_json_and_content_hash_ignore_mapping_insertion_order() -> None:
    left = {"b": 2, "a": {"y": 1, "x": 0}}
    right = {"a": {"x": 0, "y": 1}, "b": 2}

    assert canonical_json(left) == canonical_json(right)
    assert content_hash(left) == content_hash(right)
