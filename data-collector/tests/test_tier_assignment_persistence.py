from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.common.types import MarketCandidate
from prediction_collector.common.utils import canonical_json
from prediction_collector.config import Settings
from prediction_collector.database import (
    Database,
    _POSTGRESQL_JSONB_MAX_CONTAINER_BYTES,
    _tier_assignment_payload_chunks,
)
from prediction_collector.tiering import CollectionTier, TierAssignment


NOW = datetime(2026, 8, 21, 3, 1, tzinfo=UTC)


def _assignment(
    index: int,
    tier: CollectionTier = CollectionTier.METADATA_ONLY,
    *,
    reasons: tuple[str, ...] = ("inactive",),
) -> TierAssignment:
    return TierAssignment(
        MarketCandidate(
            exchange="polymarket",
            external_id=f"{index:064x}",
            ticker=None,
            status="active" if tier is not CollectionTier.METADATA_ONLY else "closed",
            active=tier is not CollectionTier.METADATA_ONLY,
            tradable=tier is not CollectionTier.METADATA_ONLY,
        ),
        tier,
        Decimal("100") if tier is CollectionTier.FULL_L2 else Decimal("20"),
        reasons,
        tier is CollectionTier.SAMPLED,
    )


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.rowcount = len(self.rows)

    async def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class _Transaction:
    def __init__(self, connection: "_TierConnection") -> None:
        self.connection = connection
        self.tiers_before: dict[str, dict[str, Any]] = {}
        self.history_before: dict[tuple[str, str, datetime], dict[str, Any]] = {}

    async def __aenter__(self) -> "_Transaction":
        self.tiers_before = deepcopy(self.connection.tiers)
        self.history_before = deepcopy(self.connection.history)
        return self

    async def __aexit__(self, error_type: type[BaseException] | None, *_: Any) -> bool:
        if error_type is not None:
            self.connection.tiers = self.tiers_before
            self.connection.history = self.history_before
            self.connection.rollbacks += 1
        else:
            self.connection.commits += 1
        return False


class _TierConnection:
    def __init__(self) -> None:
        self.tiers: dict[str, dict[str, Any]] = {}
        self.history: dict[tuple[str, str, datetime], dict[str, Any]] = {}
        self.payload_sizes: list[int] = []
        self.evaluated_at_values: list[datetime] = []
        self.history_calls = 0
        self.tier_calls = 0
        self.fail_on_history_call: int | None = None
        self.commits = 0
        self.rollbacks = 0

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    async def execute(self, query: str, params: tuple[Any, ...]) -> _Cursor:
        payload = params[0].obj
        evaluated_at = params[1]
        self.payload_sizes.append(len(canonical_json(payload).encode("utf-8")))
        self.evaluated_at_values.append(evaluated_at)
        normalized = " ".join(query.split())

        if "INSERT INTO market_collection_tier_history" in normalized:
            self.history_calls += 1
            if self.history_calls == self.fail_on_history_call:
                raise RuntimeError("injected later-chunk failure")
            inserted: list[dict[str, Any]] = []
            for value in payload:
                external_id = value["external_id"]
                previous = self.tiers.get(external_id)
                if previous is not None and previous["tier"] == value["tier"]:
                    continue
                key = (external_id, value["tier"], evaluated_at)
                if key not in self.history:
                    self.history[key] = {
                        **value,
                        "previous_tier": previous and previous["tier"],
                        "evaluated_at": evaluated_at,
                    }
                    inserted.append({"id": len(self.history)})
            return _Cursor(inserted)

        if "INSERT INTO market_collection_tiers" in normalized:
            self.tier_calls += 1
            for value in payload:
                self.tiers[value["external_id"]] = {
                    **value,
                    "evaluated_at": evaluated_at,
                }
            return _Cursor()

        raise AssertionError(normalized)


class _ConnectionContext:
    def __init__(self, connection: _TierConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _TierConnection:
        return self.connection

    async def __aexit__(self, *_: Any) -> None:
        return None


class _Pool:
    def __init__(self, connection: _TierConnection) -> None:
        self.connection_value = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self.connection_value)


def _database(connection: _TierConnection) -> Database:
    database = Database(Settings())
    database.pool = _Pool(connection)  # type: ignore[assignment]
    return database


@pytest.mark.asyncio
async def test_small_assignment_cohort_retains_one_payload_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("prediction_collector.database.utc_now", lambda: NOW)
    connection = _TierConnection()
    database = _database(connection)
    assignments = [
        _assignment(1, CollectionTier.FULL_L2, reasons=("liquidity",)),
        _assignment(2, CollectionTier.SAMPLED, reasons=("recent_volume",)),
    ]

    changed = await database.record_tier_assignments(assignments)

    assert changed == 2
    assert connection.history_calls == 1
    assert connection.tier_calls == 1
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert set(connection.tiers) == {item.market.external_id for item in assignments}


@pytest.mark.asyncio
async def test_bounded_chunks_preserve_one_cohort_and_all_10_50_assignments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("prediction_collector.database.utc_now", lambda: NOW)
    connection = _TierConnection()
    database = _database(connection)
    assignments = [
        *(
            _assignment(index, CollectionTier.FULL_L2, reasons=("full_l2_score",))
            for index in range(10)
        ),
        *(
            _assignment(
                index,
                CollectionTier.SAMPLED,
                reasons=("sampled_resource_ceiling",),
            )
            for index in range(10, 60)
        ),
        *(_assignment(index) for index in range(60, 75)),
    ]

    changed = await database.record_tier_assignments(
        assignments, _max_payload_bytes=512
    )

    assert changed == len(assignments)
    assert connection.history_calls > 1
    assert connection.history_calls == connection.tier_calls
    assert max(connection.payload_sizes) <= 512
    assert set(connection.evaluated_at_values) == {NOW}
    assert len(connection.tiers) == len(assignments)
    assert sum(row["tier"] == "full_l2" for row in connection.tiers.values()) == 10
    assert sum(row["tier"] == "sampled" for row in connection.tiers.values()) == 50
    assert len(connection.history) == len(assignments)


@pytest.mark.asyncio
async def test_later_chunk_failure_rolls_back_and_retry_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = [NOW]
    monkeypatch.setattr(
        "prediction_collector.database.utc_now", lambda: observed_at[0]
    )
    connection = _TierConnection()
    connection.fail_on_history_call = 3
    database = _database(connection)
    assignments = [_assignment(index) for index in range(20)]

    with pytest.raises(RuntimeError, match="later-chunk"):
        await database.record_tier_assignments(
            assignments, _max_payload_bytes=512
        )

    assert connection.tiers == {}
    assert connection.history == {}
    assert connection.commits == 0
    assert connection.rollbacks == 1

    connection.fail_on_history_call = None
    observed_at[0] = NOW + timedelta(minutes=1)
    assert (
        await database.record_tier_assignments(
            assignments, _max_payload_bytes=512
        )
        == len(assignments)
    )
    successful_history = deepcopy(connection.history)
    assert len(connection.tiers) == len(assignments)
    assert len(successful_history) == len(assignments)

    observed_at[0] = NOW + timedelta(minutes=2)
    assert (
        await database.record_tier_assignments(
            assignments, _max_payload_bytes=512
        )
        == 0
    )
    assert connection.history == successful_history
    assert len(connection.tiers) == len(assignments)
    assert connection.commits == 2


def test_production_scale_chunking_never_constructs_a_jsonb_sized_parameter() -> None:
    total_assignments = 177_000
    long_reason = "production-scale-reason-" + ("x" * 1_500)
    max_payload_bytes = 64 * 1024
    total_element_bytes = 0
    rows_seen = 0
    chunks_seen = 0

    assignments = (
        _assignment(index, reasons=(long_reason,))
        for index in range(total_assignments)
    )
    for chunk in _tier_assignment_payload_chunks(
        assignments, max_bytes=max_payload_bytes
    ):
        encoded = canonical_json(chunk).encode("utf-8")
        assert len(encoded) <= max_payload_bytes
        # Strip this chunk's brackets and commas to reconstruct the size of the
        # old one-array representation without ever allocating it.
        total_element_bytes += len(encoded) - 2 - max(len(chunk) - 1, 0)
        rows_seen += len(chunk)
        chunks_seen += 1

    old_single_payload_bytes = total_element_bytes + max(rows_seen - 1, 0) + 2
    assert rows_seen == total_assignments
    assert chunks_seen > 1
    assert old_single_payload_bytes > _POSTGRESQL_JSONB_MAX_CONTAINER_BYTES


def test_observed_production_cohort_exceeded_limit_even_at_minimum_row_size() -> None:
    minimum_value = {
        "external_id": "0" * 66,
        "tier": "metadata_only",
        "score": "0",
        "reason_codes": ["closed"],
        "ceiling_binding": False,
    }
    assignments_in_failed_cohort = 2_564_548
    value_bytes = len(canonical_json(minimum_value).encode("utf-8"))
    minimum_array_bytes = (
        2
        + (assignments_in_failed_cohort * value_bytes)
        + assignments_in_failed_cohort
        - 1
    )

    assert minimum_array_bytes == 435_973_161
    assert minimum_array_bytes > _POSTGRESQL_JSONB_MAX_CONTAINER_BYTES
