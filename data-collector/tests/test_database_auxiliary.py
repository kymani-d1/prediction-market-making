from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from prediction_collector.database import Database, _write_query


class FakeCursor:
    def __init__(
        self,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows or []

    async def fetchone(self) -> dict[str, Any] | None:
        return self.row

    async def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class FakeConnection:
    def __init__(
        self,
        *,
        existing_members: list[dict[str, Any]] | None = None,
        existing_relationships: list[dict[str, Any]] | None = None,
    ) -> None:
        self.executions: list[tuple[str, tuple[Any, ...]]] = []
        self.existing_members = existing_members or []
        self.existing_relationships = existing_relationships or []

    async def execute(
        self, sql: str, parameters: tuple[Any, ...]
    ) -> FakeCursor:
        compact_sql = " ".join(sql.split())
        self.executions.append((compact_sql, parameters))
        if compact_sql.startswith("SELECT m.id AS market_id"):
            leg_ticker = parameters[2]
            if leg_ticker == "LEG-A":
                return FakeCursor({"market_id": 11, "outcome_id": 111})
            if leg_ticker == "LEG-C":
                return FakeCursor({"market_id": 33, "outcome_id": 333})
            return FakeCursor(None)
        if compact_sql.startswith("SELECT id, market_id, outcome_id, valid_from"):
            return FakeCursor(rows=self.existing_members)
        if compact_sql.startswith("SELECT id, to_market_id, to_outcome_id"):
            return FakeCursor(rows=self.existing_relationships)
        return FakeCursor()


@pytest.mark.asyncio
async def test_multivariate_legs_become_memberships_and_directional_relationships() -> None:
    connection = FakeConnection()
    database = Database.__new__(Database)
    valid_from = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)
    raw = {
        "mve_collection_ticker": "COLLECTION-A",
        "mve_selected_legs": [
            {
                "event_ticker": "EVENT-A",
                "market_ticker": "LEG-A",
                "side": "yes",
                "yes_settlement_value_dollars": "1.0000",
            },
            {
                "event_ticker": "EVENT-B",
                "market_ticker": "NOT-SYNCED-YET",
                "side": "no",
                "yes_settlement_value_dollars": "0.0000",
            },
        ],
    }

    await database._record_multivariate_legs(
        connection,
        group_id=7,
        market_id=22,
        market_external_id="MVE-COMBO",
        exchange="kalshi",
        observed_at=valid_from,
        raw=raw,
    )

    lookup_calls = [
        call
        for call in connection.executions
        if call[0].startswith("SELECT m.id AS market_id")
    ]
    assert [call[1] for call in lookup_calls] == [
        ("LEG-A:yes", "kalshi", "LEG-A"),
        ("NOT-SYNCED-YET:no", "kalshi", "NOT-SYNCED-YET"),
    ]

    membership_calls = [
        call
        for call in connection.executions
        if call[0].startswith("INSERT INTO market_group_members")
    ]
    assert len(membership_calls) == 1
    assert membership_calls[0][1][:5] == (7, 22, 11, 111, valid_from)

    relationship_calls = [
        call
        for call in connection.executions
        if call[0].startswith("INSERT INTO market_relationships")
    ]
    assert len(relationship_calls) == 1
    assert relationship_calls[0][1][:5] == (
        "kalshi",
        22,
        11,
        111,
        valid_from,
    )
    assert "'multivariate_leg'" in relationship_calls[0][0]


@pytest.mark.asyncio
async def test_authoritative_multivariate_refresh_retires_missing_links() -> None:
    observed_at = datetime(2026, 8, 11, 15, 0, tzinfo=UTC)
    previous_from = observed_at - timedelta(hours=1)
    connection = FakeConnection(
        existing_members=[
            {
                "id": 501,
                "market_id": 44,
                "outcome_id": 444,
                "valid_from": previous_from,
            }
        ],
        existing_relationships=[
            {
                "id": 601,
                "to_market_id": 44,
                "to_outcome_id": 444,
                "constraint_definition": {"market_ticker": "REMOVED"},
                "valid_from": previous_from,
            }
        ],
    )
    database = Database.__new__(Database)

    await database._record_multivariate_legs(
        connection,
        group_id=7,
        market_id=22,
        market_external_id="MVE-COMBO",
        exchange="kalshi",
        observed_at=observed_at,
        raw={
            "mve_collection_ticker": "COLLECTION-A",
            "mve_selected_legs": [
                {"market_ticker": "LEG-A", "side": "yes"},
                {"market_ticker": "LEG-C", "side": "yes"},
            ],
        },
    )

    assert any(
        sql.startswith("UPDATE market_group_members SET valid_to")
        and params == (observed_at, 501)
        for sql, params in connection.executions
    )
    assert any(
        sql.startswith("UPDATE market_relationships SET valid_to")
        and params == (observed_at, 601)
        for sql, params in connection.executions
    )


def test_reference_and_sports_writes_resolve_normalized_parent_rows() -> None:
    reference_sql, _ = _write_query("reference_price_updates", {})
    sports_sql, _ = _write_query("sports_feed_updates", {})

    assert "INSERT INTO reference_instruments" in reference_sql
    assert "reference_instrument_id" in reference_sql
    assert "INSERT INTO sports_events" in sports_sql
    assert "sports_event_id" in sports_sql
