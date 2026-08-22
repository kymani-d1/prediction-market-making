from __future__ import annotations

import asyncio
import gc
import weakref
from datetime import UTC, datetime
from typing import Any

import pytest

from prediction_collector.config import Settings
from prediction_collector.database import Database
from prediction_collector.jobs.backfill import run_polymarket_backfill


NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []
        self.rowcount = len(self._rows)

    async def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


def _candidate_row(external_id: str) -> dict[str, Any]:
    return {
        "exchange": "polymarket",
        "external_id": external_id,
        "ticker": f"TICKER-{external_id}",
        "status": "active",
        "is_active": True,
        "is_tradable": True,
        "archived": False,
        "accepting_orders": True,
        "enable_order_book": True,
        "open_time": NOW,
        "close_time": None,
        "volume": None,
        "volume_24h": None,
        "liquidity": None,
        "has_maker_rewards": external_id == "0002",
        "token_ids": [f"YES-{external_id}", f"NO-{external_id}"],
    }


class _CandidateConnection:
    def __init__(self) -> None:
        self.requests: list[tuple[str, int]] = []

    async def execute(self, query: str, params: tuple[Any, ...]) -> _Cursor:
        normalized = " ".join(query.split())
        assert "FROM ( SELECT m.id" in normalized
        assert "LEFT JOIN LATERAL" in normalized
        exchange, after_external_id, batch_size = params
        assert exchange == "polymarket"
        self.requests.append((str(after_external_id), int(batch_size)))
        rows = [_candidate_row(value) for value in ("0001", "0002", "0003")]
        page = [
            row
            for row in rows
            if str(row["external_id"]) > str(after_external_id)
        ][: int(batch_size)]
        return _Cursor(page)


class _ConnectionContext:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def __aenter__(self) -> Any:
        return self._connection

    async def __aexit__(self, *_: Any) -> None:
        return None


class _Pool:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


def _database(connection: Any) -> Database:
    database = Database(Settings())
    database.pool = _Pool(connection)  # type: ignore[assignment]
    return database


@pytest.mark.asyncio
async def test_candidate_iterator_is_bounded_complete_and_compact() -> None:
    connection = _CandidateConnection()
    database = _database(connection)

    candidates = [
        candidate
        async for candidate in database.iter_live_candidates(
            "polymarket", batch_size=2
        )
    ]

    assert [candidate.external_id for candidate in candidates] == [
        "0001",
        "0002",
        "0003",
    ]
    assert connection.requests == [("", 2), ("0002", 2), ("0003", 2)]
    assert all(candidate.raw_data == {} for candidate in candidates)
    assert all(candidate.open_time == NOW for candidate in candidates)
    assert candidates[1].has_maker_rewards is True
    assert candidates[2].outcome_token_ids == ("YES-0003", "NO-0003")


class _AssignmentGraph:
    pass


class _LifecycleDatabase:
    assignment_reference: weakref.ReferenceType[_AssignmentGraph] | None = None

    async def live_candidates(self, exchange: str) -> list[object]:
        assert exchange == "polymarket"
        return []

    async def record_tier_assignments(
        self, assignments: list[_AssignmentGraph]
    ) -> None:
        assert len(assignments) == 1
        self.assignment_reference = weakref.ref(assignments[0])


class _LifecycleTierManager:
    def __init__(self, database: _LifecycleDatabase) -> None:
        self.database = database

    def evaluate(self, markets: list[object]) -> list[_AssignmentGraph]:
        assert markets == []
        return [_AssignmentGraph()]

    def counts(self) -> dict[str, int]:
        return {"metadata_only": 1}


class _LifecycleService:
    def __init__(self, database: _LifecycleDatabase) -> None:
        self.database = database

    async def sync_metadata(self, **_: Any) -> dict[str, int]:
        return {"markets": 1}

    async def sync_fees_and_incentives(self, **_: Any) -> dict[str, int]:
        gc.collect()
        assert self.database.assignment_reference is not None
        assert self.database.assignment_reference() is None
        return {"errors": 0}

    async def backfill_trades(self) -> dict[str, int]:
        return {"trades": 0}

    async def backfill_comments(self) -> dict[str, int]:
        return {"comments": 0, "errors": 0}

    async def backfill_market_data(self) -> dict[str, int]:
        return {"books": 0}


class _LifecycleWriter:
    def __init__(self, tier_manager: _LifecycleTierManager) -> None:
        self.tier_manager = tier_manager
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.archive = None
        self.failed_items = 0
        self.rows_written = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


@pytest.mark.asyncio
async def test_tier_assignment_graph_is_released_before_fee_phase() -> None:
    database = _LifecycleDatabase()
    service = _LifecycleService(database)
    writer = _LifecycleWriter(_LifecycleTierManager(database))

    result = await run_polymarket_backfill(  # type: ignore[arg-type]
        service,  # type: ignore[arg-type]
        writer,  # type: ignore[arg-type]
    )

    assert result.status == "completed"
