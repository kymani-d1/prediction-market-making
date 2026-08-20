from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.config import Settings
from prediction_collector.common.types import MarketCandidate
from prediction_collector.database import (
    Database,
    _CURRENT_TIER_STATUS_SQL,
    _EXPECTED_POLYMARKET_CRYPTO_REFERENCE_PROVIDERS,
    _discovery_status,
    _reference_data_status,
)
from prediction_collector.jobs.live import LiveCollector
from prediction_collector.writer import BatchWriter, WriteItem


NOW = datetime(2026, 8, 14, tzinfo=UTC)


def test_metadata_schema_warning_does_not_make_completed_discovery_retry() -> None:
    status = _discovery_status(
        latest_complete_discovery=NOW,
        open_refresh_failures=0,
        open_coverage_warnings=3,
        open_metadata_schema_warnings=3,
    )
    assert status == {
        "discovery_state": "ready",
        "discovery_warnings": {
            "open_total": 3,
            "market_metadata_schema_failure": 3,
        },
    }
    assert _discovery_status(
        latest_complete_discovery=NOW,
        open_refresh_failures=1,
        open_coverage_warnings=3,
        open_metadata_schema_warnings=3,
    )["discovery_state"] == "retrying"


def test_current_tier_status_excludes_stale_evaluation_cohorts() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE market_collection_tiers (
            tier TEXT NOT NULL,
            ceiling_binding INTEGER NOT NULL,
            evaluated_at TEXT NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO market_collection_tiers VALUES (?, ?, ?)",
        [
            *(('full_l2', 0, '2026-08-14T10:00:00Z') for _ in range(17)),
            *(('sampled', 0, '2026-08-14T10:00:00Z') for _ in range(59)),
            *(('full_l2', 0, '2026-08-14T10:05:00Z') for _ in range(10)),
            *(('sampled', 1, '2026-08-14T10:05:00Z') for _ in range(50)),
        ],
    )
    rows = connection.execute(_CURRENT_TIER_STATUS_SQL).fetchall()
    counts = {row["tier"]: row["markets"] for row in rows}
    assert counts == {"full_l2": 10, "sampled": 50}


def test_reference_health_requires_every_expected_crypto_provider_to_be_fresh() -> None:
    latest = {
        provider: NOW - timedelta(seconds=index + 1)
        for index, provider in enumerate(
            _EXPECTED_POLYMARKET_CRYPTO_REFERENCE_PROVIDERS
        )
    }
    status = _reference_data_status(
        configured=True,
        connected=True,
        latest_message=NOW - timedelta(seconds=1),
        latest_valid_by_provider=latest,
        stale_after_seconds=600,
        observed_at=NOW,
    )
    assert status["status"] == "healthy"
    assert status["healthy"] is True
    assert status["fresh_crypto_providers"] == 4
    assert "pyth" not in status["providers"]
    assert status["latest_message"] == NOW - timedelta(seconds=1)


def test_reference_health_distinguishes_partial_and_complete_staleness() -> None:
    partial = _reference_data_status(
        configured=True,
        connected=True,
        latest_message=NOW,
        latest_valid_by_provider={"binance": NOW},
        stale_after_seconds=600,
        observed_at=NOW,
    )
    assert partial["status"] == "degraded"
    assert partial["healthy"] is False

    stale = _reference_data_status(
        configured=True,
        connected=True,
        latest_message=NOW,
        latest_valid_by_provider={
            provider: NOW - timedelta(seconds=601)
            for provider in _EXPECTED_POLYMARKET_CRYPTO_REFERENCE_PROVIDERS
        },
        stale_after_seconds=600,
        observed_at=NOW,
    )
    assert stale["status"] == "stale"
    assert stale["healthy"] is False
    assert stale["fresh_crypto_providers"] == 0


class _Cursor:
    def __init__(self, *, row: dict[str, Any] | None = None, rowcount: int = 0) -> None:
        self.row = row
        self.rowcount = rowcount

    async def fetchone(self) -> dict[str, Any] | None:
        return self.row


class _RunConnection:
    def __init__(self) -> None:
        self.runs = [
            {
                "id": 1,
                "job_type": "live",
                "exchange": "polymarket",
                "status": "running",
            }
        ]
        self.queries: list[str] = []

    async def execute(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> _Cursor:
        self.queries.append(query)
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT pg_advisory_xact_lock"):
            return _Cursor()
        if normalized.startswith("UPDATE collector_runs SET status = 'cancelled'"):
            changed = 0
            for run in self.runs:
                if (
                    run["job_type"] == "live"
                    and run["status"] == "running"
                    and run["exchange"] == params[0]
                ):
                    run["status"] = "cancelled"
                    changed += 1
            return _Cursor(rowcount=changed)
        if normalized.startswith("INSERT INTO collector_runs"):
            self.runs.append(
                {
                    "id": 2,
                    "job_type": params[1],
                    "exchange": params[2],
                    "status": "running",
                }
            )
            return _Cursor(row={"id": 2}, rowcount=1)
        if normalized.startswith("UPDATE collector_runs SET finished_at"):
            run_id = params[-1]
            changed = 0
            for run in self.runs:
                if run["id"] == run_id and run["status"] == "running":
                    run["status"] = params[0]
                    changed += 1
            return _Cursor(rowcount=changed)
        raise AssertionError(normalized)


class _ConnectionContext:
    def __init__(self, connection: _RunConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _RunConnection:
        return self.connection

    async def __aexit__(self, *_: Any) -> None:
        return None


class _Pool:
    def __init__(self, connection: _RunConnection) -> None:
        self.connection_value = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self.connection_value)


class _SelectionConnection:
    def __init__(self) -> None:
        self.query = ""
        self.params: tuple[Any, ...] = ()

    async def execute(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> _Cursor:
        self.query = query
        self.params = params or ()
        return _Cursor(rowcount=1)


@pytest.mark.asyncio
async def test_live_selection_rows_share_one_explicit_evaluation_cohort() -> None:
    connection = _SelectionConnection()
    database = Database(Settings())
    database.pool = _Pool(connection)  # type: ignore[assignment]
    market = MarketCandidate(
        exchange="polymarket",
        external_id="market-1",
        ticker="market-1",
        status="active",
        active=True,
        tradable=True,
        accepting_orders=True,
        enable_order_book=True,
        liquidity=Decimal("100"),
    )

    await database.record_live_selection(
        7,
        [market],
        {("polymarket", "market-1")},
        {},
    )

    normalized = " ".join(connection.query.split())
    assert "market_external_id, evaluated_at, is_active" in normalized
    assert connection.params[1] == 7
    assert isinstance(connection.params[2], datetime)


@pytest.mark.asyncio
async def test_new_live_run_cancels_superseded_running_row_without_reopening_it() -> None:
    connection = _RunConnection()
    database = Database(Settings())
    database.pool = _Pool(connection)  # type: ignore[assignment]

    assert await database.start_run("live", "polymarket") == 2
    assert [(run["id"], run["status"]) for run in connection.runs] == [
        (1, "cancelled"),
        (2, "running"),
    ]

    # A late graceful finish from the draining superseded container cannot
    # rewrite the reconciled cancellation as a successful current run.
    await database.finish_run(
        1,
        status="completed",
        records_processed=0,
        rows_written=0,
    )
    assert connection.runs[0]["status"] == "cancelled"


class _DegradationDatabase:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def record_archive_degradation(self, **value: Any) -> None:
        self.events.append(value)


@pytest.mark.asyncio
async def test_optional_hot_write_shed_records_storage_sources() -> None:
    degradation_database = _DegradationDatabase()
    writer = BatchWriter.__new__(BatchWriter)
    writer.archive = type("Archive", (), {"database": degradation_database})()
    writer.run_id = 8
    writer.set_storage_pressure(
        "critical",
        details={
            "critical_sources": ["archive_queue_rows", "archive_queue_bytes"],
            "archive_queue_rows": 4096,
            "archive_queue_bytes": 67_108_864,
        },
    )
    await writer._record_pressure_degradation(
        WriteItem("reference_price_updates", {}), priority=4
    )
    assert degradation_database.events == [
        {
            "run_id": 8,
            "stream": "reference_price_updates",
            "priority": 4,
            "reason": "storage_critical_optional_hot_write_shed",
            "rows_affected": 1,
            "bytes_affected": 0,
            "details": {
                "critical_sources": ["archive_queue_rows", "archive_queue_bytes"],
                "archive_queue_rows": 4096,
                "archive_queue_bytes": 67_108_864,
            },
        }
    ]


class _StorageDatabase:
    def __init__(self, collector: LiveCollector, *, database_bytes: int) -> None:
        self.collector = collector
        self.database_bytes = database_bytes
        self.resolutions = 0
        self.metrics: list[dict[str, Any]] = []

    async def storage_snapshot(self) -> dict[str, Any]:
        return {
            "postgres_database_bytes": self.database_bytes,
            "major_table_bytes": {},
        }

    async def record_storage_metrics(self, **value: Any) -> None:
        self.metrics.append(value)
        self.collector.stop.set()

    async def resolve_optional_hot_write_degradations(self) -> None:
        self.resolutions += 1


class _StorageArchive:
    def __init__(self, *, queue_rows: int, queue_bytes: int) -> None:
        self.queue_rows = queue_rows
        self.queue_bytes = queue_bytes

    def metrics(self) -> dict[str, Any]:
        return {
            "queue_depth": self.queue_rows,
            "queue_bytes": self.queue_bytes,
        }


class _StorageWriter:
    def __init__(self, archive: _StorageArchive) -> None:
        self.archive = archive
        self.pressure: str | None = None
        self.details: dict[str, Any] = {}

    def set_storage_pressure(
        self, pressure: str, *, details: dict[str, Any] | None = None
    ) -> None:
        self.pressure = pressure
        self.details = dict(details or {})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("queue_rows", "expected_pressure", "expected_resolutions"),
    [(0, "normal", 1), (20, "critical", 0)],
)
async def test_storage_loop_resolves_optional_shedding_only_below_critical(
    queue_rows: int,
    expected_pressure: str,
    expected_resolutions: int,
) -> None:
    collector = LiveCollector.__new__(LiveCollector)
    collector.settings = Settings(
        postgres_storage_warn_gb=10,
        postgres_storage_critical_gb=20,
        archive_queue_warn_rows=5,
        archive_queue_critical_rows=10,
        archive_queue_warn_bytes=5_000,
        archive_queue_critical_bytes=10_000,
        storage_metrics_interval_seconds=60,
    )
    collector.stop = asyncio.Event()
    collector.run_id = 9
    writer = _StorageWriter(
        _StorageArchive(queue_rows=queue_rows, queue_bytes=0)
    )
    collector.writer = writer  # type: ignore[assignment]
    database = _StorageDatabase(collector, database_bytes=1_000)
    collector.database = database  # type: ignore[assignment]

    await asyncio.wait_for(collector._storage_loop(), timeout=1)
    assert writer.pressure == expected_pressure
    assert database.resolutions == expected_resolutions
    if expected_pressure == "critical":
        assert writer.details["critical_sources"] == ["archive_queue_rows"]


class _SqlConnection:
    def __init__(self) -> None:
        self.query = ""

    async def execute(self, query: str) -> _Cursor:
        self.query = query
        return _Cursor()


@pytest.mark.asyncio
async def test_optional_shed_resolution_is_compatible_and_reason_scoped() -> None:
    connection = _SqlConnection()
    database = Database(Settings())
    database.pool = _Pool(connection)  # type: ignore[assignment]
    await database.resolve_optional_hot_write_degradations()
    assert "storage_critical_optional_hot_write_shed" in connection.query
    assert "postgres_critical_optional_hot_write_shed" in connection.query
    assert "archive_spool_hard_capacity_exceeded" not in connection.query
