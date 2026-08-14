from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from prediction_collector.common.http import HttpResult
from prediction_collector.jobs.backfill import run_polymarket_backfill
from prediction_collector.polymarket.service import PolymarketService


NOW = datetime(2026, 8, 14, 12, tzinfo=UTC)


def market_page_result() -> HttpResult:
    return HttpResult(
        data=None,
        status_code=200,
        requested_at=NOW,
        response_timestamp=NOW,
        url="https://gamma-api.polymarket.com/markets?closed=false&limit=100",
    )


class MetadataRest:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages

    async def iter_series(self):
        if False:
            yield

    async def iter_tags(self):
        if False:
            yield

    async def iter_events(self, *, closed: bool):
        del closed
        if False:
            yield

    async def iter_markets(self, *, closed: bool):
        del closed
        for index, page in enumerate(self.pages):
            yield page, market_page_result(), f"page-{index}"


class MetadataDatabase:
    def __init__(self, *, market_error: Exception | None = None) -> None:
        self.market_error = market_error
        self.markets: list[dict[str, Any]] = []
        self.outcomes: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.checkpoints: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def upsert_market(self, market: dict[str, Any], **_: Any) -> int:
        self.markets.append(market)
        if self.market_error is not None:
            raise self.market_error
        return len(self.markets)

    async def upsert_outcome(self, _market_id: int, outcome: dict[str, Any]) -> None:
        self.outcomes.append(outcome)

    async def upsert_event(self, _event: dict[str, Any]) -> int:
        return 1

    async def upsert_series(self, _series: dict[str, Any]) -> int:
        return 1

    async def upsert_tag(self, _exchange: str, _tag: dict[str, Any]) -> int:
        return 1

    async def checkpoint(self, *args: Any, **kwargs: Any) -> None:
        self.checkpoints.append((args, kwargs))

    async def record_gap(self, **value: Any) -> int:
        self.gaps.append(value)
        return len(self.gaps)


class MetadataWriter:
    def __init__(self) -> None:
        self.run_id = 71
        self.raw_items: list[Any] = []

    async def put(self, item: Any) -> None:
        self.raw_items.append(item)


def malformed_market() -> dict[str, Any]:
    return {
        "id": "2290078",
        "slug": "highest-temperature-in-jinan-on-may-20-2026-15corbelow",
        "conditionId": "",
        "questionID": "question-2290078",
        "question": "Highest temperature in Jinan?",
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["bad-yes", "bad-no"]',
    }


def valid_market() -> dict[str, Any]:
    return {
        "id": "2290079",
        "slug": "valid-market",
        "conditionId": "0xvalid-condition",
        "questionID": "question-2290079",
        "question": "Valid market?",
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["valid-yes", "valid-no"]',
    }


@pytest.mark.asyncio
async def test_metadata_sync_skips_blank_identity_and_records_one_aggregate_gap() -> None:
    database = MetadataDatabase()
    writer = MetadataWriter()
    service = PolymarketService(
        rest=MetadataRest([[malformed_market()], [valid_market()]]),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    result = await service.sync_metadata(include_closed=False)

    assert [market["external_id"] for market in database.markets] == [
        "0xvalid-condition"
    ]
    assert len(database.outcomes) == 2
    assert result["markets"] == 1
    assert result["malformed_markets_skipped"] == 1
    assert result["malformed_market_samples"] == [
        {
            "raw_id": "2290078",
            "slug": "highest-temperature-in-jinan-on-may-20-2026-15corbelow",
            "reason": "market_identity_missing",
            "conditionId": "",
            "questionID": "question-2290078",
        }
    ]
    assert len(database.gaps) == 1
    assert database.gaps[0]["run_id"] == 71
    assert database.gaps[0]["channel"] == "rest:metadata_backfill"
    assert database.gaps[0]["gap_type"] == "market_metadata_schema_failure"
    assert database.gaps[0]["details"] == {
        "malformed_markets_skipped": 1,
        "samples": result["malformed_market_samples"],
    }
    assert len(writer.raw_items) == 2
    assert all(item.kind == "raw_rest_payloads" for item in writer.raw_items)
    assert writer.raw_items[0].data["payload"] == [malformed_market()]


@pytest.mark.asyncio
async def test_metadata_sync_does_not_swallow_unrelated_market_database_error() -> None:
    database = MetadataDatabase(market_error=RuntimeError("unrelated database defect"))
    writer = MetadataWriter()
    service = PolymarketService(
        rest=MetadataRest([[valid_market()]]),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="unrelated database defect"):
        await service.sync_metadata(include_closed=False)
    assert len(writer.raw_items) == 1
    assert database.gaps == []


@pytest.mark.asyncio
async def test_metadata_identity_failure_samples_are_bounded() -> None:
    malformed = [
        {
            **malformed_market(),
            "id": str(2_290_078 + index),
            "slug": f"malformed-{index}",
        }
        for index in range(12)
    ]
    database = MetadataDatabase()
    writer = MetadataWriter()
    service = PolymarketService(
        rest=MetadataRest([malformed]),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    result = await service.sync_metadata(include_closed=False)
    assert result["malformed_markets_skipped"] == 12
    assert len(result["malformed_market_samples"]) == 10
    assert len(database.gaps) == 1
    assert len(database.gaps[0]["details"]["samples"]) == 10


class BackfillService:
    def __init__(self) -> None:
        self.database = None

    async def sync_metadata(self, **_: Any) -> dict[str, Any]:
        return {
            "markets": 1,
            "malformed_markets_skipped": 1,
            "malformed_market_samples": [
                {"raw_id": "2290078", "reason": "market_identity_missing"}
            ],
        }

    async def sync_fees_and_incentives(self, **_: Any) -> dict[str, int]:
        return {"errors": 0}

    async def backfill_trades(self) -> dict[str, int]:
        return {"trades": 0}

    async def backfill_comments(self) -> dict[str, int]:
        return {"comments": 0, "errors": 0}

    async def backfill_market_data(self) -> dict[str, int]:
        return {"markets": 0}


class BackfillWriter:
    def __init__(self) -> None:
        self.tier_manager = None
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.archive = None
        self.failed_items = 0
        self.rows_written = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


@pytest.mark.asyncio
async def test_backfill_is_partial_when_metadata_identity_was_skipped() -> None:
    result = await run_polymarket_backfill(
        BackfillService(),  # type: ignore[arg-type]
        BackfillWriter(),  # type: ignore[arg-type]
    )
    assert result.status == "partial"
    assert result.details["metadata"] == {
        "markets": 1,
        "malformed_markets_skipped": 1,
        "malformed_market_samples": [
            {"raw_id": "2290078", "reason": "market_identity_missing"}
        ],
    }
