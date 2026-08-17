from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
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

    async def iter_events(self, *, closed: bool, after_cursor: str | None = None):
        del closed, after_cursor
        if False:
            yield

    async def iter_markets(self, *, closed: bool, after_cursor: str | None = None):
        del closed, after_cursor
        for index, page in enumerate(self.pages):
            yield page, market_page_result(), f"page-{index}"


class MetadataDatabase:
    def __init__(self, *, market_error: Exception | None = None) -> None:
        self.market_error = market_error
        self.markets: list[dict[str, Any]] = []
        self.outcomes: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.checkpoints: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.checkpoint_values: dict[tuple[str, str, str], str | None] = {}

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
        self.checkpoint_values[(args[0], args[1], kwargs["checkpoint_key"])] = (
            kwargs.get("cursor")
        )

    async def checkpoint_cursor(
        self, exchange: str, job: str, *, checkpoint_key: str = "default"
    ) -> str | None:
        return self.checkpoint_values.get((exchange, job, checkpoint_key))

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


def invalid_metric_market(index: int = 0) -> dict[str, Any]:
    return {
        **valid_market(),
        "id": str(248_410 + index),
        "slug": (
            "will-the-buffalo-bills-win-super-bowl-lvii"
            if index == 0
            else f"invalid-metric-{index}"
        ),
        "conditionId": f"0xinvalid-metric-{index}",
        "volumeNum": "616.31",
        "volume24hr": 0,
        "liquidityNum": "13.28",
        "orderPriceMinTickSize": 0,
        "feeRate": "16000000000000000",
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
    assert not any(args[1] == "metadata_markets" for args, _ in database.checkpoints)


class CursorFailureRest(MetadataRest):
    def __init__(self, *, fail: bool) -> None:
        super().__init__([])
        self.fail = fail
        self.market_start_cursors: list[str | None] = []

    async def iter_markets(
        self, *, closed: bool, after_cursor: str | None = None
    ):
        del closed
        self.market_start_cursors.append(after_cursor)
        if after_cursor is None:
            yield [valid_market()], market_page_result(), "cursor-A"
        if self.fail:
            request = httpx.Request(
                "GET",
                "https://gamma-api.polymarket.com/markets/keyset",
            )
            response = httpx.Response(403, request=request)
            raise httpx.HTTPStatusError(
                "persistent Gamma denial", request=request, response=response
            )
        yield [
            {
                **valid_market(),
                "id": "2290080",
                "conditionId": "0xresumed-condition",
            }
        ], market_page_result(), None


@pytest.mark.asyncio
async def test_metadata_checkpoint_stays_on_last_completed_page_and_restart_resumes() -> None:
    database = MetadataDatabase()
    writer = MetadataWriter()
    failing_rest = CursorFailureRest(fail=True)
    service = PolymarketService(
        rest=failing_rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    with pytest.raises(httpx.HTTPStatusError, match="persistent Gamma denial"):
        await service.sync_metadata(include_closed=False)

    checkpoint_key = ("polymarket", "metadata_markets", "closed=false")
    assert database.checkpoint_values[checkpoint_key] == "cursor-A"
    assert failing_rest.market_start_cursors == [None]
    assert any(
        market["external_id"] == "0xvalid-condition"
        for market in database.markets
    )

    resumed_rest = CursorFailureRest(fail=False)
    resumed_service = PolymarketService(
        rest=resumed_rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )
    await resumed_service.sync_metadata(include_closed=False)

    assert resumed_rest.market_start_cursors == ["cursor-A"]
    assert any(
        market["external_id"] == "0xresumed-condition"
        for market in database.markets
    )
    assert database.checkpoint_values[checkpoint_key] is None


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


@pytest.mark.asyncio
async def test_metadata_sync_normalizes_invalid_metric_and_preserves_raw_evidence() -> None:
    raw = invalid_metric_market()
    database = MetadataDatabase()
    writer = MetadataWriter()
    service = PolymarketService(
        rest=MetadataRest([[raw]]),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    result = await service.sync_metadata(include_closed=False)

    assert len(database.markets) == 1
    market = database.markets[0]
    assert market["tick_size"] is None
    assert market["fee_rate"] == Decimal("16000000000000000")
    assert market["raw_data"]["orderPriceMinTickSize"] == 0
    assert result["invalid_market_metric_values_normalized"] == 1
    assert result["invalid_market_metric_counts"] == {"tick_size": 1}
    assert result["invalid_market_metric_samples"] == [
        {
            "raw_id": "248410",
            "external_id": "0xinvalid-metric-0",
            "conditionId": "0xinvalid-metric-0",
            "slug": "will-the-buffalo-bills-win-super-bowl-lvii",
            "metric_name": "tick_size",
            "raw_value": 0,
            "reason": "invalid_market_metric_normalized",
        }
    ]
    assert len(database.gaps) == 1
    assert database.gaps[0]["channel"] == "rest:metadata_backfill"
    assert database.gaps[0]["gap_type"] == "market_metadata_schema_failure"
    assert database.gaps[0]["details"] == {
        "invalid_market_metric_values_normalized": 1,
        "invalid_market_metric_counts": {"tick_size": 1},
        "invalid_market_metric_samples": result[
            "invalid_market_metric_samples"
        ],
    }
    assert writer.raw_items[0].data["payload"][0]["orderPriceMinTickSize"] == 0


@pytest.mark.asyncio
async def test_invalid_metric_diagnostics_are_aggregated_and_bounded() -> None:
    raw_markets = [invalid_metric_market(index) for index in range(12)]
    for raw in raw_markets:
        raw["volumeNum"] = "-1"
    database = MetadataDatabase()
    writer = MetadataWriter()
    service = PolymarketService(
        rest=MetadataRest([raw_markets]),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
    )

    result = await service.sync_metadata(include_closed=False)

    assert len(database.markets) == 12
    assert all(market["tick_size"] is None for market in database.markets)
    assert all(market["volume"] is None for market in database.markets)
    assert result["invalid_market_metric_values_normalized"] == 24
    assert result["invalid_market_metric_counts"] == {
        "volume": 12,
        "tick_size": 12,
    }
    assert len(result["invalid_market_metric_samples"]) == 10
    assert len(database.gaps) == 1
    assert len(database.gaps[0]["details"]["invalid_market_metric_samples"]) == 10


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


class MetricBackfillService(BackfillService):
    async def sync_metadata(self, **_: Any) -> dict[str, Any]:
        return {
            "markets": 1,
            "malformed_markets_skipped": 0,
            "invalid_market_metric_values_normalized": 1,
            "invalid_market_metric_counts": {"tick_size": 1},
        }


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


@pytest.mark.asyncio
async def test_backfill_is_partial_when_invalid_market_metric_was_normalized() -> None:
    result = await run_polymarket_backfill(
        MetricBackfillService(),  # type: ignore[arg-type]
        BackfillWriter(),  # type: ignore[arg-type]
    )
    assert result.status == "partial"
    assert result.details["metadata"] == {
        "markets": 1,
        "malformed_markets_skipped": 0,
        "invalid_market_metric_values_normalized": 1,
        "invalid_market_metric_counts": {"tick_size": 1},
    }
