from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from prediction_collector.kalshi.service import KalshiService


@dataclass
class FakeResult:
    url: str
    response_timestamp: datetime


class FakeRest:
    def __init__(self) -> None:
        self.market_filters: list[str | None] = []

    async def iter_series(self):  # type: ignore[no-untyped-def]
        yield (
            [{"ticker": "KX-SERIES", "title": "Series"}],
            FakeResult("https://kalshi.test/series", NOW),
            None,
        )

    async def iter_events(self, *, status: str | None = None):  # type: ignore[no-untyped-def]
        assert status is None
        yield (
            [
                {
                    "event_ticker": "EVENT-A",
                    "series_ticker": "KX-SERIES",
                    "title": "Ordinary event",
                }
            ],
            FakeResult("https://kalshi.test/events", NOW),
            None,
        )

    async def iter_markets(  # type: ignore[no-untyped-def]
        self, *, status: str | None = None, mve_filter: str | None = None
    ):
        assert status is None
        self.market_filters.append(mve_filter)
        yield (
            [
                {
                    "ticker": "LEG-A",
                    "event_ticker": "EVENT-A",
                    "title": "Selected leg",
                    "status": "open",
                }
            ],
            FakeResult("https://kalshi.test/markets", NOW),
            None,
        )

    async def iter_historical_markets(self):  # type: ignore[no-untyped-def]
        if False:
            yield None

    async def iter_multivariate_events(  # type: ignore[no-untyped-def]
        self, *, with_nested_markets: bool
    ):
        assert with_nested_markets is True
        yield (
            [
                {
                    "event_ticker": "MVE-EVENT",
                    "series_ticker": "KX-SERIES",
                    "title": "Generated MVE event",
                    "markets": [
                        {
                            "ticker": "MVE-COMBO",
                            "title": "Combination",
                            "status": "open",
                            "mve_collection_ticker": "COLLECTION-A",
                            "mve_selected_legs": [
                                {
                                    "event_ticker": "EVENT-A",
                                    "market_ticker": "LEG-A",
                                    "side": "yes",
                                    "yes_settlement_value_dollars": "1.0000",
                                }
                            ],
                        }
                    ],
                }
            ],
            FakeResult("https://kalshi.test/events/multivariate", NOW),
            None,
        )

    async def iter_multivariate_event_collections(self):  # type: ignore[no-untyped-def]
        yield (
            [
                {
                    "collection_ticker": "COLLECTION-A",
                    "series_ticker": "KX-SERIES",
                    "title": "Collection A",
                    "associated_event_tickers": ["EVENT-A"],
                }
            ],
            FakeResult("https://kalshi.test/multivariate_event_collections", NOW),
            None,
        )


class FakeDatabase:
    def __init__(self) -> None:
        self.market_external_ids: list[str] = []
        self.events: list[dict[str, Any]] = []
        self.groups: list[dict[str, Any]] = []
        self.checkpoints: list[str] = []

    async def upsert_series(self, value: dict[str, Any]) -> int:
        return 1

    async def upsert_event(self, value: dict[str, Any]) -> int:
        self.events.append(value)
        return len(self.events)

    async def upsert_market(
        self, value: dict[str, Any], *, diagnostics: Any = None
    ) -> int:
        self.market_external_ids.append(value["external_id"])
        if value["external_id"] == "MVE-COMBO":
            assert value["event_external_id"] == "MVE-EVENT"
            assert diagnostics is not None
            diagnostics.stale_lifecycle_states_preserved += 1
            diagnostics.unresolved_multivariate_leg_markets += 2
            diagnostics.unresolved_multivariate_leg_outcomes += 1
        return len(self.market_external_ids)

    async def upsert_outcome(self, market_id: int, value: dict[str, Any]) -> int:
        return market_id * 10 + int(value["outcome_index"])

    async def upsert_market_group(self, value: dict[str, Any]) -> int:
        self.groups.append(value)
        return len(self.groups)

    async def checkpoint(
        self,
        exchange: str,
        entity: str,
        **kwargs: Any,
    ) -> None:
        assert exchange == "kalshi"
        self.checkpoints.append(entity)


NOW = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_metadata_sync_upserts_multivariate_events_markets_and_collections(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="prediction_collector.kalshi.service")
    rest = FakeRest()
    database = FakeDatabase()
    service = KalshiService(
        rest=rest,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        writer=object(),  # type: ignore[arg-type]
        store_raw_rest=False,
    )

    counts = await service.sync_metadata(include_historical=False)

    assert counts == {
        "series": 1,
        "events": 2,
        "markets": 2,
        "outcomes": 4,
        "market_groups": 1,
    }
    assert rest.market_filters == ["exclude"]
    assert database.market_external_ids == ["LEG-A", "MVE-COMBO"]
    assert [event["external_id"] for event in database.events] == [
        "EVENT-A",
        "MVE-EVENT",
    ]
    assert database.groups[0]["external_id"] == "mve:COLLECTION-A"
    assert database.groups[0]["raw_data"]["collection_ticker"] == "COLLECTION-A"
    assert database.checkpoints == [
        "markets",
        "multivariate_events",
        "multivariate_event_collections",
    ]
    summaries = [
        record
        for record in caplog.records
        if record.getMessage() == "Kalshi metadata sync complete"
    ]
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.stale_lifecycle_states_preserved == 1
    assert summary.unresolved_multivariate_legs == 3
    assert summary.unresolved_multivariate_leg_markets == 2
    assert summary.unresolved_multivariate_leg_outcomes == 1
