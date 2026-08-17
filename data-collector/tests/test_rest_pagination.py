from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from prediction_collector.polymarket.rest import PolymarketRestClient


@dataclass
class FakeResult:
    data: Any


class FakeHttp:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.retryable_statuses: list[frozenset[int]] = []

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        retryable_status_codes: frozenset[int] = frozenset(),
    ) -> FakeResult:
        self.calls.append((url, dict(params or {})))
        self.retryable_statuses.append(retryable_status_codes)
        return FakeResult(self.responses.pop(0))


def client(http: FakeHttp) -> PolymarketRestClient:
    return PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )


@pytest.mark.asyncio
async def test_keyset_pagination_forwards_after_cursor_without_offset() -> None:
    http = FakeHttp(
        [
            {"markets": [{"id": "1"}], "next_cursor": "cursor-A"},
            {"markets": [{"id": "2"}]},
        ]
    )
    pages = [
        page async for page in client(http).iter_markets(active=True, closed=False)
    ]
    assert [[item["id"] for item in page[0]] for page in pages] == [["1"], ["2"]]
    assert http.calls[1][1]["after_cursor"] == "cursor-A"
    assert all("offset" not in params for _, params in http.calls)
    assert http.retryable_statuses == [frozenset({403}), frozenset({403})]


@pytest.mark.asyncio
async def test_keyset_pagination_can_resume_from_durable_cursor() -> None:
    http = FakeHttp([{"markets": [{"id": "2"}]}])

    pages = [
        page
        async for page in client(http).iter_markets(
            closed=True, after_cursor="cursor-A"
        )
    ]

    assert [row["id"] for row in pages[0][0]] == ["2"]
    assert http.calls == [
        (
            "https://gamma.test/markets/keyset",
            {"closed": "true", "limit": 100, "after_cursor": "cursor-A"},
        )
    ]


@pytest.mark.asyncio
async def test_repeated_or_missing_full_page_cursor_fails_closed() -> None:
    repeated = FakeHttp(
        [
            {"markets": [{"id": "1"}], "next_cursor": "same"},
            {"markets": [{"id": "2"}], "next_cursor": "same"},
        ]
    )
    with pytest.raises(RuntimeError, match="cursor repeated"):
        _ = [page async for page in client(repeated).iter_markets()]

    missing = FakeHttp([{"markets": [{"id": str(i)} for i in range(100)]}])
    with pytest.raises(RuntimeError, match="omitted next_cursor"):
        _ = [page async for page in client(missing).iter_markets()]


@pytest.mark.asyncio
async def test_trade_window_bounds_and_offsets_are_forwarded() -> None:
    http = FakeHttp([[{"id": "1"}, {"id": "2"}], [{"id": "3"}]])
    pages = [
        page
        async for page in client(http).iter_trades(
            market="0xcondition", start=100, end=199, page_size=2
        )
    ]
    assert [[row["id"] for row in page[0]] for page in pages] == [["1", "2"], ["3"]]
    assert [params["offset"] for _, params in http.calls] == [0, 2]
    assert all(
        params["start"] == 100 and params["end"] == 199
        for _, params in http.calls
    )


@pytest.mark.asyncio
async def test_malformed_completeness_sensitive_page_fails_closed() -> None:
    rows: list[Any] = [{"id": str(index)} for index in range(9_999)]
    rows.append("schema-drift")
    http = FakeHttp([rows])
    with pytest.raises(RuntimeError, match="malformed rows"):
        _ = [page async for page in client(http).iter_trades(market="0xcondition")]
    assert len(http.calls) == 1


@pytest.mark.asyncio
async def test_rewards_uses_documented_next_cursor_parameter() -> None:
    http = FakeHttp(
        [
            {"data": [{"condition_id": "one"}], "next_cursor": "next"},
            {"data": [{"condition_id": "two"}], "next_cursor": "LTE="},
        ]
    )
    pages = [
        page async for page in client(http).iter_current_rewards(sponsored=True)
    ]
    assert [page[0][0]["condition_id"] for page in pages] == ["one", "two"]
    assert http.calls[1][1] == {"sponsored": "true", "next_cursor": "next"}


@pytest.mark.asyncio
async def test_comments_use_stable_oldest_first_order() -> None:
    http = FakeHttp([[{"id": "1", "createdAt": "2026-01-01T00:00:00Z"}]])
    _ = [page async for page in client(http).iter_comments()]
    assert http.calls[0][1] == {
        "order": "createdAt,id",
        "ascending": "true",
        "limit": 100,
        "offset": 0,
    }


@pytest.mark.asyncio
async def test_live_event_discovery_uses_documented_active_and_closed_filters() -> None:
    http = FakeHttp([{"events": [], "next_cursor": ""}])
    assert [page async for page in client(http).iter_live_events()] == []
    assert http.calls[0][1]["active"] == "true"
    assert http.calls[0][1]["closed"] == "false"


@pytest.mark.asyncio
async def test_forbidden_retry_policy_is_scoped_to_public_gamma_reads() -> None:
    http = FakeHttp([[], []])
    rest = client(http)

    _ = [page async for page in rest.iter_series()]
    _ = [page async for page in rest.iter_trades(market="0xcondition")]

    assert http.retryable_statuses == [frozenset({403}), frozenset()]
