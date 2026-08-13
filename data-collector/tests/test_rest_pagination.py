from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from prediction_collector.kalshi.rest import KalshiRestClient
from prediction_collector.polymarket.rest import PolymarketRestClient


@dataclass
class FakeResult:
    data: Any


class FakeHttp:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_json(self, url: str, *, params: dict[str, Any] | None = None) -> FakeResult:
        self.calls.append((url, dict(params or {})))
        return FakeResult(self.responses.pop(0))


@pytest.mark.asyncio
async def test_polymarket_keyset_uses_after_cursor_and_no_offset() -> None:
    http = FakeHttp(
        [
            {"markets": [{"id": "1"}], "next_cursor": "cursor-A"},
            {"markets": [{"id": "2"}]},
        ]
    )
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test/",
        data_url="https://data.test/",
        clob_url="https://clob.test/",
    )

    pages = [page async for page in client.iter_markets(active=True, closed=False)]

    assert [[item["id"] for item in page[0]] for page in pages] == [["1"], ["2"]]
    assert http.calls == [
        (
            "https://gamma.test/markets/keyset",
            {"active": "true", "closed": "false", "limit": 100},
        ),
        (
            "https://gamma.test/markets/keyset",
            {
                "active": "true",
                "closed": "false",
                "limit": 100,
                "after_cursor": "cursor-A",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_polymarket_live_events_use_documented_keyset_pages() -> None:
    http = FakeHttp(
        [
            {"events": [{"id": "1", "markets": []}], "next_cursor": "next"},
            {"events": [{"id": "2", "markets": []}]},
        ]
    )
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    pages = [page async for page in client.iter_live_events()]

    assert [[event["id"] for event in page[0]] for page in pages] == [["1"], ["2"]]
    assert http.calls == [
        ("https://gamma.test/events/keyset", {"closed": "false", "limit": 100}),
        (
            "https://gamma.test/events/keyset",
            {"closed": "false", "limit": 100, "after_cursor": "next"},
        ),
    ]


@pytest.mark.asyncio
async def test_polymarket_keyset_repeated_cursor_fails_instead_of_looping() -> None:
    http = FakeHttp(
        [
            {"markets": [{"id": "1"}], "next_cursor": "same"},
            {"markets": [{"id": "2"}], "next_cursor": "same"},
        ]
    )
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    with pytest.raises(RuntimeError, match="cursor repeated"):
        _ = [page async for page in client.iter_markets()]


@pytest.mark.asyncio
async def test_polymarket_full_keyset_page_without_cursor_is_incomplete() -> None:
    http = FakeHttp([{"markets": [{"id": str(index)} for index in range(100)]}])
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    with pytest.raises(RuntimeError, match="omitted next_cursor"):
        _ = [page async for page in client.iter_markets()]


@pytest.mark.asyncio
async def test_polymarket_trade_offsets_stop_at_short_page() -> None:
    http = FakeHttp(
        [
            [{"id": "1"}, {"id": "2"}],
            [{"id": "3"}],
        ]
    )
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    pages = [page async for page in client.iter_trades(market="0xcondition", page_size=2)]

    assert [[row["id"] for row in page[0]] for page in pages] == [["1", "2"], ["3"]]
    assert [params["offset"] for _, params in http.calls] == [0, 2]
    assert all(params["market"] == "0xcondition" for _, params in http.calls)


@pytest.mark.asyncio
async def test_polymarket_trade_page_with_malformed_row_fails_closed() -> None:
    rows: list[Any] = [{"id": str(index)} for index in range(9_999)]
    rows.append("schema-drift")
    http = FakeHttp([rows])
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    with pytest.raises(RuntimeError, match="malformed rows"):
        _ = [page async for page in client.iter_trades(market="0xcondition")]

    assert len(http.calls) == 1


@pytest.mark.asyncio
async def test_polymarket_offset_entity_with_malformed_row_fails_closed() -> None:
    http = FakeHttp([[{"id": "1"}, None]])
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    with pytest.raises(RuntimeError, match="malformed rows"):
        _ = [page async for page in client.iter_comments()]


@pytest.mark.asyncio
async def test_polymarket_trade_window_bounds_are_forwarded() -> None:
    http = FakeHttp([[{"id": "1"}]])
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    _ = [
        page
        async for page in client.iter_trades(
            market="0xcondition", start=100, end=199, page_size=2
        )
    ]

    assert http.calls == [
        (
            "https://data.test/trades",
            {
                "limit": 2,
                "offset": 0,
                "market": "0xcondition",
                "start": 100,
                "end": 199,
            },
        )
    ]


@pytest.mark.asyncio
async def test_polymarket_rewards_forwards_documented_next_cursor() -> None:
    http = FakeHttp(
        [
            {"data": [{"condition_id": "one"}], "next_cursor": "next"},
            {"data": [{"condition_id": "two"}], "next_cursor": "LTE="},
        ]
    )
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    pages = [page async for page in client.iter_current_rewards(sponsored=True)]

    assert [page[0][0]["condition_id"] for page in pages] == ["one", "two"]
    assert http.calls == [
        (
            "https://clob.test/rewards/markets/current",
            {"sponsored": "true"},
        ),
        (
            "https://clob.test/rewards/markets/current",
            {"sponsored": "true", "next_cursor": "next"},
        ),
    ]


@pytest.mark.asyncio
async def test_polymarket_comments_use_stable_oldest_first_order() -> None:
    http = FakeHttp([[{"id": "1", "createdAt": "2026-01-01T00:00:00Z"}]])
    client = PolymarketRestClient(
        http,  # type: ignore[arg-type]
        gamma_url="https://gamma.test",
        data_url="https://data.test",
        clob_url="https://clob.test",
    )

    _ = [page async for page in client.iter_comments()]

    assert http.calls == [
        (
            "https://gamma.test/comments",
            {
                "order": "createdAt,id",
                "ascending": "true",
                "limit": 100,
                "offset": 0,
            },
        )
    ]


@pytest.mark.asyncio
async def test_kalshi_cursor_is_forwarded_with_status_filter() -> None:
    http = FakeHttp(
        [
            {"markets": [{"ticker": "A"}], "cursor": "next"},
            {"markets": [{"ticker": "B"}], "cursor": ""},
        ]
    )
    client = KalshiRestClient(http, base_url="https://kalshi.test/")  # type: ignore[arg-type]

    pages = [page async for page in client.iter_markets(status="open")]

    assert [[row["ticker"] for row in page[0]] for page in pages] == [["A"], ["B"]]
    assert http.calls == [
        ("https://kalshi.test/markets", {"status": "open", "limit": 1000}),
        (
            "https://kalshi.test/markets",
            {"status": "open", "limit": 1000, "cursor": "next"},
        ),
    ]


@pytest.mark.asyncio
async def test_kalshi_cursor_wrapper_without_cursor_is_incomplete() -> None:
    http = FakeHttp([{"markets": [{"ticker": "A"}]}])
    client = KalshiRestClient(http, base_url="https://kalshi.test/")  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="omitted cursor"):
        _ = [page async for page in client.iter_markets(status="open")]


@pytest.mark.asyncio
async def test_kalshi_multivariate_events_use_documented_cursor_and_page_size() -> None:
    http = FakeHttp(
        [
            {"events": [{"event_ticker": "MVE-A"}], "cursor": "next-mve"},
            {"events": [{"event_ticker": "MVE-B"}], "cursor": ""},
        ]
    )
    client = KalshiRestClient(http, base_url="https://kalshi.test/")  # type: ignore[arg-type]

    pages = [page async for page in client.iter_multivariate_events()]

    assert [[row["event_ticker"] for row in page[0]] for page in pages] == [
        ["MVE-A"],
        ["MVE-B"],
    ]
    assert http.calls == [
        (
            "https://kalshi.test/events/multivariate",
            {"with_nested_markets": "true", "limit": 200},
        ),
        (
            "https://kalshi.test/events/multivariate",
            {
                "with_nested_markets": "true",
                "limit": 200,
                "cursor": "next-mve",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_kalshi_multivariate_collections_accept_current_response_key() -> None:
    http = FakeHttp(
        [
            {
                "multivariate_contracts": [{"collection_ticker": "COLL-A"}],
                "cursor": "next-collection",
            },
            {
                "multivariate_event_collections": [
                    {"collection_ticker": "COLL-B"}
                ],
                "cursor": "",
            },
        ]
    )
    client = KalshiRestClient(http, base_url="https://kalshi.test/")  # type: ignore[arg-type]

    pages = [page async for page in client.iter_multivariate_event_collections()]

    assert [[row["collection_ticker"] for row in page[0]] for page in pages] == [
        ["COLL-A"],
        ["COLL-B"],
    ]
    assert http.calls == [
        ("https://kalshi.test/multivariate_event_collections", {"limit": 200}),
        (
            "https://kalshi.test/multivariate_event_collections",
            {"limit": 200, "cursor": "next-collection"},
        ),
    ]


@pytest.mark.asyncio
async def test_kalshi_multivariate_events_reject_incompatible_filters() -> None:
    client = KalshiRestClient(
        FakeHttp([]),  # type: ignore[arg-type]
        base_url="https://kalshi.test/",
    )

    with pytest.raises(ValueError, match="both series_ticker and collection_ticker"):
        _ = [
            page
            async for page in client.iter_multivariate_events(
                series_ticker="SERIES", collection_ticker="COLLECTION"
            )
        ]
