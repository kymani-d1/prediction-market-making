from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from prediction_collector.common.http import AsyncHttpClient, HttpResult
from prediction_collector.common.pagination import extract_items


LOGGER = logging.getLogger(__name__)


class KalshiRestClient:
    def __init__(self, http: AsyncHttpClient, *, base_url: str) -> None:
        self.http = http
        self.base_url = base_url.rstrip("/")

    async def _cursor(
        self,
        path: str,
        key: str,
        *,
        alternate_keys: tuple[str, ...] = (),
        parameters: dict[str, Any] | None = None,
        page_size: int = 1000,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params = dict(parameters or {})
        params["limit"] = page_size
        cursor: str | None = None
        seen: set[str] = set()
        page_number = 0
        while True:
            if cursor:
                params["cursor"] = cursor
            result = await self.http.get_json(f"{self.base_url}{path}", params=params)
            payload = result.data
            if not isinstance(payload, dict):
                raise RuntimeError(f"Kalshi {key} cursor response was not an object")
            item_keys = (key, *alternate_keys)
            present_item_key = next(
                (
                    item_key
                    for item_key in item_keys
                    if isinstance(payload.get(item_key), list)
                ),
                None,
            )
            if present_item_key is None:
                raise RuntimeError(
                    f"Kalshi cursor response omitted a {item_keys!r} list"
                )
            if any(not isinstance(item, dict) for item in payload[present_item_key]):
                raise RuntimeError(f"Kalshi {key} cursor response contained malformed rows")
            if "cursor" not in payload and "next_cursor" not in payload:
                raise RuntimeError(f"Kalshi {key} cursor response omitted cursor")
            items = extract_items(payload, key, *alternate_keys)
            next_cursor = payload.get("cursor") or payload.get("next_cursor")
            page_number += 1
            LOGGER.info(
                "Fetched Kalshi page",
                extra={"entity": key, "page": page_number, "records": len(items)},
            )
            if items:
                yield items, result, str(next_cursor) if next_cursor else None
            if not next_cursor:
                return
            next_cursor = str(next_cursor)
            if next_cursor == cursor or next_cursor in seen:
                raise RuntimeError(f"Kalshi {key} cursor repeated")
            seen.add(next_cursor)
            cursor = next_cursor

    async def iter_series(self) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        async for page in self._cursor("/series", "series", page_size=1000):
            yield page

    async def iter_events(
        self, *, status: str | None = None
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params = {"status": status} if status else None
        async for page in self._cursor("/events", "events", parameters=params, page_size=200):
            yield page

    async def iter_markets(
        self,
        *,
        status: str | None = None,
        mve_filter: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        if mve_filter:
            params["mve_filter"] = mve_filter
        async for page in self._cursor("/markets", "markets", parameters=params):
            yield page

    async def iter_multivariate_events(
        self,
        *,
        series_ticker: str | None = None,
        collection_ticker: str | None = None,
        with_nested_markets: bool = True,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        if series_ticker and collection_ticker:
            raise ValueError(
                "Kalshi multivariate events cannot be filtered by both "
                "series_ticker and collection_ticker"
            )
        params: dict[str, Any] = {
            "with_nested_markets": str(with_nested_markets).lower(),
        }
        if series_ticker:
            params["series_ticker"] = series_ticker
        if collection_ticker:
            params["collection_ticker"] = collection_ticker
        async for page in self._cursor(
            "/events/multivariate",
            "events",
            parameters=params,
            page_size=200,
        ):
            yield page

    async def iter_multivariate_event_collections(
        self,
        *,
        status: str | None = None,
        associated_event_ticker: str | None = None,
        series_ticker: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        if associated_event_ticker:
            params["associated_event_ticker"] = associated_event_ticker
        if series_ticker:
            params["series_ticker"] = series_ticker
        async for page in self._cursor(
            "/multivariate_event_collections",
            "multivariate_contracts",
            alternate_keys=("multivariate_event_collections", "collections"),
            parameters=params,
            page_size=200,
        ):
            yield page

    async def iter_historical_markets(
        self,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        async for page in self._cursor("/historical/markets", "markets"):
            yield page

    async def iter_trades(
        self,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        async for page in self._cursor("/markets/trades", "trades"):
            yield page

    async def iter_historical_trades(
        self,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        async for page in self._cursor("/historical/trades", "trades"):
            yield page

    async def historical_cutoff(self) -> HttpResult:
        return await self.http.get_json(f"{self.base_url}/historical/cutoff")

    async def iter_series_fee_changes(
        self, series_ticker: str
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        # This endpoint is not cursor-paginated. show_historical is required to
        # retain prior schedules rather than only future changes.
        result = await self.http.get_json(
            f"{self.base_url}/series/fee_changes",
            params={"series_ticker": series_ticker, "show_historical": "true"},
        )
        items = extract_items(result.data, "series_fee_change_arr")
        yield items, result, None

    async def iter_event_fee_changes(
        self,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        async for page in self._cursor(
            "/events/fee_changes", "event_fee_changes", page_size=1000
        ):
            yield page

    async def iter_incentive_programs(
        self,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        async for page in self._cursor(
            "/incentive_programs",
            "incentive_programs",
            parameters={"status": "all", "type": "all"},
            page_size=10000,
        ):
            yield page

    async def orderbook(self, ticker: str, *, depth: int = 0) -> HttpResult:
        params = {"depth": depth} if depth else None
        return await self.http.get_json(
            f"{self.base_url}/markets/{ticker}/orderbook", params=params
        )

    async def candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        historical: bool = False,
    ) -> HttpResult:
        if historical:
            path = f"/historical/markets/{market_ticker}/candlesticks"
        else:
            path = f"/series/{series_ticker}/markets/{market_ticker}/candlesticks"
        return await self.http.get_json(
            f"{self.base_url}{path}",
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )

    async def market(self, ticker: str, *, historical: bool = False) -> HttpResult:
        prefix = "/historical" if historical else ""
        return await self.http.get_json(f"{self.base_url}{prefix}/markets/{ticker}")
