from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from prediction_collector.common.http import AsyncHttpClient, HttpResult
from prediction_collector.common.pagination import extract_items


LOGGER = logging.getLogger(__name__)


def _validated_page_items(
    payload: Any,
    *,
    entity: str,
    keys: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Return a complete page or fail closed on response-shape drift.

    Offset completion is inferred from the number of rows returned.  Silently
    discarding one malformed row would therefore make a full page look short,
    prematurely terminate the crawl, and allow an incomplete checkpoint.
    """
    raw_items: Any
    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, dict):
        raw_items = next(
            (payload[key] for key in keys if isinstance(payload.get(key), list)),
            None,
        )
    else:
        raw_items = None
    if not isinstance(raw_items, list):
        raise RuntimeError(f"Polymarket {entity} response omitted its row list")
    if any(not isinstance(item, dict) for item in raw_items):
        raise RuntimeError(f"Polymarket {entity} response contained malformed rows")
    return raw_items


class PolymarketRestClient:
    def __init__(
        self,
        http: AsyncHttpClient,
        *,
        gamma_url: str,
        data_url: str,
        clob_url: str,
    ) -> None:
        self.http = http
        self.gamma_url = gamma_url.rstrip("/")
        self.data_url = data_url.rstrip("/")
        self.clob_url = clob_url.rstrip("/")

    async def _keyset(
        self,
        entity: str,
        *,
        parameters: dict[str, Any] | None = None,
        page_size: int = 100,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params = dict(parameters or {})
        params["limit"] = page_size
        cursor: str | None = None
        seen: set[str] = set()
        page_number = 0
        while True:
            if cursor:
                params["after_cursor"] = cursor
            result = await self.http.get_json(f"{self.gamma_url}/{entity}/keyset", params=params)
            payload = result.data
            if not isinstance(payload, dict) or not isinstance(payload.get(entity), list):
                raise RuntimeError(
                    f"Polymarket {entity} keyset response omitted the {entity!r} list"
                )
            raw_items = payload[entity]
            if any(not isinstance(item, dict) for item in raw_items):
                raise RuntimeError(f"Polymarket {entity} keyset contained malformed rows")
            items = extract_items(payload, entity)
            has_cursor = "next_cursor" in payload
            next_cursor = payload.get("next_cursor")
            if not has_cursor and len(items) >= page_size:
                raise RuntimeError(
                    f"Polymarket {entity} full keyset page omitted next_cursor"
                )
            page_number += 1
            LOGGER.info(
                "Fetched Polymarket page",
                extra={"entity": entity, "page": page_number, "records": len(items)},
            )
            if items:
                yield items, result, str(next_cursor) if next_cursor else None
            if not next_cursor:
                return
            next_cursor = str(next_cursor)
            if next_cursor == cursor or next_cursor in seen:
                raise RuntimeError(f"Polymarket {entity} cursor repeated")
            seen.add(next_cursor)
            cursor = next_cursor

    async def iter_events(
        self, *, active: bool | None = None, closed: bool | None = None
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params: dict[str, Any] = {}
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        async for page in self._keyset("events", parameters=params):
            yield page

    async def iter_markets(
        self, *, active: bool | None = None, closed: bool | None = None
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        params: dict[str, Any] = {}
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        async for page in self._keyset("markets", parameters=params):
            yield page

    async def _offset_entities(
        self,
        entity: str,
        *,
        page_size: int = 100,
        params: dict[str, Any] | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult]]:
        offset = 0
        while True:
            query = dict(params or {})
            query.update({"limit": page_size, "offset": offset})
            result = await self.http.get_json(f"{self.gamma_url}/{entity}", params=query)
            items = _validated_page_items(
                result.data,
                entity=entity,
                keys=(entity, "data"),
            )
            if not items:
                return
            yield items, result
            if len(items) < page_size:
                return
            offset += len(items)

    async def iter_series(self) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult]]:
        async for page in self._offset_entities("series"):
            yield page

    async def iter_tags(self) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult]]:
        async for page in self._offset_entities("tags"):
            yield page

    async def iter_comments(self) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult]]:
        # Gamma offset pagination is only safe while new rows arrive if the
        # ordering is immutable and deterministic.  The secondary id key
        # breaks createdAt ties; repeated-page detection prevents an API
        # regression from turning this into an infinite loop.
        seen_pages: set[tuple[str, ...]] = set()
        async for items, result in self._offset_entities(
            "comments",
            page_size=100,
            params={"order": "createdAt,id", "ascending": "true"},
        ):
            fingerprint = tuple(str(item.get("id")) for item in items)
            if fingerprint in seen_pages:
                raise RuntimeError("Polymarket comments page repeated")
            seen_pages.add(fingerprint)
            yield items, result

    async def iter_trades(
        self,
        *,
        market: str | None = None,
        start: int | None = None,
        end: int | None = None,
        page_size: int = 10_000,
        max_offset: int = 10_000,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult]]:
        offset = 0
        while offset <= max_offset:
            params: dict[str, Any] = {"limit": page_size, "offset": offset}
            if market:
                params["market"] = market
            if start is not None:
                params["start"] = start
            if end is not None:
                params["end"] = end
            result = await self.http.get_json(f"{self.data_url}/trades", params=params)
            items = _validated_page_items(
                result.data,
                entity="trades",
                keys=("trades", "data"),
            )
            if not items:
                return
            yield items, result
            if len(items) < page_size:
                return
            offset += len(items)

    async def orderbook(self, token_id: str) -> HttpResult:
        return await self.http.get_json(f"{self.clob_url}/book", params={"token_id": token_id})

    async def market(self, gamma_market_id: str) -> HttpResult:
        return await self.http.get_json(f"{self.gamma_url}/markets/{gamma_market_id}")

    async def price_history(
        self,
        token_id: str,
        *,
        interval: str = "max",
        fidelity_minutes: int = 1,
    ) -> HttpResult:
        return await self.http.get_json(
            f"{self.clob_url}/prices-history",
            params={"market": token_id, "interval": interval, "fidelity": fidelity_minutes},
        )

    async def open_interest(self, condition_ids: list[str]) -> HttpResult:
        return await self.http.get_json(
            f"{self.data_url}/oi", params={"market": ",".join(condition_ids)}
        )

    async def holders(self, condition_id: str, *, limit: int = 20) -> HttpResult:
        return await self.http.get_json(
            f"{self.data_url}/holders", params={"market": condition_id, "limit": limit}
        )

    async def fee_rate(self, token_id: str) -> HttpResult:
        return await self.http.get_json(
            f"{self.clob_url}/fee-rate", params={"token_id": token_id}
        )

    async def iter_current_rewards(
        self,
        *,
        sponsored: bool = False,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], HttpResult, str | None]]:
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            params: dict[str, Any] = {"sponsored": str(sponsored).lower()}
            if cursor:
                params["next_cursor"] = cursor
            result = await self.http.get_json(
                f"{self.clob_url}/rewards/markets/current", params=params
            )
            payload = result.data
            items = _validated_page_items(
                payload,
                entity="rewards",
                keys=("data", "markets", "rewards"),
            )
            next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
            if items:
                yield items, result, str(next_cursor) if next_cursor else None
            if not next_cursor or next_cursor == "LTE=":
                return
            next_cursor = str(next_cursor)
            if next_cursor in seen:
                raise RuntimeError("Polymarket rewards cursor repeated")
            seen.add(next_cursor)
            cursor = next_cursor
