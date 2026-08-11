from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar


T = TypeVar("T")


async def offset_pages(
    fetch_page: Callable[[int, int], Awaitable[list[T]]],
    *,
    page_size: int,
    start_offset: int = 0,
) -> AsyncIterator[list[T]]:
    if page_size <= 0 or start_offset < 0:
        raise ValueError("invalid offset pagination parameters")
    offset = start_offset
    while True:
        page = await fetch_page(offset, page_size)
        if not page:
            return
        yield page
        if len(page) < page_size:
            return
        offset += len(page)


async def cursor_pages(
    fetch_page: Callable[[str | None], Awaitable[tuple[list[T], str | None]]],
    *,
    start_cursor: str | None = None,
) -> AsyncIterator[tuple[list[T], str | None]]:
    cursor = start_cursor
    seen: set[str] = set()
    while True:
        page, next_cursor = await fetch_page(cursor)
        if page:
            yield page, next_cursor
        if not next_cursor:
            return
        if next_cursor == cursor or next_cursor in seen:
            raise RuntimeError(f"pagination cursor repeated: {next_cursor!r}")
        seen.add(next_cursor)
        cursor = next_cursor


def extract_items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []

