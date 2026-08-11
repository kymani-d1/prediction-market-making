from __future__ import annotations

import pytest

from prediction_collector.common.pagination import cursor_pages, extract_items, offset_pages
from prediction_collector.common.retry import RetryPolicy, retry_async


@pytest.mark.asyncio
async def test_offset_pages_advances_by_actual_page_length_and_stops_on_short_page() -> None:
    calls: list[tuple[int, int]] = []

    async def fetch(offset: int, limit: int) -> list[int]:
        calls.append((offset, limit))
        return {0: [1, 2], 2: [3, 4], 4: [5]}[offset]

    pages = [page async for page in offset_pages(fetch, page_size=2)]

    assert pages == [[1, 2], [3, 4], [5]]
    assert calls == [(0, 2), (2, 2), (4, 2)]


@pytest.mark.asyncio
async def test_offset_pages_empty_first_page_yields_nothing() -> None:
    async def fetch(_offset: int, _limit: int) -> list[int]:
        return []

    assert [page async for page in offset_pages(fetch, page_size=10)] == []


@pytest.mark.asyncio
async def test_cursor_pages_forwards_cursor_and_detects_repetition() -> None:
    calls: list[str | None] = []

    async def fetch(cursor: str | None) -> tuple[list[int], str | None]:
        calls.append(cursor)
        return {
            None: ([1], "A"),
            "A": ([2], "B"),
            "B": ([3], None),
        }[cursor]

    pages = [page async for page in cursor_pages(fetch)]
    assert pages == [([1], "A"), ([2], "B"), ([3], None)]
    assert calls == [None, "A", "B"]

    async def repeated(cursor: str | None) -> tuple[list[int], str | None]:
        return [1], "A" if cursor is None else "A"

    with pytest.raises(RuntimeError, match="cursor repeated"):
        _ = [page async for page in cursor_pages(repeated)]


@pytest.mark.parametrize("kwargs", [{"page_size": 0}, {"page_size": 1, "start_offset": -1}])
@pytest.mark.asyncio
async def test_invalid_offset_parameters_are_rejected(kwargs: dict[str, int]) -> None:
    async def fetch(_offset: int, _limit: int) -> list[int]:
        return []

    with pytest.raises(ValueError, match="invalid offset"):
        _ = [page async for page in offset_pages(fetch, **kwargs)]


def test_extract_items_accepts_list_or_named_list_and_filters_non_objects() -> None:
    assert extract_items([{"id": 1}, None, "bad", {"id": 2}]) == [
        {"id": 1},
        {"id": 2},
    ]
    assert extract_items({"data": "wrong", "markets": [{"id": 3}, 4]}, "data", "markets") == [
        {"id": 3}
    ]
    assert extract_items(None, "data") == []


def test_retry_policy_exponential_delay_and_jitter_are_deterministic() -> None:
    policy = RetryPolicy(
        max_attempts=5,
        base_delay_seconds=1,
        max_delay_seconds=4,
        jitter_ratio=0.25,
    )

    assert policy.delay(1, random_value=0.0) == pytest.approx(0.75)
    assert policy.delay(2, random_value=0.5) == pytest.approx(2.0)
    assert policy.delay(3, random_value=1.0) == pytest.approx(5.0)
    assert policy.delay(8, random_value=0.5) == pytest.approx(4.0)
    with pytest.raises(ValueError, match="one-based"):
        policy.delay(0)


@pytest.mark.asyncio
async def test_retry_async_retries_only_retryable_failures_and_reports_attempt() -> None:
    attempts = 0
    sleeps: list[float] = []
    callbacks: list[tuple[str, int, float]] = []

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TimeoutError("temporary")
        return "ok"

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    policy = RetryPolicy(max_attempts=4, base_delay_seconds=0.25, jitter_ratio=0)
    result = await retry_async(
        operation,
        policy=policy,
        retryable=lambda exc: isinstance(exc, TimeoutError),
        on_retry=lambda exc, attempt, delay: callbacks.append(
            (type(exc).__name__, attempt, delay)
        ),
        sleep=sleep,
    )

    assert result == "ok"
    assert attempts == 3
    assert sleeps == [0.25, 0.5]
    assert callbacks == [("TimeoutError", 1, 0.25), ("TimeoutError", 2, 0.5)]


@pytest.mark.asyncio
async def test_retry_async_immediately_raises_non_retryable_error() -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        raise ValueError("permanent")

    async def forbidden_sleep(_delay: float) -> None:
        raise AssertionError("non-retryable failures must not sleep")

    with pytest.raises(ValueError, match="permanent"):
        await retry_async(
            operation,
            policy=RetryPolicy(max_attempts=3, jitter_ratio=0),
            retryable=lambda exc: isinstance(exc, TimeoutError),
            sleep=forbidden_sleep,
        )
    assert calls == 1
