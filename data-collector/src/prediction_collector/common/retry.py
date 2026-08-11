from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 6
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0
    jitter_ratio: float = 0.25

    def delay(self, attempt: int, *, random_value: float | None = None) -> float:
        if attempt < 1:
            raise ValueError("attempt is one-based")
        base = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (attempt - 1)))
        sample = random.random() if random_value is None else random_value
        jitter = base * self.jitter_ratio * ((sample * 2) - 1)
        return max(0.0, base + jitter)


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    retryable: Callable[[Exception], bool],
    on_retry: Callable[[Exception, int, float], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            last_error = exc
            if attempt >= policy.max_attempts or not retryable(exc):
                raise
            delay = policy.delay(attempt)
            if on_retry:
                on_retry(exc, attempt, delay)
            await sleep(delay)
    assert last_error is not None
    raise last_error

