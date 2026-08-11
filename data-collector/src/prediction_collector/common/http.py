from __future__ import annotations

import asyncio
import email.utils
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

import httpx

from prediction_collector.common.retry import RetryPolicy
from prediction_collector.common.utils import parse_timestamp, utc_now


LOGGER = logging.getLogger(__name__)


class RetryableHttpError(RuntimeError):
    def __init__(self, status_code: int, retry_after: float | None = None) -> None:
        super().__init__(f"Transient HTTP status {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class HttpResult:
    data: Any
    status_code: int
    requested_at: datetime
    response_timestamp: datetime | None
    url: str
    received_at: datetime | None = None


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        return max(0.0, (parsed - utc_now()).total_seconds())


def _http_date_timestamp(value: str | None) -> datetime | None:
    """Parse the RFC 7231 Date header without weakening generic source parsing."""
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        # Some non-conforming upstreams send ISO-8601 here; retain the existing
        # tolerant fallback for those responses.
        return parse_timestamp(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class AsyncHttpClient:
    def __init__(
        self,
        *,
        concurrency: int = 8,
        timeout_seconds: float = 30,
        max_attempts: int = 6,
        user_agent: str = "prediction-collector/0.1",
    ) -> None:
        self._semaphore = asyncio.Semaphore(concurrency)
        self._policy = RetryPolicy(max_attempts=max_attempts)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(
                max_connections=max(concurrency * 2, 10),
                max_keepalive_connections=max(concurrency, 5),
            ),
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    async def __aenter__(self) -> "AsyncHttpClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def request_json(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResult:
        last_error: Exception | None = None
        for attempt in range(1, self._policy.max_attempts + 1):
            requested_at = utc_now()
            try:
                async with self._semaphore:
                    response = await self._client.request(
                        method, url, params=params, headers=headers
                    )
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    raise RetryableHttpError(
                        response.status_code,
                        _retry_after_seconds(response.headers.get("Retry-After")),
                    )
                response.raise_for_status()
                server_time = _http_date_timestamp(response.headers.get("Date"))
                return HttpResult(
                    data=response.json(),
                    status_code=response.status_code,
                    requested_at=requested_at,
                    response_timestamp=server_time,
                    url=str(response.url),
                    received_at=utc_now(),
                )
            except (httpx.TransportError, RetryableHttpError) as exc:
                last_error = exc
                if attempt >= self._policy.max_attempts:
                    raise
                policy_delay = self._policy.delay(attempt)
                retry_after = exc.retry_after if isinstance(exc, RetryableHttpError) else None
                delay = max(policy_delay, retry_after or 0.0)
                LOGGER.warning(
                    "HTTP request retry",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "delay_seconds": round(delay, 3),
                        "error": type(exc).__name__,
                    },
                )
                await asyncio.sleep(delay)
        assert last_error is not None
        raise last_error

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResult:
        return await self.request_json("GET", url, params=params, headers=headers)
