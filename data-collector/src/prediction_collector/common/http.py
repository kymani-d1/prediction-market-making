from __future__ import annotations

import asyncio
import email.utils
import logging
from collections.abc import Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

import httpx

from prediction_collector.common.retry import RetryPolicy
from prediction_collector.common.utils import parse_timestamp, utc_now


LOGGER = logging.getLogger(__name__)


class RetryableHttpError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        retry_after: float | None = None,
        *,
        response: httpx.Response | None = None,
        explicitly_allowed: bool = False,
    ) -> None:
        super().__init__(f"Transient HTTP status {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after
        self.response = response
        self.explicitly_allowed = explicitly_allowed


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
        parsed = (
            parsed.replace(tzinfo=UTC)
            if parsed.tzinfo is None
            else parsed.astimezone(UTC)
        )
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


def _request_target(method: str, url: str | httpx.URL) -> dict[str, Any]:
    parsed = httpx.URL(url)
    return {
        "method": method.upper(),
        "host": parsed.host,
        "path": parsed.path,
    }


def _response_diagnostics(
    response: httpx.Response, *, include_body: bool
) -> dict[str, Any]:
    fields: dict[str, Any] = {
        **_request_target(response.request.method, response.request.url),
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type"),
        "server": response.headers.get("Server"),
        "cf_ray": response.headers.get("CF-Ray"),
        "request_id": (
            response.headers.get("X-Request-ID")
            or response.headers.get("X-Amzn-Trace-Id")
        ),
        "retry_after": response.headers.get("Retry-After"),
    }
    if include_body:
        content_type = (response.headers.get("Content-Type") or "").lower()
        if any(kind in content_type for kind in ("text/", "json", "html", "xml")):
            # Public Gamma denial pages are useful for distinguishing an
            # exchange response from a CDN/WAF response. Keep this deliberately
            # small and never log request headers, query strings, or credentials.
            preview = " ".join(response.text.split())[:256]
            if preview:
                fields["response_body_preview"] = preview
    return fields


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
        retryable_status_codes: Collection[int] = (),
    ) -> HttpResult:
        method = method.upper()
        explicitly_retryable = frozenset(int(code) for code in retryable_status_codes)
        if explicitly_retryable and method not in {"GET", "HEAD"}:
            raise ValueError("additional retryable statuses require an idempotent method")
        last_error: Exception | None = None
        for attempt in range(1, self._policy.max_attempts + 1):
            requested_at = utc_now()
            try:
                async with self._semaphore:
                    response = await self._client.request(
                        method, url, params=params, headers=headers
                    )
                explicitly_allowed = response.status_code in explicitly_retryable
                if (
                    response.status_code == 429
                    or 500 <= response.status_code < 600
                    or explicitly_allowed
                ):
                    raise RetryableHttpError(
                        response.status_code,
                        _retry_after_seconds(response.headers.get("Retry-After")),
                        response=response,
                        explicitly_allowed=explicitly_allowed,
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
                    if (
                        isinstance(exc, RetryableHttpError)
                        and exc.explicitly_allowed
                        and exc.response is not None
                    ):
                        diagnostics = _response_diagnostics(
                            exc.response, include_body=True
                        )
                        LOGGER.error(
                            "HTTP denial persisted after bounded retry policy",
                            extra={
                                **diagnostics,
                                "attempt": attempt,
                                "max_attempts": self._policy.max_attempts,
                            },
                        )
                        target = (
                            f"{diagnostics.get('host') or '<unknown>'}"
                            f"{diagnostics.get('path') or ''}"
                        )
                        raise httpx.HTTPStatusError(
                            f"Persistent HTTP {exc.status_code} denial after "
                            f"{attempt} attempts for {method} {target}; bounded "
                            "retry policy exhausted",
                            request=exc.response.request,
                            response=exc.response,
                        ) from exc
                    raise
                policy_delay = self._policy.delay(attempt)
                retry_after = exc.retry_after if isinstance(exc, RetryableHttpError) else None
                delay = max(policy_delay, retry_after or 0.0)
                diagnostics = (
                    _response_diagnostics(
                        exc.response, include_body=exc.explicitly_allowed
                    )
                    if isinstance(exc, RetryableHttpError)
                    and exc.response is not None
                    else _request_target(method, url)
                )
                LOGGER.warning(
                    "HTTP request retry",
                    extra={
                        **diagnostics,
                        "attempt": attempt,
                        "max_attempts": self._policy.max_attempts,
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
        retryable_status_codes: Collection[int] = (),
    ) -> HttpResult:
        return await self.request_json(
            "GET",
            url,
            params=params,
            headers=headers,
            retryable_status_codes=retryable_status_codes,
        )
