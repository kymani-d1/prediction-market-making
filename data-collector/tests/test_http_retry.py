from __future__ import annotations

import logging
from collections.abc import Callable

import httpx
import pytest

from prediction_collector.common.http import AsyncHttpClient
from prediction_collector.common.retry import RetryPolicy


async def _client(
    handler: Callable[[httpx.Request], httpx.Response], *, max_attempts: int = 4
) -> AsyncHttpClient:
    client = AsyncHttpClient(max_attempts=max_attempts)
    await client._client.aclose()
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client._policy = RetryPolicy(
        max_attempts=max_attempts,
        base_delay_seconds=0.5,
        max_delay_seconds=30,
        jitter_ratio=0,
    )
    return client


def _responses(
    *statuses: int, retry_after: str | None = None
) -> tuple[Callable[[httpx.Request], httpx.Response], list[httpx.Request]]:
    calls: list[httpx.Request] = []
    pending = list(statuses)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        status = pending.pop(0)
        if status == 200:
            return httpx.Response(status, json={"ok": True}, request=request)
        headers = {
            "Content-Type": "text/html; charset=utf-8",
            "Server": "cloudflare",
            "CF-Ray": "safe-ray-id",
        }
        if retry_after is not None:
            headers["Retry-After"] = retry_after
        return httpx.Response(
            status,
            text="<html>temporary access denied by edge</html>",
            headers=headers,
            request=request,
        )

    return handler, calls


@pytest.mark.asyncio
@pytest.mark.parametrize("statuses", [(403, 200), (403, 403, 200)])
async def test_explicit_public_get_policy_recovers_from_transient_forbidden(
    monkeypatch: pytest.MonkeyPatch,
    statuses: tuple[int, ...],
) -> None:
    handler, calls = _responses(*statuses)
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("prediction_collector.common.http.asyncio.sleep", sleep)
    client = await _client(handler)
    try:
        result = await client.get_json(
            "https://gamma-api.polymarket.com/markets/keyset?secret=not-logged",
            retryable_status_codes={403},
        )
    finally:
        await client.close()

    assert result.data == {"ok": True}
    assert len(calls) == len(statuses)
    assert sleeps == [0.5, 1.0][: len(statuses) - 1]


@pytest.mark.asyncio
async def test_persistent_forbidden_fails_after_bounded_attempts_with_safe_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    handler, calls = _responses(403, 403, 403)
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("prediction_collector.common.http.asyncio.sleep", sleep)
    client = await _client(handler, max_attempts=3)
    try:
        with caplog.at_level(logging.WARNING):
            with pytest.raises(httpx.HTTPStatusError, match="Persistent HTTP 403"):
                await client.get_json(
                    "https://gamma-api.polymarket.com/markets/keyset?after_cursor=sensitive",
                    retryable_status_codes={403},
                )
    finally:
        await client.close()

    assert len(calls) == 3
    assert sleeps == [0.5, 1.0]
    retry_record = next(
        record for record in caplog.records if record.message == "HTTP request retry"
    )
    terminal_record = next(
        record
        for record in caplog.records
        if record.message == "HTTP denial persisted after bounded retry policy"
    )
    assert retry_record.status_code == 403
    assert retry_record.host == "gamma-api.polymarket.com"
    assert retry_record.path == "/markets/keyset"
    assert retry_record.attempt == 1
    assert retry_record.max_attempts == 3
    assert retry_record.content_type == "text/html; charset=utf-8"
    assert retry_record.server == "cloudflare"
    assert retry_record.cf_ray == "safe-ray-id"
    assert retry_record.response_body_preview.startswith("<html>")
    assert not hasattr(retry_record, "url")
    assert terminal_record.attempt == 3
    assert terminal_record.max_attempts == 3


@pytest.mark.asyncio
async def test_retry_after_controls_forbidden_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler, _ = _responses(403, 200, retry_after="7")
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("prediction_collector.common.http.asyncio.sleep", sleep)
    client = await _client(handler)
    try:
        await client.get_json(
            "https://gamma-api.polymarket.com/markets/keyset",
            retryable_status_codes={403},
        )
    finally:
        await client.close()

    assert sleeps == [7.0]


@pytest.mark.asyncio
async def test_unapproved_forbidden_remains_non_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler, calls = _responses(403, 200)

    async def forbidden_sleep(_delay: float) -> None:
        raise AssertionError("ordinary 403 must not enter retry backoff")

    monkeypatch.setattr("prediction_collector.common.http.asyncio.sleep", forbidden_sleep)
    client = await _client(handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_json("https://private.example.test/account")
    finally:
        await client.close()

    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["429", "503", "transport"])
async def test_existing_retry_classes_remain_retryable(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            if failure == "transport":
                raise httpx.ConnectError("temporary connect failure", request=request)
            return httpx.Response(int(failure), request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("prediction_collector.common.http.asyncio.sleep", sleep)
    client = await _client(handler)
    try:
        result = await client.get_json("https://ordinary.example.test/public")
    finally:
        await client.close()

    assert result.data == {"ok": True}
    assert len(calls) == 2
    assert sleeps == [0.5]


@pytest.mark.asyncio
async def test_extra_status_policy_rejects_non_idempotent_method() -> None:
    handler, calls = _responses(200)
    client = await _client(handler)
    try:
        with pytest.raises(ValueError, match="idempotent"):
            await client.request_json(
                "POST",
                "https://gamma-api.polymarket.com/markets/keyset",
                retryable_status_codes={403},
            )
    finally:
        await client.close()
    assert calls == []
