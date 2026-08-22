from __future__ import annotations

from typing import Any

import pytest

from prediction_collector.config import Settings
from prediction_collector.database import Database


class _Cursor:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row
        self.rowcount = 1

    async def fetchone(self) -> dict[str, Any]:
        return self._row


class _TagConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, params: tuple[Any, ...]) -> _Cursor:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        return _Cursor({"id": len(self.calls)})


class _ConnectionContext:
    def __init__(self, connection: _TagConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _TagConnection:
        return self._connection

    async def __aexit__(self, *_: Any) -> None:
        return None


class _Pool:
    def __init__(self, connection: _TagConnection) -> None:
        self._connection = connection

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


@pytest.mark.asyncio
async def test_tag_with_stable_external_id_conflicts_on_that_identity() -> None:
    connection = _TagConnection()
    database = Database(Settings())
    database.pool = _Pool(connection)  # type: ignore[assignment]

    await database.upsert_tag(
        "polymarket", {"id": 193, "label": "Military", "slug": "military"}
    )
    await database.upsert_tag(
        "polymarket", {"label": "Unnumbered", "slug": "unnumbered"}
    )

    identified_sql, identified_params = connection.calls[0]
    assert (
        "ON CONFLICT (exchange, external_id) WHERE external_id IS NOT NULL"
        in identified_sql
    )
    assert "name = EXCLUDED.name" in identified_sql
    assert identified_params[:4] == (
        "polymarket",
        "193",
        "Military",
        "military",
    )
    anonymous_sql, anonymous_params = connection.calls[1]
    assert "ON CONFLICT (exchange, name)" in anonymous_sql
    assert anonymous_params[1] is None
