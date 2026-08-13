from __future__ import annotations

import argparse
from typing import Any

import pytest

from prediction_collector.config import Settings
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector import main as main_module


class FakeDatabase:
    instance: "FakeDatabase | None" = None

    def __init__(self, settings: Settings, metrics: ThroughputMetrics) -> None:
        self.migrate_calls = 0
        self.open_calls = 0
        self.close_calls = 0
        FakeDatabase.instance = self

    async def migrate(self) -> list[str]:
        self.migrate_calls += 1
        raise AssertionError("status must never apply migrations")

    async def verify_migrations(self) -> dict[str, object]:
        return {
            "current": True,
            "applied": ["001_initial.sql"],
            "pending": [],
            "checksum_mismatches": [],
        }

    async def open(self) -> None:
        self.open_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def status(self, **_: Any) -> dict[str, Any]:
        return {
            "database_connected": True,
            "healthy": False,
            "live": {
                "polymarket": {
                    "discovery_state": "retrying",
                    "connections_active": 0,
                    "markets_selected": 0,
                    "markets_confirmed_subscribed": 0,
                    "latest_ws_message": None,
                    "healthy": False,
                }
            },
        }


@pytest.mark.asyncio
async def test_status_is_read_only_and_degraded_live_state_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(main_module, "Database", FakeDatabase)

    exit_code = await main_module._dispatch(
        argparse.Namespace(command="status"),
        Settings(),
    )

    database = FakeDatabase.instance
    assert database is not None
    assert database.migrate_calls == 0
    assert database.open_calls == 1
    assert database.close_calls == 1
    assert exit_code == 1
    output = capsys.readouterr().out
    assert '"discovery_state": "retrying"' in output
    assert '"connections_active": 0' in output


@pytest.mark.asyncio
async def test_status_reports_pending_migration_without_opening_or_applying(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class PendingDatabase(FakeDatabase):
        async def verify_migrations(self) -> dict[str, object]:
            return {
                "current": False,
                "applied": [],
                "pending": ["001_initial.sql"],
                "checksum_mismatches": [],
            }

    monkeypatch.setattr(main_module, "Database", PendingDatabase)

    exit_code = await main_module._dispatch(
        argparse.Namespace(command="status"), Settings()
    )

    database = FakeDatabase.instance
    assert database is not None
    assert database.migrate_calls == 0
    assert database.open_calls == 0
    assert exit_code == 1
    assert '"pending": [' in capsys.readouterr().out
