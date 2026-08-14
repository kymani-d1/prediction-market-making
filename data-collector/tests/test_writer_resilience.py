from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from prediction_collector.writer import BatchWriter, WriteItem


class FailingDatabase:
    def __init__(self) -> None:
        self.persisted: list[WriteItem] = []
        self.quarantined: list[tuple[WriteItem, BaseException, int | None]] = []

    async def write_items(self, items: list[WriteItem]) -> dict[str, int]:
        if any(item.kind == "poison" for item in items):
            raise ValueError("constraint violation")
        self.persisted.extend(items)
        return dict(Counter(item.kind for item in items))

    async def record_write_failure(
        self, item: WriteItem, error: BaseException, *, run_id: int | None = None
    ) -> None:
        self.quarantined.append((item, error, run_id))


@pytest.mark.asyncio
async def test_poison_row_is_quarantined_without_deadlocking_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_sleep = asyncio.sleep

    async def immediate_sleep(_seconds: float) -> None:
        await original_sleep(0)

    monkeypatch.setattr("prediction_collector.writer.asyncio.sleep", immediate_sleep)
    database = FailingDatabase()
    writer = BatchWriter(
        database,  # type: ignore[arg-type]
        max_queue_size=10,
        batch_size=3,
        flush_interval_seconds=0.01,
    )
    writer.run_id = 42
    realistic_payload: dict[str, Any] = {
        "received_at": datetime(2026, 1, 1, tzinfo=UTC),
        "price": Decimal("0.123456789"),
    }

    await writer.start()
    await writer.put(WriteItem("trades", {"id": "good-1"}))
    await writer.put(WriteItem("poison", realistic_payload))
    await writer.put(WriteItem("trades", {"id": "good-2"}))
    await asyncio.wait_for(writer.queue.join(), timeout=1)
    await writer.stop()

    assert [item.data["id"] for item in database.persisted] == ["good-1", "good-2"]
    assert writer.rows_written == 2
    assert writer.failed_items == 1
    assert database.quarantined[0][0].data == realistic_payload
    assert database.quarantined[0][2] == 42


@pytest.mark.asyncio
async def test_fifo_flush_boundary_does_not_wait_for_future_live_records() -> None:
    database = FailingDatabase()
    writer = BatchWriter(
        database,  # type: ignore[arg-type]
        max_queue_size=10,
        batch_size=100,
        flush_interval_seconds=300,
    )
    await writer.start()
    await writer.put(WriteItem("trades", {"id": "before-boundary"}))
    await asyncio.wait_for(writer.flush(), timeout=1)
    assert [item.data["id"] for item in database.persisted] == ["before-boundary"]

    await writer.put(WriteItem("trades", {"id": "after-boundary"}))
    await asyncio.wait_for(writer.stop(), timeout=1)
    assert [item.data["id"] for item in database.persisted] == [
        "before-boundary",
        "after-boundary",
    ]
