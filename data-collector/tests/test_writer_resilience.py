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


class GatedDatabase(FailingDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.write_started = asyncio.Event()
        self.release_write = asyncio.Event()

    async def write_items(self, items: list[WriteItem]) -> dict[str, int]:
        self.write_started.set()
        await self.release_write.wait()
        return await super().write_items(items)


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


@pytest.mark.asyncio
async def test_batch_writer_reports_queue_put_pressure_and_task_state() -> None:
    database = GatedDatabase()
    writer = BatchWriter(
        database,  # type: ignore[arg-type]
        max_queue_size=1,
        batch_size=1,
        flush_interval_seconds=0.01,
    )
    await writer.start()
    await writer.put(WriteItem("trades", {"id": "inflight"}))
    await asyncio.wait_for(database.write_started.wait(), timeout=1)
    await writer.put(WriteItem("trades", {"id": "queued"}))
    waiting = asyncio.create_task(
        writer.put(WriteItem("trades", {"id": "waiting"}))
    )
    await asyncio.sleep(0.05)

    pressured = writer.metrics()
    assert pressured["queue_depth"] == 1
    assert pressured["queue_max_rows"] == 1
    assert pressured["oldest_queued_seconds"] >= 0.04
    assert pressured["queue_put_waiters"] == 1
    assert pressured["oldest_queue_put_wait_seconds"] >= 0.04
    assert pressured["task_state"] == "running"
    assert not waiting.done()

    database.release_write.set()
    await asyncio.wait_for(waiting, timeout=1)
    await asyncio.wait_for(writer.queue.join(), timeout=1)
    drained = writer.metrics()
    assert drained["queue_put_waiters"] == 0
    assert drained["queue_put_blocked_count"] >= 1
    assert drained["queue_put_wait_seconds_max"] >= 0.04
    assert drained["max_queue_depth"] == 1
    await writer.stop()
    assert writer.metrics()["task_state"] == "stopped"
