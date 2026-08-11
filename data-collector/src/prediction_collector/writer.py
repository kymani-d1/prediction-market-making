from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from prediction_collector.database import Database


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WriteItem:
    kind: str
    data: dict[str, Any]


class BatchWriter:
    """Bounded, backpressured, transactional high-volume database writer."""

    def __init__(
        self,
        database: "Database",
        *,
        max_queue_size: int,
        batch_size: int,
        flush_interval_seconds: float,
    ) -> None:
        self.database = database
        self.queue: asyncio.Queue[WriteItem] = asyncio.Queue(maxsize=max_queue_size)
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.rows_written = 0
        self.failed_items = 0
        self.run_id: int | None = None

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="database-batch-writer")

    async def put(self, item: WriteItem) -> None:
        if self._task is not None and self._task.done():
            await self._task
            raise RuntimeError("database writer stopped unexpectedly")
        await self.queue.put(item)

    def put_nowait(self, item: WriteItem) -> bool:
        try:
            self.queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            return False

    async def run(self) -> None:
        batch: list[WriteItem] = []
        while not self._stop.is_set() or not self.queue.empty() or batch:
            timeout = self.flush_interval_seconds if not batch else min(
                self.flush_interval_seconds, 0.25
            )
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                batch.append(item)
            except TimeoutError:
                pass
            if batch and (
                len(batch) >= self.batch_size
                or self._stop.is_set()
                or self.queue.empty()
            ):
                flushing = batch
                batch = []
                try:
                    await self._flush_with_retry(flushing)
                finally:
                    for _ in flushing:
                        self.queue.task_done()

    async def _flush_with_retry(self, batch: list[WriteItem]) -> None:
        delay = 0.5
        last_error: BaseException | None = None
        for attempt in range(1, 7):
            try:
                counts = await self.database.write_items(batch)
                self.rows_written += sum(counts.values())
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt == 6:
                    LOGGER.exception(
                        "Database batch failed after retries; isolating bad rows",
                        extra={"batch_size": len(batch)},
                    )
                    break
                LOGGER.exception(
                    "Database batch failed; retrying",
                    extra={"batch_size": len(batch), "attempt": attempt, "delay": delay},
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15)
        assert last_error is not None
        await self._isolate_failed_batch(batch, last_error)

    async def _isolate_failed_batch(
        self, batch: list[WriteItem], batch_error: BaseException
    ) -> None:
        if len(batch) > 1:
            midpoint = len(batch) // 2
            for subset in (batch[:midpoint], batch[midpoint:]):
                try:
                    counts = await self.database.write_items(subset)
                    self.rows_written += sum(counts.values())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._isolate_failed_batch(subset, exc)
            return

        item = batch[0]
        try:
            await self.database.record_write_failure(
                item, batch_error, run_id=self.run_id
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "Unable to quarantine failed database row",
                extra={"item_kind": item.kind},
            )
            raise batch_error
        self.failed_items += 1
        LOGGER.error(
            "Database row quarantined after permanent write failure",
            extra={
                "item_kind": item.kind,
                "error_type": type(batch_error).__name__,
                "error": str(batch_error)[:1000],
            },
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
