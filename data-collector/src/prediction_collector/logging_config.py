from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import Counter
from datetime import UTC, datetime
from typing import Any


_STANDARD = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


class ContextFormatter(logging.Formatter):
    """Human-readable formatter that does not discard structured context."""

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD and not key.startswith("_")
        }
        if context:
            rendered += " " + json.dumps(context, default=str, sort_keys=True)
        return rendered


def configure_logging(level: str = "INFO", *, json_logs: bool = False) -> None:
    handler = logging.StreamHandler()
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            ContextFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


class ThroughputMetrics:
    def __init__(self) -> None:
        self._messages: Counter[str] = Counter()
        self._rows: Counter[str] = Counter()
        self._started = time.monotonic()
        self._lock = asyncio.Lock()

    async def message(self, exchange: str, count: int = 1) -> None:
        async with self._lock:
            self._messages[exchange] += count

    async def rows(self, table: str, count: int = 1) -> None:
        async with self._lock:
            self._rows[table] += count

    async def snapshot_and_reset(self) -> dict[str, Any]:
        async with self._lock:
            elapsed = max(time.monotonic() - self._started, 0.001)
            scale = 60.0 / elapsed
            result = {
                "window_seconds": round(elapsed, 3),
                "websocket_messages_per_minute": {
                    key: round(value * scale, 2) for key, value in self._messages.items()
                },
                "database_rows_per_minute": {
                    key: round(value * scale, 2) for key, value in self._rows.items()
                },
            }
            self._messages.clear()
            self._rows.clear()
            self._started = time.monotonic()
            return result
