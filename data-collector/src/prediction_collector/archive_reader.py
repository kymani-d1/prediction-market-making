from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pyarrow.dataset as ds


def archive_partition_prefixes(
    *, prefix: str, stream: str, start: datetime, end: datetime
) -> list[str]:
    """Return only hour partitions intersecting [start, end)."""
    cursor = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    stop = end.astimezone(UTC)
    values: list[str] = []
    root = f"{prefix.strip('/')}/" if prefix else ""
    while cursor < stop:
        values.append(
            f"{root}schema_version=1/exchange=polymarket/stream={stream}/"
            f"date={cursor.date().isoformat()}/hour={cursor.hour:02d}/"
        )
        cursor += timedelta(hours=1)
    return values


def load_archive(
    paths: Iterable[str | Path],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    markets: Iterable[str] | None = None,
    columns: list[str] | None = None,
):
    """Load selected Parquet objects with predicate pushdown for notebooks."""
    dataset = ds.dataset([str(path) for path in paths], format="parquet")
    predicate = None
    names = set(dataset.schema.names)
    if "received_at" in names and start is not None:
        predicate = ds.field("received_at") >= start.astimezone(UTC)
    if "received_at" in names and end is not None:
        end_predicate = ds.field("received_at") < end.astimezone(UTC)
        predicate = end_predicate if predicate is None else predicate & end_predicate
    market_values = list(markets or [])
    if market_values and "market_external_id" in names:
        market_predicate = ds.field("market_external_id").isin(market_values)
        predicate = market_predicate if predicate is None else predicate & market_predicate
    return dataset.to_table(columns=columns, filter=predicate)
