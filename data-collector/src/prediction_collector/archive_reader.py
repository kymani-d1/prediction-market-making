from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pyarrow.dataset as ds


def archive_partition_prefixes(
    *, prefix: str, stream: str, start: datetime, end: datetime,
    schema_version: int = 2,
) -> list[str]:
    """Return only hour partitions intersecting [start, end)."""
    cursor = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    stop = end.astimezone(UTC)
    values: list[str] = []
    root = f"{prefix.strip('/')}/" if prefix else ""
    while cursor < stop:
        values.append(
            f"{root}schema_version={schema_version}/exchange=polymarket/stream={stream}/"
            f"date={cursor.date().isoformat()}/hour={cursor.hour:02d}/"
        )
        cursor += timedelta(hours=1)
    return values


def load_archive(
    paths: Iterable[str | Path] | str | Path,
    *,
    stream: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    markets: Iterable[str] | None = None,
    columns: list[str] | None = None,
    dictionary_paths: Iterable[str | Path] | None = None,
):
    """Load compact archives with pruning and transparent external-ID resolution."""
    values = _resolve_paths(paths, stream=stream)
    dataset = ds.dataset(values, format="parquet")
    predicate = None
    names = set(dataset.schema.names)
    if "received_at" in names and start is not None:
        predicate = ds.field("received_at") >= start.astimezone(UTC)
    if "received_at" in names and end is not None:
        end_predicate = ds.field("received_at") < end.astimezone(UTC)
        predicate = end_predicate if predicate is None else predicate & end_predicate
    if "received_ts_ns" in names and start is not None:
        predicate = ds.field("received_ts_ns") >= _datetime_ns(start)
    if "received_ts_ns" in names and end is not None:
        end_predicate = ds.field("received_ts_ns") < _datetime_ns(end)
        predicate = end_predicate if predicate is None else predicate & end_predicate
    market_values = list(markets or [])
    needs_dictionary = bool(
        market_values
        or (columns and any(value in columns for value in (
            "market_external_id", "outcome_external_id"
        )))
    )
    dictionaries = (
        _load_dictionary(paths, dictionary_paths=dictionary_paths)
        if "market_key" in names and needs_dictionary else ({}, {})
    )
    market_keys = [
        key for key, external_id in dictionaries[0].items()
        if external_id in market_values
    ]
    if market_values and "market_key" in names:
        market_predicate = ds.field("market_key").isin(market_keys)
        predicate = market_predicate if predicate is None else predicate & market_predicate
    elif market_values and "market_external_id" in names:
        market_predicate = ds.field("market_external_id").isin(market_values)
        predicate = market_predicate if predicate is None else predicate & market_predicate
    requested = list(columns) if columns else None
    physical_columns = requested
    if requested and "market_key" in names and any(
        value in requested for value in ("market_external_id", "outcome_external_id")
    ):
        physical_columns = [value for value in requested if value in names]
        for key in ("market_key", "token_key"):
            if key in names and key not in physical_columns:
                physical_columns.append(key)
    table = dataset.to_table(columns=physical_columns, filter=predicate)
    if "market_key" in table.column_names and needs_dictionary:
        import pyarrow as pa

        market_map, token_map = dictionaries
        table = table.append_column(
            "market_external_id",
            pa.array([market_map.get(value.as_py()) for value in table["market_key"]]),
        )
        if "token_key" in table.column_names:
            table = table.append_column(
                "outcome_external_id",
                pa.array([token_map.get(value.as_py()) for value in table["token_key"]]),
            )
    if requested:
        table = table.select(requested)
    return table


def _resolve_paths(
    paths: Iterable[str | Path] | str | Path, *, stream: str | None
) -> list[str]:
    if isinstance(paths, (str, Path)):
        root = Path(paths)
        if root.is_dir():
            pattern = f"**/stream={stream}/**/*.parquet" if stream else "**/*.parquet"
            return [str(path) for path in sorted(root.glob(pattern))]
        return [str(root)]
    return [str(path) for path in paths]


def _load_dictionary(
    paths: Iterable[str | Path] | str | Path,
    *,
    dictionary_paths: Iterable[str | Path] | None,
) -> tuple[dict[int, str], dict[int, str]]:
    candidates = [str(path) for path in dictionary_paths] if dictionary_paths else []
    if not candidates and isinstance(paths, (str, Path)) and Path(paths).is_dir():
        candidates = [
            str(path)
            for path in sorted(Path(paths).glob("**/stream=archive_dictionary/**/*.parquet"))
        ]
    if not candidates:
        raise ValueError(
            "compact archive market filtering requires dictionary_paths or an archive root"
        )
    table = ds.dataset(candidates, format="parquet").to_table(
        columns=["entity_kind", "archive_key", "external_id"]
    )
    markets: dict[int, str] = {}
    tokens: dict[int, str] = {}
    for row in table.to_pylist():
        target = markets if int(row["entity_kind"]) == 1 else tokens
        target[int(row["archive_key"])] = str(row["external_id"])
    return markets, tokens


def _datetime_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value.astimezone(UTC) - epoch
    return delta.days * 86_400_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1_000
