"""Benchmark verbose versus compact order-book Parquet encodings.

Pass one or more schema-v1 orderbook-update objects downloaded from the
disposable integration bucket. The script writes only to a temporary local
directory and emits machine-readable JSON.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from prediction_collector.archive import (
    STREAM_SCHEMAS,
    compact_archive_row,
    decimal_from_components,
)
from prediction_collector.common.utils import canonical_json


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--limit", type=int, default=250_000)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def timed_write(
    table: pa.Table,
    path: Path,
    *,
    level: int,
    row_group_rows: int,
    dictionary: bool,
    filter_column: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=level,
        use_dictionary=dictionary,
        write_statistics=True,
        row_group_size=row_group_rows,
        data_page_size=1024 * 1024,
    )
    write_seconds = time.perf_counter() - started
    started = time.perf_counter()
    restored = pq.read_table(path)
    full_read_seconds = time.perf_counter() - started
    filter_value = restored[filter_column][0].as_py()
    started = time.perf_counter()
    filtered = pq.read_table(path, filters=[(filter_column, "=", filter_value)])
    filtered_read_seconds = time.perf_counter() - started
    return {
        "zstd_level": level,
        "row_group_rows": row_group_rows,
        "dictionary_encoding": dictionary,
        "rows": table.num_rows,
        "parquet_bytes": path.stat().st_size,
        "bytes_per_row": path.stat().st_size / max(table.num_rows, 1),
        "serialization_seconds": write_seconds,
        "full_read_seconds": full_read_seconds,
        "filtered_read_seconds": filtered_read_seconds,
        "filtered_rows": filtered.num_rows,
    }


def main() -> None:
    args = arguments()
    tables = [pq.read_table(path) for path in args.inputs]
    source = pa.concat_tables(tables, promote_options="default").slice(0, args.limit)
    if "market_key" in source.column_names:
        compact = source.cast(STREAM_SCHEMAS["orderbook_updates"])
        rows = [_verbose_v1_row(row) for row in compact.to_pylist()]
        verbose = pa.Table.from_pylist(rows, schema=_verbose_v1_schema())
    else:
        verbose = source
        rows = verbose.to_pylist()
        compact_rows = [compact_archive_row("orderbook_updates", row) for row in rows]
        compact = pa.Table.from_pylist(
            compact_rows, schema=STREAM_SCHEMAS["orderbook_updates"]
        )
    logical_bytes = sum(len(canonical_json(row).encode("utf-8")) for row in rows)
    combinations = [
        (1, 10_000, True),
        (3, 10_000, True),
        (3, 100_000, True),
        (3, 100_000, False),
        (6, 100_000, True),
    ]
    results: dict[str, Any] = {
        "source_objects": len(args.inputs),
        "rows": len(rows),
        "logical_json_bytes": logical_bytes,
        "verbose_schema": str(verbose.schema),
        "compact_schema": str(compact.schema),
        "verbose": [],
        "compact": [],
    }
    with tempfile.TemporaryDirectory(prefix="archive-encoding-") as temporary:
        root = Path(temporary)
        for level, row_group, dictionary in combinations:
            verbose_result = timed_write(
                verbose,
                root / f"verbose-{level}-{row_group}-{dictionary}.parquet",
                level=level,
                row_group_rows=row_group,
                dictionary=dictionary,
                filter_column="market_external_id",
            )
            compact_result = timed_write(
                compact,
                root / f"compact-{level}-{row_group}-{dictionary}.parquet",
                level=level,
                row_group_rows=row_group,
                dictionary=dictionary,
                filter_column="market_key",
            )
            verbose_result["compression_ratio_vs_logical"] = (
                logical_bytes / verbose_result["parquet_bytes"]
            )
            compact_result["compression_ratio_vs_logical"] = (
                logical_bytes / compact_result["parquet_bytes"]
            )
            results["verbose"].append(verbose_result)
            results["compact"].append(compact_result)
    encoded = json.dumps(results, indent=2, default=str)
    if args.output:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)


def _verbose_v1_schema() -> pa.Schema:
    return pa.schema(
        [
            ("exchange", pa.string()),
            ("market_external_id", pa.string()),
            ("outcome_external_id", pa.string()),
            ("connection_id", pa.int64()),
            ("source_timestamp", pa.timestamp("us", tz="UTC")),
            ("exchange_timestamp", pa.timestamp("us", tz="UTC")),
            ("source_timestamp_raw", pa.string()),
            ("exchange_timestamp_raw", pa.string()),
            ("received_at", pa.timestamp("us", tz="UTC")),
            ("received_monotonic_ns", pa.int64()),
            ("sequence_number", pa.int64()),
            ("book_hash", pa.string()),
            ("side", pa.string()),
            ("price", pa.string()),
            ("size", pa.string()),
            ("size_delta", pa.string()),
            ("operation", pa.string()),
            ("event_type", pa.string()),
        ],
        metadata={b"schema_version": b"1", b"exchange": b"polymarket"},
    )


def _timestamp_from_ns(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)


def _verbose_v1_row(row: dict[str, Any]) -> dict[str, Any]:
    market_key = int(row["market_key"])
    token_key = int(row["token_key"])
    price = decimal_from_components(row.get("price_mantissa"), row.get("price_scale"))
    size = decimal_from_components(row.get("size_mantissa"), row.get("size_scale"))
    action = int(row.get("action") or 1)
    return {
        "exchange": "polymarket",
        # Match the length/distribution of real condition and CLOB token IDs.
        "market_external_id": f"0x{market_key:064x}",
        "outcome_external_id": str(token_key).zfill(77),
        "connection_id": row.get("connection_id"),
        "source_timestamp": _timestamp_from_ns(row.get("source_ts_ns")),
        "exchange_timestamp": _timestamp_from_ns(row.get("exchange_ts_ns")),
        "source_timestamp_raw": None,
        "exchange_timestamp_raw": None,
        "received_at": _timestamp_from_ns(row.get("received_ts_ns")),
        "received_monotonic_ns": row.get("received_monotonic_ns"),
        "sequence_number": None,
        "book_hash": row.get("book_hash"),
        "side": {0: "buy", 1: "sell"}.get(row.get("side"), "unknown"),
        "price": str(price) if price is not None else None,
        "size": str(size) if size is not None else None,
        "size_delta": None,
        "operation": {0: "delta", 1: "set", 2: "delete"}.get(action, "set"),
        "event_type": "price_change",
    }


if __name__ == "__main__":
    main()
