from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from prediction_collector.common.types import MarketCandidate
from prediction_collector.common.utils import (
    canonical_json,
    content_hash,
    parse_timestamp,
    utc_now,
)
from prediction_collector.config import Settings
from prediction_collector.logging_config import ThroughputMetrics
from prediction_collector.migrations import migrate_database


LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from prediction_collector.writer import WriteItem


@dataclass(slots=True)
class MetadataSyncDiagnostics:
    stale_lifecycle_states_preserved: int = 0
    unresolved_multivariate_leg_markets: int = 0
    unresolved_multivariate_leg_outcomes: int = 0

    def as_log_fields(self) -> dict[str, int]:
        return {
            "stale_lifecycle_states_preserved": (
                self.stale_lifecycle_states_preserved
            ),
            "unresolved_multivariate_legs": (
                self.unresolved_multivariate_leg_markets
                + self.unresolved_multivariate_leg_outcomes
            ),
            "unresolved_multivariate_leg_markets": (
                self.unresolved_multivariate_leg_markets
            ),
            "unresolved_multivariate_leg_outcomes": (
                self.unresolved_multivariate_leg_outcomes
            ),
        }


def _json(value: Any) -> Jsonb:
    return Jsonb(value if value is not None else {})


def _market_metadata_digest(value: Mapping[str, Any]) -> str:
    raw = value.get("raw_data")
    raw = raw if isinstance(raw, Mapping) else {}
    stable_raw_keys = (
        "clobTokenIds",
        "clob_token_ids",
        "outcomes",
        "negativeRisk",
        "negative_risk",
        "negRisk",
        "negRiskAugmented",
        "is_provisional",
        "feeSchedule",
        "fee_schedule",
        "feesEnabled",
        "makerBaseFee",
        "rewardsMinSize",
        "rewardsMaxSpread",
        "rewards_min_size",
        "rewards_max_spread",
        "price_level_structure",
        "price_ranges",
        "mve_collection_ticker",
        "mve_selected_legs",
    )
    return content_hash(
        {
            "normalized": {
                key: value.get(key)
                for key in (
                    "ticker",
                    "slug",
                    "condition_id",
                    "question",
                    "subtitle",
                    "description",
                    "rules",
                    "resolution_source",
                    "status",
                    "market_type",
                    "is_active",
                    "is_tradable",
                    "clob_enabled",
                    "enable_order_book",
                    "accepting_orders",
                    "negative_risk",
                    "open_time",
                    "close_time",
                    "settlement_time",
                    "result",
                    "settlement_value",
                    "tick_size",
                    "fee_rate",
                    "price_level_structure",
                    "structural_metadata",
                )
            },
            # Full payloads remain in raw_rest_payloads and the current market
            # row. Metadata history should not version on bid/ask/last/volume
            # churn, but it must retain exchange-specific structural fields.
            "stable_raw": {key: raw.get(key) for key in stable_raw_keys if key in raw},
        }
    )


_LIFECYCLE_MUTABLE_MARKET_FIELDS = (
    "question",
    "subtitle",
    "description",
    "rules",
    "status",
    "is_active",
    "is_tradable",
    "accepting_orders",
    "open_time",
    "close_time",
    "settlement_time",
    "result",
    "settlement_value",
    "tick_size",
    "fee_rate",
    "price_level_structure",
    "structural_metadata",
)


def _market_metadata_upstream_timestamp(value: Mapping[str, Any]) -> datetime | None:
    """Return the strongest exchange/source event timestamp available."""
    source_timestamp = parse_timestamp(value.get("source_timestamp"))
    if source_timestamp is not None:
        return source_timestamp
    if value.get("exchange_timestamp_is_transport"):
        return None
    return parse_timestamp(value.get("exchange_timestamp"))


def _market_metadata_observed_timestamp(value: Mapping[str, Any]) -> datetime | None:
    return parse_timestamp(value.get("observed_at"))


def _market_metadata_is_stale(
    value: Mapping[str, Any], current: Mapping[str, Any]
) -> bool:
    incoming_upstream = _market_metadata_upstream_timestamp(value)
    current_upstream = parse_timestamp(current.get("metadata_source_timestamp"))
    if (
        current_upstream is None
        and not current.get("metadata_exchange_timestamp_is_transport")
    ):
        current_upstream = parse_timestamp(current.get("metadata_exchange_timestamp"))
    if incoming_upstream is not None and current_upstream is not None:
        return incoming_upstream <= current_upstream
    incoming_observed = _market_metadata_observed_timestamp(value)
    current_observed = parse_timestamp(current.get("metadata_observation_timestamp"))
    return current_observed is not None and (
        incoming_observed is None or incoming_observed <= current_observed
    )


def _preserve_newer_market_state(
    value: Mapping[str, Any], current: Mapping[str, Any]
) -> tuple[dict[str, Any], bool]:
    """Merge a stale full REST object without regressing newer lifecycle state."""
    merged = dict(value)
    stale_state = _market_metadata_is_stale(value, current)
    if not stale_state:
        return merged, False
    for field in _LIFECYCLE_MUTABLE_MARKET_FIELDS:
        merged[field] = current.get(field)
    merged["resolution_source"] = current.get("metadata_resolution_source")
    merged["source_timestamp"] = current.get("metadata_source_timestamp")
    merged["exchange_timestamp"] = current.get("metadata_exchange_timestamp")
    merged["exchange_timestamp_is_transport"] = bool(
        current.get("metadata_exchange_timestamp_is_transport")
    )
    merged["observed_at"] = current.get("metadata_observation_timestamp")
    current_raw = current.get("raw_data")
    current_raw = current_raw if isinstance(current_raw, dict) else {}
    incoming_raw = value.get("raw_data")
    incoming_raw = dict(incoming_raw) if isinstance(incoming_raw, dict) else {}
    if "_latest_lifecycle_event" in current_raw:
        incoming_raw["_latest_lifecycle_event"] = current_raw[
            "_latest_lifecycle_event"
        ]
    merged["raw_data"] = incoming_raw
    return merged, True


def _debug_preserved_newer_market_state(
    *,
    value: Mapping[str, Any],
    current: Mapping[str, Any],
    incoming_timestamp: datetime | None,
    diagnostics: MetadataSyncDiagnostics | None,
) -> None:
    if diagnostics is not None:
        diagnostics.stale_lifecycle_states_preserved += 1
    LOGGER.debug(
        "Preserved newer market lifecycle state over stale metadata",
        extra={
            "exchange": value["exchange"],
            "market": value["external_id"],
            "incoming_timestamp": incoming_timestamp,
            "current_timestamp": (
                current["metadata_source_timestamp"]
                or current["metadata_exchange_timestamp"]
            ),
        },
    )


def _fee_configuration_digest(
    *,
    configuration: Mapping[str, Any],
    semantic_configuration: Mapping[str, Any] | None,
    maker_rate: Any,
    taker_rate: Any,
    fee_rate: Any,
    multiplier: Any,
    fixed_fee: Any,
    currency: str | None,
) -> str:
    return content_hash(
        {
            "configuration": semantic_configuration or configuration,
            "maker_rate": maker_rate,
            "taker_rate": taker_rate,
            "fee_rate": fee_rate,
            "multiplier": multiplier,
            "fixed_fee": fixed_fee,
            "currency": currency,
        }
    )


def _configuration_stream_key(
    kind: str,
    exchange: str,
    scope_type: str,
    scope_external_id: str,
    configuration_type: str,
) -> str:
    # Length-prefixed components prevent ambiguous delimiter collisions.
    components = (kind, exchange, scope_type, scope_external_id, configuration_type)
    return "".join(f"{len(component)}:{component}" for component in components)


async def _lock_configuration_stream(
    connection: Any,
    *,
    kind: str,
    exchange: str,
    scope_type: str,
    scope_external_id: str,
    configuration_type: str,
) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (
            _configuration_stream_key(
                kind,
                exchange,
                scope_type,
                scope_external_id,
                configuration_type,
            ),
        ),
    )


async def _stitch_configuration_intervals(
    connection: Any,
    *,
    table: str,
    type_column: str,
    exchange: str,
    scope_type: str,
    scope_external_id: str,
    configuration_type: str,
) -> int:
    allowed = {
        ("fee_configuration_history", "fee_type"),
        ("incentive_configuration_history", "incentive_type"),
    }
    if (table, type_column) not in allowed:
        raise ValueError("unsupported configuration history table")
    cursor = await connection.execute(
        f"""
        WITH ordered AS (
            SELECT id, effective_from, declared_effective_to,
                   lead(effective_from) OVER (
                       ORDER BY effective_from, id
                   ) AS next_effective_from
            FROM {table}
            WHERE exchange = %s AND scope_type = %s
              AND scope_external_id = %s AND {type_column} = %s
        ), desired AS (
            SELECT id,
                   CASE
                       WHEN declared_effective_to IS NULL
                           THEN next_effective_from
                       WHEN next_effective_from IS NULL
                           THEN declared_effective_to
                       ELSE LEAST(declared_effective_to, next_effective_from)
                   END AS effective_to
            FROM ordered
        )
        UPDATE {table} AS target
        SET effective_to = desired.effective_to
        FROM desired
        WHERE target.id = desired.id
          AND target.effective_to IS DISTINCT FROM desired.effective_to
        """,
        (exchange, scope_type, scope_external_id, configuration_type),
    )
    return max(int(cursor.rowcount or 0), 0)


def parse_effective_time(
    reward: Mapping[str, Any], value: Mapping[str, Any], observed_at: datetime
) -> datetime:
    return (
        parse_timestamp(reward.get("startDate") or reward.get("start_date"))
        or value.get("open_time")
        or observed_at
    )


class Database:
    def __init__(self, settings: Settings, metrics: ThroughputMetrics | None = None) -> None:
        self.settings = settings
        self.metrics = metrics or ThroughputMetrics()
        self.pool = AsyncConnectionPool(
            conninfo=settings.database_dsn,
            min_size=settings.database_pool_min_size,
            max_size=settings.database_pool_max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )
        self._database_write_baseline: dict[str, int] | None = None
        self._database_write_baseline_lock = asyncio.Lock()

    async def open(self) -> None:
        await self.pool.open(wait=True)

    async def close(self) -> None:
        await self.pool.close()

    async def __aenter__(self) -> "Database":
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def migrate(self) -> list[str]:
        return await migrate_database(self.settings.database_dsn)

    async def ping(self) -> bool:
        try:
            async with self.pool.connection() as connection:
                row = await (await connection.execute("SELECT 1 AS ok")).fetchone()
            return bool(row and row["ok"] == 1)
        except Exception:
            return False

    async def start_run(self, job_type: str, exchange: str | None) -> int:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO collector_runs
                        (run_uuid, job_type, exchange, status, metadata)
                    VALUES (%s, %s, %s, 'running', %s)
                    RETURNING id
                    """,
                    (uuid.uuid4(), job_type, exchange, _json(self.settings.safe_summary())),
                )
            ).fetchone()
        assert row is not None
        return int(row["id"])

    async def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        records_processed: int,
        rows_written: int,
        error_summary: str | None = None,
        coverage: Mapping[str, int] | None = None,
    ) -> None:
        coverage = coverage or {}
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE collector_runs
                SET finished_at = clock_timestamp(), status = %s,
                    records_processed = %s, rows_written = %s,
                    markets_discovered = %s, markets_active = %s,
                    markets_tradable = %s, markets_subscribed = %s,
                    markets_excluded = %s,
                    error_summary = %s
                WHERE id = %s
                """,
                (
                    status,
                    records_processed,
                    rows_written,
                    coverage.get("discovered", 0),
                    coverage.get("active", 0),
                    coverage.get("tradable", 0),
                    coverage.get("subscribed", 0),
                    coverage.get("excluded", 0),
                    error_summary,
                    run_id,
                ),
            )

    async def upsert_series(self, value: Mapping[str, Any]) -> int:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO series
                        (exchange, external_id, ticker, title, category, frequency, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, external_id) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        title = EXCLUDED.title,
                        category = EXCLUDED.category,
                        frequency = EXCLUDED.frequency,
                        raw_data = EXCLUDED.raw_data,
                        last_seen_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    (
                        value["exchange"],
                        value["external_id"],
                        value.get("ticker"),
                        value["title"],
                        value.get("category"),
                        value.get("frequency"),
                        _json(value.get("raw_data")),
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("series")
        return int(row["id"])

    async def upsert_event(self, value: Mapping[str, Any]) -> int:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO events
                        (exchange, external_id, series_id, ticker, slug, title, description,
                         category, status, start_time, end_time, created_time, updated_time, rules,
                         resolution_source, raw_data)
                    VALUES
                        (%s, %s,
                        (SELECT id FROM series WHERE exchange = %s AND external_id = %s),
                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, external_id) DO UPDATE SET
                        series_id = COALESCE(EXCLUDED.series_id, events.series_id),
                        ticker = COALESCE(EXCLUDED.ticker, events.ticker),
                        slug = COALESCE(EXCLUDED.slug, events.slug),
                        title = EXCLUDED.title,
                        description = COALESCE(EXCLUDED.description, events.description),
                        category = COALESCE(EXCLUDED.category, events.category),
                        status = CASE
                            WHEN %s THEN EXCLUDED.status
                            ELSE events.status
                        END,
                        start_time = COALESCE(EXCLUDED.start_time, events.start_time),
                        end_time = COALESCE(EXCLUDED.end_time, events.end_time),
                        created_time = COALESCE(EXCLUDED.created_time, events.created_time),
                        updated_time = COALESCE(EXCLUDED.updated_time, events.updated_time),
                        rules = COALESCE(EXCLUDED.rules, events.rules),
                        resolution_source = COALESCE(EXCLUDED.resolution_source, events.resolution_source),
                        raw_data = events.raw_data || EXCLUDED.raw_data,
                        last_seen_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    (
                        value["exchange"],
                        value["external_id"],
                        value["exchange"],
                        value.get("series_external_id"),
                        value.get("ticker"),
                        value.get("slug"),
                        value["title"],
                        value.get("description"),
                        value.get("category"),
                        value.get("status") or "unknown",
                        value.get("start_time"),
                        value.get("end_time"),
                        value.get("created_time"),
                        value.get("updated_time"),
                        value.get("rules"),
                        value.get("resolution_source"),
                        _json(value.get("raw_data")),
                        value.get("status") is not None,
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("events")
        return int(row["id"])

    async def upsert_market_group(self, value: Mapping[str, Any]) -> int:
        external_id = str(value.get("external_id") or "")
        if not external_id:
            raise ValueError("market group external_id is required")
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO market_groups
                        (exchange, external_id, event_id, group_type, name, description,
                         status, constraint_definition, raw_data)
                    VALUES
                        (%s, %s,
                         (SELECT id FROM events WHERE exchange = %s AND external_id = %s),
                         %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, external_id) WHERE external_id IS NOT NULL DO UPDATE SET
                        event_id = EXCLUDED.event_id,
                        group_type = EXCLUDED.group_type,
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        constraint_definition = EXCLUDED.constraint_definition,
                        raw_data = EXCLUDED.raw_data,
                        last_seen_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    (
                        value["exchange"],
                        external_id,
                        value["exchange"],
                        value.get("event_external_id"),
                        value["group_type"],
                        value.get("name"),
                        value.get("description"),
                        value.get("status"),
                        _json(value.get("constraint_definition")),
                        _json(value.get("raw_data")),
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("market_groups")
        return int(row["id"])

    async def upsert_market(
        self,
        value: Mapping[str, Any],
        *,
        diagnostics: MetadataSyncDiagnostics | None = None,
    ) -> int:
        value = dict(value)
        value.setdefault("observed_at", utc_now())
        history_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                existing = await (
                    await connection.execute(
                        """
                        SELECT m.*,
                               h.source_timestamp AS metadata_source_timestamp,
                               h.exchange_timestamp AS metadata_exchange_timestamp,
                               h.observation_timestamp AS metadata_observation_timestamp,
                               h.exchange_timestamp_is_transport AS
                                   metadata_exchange_timestamp_is_transport,
                               h.resolution_source AS metadata_resolution_source
                        FROM markets m
                        LEFT JOIN LATERAL (
                            SELECT source_timestamp, exchange_timestamp,
                                   observation_timestamp,
                                   exchange_timestamp_is_transport,
                                   resolution_source
                            FROM market_metadata_history
                            WHERE market_id = m.id AND valid_to IS NULL
                            ORDER BY version_number DESC LIMIT 1
                        ) h ON TRUE
                        WHERE m.exchange = %s AND m.external_id = %s
                        FOR UPDATE OF m
                        """,
                        (value["exchange"], value["external_id"]),
                    )
                ).fetchone()
                if existing is not None:
                    incoming_timestamp = _market_metadata_upstream_timestamp(value)
                    value, stale_state = _preserve_newer_market_state(value, existing)
                    if stale_state:
                        _debug_preserved_newer_market_state(
                            value=value,
                            current=existing,
                            incoming_timestamp=incoming_timestamp,
                            diagnostics=diagnostics,
                        )
                raw_value = value.get("raw_data")
                raw = dict(raw_value) if isinstance(raw_value, Mapping) else {}
                value["raw_data"] = raw
                active = bool(
                    value.get("is_active", value.get("status") in {"active", "open"})
                )
                tradable = bool(
                    value.get("is_tradable", value.get("accepting_orders", active))
                )
                row = await (
                    await connection.execute(
                        """
                        INSERT INTO markets
                            (exchange, external_id, event_id, ticker, slug, condition_id,
                             question, subtitle, description, rules, status, market_type,
                             is_active, is_tradable,
                             clob_enabled, enable_order_book, accepting_orders, negative_risk,
                              open_time, close_time, settlement_time, result,
                              settlement_value, volume, volume_24h,
                             open_interest, liquidity, tick_size, fee_rate,
                             price_level_structure, structural_metadata, raw_data)
                        VALUES
                            (%s, %s,
                             (SELECT id FROM events WHERE exchange = %s AND external_id = %s),
                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                             %s)
                        ON CONFLICT (exchange, external_id) DO UPDATE SET
                            event_id = COALESCE(EXCLUDED.event_id, markets.event_id),
                            ticker = EXCLUDED.ticker, slug = EXCLUDED.slug,
                            condition_id = EXCLUDED.condition_id, question = EXCLUDED.question,
                            subtitle = EXCLUDED.subtitle, description = EXCLUDED.description,
                            rules = EXCLUDED.rules, status = EXCLUDED.status,
                            market_type = EXCLUDED.market_type, is_active = EXCLUDED.is_active,
                            is_tradable = EXCLUDED.is_tradable,
                            clob_enabled = EXCLUDED.clob_enabled,
                            enable_order_book = EXCLUDED.enable_order_book,
                            accepting_orders = EXCLUDED.accepting_orders,
                            negative_risk = EXCLUDED.negative_risk,
                            open_time = EXCLUDED.open_time, close_time = EXCLUDED.close_time,
                            settlement_time = EXCLUDED.settlement_time, result = EXCLUDED.result,
                            settlement_value = EXCLUDED.settlement_value,
                            volume = EXCLUDED.volume, volume_24h = EXCLUDED.volume_24h,
                            open_interest = EXCLUDED.open_interest,
                            liquidity = EXCLUDED.liquidity, tick_size = EXCLUDED.tick_size,
                            fee_rate = EXCLUDED.fee_rate, raw_data = EXCLUDED.raw_data,
                            price_level_structure = EXCLUDED.price_level_structure,
                            structural_metadata = EXCLUDED.structural_metadata,
                            last_seen_at = clock_timestamp(), updated_at = clock_timestamp()
                        RETURNING id
                        """,
                        (
                            value["exchange"],
                            value["external_id"],
                            value["exchange"],
                            value.get("event_external_id"),
                            value.get("ticker"),
                            value.get("slug"),
                            value.get("condition_id"),
                            value["question"],
                            value.get("subtitle"),
                            value.get("description"),
                            value.get("rules"),
                            value.get("status") or "unknown",
                            value.get("market_type"),
                            active,
                            tradable,
                            value.get("clob_enabled", value.get("enable_order_book")),
                            value.get("enable_order_book"),
                            value.get("accepting_orders"),
                            bool(value.get("negative_risk")),
                            value.get("open_time"),
                            value.get("close_time"),
                            value.get("settlement_time"),
                            value.get("result"),
                            value.get("settlement_value"),
                            value.get("volume"),
                            value.get("volume_24h"),
                            value.get("open_interest"),
                            value.get("liquidity"),
                            value.get("tick_size"),
                            value.get("fee_rate"),
                            _json(value.get("price_level_structure")) if value.get("price_level_structure") else None,
                            _json(value.get("structural_metadata")),
                            _json(raw),
                        ),
                    )
                ).fetchone()
                assert row is not None
                market_id = int(row["id"])
                history_rows = await self._record_market_metadata_history(
                    connection,
                    market_id=market_id,
                    value=value,
                )
                await self._record_market_auxiliary(
                    connection,
                    market_id=market_id,
                    value=value,
                    observed_at=utc_now(),
                    diagnostics=diagnostics,
                )
        await self.metrics.rows("markets")
        if history_rows:
            await self.metrics.rows("market_metadata_history", history_rows)
        return market_id

    async def apply_market_metadata_patch(
        self,
        *,
        exchange: str,
        market_external_id: str,
        updates: Mapping[str, Any],
        lifecycle_payload: Mapping[str, Any],
        source_timestamp: datetime | None,
        exchange_timestamp: datetime | None,
        observed_at: datetime | None = None,
    ) -> bool:
        """Apply a partial lifecycle update without erasing current metadata.

        Lifecycle frames are intentionally sparse.  Rebuilding a normal market
        object from only that frame would null rules, descriptions, volume, and
        relationship fields.  This reads the current row, overlays an explicit
        allowlist of mutable fields, and routes the complete result through the
        normal versioning path.
        """
        allowed = set(_LIFECYCLE_MUTABLE_MARKET_FIELDS)
        unexpected = set(updates) - allowed
        if unexpected:
            raise ValueError(f"unsupported lifecycle market fields: {sorted(unexpected)}")
        observed_at = observed_at or utc_now()
        history_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                row = await (
                    await connection.execute(
                        """
                        SELECT m.*, e.external_id AS event_external_id,
                               h.resolution_source,
                               h.source_timestamp AS metadata_source_timestamp,
                               h.exchange_timestamp AS metadata_exchange_timestamp,
                               h.observation_timestamp AS metadata_observation_timestamp,
                               h.exchange_timestamp_is_transport AS
                                   metadata_exchange_timestamp_is_transport
                        FROM markets m
                        LEFT JOIN events e ON e.id = m.event_id
                        LEFT JOIN LATERAL (
                            SELECT resolution_source, source_timestamp, exchange_timestamp,
                                   observation_timestamp,
                                   exchange_timestamp_is_transport
                            FROM market_metadata_history
                            WHERE market_id = m.id AND valid_to IS NULL
                            ORDER BY version_number DESC LIMIT 1
                        ) h ON TRUE
                        WHERE m.exchange = %s
                          AND (m.external_id = %s OR m.condition_id = %s OR m.ticker = %s)
                        ORDER BY CASE WHEN m.external_id = %s THEN 0 ELSE 1 END
                        LIMIT 1
                        FOR UPDATE OF m
                        """,
                        (
                            exchange,
                            market_external_id,
                            market_external_id,
                            market_external_id,
                            market_external_id,
                        ),
                    )
                ).fetchone()
                if row is None:
                    return False

                incoming_order = {
                    "source_timestamp": source_timestamp,
                    "exchange_timestamp": exchange_timestamp,
                    "exchange_timestamp_is_transport": False,
                    "observed_at": observed_at,
                }
                if _market_metadata_is_stale(incoming_order, row):
                    LOGGER.info(
                        "Ignored out-of-order market lifecycle state patch",
                        extra={
                            "exchange": exchange,
                            "market": market_external_id,
                            "incoming_timestamp": _market_metadata_upstream_timestamp(
                                incoming_order
                            ),
                            "incoming_observed_at": observed_at,
                            "current_timestamp": (
                                row["metadata_source_timestamp"]
                                or row["metadata_exchange_timestamp"]
                            ),
                            "current_observed_at": row[
                                "metadata_observation_timestamp"
                            ],
                        },
                    )
                    return True

                current_raw = row["raw_data"] if isinstance(row["raw_data"], dict) else {}
                value: dict[str, Any] = {
                    "exchange": row["exchange"],
                    "external_id": row["external_id"],
                    "event_external_id": row["event_external_id"],
                    "ticker": row["ticker"],
                    "slug": row["slug"],
                    "condition_id": row["condition_id"],
                    "question": row["question"],
                    "subtitle": row["subtitle"],
                    "description": row["description"],
                    "rules": row["rules"],
                    "resolution_source": row["resolution_source"],
                    "status": row["status"],
                    "market_type": row["market_type"],
                    "is_active": row["is_active"],
                    "is_tradable": row["is_tradable"],
                    "clob_enabled": row["clob_enabled"],
                    "enable_order_book": row["enable_order_book"],
                    "accepting_orders": row["accepting_orders"],
                    "negative_risk": row["negative_risk"],
                    "open_time": row["open_time"],
                    "close_time": row["close_time"],
                    "settlement_time": row["settlement_time"],
                    "result": row["result"],
                    "settlement_value": row["settlement_value"],
                    "volume": row["volume"],
                    "volume_24h": row["volume_24h"],
                    "open_interest": row["open_interest"],
                    "liquidity": row["liquidity"],
                    "tick_size": row["tick_size"],
                    "fee_rate": row["fee_rate"],
                    "price_level_structure": row["price_level_structure"],
                    "structural_metadata": row["structural_metadata"],
                    "source_timestamp": source_timestamp,
                    "exchange_timestamp": exchange_timestamp,
                    "exchange_timestamp_is_transport": False,
                    "observed_at": observed_at,
                    "raw_data": {
                        **current_raw,
                        "_latest_lifecycle_event": dict(lifecycle_payload),
                    },
                }
                merged_updates = dict(updates)
                if isinstance(merged_updates.get("structural_metadata"), Mapping):
                    current_structure = value.get("structural_metadata")
                    current_structure = (
                        dict(current_structure)
                        if isinstance(current_structure, Mapping)
                        else {}
                    )
                    merged_updates["structural_metadata"] = {
                        **current_structure,
                        **dict(merged_updates["structural_metadata"]),
                    }
                value.update(merged_updates)
                active = bool(value.get("is_active"))
                tradable = bool(value.get("is_tradable"))
                await connection.execute(
                    """
                    UPDATE markets SET
                        question = %s, subtitle = %s, description = %s, rules = %s,
                        status = %s, is_active = %s, is_tradable = %s,
                        accepting_orders = %s, open_time = %s, close_time = %s,
                        settlement_time = %s, result = %s, settlement_value = %s,
                        tick_size = %s, fee_rate = %s, price_level_structure = %s,
                        structural_metadata = %s, raw_data = %s,
                        last_seen_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE id = %s
                    """,
                    (
                        value.get("question"),
                        value.get("subtitle"),
                        value.get("description"),
                        value.get("rules"),
                        value.get("status") or "unknown",
                        active,
                        tradable,
                        value.get("accepting_orders"),
                        value.get("open_time"),
                        value.get("close_time"),
                        value.get("settlement_time"),
                        value.get("result"),
                        value.get("settlement_value"),
                        value.get("tick_size"),
                        value.get("fee_rate"),
                        _json(value.get("price_level_structure"))
                        if value.get("price_level_structure")
                        else None,
                        _json(value.get("structural_metadata")),
                        _json(value["raw_data"]),
                        row["id"],
                    ),
                )
                history_rows = await self._record_market_metadata_history(
                    connection,
                    market_id=int(row["id"]),
                    value=value,
                )
                await self._record_market_auxiliary(
                    connection,
                    market_id=int(row["id"]),
                    value=value,
                    observed_at=utc_now(),
                )
        await self.metrics.rows("markets")
        if history_rows:
            await self.metrics.rows("market_metadata_history", history_rows)
        return True

    async def _record_market_metadata_history(
        self,
        connection: Any,
        *,
        market_id: int,
        value: Mapping[str, Any],
    ) -> int:
        raw = value.get("raw_data") or {}
        observed_at = parse_timestamp(value.get("observed_at")) or utc_now()
        digest = _market_metadata_digest(value)
        active = bool(value.get("is_active", value.get("status") in {"active", "open"}))
        tradable = bool(value.get("is_tradable", value.get("accepting_orders", active)))
        rows_written = 0
        current = await (
            await connection.execute(
                """
                SELECT id, content_hash
                FROM market_metadata_history
                WHERE market_id = %s AND valid_to IS NULL
                ORDER BY version_number DESC LIMIT 1
                """,
                (market_id,),
            )
        ).fetchone()
        if current is None or current["content_hash"] != digest:
            if current is not None:
                cursor = await connection.execute(
                    "UPDATE market_metadata_history SET valid_to = clock_timestamp() WHERE id = %s",
                    (current["id"],),
                )
                rows_written += max(int(cursor.rowcount or 0), 0)
            cursor = await connection.execute(
                """
                INSERT INTO market_metadata_history
                    (market_id, exchange, version_number, content_hash, valid_from,
                     first_observed_at, last_observed_at, observation_timestamp,
                     source_timestamp, exchange_timestamp,
                     exchange_timestamp_is_transport, status, is_active, is_tradable,
                     open_time, close_time, settlement_time, description, rules,
                     resolution_source, result, settlement_value, volume,
                     volume_24h, open_interest,
                     liquidity, tick_size, fee_rate, price_level_structure,
                     structural_metadata, raw_data)
                SELECT %s, %s, COALESCE(MAX(version_number), 0) + 1, %s,
                       clock_timestamp(), clock_timestamp(), clock_timestamp(), %s, %s,
                       %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s
                FROM market_metadata_history WHERE market_id = %s
                """,
                (
                    market_id,
                    value["exchange"],
                    digest,
                    observed_at,
                    value.get("source_timestamp"),
                    value.get("exchange_timestamp"),
                    bool(value.get("exchange_timestamp_is_transport")),
                    value.get("status"),
                    active,
                    tradable,
                    value.get("open_time"),
                    value.get("close_time"),
                    value.get("settlement_time"),
                    value.get("description"),
                    value.get("rules"),
                    value.get("resolution_source"),
                    value.get("result"),
                    value.get("settlement_value"),
                    value.get("volume"),
                    value.get("volume_24h"),
                    value.get("open_interest"),
                    value.get("liquidity"),
                    value.get("tick_size"),
                    value.get("fee_rate"),
                    _json(value.get("price_level_structure"))
                    if value.get("price_level_structure")
                    else None,
                    _json(value.get("structural_metadata")),
                    _json(raw),
                    market_id,
                ),
            )
            rows_written += max(int(cursor.rowcount or 0), 0)
        else:
            cursor = await connection.execute(
                """
                UPDATE market_metadata_history SET
                    last_observed_at = GREATEST(last_observed_at, clock_timestamp()),
                    observation_timestamp = GREATEST(observation_timestamp, %s),
                    source_timestamp = CASE
                        WHEN %s IS NOT NULL
                         AND (source_timestamp IS NULL OR %s > source_timestamp)
                        THEN %s ELSE source_timestamp
                    END,
                    exchange_timestamp = CASE
                        WHEN NOT %s AND %s IS NOT NULL
                         AND (exchange_timestamp IS NULL
                              OR exchange_timestamp_is_transport
                              OR %s > exchange_timestamp)
                        THEN %s ELSE exchange_timestamp
                    END,
                    exchange_timestamp_is_transport = CASE
                        WHEN NOT %s AND %s IS NOT NULL
                         AND (exchange_timestamp IS NULL
                              OR exchange_timestamp_is_transport
                              OR %s > exchange_timestamp)
                        THEN FALSE ELSE exchange_timestamp_is_transport
                    END
                WHERE id = %s
                """,
                (
                    observed_at,
                    value.get("source_timestamp"),
                    value.get("source_timestamp"),
                    value.get("source_timestamp"),
                    bool(value.get("exchange_timestamp_is_transport")),
                    value.get("exchange_timestamp"),
                    value.get("exchange_timestamp"),
                    value.get("exchange_timestamp"),
                    bool(value.get("exchange_timestamp_is_transport")),
                    value.get("exchange_timestamp"),
                    value.get("exchange_timestamp"),
                    current["id"],
                ),
            )
            rows_written += max(int(cursor.rowcount or 0), 0)
        return rows_written

    async def absent_active_markets(
        self,
        *,
        exchange: str,
        discovered_external_ids: Iterable[str],
    ) -> list[dict[str, Any]]:
        """Return live DB rows absent from a complete exchange discovery pass."""
        discovered = list(dict.fromkeys(discovered_external_ids))
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT external_id, status, raw_data->>'id' AS source_id
                    FROM markets
                    WHERE exchange = %s
                      AND (is_active OR is_tradable)
                      AND NOT (external_id = ANY(%s::TEXT[]))
                    ORDER BY external_id
                    """,
                    (exchange, discovered),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def _record_market_auxiliary(
        self,
        connection: Any,
        *,
        market_id: int,
        value: Mapping[str, Any],
        observed_at: datetime,
        diagnostics: MetadataSyncDiagnostics | None = None,
    ) -> None:
        raw = value.get("raw_data") if isinstance(value.get("raw_data"), dict) else {}
        exchange = str(value["exchange"])
        external_id = str(value["external_id"])

        fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
        if fee_schedule or value.get("fee_rate") is not None:
            fee_payload = {
                "schedule": fee_schedule,
                "fees_enabled": raw.get("feesEnabled"),
                "maker_base_fee": raw.get("makerBaseFee"),
                "taker_base_fee": raw.get("takerBaseFee"),
                "fee_rate": str(value.get("fee_rate")) if value.get("fee_rate") is not None else None,
            }
            digest = content_hash(fee_payload)
            schedule = fee_schedule if isinstance(fee_schedule, dict) else fee_payload
            fee_type = (
                str((schedule or {}).get("type") or "market_fee")
                if isinstance(schedule, dict)
                else "market_fee"
            )
            latest_fee = await (
                await connection.execute(
                    """
                    SELECT id, content_hash, effective_from
                    FROM fee_configuration_history
                    WHERE exchange = %s AND scope_type = 'market'
                      AND scope_external_id = %s AND fee_type = %s
                    ORDER BY effective_from DESC, id DESC LIMIT 1
                    """,
                    (exchange, external_id, fee_type),
                )
            ).fetchone()
            if latest_fee is None or latest_fee["content_hash"] != digest:
                effective_from = observed_at
                if latest_fee is not None:
                    if latest_fee["effective_from"] >= effective_from:
                        effective_from = latest_fee["effective_from"] + timedelta(
                            microseconds=1
                        )
                    await connection.execute(
                        """
                        UPDATE fee_configuration_history
                        SET effective_to = %s
                        WHERE id = %s AND effective_to IS NULL
                        """,
                        (effective_from, latest_fee["id"]),
                    )
                await connection.execute(
                    """
                    INSERT INTO fee_configuration_history
                        (exchange, scope_type, scope_external_id, market_id, fee_type,
                         maker_rate, taker_rate, fee_rate, currency, effective_from,
                         observed_at, content_hash, schedule, raw_data)
                    VALUES (%s, 'market', %s, %s, %s, %s, %s, %s, 'USD',
                            %s, %s, %s, %s, %s)
                    ON CONFLICT
                        (exchange, scope_type, scope_external_id, fee_type,
                         effective_from)
                    DO NOTHING
                    """,
                    (
                        exchange,
                        external_id,
                        market_id,
                        fee_type,
                        (schedule or {}).get("makerRate")
                        if isinstance(schedule, dict)
                        else raw.get("makerBaseFee"),
                        (schedule or {}).get("takerRate")
                        if isinstance(schedule, dict)
                        else raw.get("takerBaseFee"),
                        value.get("fee_rate"),
                        effective_from,
                        observed_at,
                        digest,
                        _json(schedule),
                        _json(fee_payload),
                    ),
                )

        rewards = raw.get("clobRewards") or raw.get("rewards") or []
        if not isinstance(rewards, list):
            rewards = [rewards]
        reward_settings = {
            "rewards": rewards,
            "minimum_size": raw.get("rewardsMinSize"),
            "maximum_spread": raw.get("rewardsMaxSpread"),
            "maker_rebate": raw.get("makerRebate") or raw.get("maker_rebate"),
        }
        if rewards or any(
            reward_settings[key] is not None
            for key in ("minimum_size", "maximum_spread", "maker_rebate")
        ):
            digest = content_hash(reward_settings)
            first_reward = rewards[0] if rewards and isinstance(rewards[0], dict) else {}
            latest_reward = await (
                await connection.execute(
                    """
                    SELECT id, content_hash, effective_from
                    FROM incentive_configuration_history
                    WHERE exchange = %s AND scope_type = 'market'
                      AND scope_external_id = %s
                      AND incentive_type = 'liquidity_reward'
                    ORDER BY effective_from DESC, id DESC LIMIT 1
                    """,
                    (exchange, external_id),
                )
            ).fetchone()
            if latest_reward is None or latest_reward["content_hash"] != digest:
                effective_from = observed_at
                if latest_reward is not None:
                    if latest_reward["effective_from"] >= effective_from:
                        effective_from = latest_reward["effective_from"] + timedelta(
                            microseconds=1
                        )
                    await connection.execute(
                        """
                        UPDATE incentive_configuration_history
                        SET effective_to = %s
                        WHERE id = %s AND effective_to IS NULL
                        """,
                        (effective_from, latest_reward["id"]),
                    )
                await connection.execute(
                    """
                    INSERT INTO incentive_configuration_history
                        (exchange, scope_type, scope_external_id, market_id,
                         incentive_type, maker_rebate_rate, reward_rate,
                         reward_amount, reward_currency, minimum_size,
                         maximum_spread, effective_from, observed_at,
                         content_hash, configuration, raw_data)
                    VALUES (%s, 'market', %s, %s, 'liquidity_reward', %s, %s,
                            %s, 'USDC', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT
                        (exchange, scope_type, scope_external_id, incentive_type,
                         effective_from)
                    DO NOTHING
                    """,
                    (
                        exchange,
                        external_id,
                        market_id,
                        reward_settings["maker_rebate"],
                        first_reward.get("rewardsDailyRate"),
                        first_reward.get("rewardsAmount"),
                        reward_settings["minimum_size"],
                        reward_settings["maximum_spread"],
                        effective_from,
                        observed_at,
                        digest,
                        _json(reward_settings),
                        _json(raw),
                    ),
                )

        event_external_id = value.get("event_external_id")
        group_external_id: str | None = None
        group_type: str | None = None
        if value.get("negative_risk") and event_external_id:
            group_external_id = f"negative-risk:{event_external_id}"
            group_type = "negative_risk"
        elif raw.get("mve_collection_ticker"):
            group_external_id = f"mve:{raw['mve_collection_ticker']}"
            group_type = "multivariate"
        elif raw.get("marketGroup"):
            group_external_id = f"market-group:{raw['marketGroup']}"
            group_type = "exchange_group"
        membership_role = str(raw.get("groupItemTitle") or "member")
        if group_external_id is None:
            await connection.execute(
                """
                UPDATE market_group_members
                SET valid_to = GREATEST(%s, valid_from + INTERVAL '1 microsecond')
                WHERE market_id = %s AND source_market_id IS NULL
                  AND valid_to IS NULL
                """,
                (observed_at, market_id),
            )
        if group_type != "multivariate":
            await connection.execute(
                """
                UPDATE market_group_members
                SET valid_to = GREATEST(%s, valid_from + INTERVAL '1 microsecond')
                WHERE source_market_id = %s AND member_role = 'selected_leg'
                  AND valid_to IS NULL
                """,
                (observed_at, market_id),
            )
            await connection.execute(
                """
                UPDATE market_relationships
                SET valid_to = GREATEST(%s, valid_from + INTERVAL '1 microsecond')
                WHERE exchange = %s AND from_market_id = %s
                  AND relationship_type = 'multivariate_leg'
                  AND valid_to IS NULL
                """,
                (observed_at, exchange, market_id),
            )
        if group_external_id and group_type:
            group_event_external_id = (
                None if group_type == "multivariate" else event_external_id
            )
            group_name = (
                raw.get("mve_collection_ticker")
                if group_type == "multivariate"
                else raw.get("marketGroup") or event_external_id
            )
            group_row = await (
                await connection.execute(
                    """
                    INSERT INTO market_groups
                        (exchange, external_id, event_id, group_type, name,
                         constraint_definition, raw_data)
                    VALUES
                        (%s, %s,
                         (SELECT id FROM events WHERE exchange = %s AND external_id = %s),
                         %s, %s, %s, %s)
                    ON CONFLICT (exchange, external_id) WHERE external_id IS NOT NULL DO UPDATE SET
                        event_id = COALESCE(EXCLUDED.event_id, market_groups.event_id),
                        name = EXCLUDED.name,
                        constraint_definition = EXCLUDED.constraint_definition,
                        raw_data = EXCLUDED.raw_data,
                        last_seen_at = clock_timestamp(), updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    (
                        exchange,
                        group_external_id,
                        exchange,
                        group_event_external_id,
                        group_type,
                        group_name or group_external_id,
                        _json(
                            {
                                "negRiskOther": raw.get("negRiskOther"),
                                "mve_selected_legs": raw.get("mve_selected_legs"),
                            }
                        ),
                        _json(raw),
                    ),
                )
            ).fetchone()
            if group_row:
                await connection.execute(
                    """
                    UPDATE market_group_members
                    SET valid_to = GREATEST(%s, valid_from + INTERVAL '1 microsecond')
                    WHERE market_id = %s AND source_market_id IS NULL
                      AND valid_to IS NULL
                      AND NOT (group_id = %s AND member_role = %s)
                    """,
                    (observed_at, market_id, group_row["id"], membership_role),
                )
                await connection.execute(
                    """
                    INSERT INTO market_group_members
                        (group_id, source_market_id, market_id, member_role,
                         valid_from, raw_data)
                    SELECT %s, NULL, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM market_group_members
                        WHERE group_id = %s AND source_market_id IS NULL
                          AND market_id = %s AND outcome_id IS NULL
                          AND member_role = %s AND valid_to IS NULL
                    )
                    """,
                    (
                        group_row["id"],
                        market_id,
                        membership_role,
                        observed_at,
                        _json(raw),
                        group_row["id"],
                        market_id,
                        membership_role,
                    ),
                )
                if raw.get("mve_collection_ticker"):
                    await self._record_multivariate_legs(
                        connection,
                        group_id=int(group_row["id"]),
                        market_id=market_id,
                        market_external_id=external_id,
                        exchange=exchange,
                        observed_at=observed_at,
                        raw=raw,
                        diagnostics=diagnostics,
                    )

    async def _record_multivariate_legs(
        self,
        connection: Any,
        *,
        group_id: int,
        market_id: int,
        market_external_id: str,
        exchange: str,
        observed_at: datetime,
        raw: Mapping[str, Any],
        diagnostics: MetadataSyncDiagnostics | None = None,
    ) -> None:
        legs = raw.get("mve_selected_legs")
        if not isinstance(legs, list):
            return
        resolved: list[tuple[int, int | None, dict[str, Any], dict[str, Any]]] = []
        complete = True
        for position, leg in enumerate(legs):
            if not isinstance(leg, Mapping):
                complete = False
                LOGGER.warning(
                    "Skipping malformed Kalshi multivariate leg",
                    extra={"market": market_external_id, "position": position},
                )
                continue
            leg_ticker = str(leg.get("market_ticker") or "")
            if not leg_ticker:
                complete = False
                LOGGER.warning(
                    "Skipping Kalshi multivariate leg without market_ticker",
                    extra={"market": market_external_id, "position": position},
                )
                continue
            side = str(leg.get("side") or "").lower()
            outcome_external_id = (
                f"{leg_ticker}:{side}" if side in {"yes", "no"} else None
            )
            target = await (
                await connection.execute(
                    """
                    SELECT m.id AS market_id, o.id AS outcome_id
                    FROM markets m
                    LEFT JOIN outcomes o
                      ON o.market_id = m.id AND o.external_id = %s
                    WHERE m.exchange = %s AND m.external_id = %s
                    LIMIT 1
                    """,
                    (outcome_external_id, exchange, leg_ticker),
                )
            ).fetchone()
            if target is None:
                complete = False
                if diagnostics is not None:
                    diagnostics.unresolved_multivariate_leg_markets += 1
                LOGGER.debug(
                    "Kalshi multivariate leg market is not available yet",
                    extra={
                        "market": market_external_id,
                        "leg_market": leg_ticker,
                        "position": position,
                    },
                )
                continue
            target_market_id = int(target["market_id"])
            if target_market_id == market_id:
                complete = False
                LOGGER.warning(
                    "Skipping self-referential Kalshi multivariate leg",
                    extra={"market": market_external_id, "position": position},
                )
                continue
            target_outcome_id = (
                int(target["outcome_id"])
                if target.get("outcome_id") is not None
                else None
            )
            if outcome_external_id and target_outcome_id is None:
                complete = False
                if diagnostics is not None:
                    diagnostics.unresolved_multivariate_leg_outcomes += 1
                LOGGER.debug(
                    "Kalshi multivariate leg outcome is not available yet",
                    extra={
                        "market": market_external_id,
                        "leg_market": leg_ticker,
                        "leg_outcome": outcome_external_id,
                        "position": position,
                    },
                )
                continue
            constraint = {
                "position": position,
                "collection_ticker": raw.get("mve_collection_ticker"),
                "event_ticker": leg.get("event_ticker"),
                "market_ticker": leg_ticker,
                "side": side or None,
                "yes_settlement_value_dollars": leg.get(
                    "yes_settlement_value_dollars"
                ),
            }
            resolved.append(
                (target_market_id, target_outcome_id, constraint, dict(leg))
            )

        for target_market_id, target_outcome_id, constraint, leg in resolved:
            await connection.execute(
                """
                INSERT INTO market_group_members
                    (group_id, source_market_id, market_id, outcome_id,
                     member_role, valid_from, raw_data)
                SELECT %s, %s, %s, %s, 'selected_leg', %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM market_group_members
                    WHERE group_id = %s AND source_market_id = %s
                      AND market_id = %s AND outcome_id IS NOT DISTINCT FROM %s
                      AND member_role = 'selected_leg' AND valid_to IS NULL
                )
                """,
                (
                    group_id,
                    market_id,
                    target_market_id,
                    target_outcome_id,
                    observed_at,
                    _json(leg),
                    group_id,
                    market_id,
                    target_market_id,
                    target_outcome_id,
                ),
            )
            await connection.execute(
                """
                INSERT INTO market_relationships
                    (exchange, from_market_id, to_market_id, to_outcome_id,
                     relationship_type, is_directional, valid_from,
                     constraint_definition, raw_data)
                SELECT %s, %s, %s, %s, 'multivariate_leg', TRUE, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM market_relationships
                    WHERE exchange = %s AND from_market_id = %s
                      AND from_outcome_id IS NULL AND to_market_id = %s
                      AND to_outcome_id IS NOT DISTINCT FROM %s
                      AND relationship_type = 'multivariate_leg'
                      AND constraint_definition = %s AND valid_to IS NULL
                )
                """,
                (
                    exchange,
                    market_id,
                    target_market_id,
                    target_outcome_id,
                    observed_at,
                    _json(constraint),
                    _json(leg),
                    exchange,
                    market_id,
                    target_market_id,
                    target_outcome_id,
                    _json(constraint),
                ),
            )

        # Do not retire prior links when the authoritative payload could not be
        # fully resolved yet; metadata ordering can temporarily hide a leg.
        if not complete:
            return
        desired_members = {
            (target_market_id, target_outcome_id)
            for target_market_id, target_outcome_id, _, _ in resolved
        }
        member_rows = await (
            await connection.execute(
                """
                SELECT id, market_id, outcome_id, valid_from
                FROM market_group_members
                WHERE group_id = %s AND source_market_id = %s
                  AND member_role = 'selected_leg' AND valid_to IS NULL
                FOR UPDATE
                """,
                (group_id, market_id),
            )
        ).fetchall()
        for member in member_rows:
            identity = (int(member["market_id"]), member.get("outcome_id"))
            if identity in desired_members:
                continue
            close_at = max(
                observed_at,
                member["valid_from"] + timedelta(microseconds=1),
            )
            await connection.execute(
                "UPDATE market_group_members SET valid_to = %s WHERE id = %s",
                (close_at, member["id"]),
            )

        desired_relationships = {
            (target_market_id, target_outcome_id, content_hash(constraint))
            for target_market_id, target_outcome_id, constraint, _ in resolved
        }
        relationship_rows = await (
            await connection.execute(
                """
                SELECT id, to_market_id, to_outcome_id, constraint_definition,
                       valid_from
                FROM market_relationships
                WHERE exchange = %s AND from_market_id = %s
                  AND relationship_type = 'multivariate_leg'
                  AND valid_to IS NULL
                FOR UPDATE
                """,
                (exchange, market_id),
            )
        ).fetchall()
        for relationship in relationship_rows:
            definition = relationship.get("constraint_definition")
            definition = definition if isinstance(definition, Mapping) else {}
            identity = (
                int(relationship["to_market_id"]),
                relationship.get("to_outcome_id"),
                content_hash(definition),
            )
            if identity in desired_relationships:
                continue
            close_at = max(
                observed_at,
                relationship["valid_from"] + timedelta(microseconds=1),
            )
            await connection.execute(
                "UPDATE market_relationships SET valid_to = %s WHERE id = %s",
                (close_at, relationship["id"]),
            )

    async def record_fee_configuration(
        self,
        *,
        exchange: str,
        scope_type: str,
        scope_external_id: str,
        fee_type: str,
        effective_from: datetime,
        observed_at: datetime,
        configuration: Mapping[str, Any],
        semantic_configuration: Mapping[str, Any] | None = None,
        source_timestamp: datetime | None = None,
        effective_to: datetime | None = None,
        maker_rate: Any = None,
        taker_rate: Any = None,
        fee_rate: Any = None,
        multiplier: Any = None,
        fixed_fee: Any = None,
        currency: str | None = "USD",
        version_current: bool = False,
    ) -> bool:
        """Append a deduplicated series/event/market fee configuration."""
        if scope_type not in {"global", "series", "event", "market"}:
            raise ValueError(f"unsupported fee scope {scope_type!r}")
        digest = _fee_configuration_digest(
            configuration=configuration,
            semantic_configuration=semantic_configuration,
            maker_rate=maker_rate,
            taker_rate=taker_rate,
            fee_rate=fee_rate,
            multiplier=multiplier,
            fixed_fee=fixed_fee,
            currency=currency,
        )
        changed = False
        stitched_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await _lock_configuration_stream(
                    connection,
                    kind="fee",
                    exchange=exchange,
                    scope_type=scope_type,
                    scope_external_id=scope_external_id,
                    configuration_type=fee_type,
                )
                latest = await (
                    await connection.execute(
                        """
                        SELECT id, content_hash, effective_from,
                               declared_effective_to
                        FROM fee_configuration_history
                        WHERE exchange = %s AND scope_type = %s
                          AND scope_external_id = %s AND fee_type = %s
                        ORDER BY effective_from DESC, id DESC LIMIT 1
                        """,
                        (exchange, scope_type, scope_external_id, fee_type),
                    )
                ).fetchone()
                if version_current and latest is not None and latest["content_hash"] == digest:
                    await connection.execute(
                        """
                        UPDATE fee_configuration_history
                        SET observed_at = GREATEST(observed_at, %s),
                            source_timestamp = GREATEST(source_timestamp, %s)
                        WHERE id = %s
                        """,
                        (observed_at, source_timestamp, latest["id"]),
                    )
                else:
                    exact = await (
                        await connection.execute(
                            """
                            SELECT id, content_hash, declared_effective_to
                            FROM fee_configuration_history
                            WHERE exchange = %s AND scope_type = %s
                              AND scope_external_id = %s AND fee_type = %s
                              AND effective_from = %s
                            """,
                            (
                                exchange,
                                scope_type,
                                scope_external_id,
                                fee_type,
                                effective_from,
                            ),
                        )
                    ).fetchone()
                    changed = exact is None or (
                        exact["content_hash"] != digest
                        or exact["declared_effective_to"] != effective_to
                    )
                    if changed:
                        await connection.execute(
                            """
                            INSERT INTO fee_configuration_history
                                (exchange, scope_type, scope_external_id,
                                 series_id, event_id, market_id, fee_type,
                                 maker_rate, taker_rate, fee_rate, multiplier, fixed_fee,
                                 currency, effective_from, declared_effective_to,
                                 effective_to, observed_at, source_timestamp,
                                 content_hash, schedule, raw_data)
                            VALUES
                                (%s, %s, %s,
                                 CASE WHEN %s = 'series' THEN
                                     (SELECT id FROM series WHERE exchange = %s AND external_id = %s)
                                 END,
                                 CASE WHEN %s = 'event' THEN
                                     (SELECT id FROM events WHERE exchange = %s AND external_id = %s)
                                 END,
                                 CASE WHEN %s = 'market' THEN
                                     (SELECT id FROM markets WHERE exchange = %s
                                      AND (external_id = %s OR condition_id = %s OR ticker = %s
                                           OR raw_data->>'id' = %s)
                                      ORDER BY CASE WHEN external_id = %s THEN 0 ELSE 1 END
                                      LIMIT 1)
                                 END,
                                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s)
                            ON CONFLICT
                                (exchange, scope_type, scope_external_id, fee_type,
                                 effective_from)
                            DO UPDATE SET
                                series_id = COALESCE(EXCLUDED.series_id,
                                                     fee_configuration_history.series_id),
                                event_id = COALESCE(EXCLUDED.event_id,
                                                    fee_configuration_history.event_id),
                                market_id = COALESCE(EXCLUDED.market_id,
                                                     fee_configuration_history.market_id),
                                maker_rate = EXCLUDED.maker_rate,
                                taker_rate = EXCLUDED.taker_rate,
                                fee_rate = EXCLUDED.fee_rate,
                                multiplier = EXCLUDED.multiplier,
                                fixed_fee = EXCLUDED.fixed_fee,
                                currency = EXCLUDED.currency,
                                declared_effective_to = EXCLUDED.declared_effective_to,
                                effective_to = EXCLUDED.effective_to,
                                observed_at = GREATEST(
                                    fee_configuration_history.observed_at,
                                    EXCLUDED.observed_at
                                ),
                                source_timestamp = GREATEST(
                                    fee_configuration_history.source_timestamp,
                                    EXCLUDED.source_timestamp
                                ),
                                content_hash = EXCLUDED.content_hash,
                                schedule = EXCLUDED.schedule,
                                raw_data = EXCLUDED.raw_data
                            """,
                            (
                                exchange,
                                scope_type,
                                scope_external_id,
                                scope_type,
                                exchange,
                                scope_external_id,
                                scope_type,
                                exchange,
                                scope_external_id,
                                scope_type,
                                exchange,
                                scope_external_id,
                                scope_external_id,
                                scope_external_id,
                                scope_external_id,
                                scope_external_id,
                                fee_type,
                                maker_rate,
                                taker_rate,
                                fee_rate,
                                multiplier,
                                fixed_fee,
                                currency,
                                effective_from,
                                effective_to,
                                effective_to,
                                observed_at,
                                source_timestamp,
                                digest,
                                _json(configuration),
                                _json(configuration),
                            ),
                        )
                    elif exact is not None:
                        await connection.execute(
                            """
                            UPDATE fee_configuration_history
                            SET observed_at = GREATEST(observed_at, %s),
                                source_timestamp = GREATEST(source_timestamp, %s)
                            WHERE id = %s
                            """,
                            (observed_at, source_timestamp, exact["id"]),
                        )
                stitched_rows = await _stitch_configuration_intervals(
                    connection,
                    table="fee_configuration_history",
                    type_column="fee_type",
                    exchange=exchange,
                    scope_type=scope_type,
                    scope_external_id=scope_external_id,
                    configuration_type=fee_type,
                )
        if changed:
            await self.metrics.rows("fee_configuration_history")
        if stitched_rows:
            await self.metrics.rows("fee_configuration_history", stitched_rows)
        return changed

    async def record_incentive_configuration(
        self,
        *,
        exchange: str,
        scope_type: str,
        scope_external_id: str,
        incentive_type: str,
        effective_from: datetime,
        observed_at: datetime,
        configuration: Mapping[str, Any],
        source_timestamp: datetime | None = None,
        effective_to: datetime | None = None,
        maker_rebate_rate: Any = None,
        reward_rate: Any = None,
        reward_amount: Any = None,
        reward_currency: str | None = "USD",
        minimum_size: Any = None,
        maximum_spread: Any = None,
        multiplier: Any = None,
        version_current: bool = False,
    ) -> bool:
        """Append a deduplicated incentive or liquidity-reward configuration."""
        if scope_type not in {"global", "series", "event", "market"}:
            raise ValueError(f"unsupported incentive scope {scope_type!r}")
        digest = content_hash(
            {
                "configuration": configuration,
                "maker_rebate_rate": maker_rebate_rate,
                "reward_rate": reward_rate,
                "reward_amount": reward_amount,
                "reward_currency": reward_currency,
                "minimum_size": minimum_size,
                "maximum_spread": maximum_spread,
                "multiplier": multiplier,
            }
        )
        changed = False
        stitched_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await _lock_configuration_stream(
                    connection,
                    kind="incentive",
                    exchange=exchange,
                    scope_type=scope_type,
                    scope_external_id=scope_external_id,
                    configuration_type=incentive_type,
                )
                latest = await (
                    await connection.execute(
                        """
                        SELECT id, content_hash, effective_from,
                               declared_effective_to
                        FROM incentive_configuration_history
                        WHERE exchange = %s AND scope_type = %s
                          AND scope_external_id = %s AND incentive_type = %s
                        ORDER BY effective_from DESC, id DESC LIMIT 1
                        """,
                        (exchange, scope_type, scope_external_id, incentive_type),
                    )
                ).fetchone()
                if version_current and latest is not None and latest["content_hash"] == digest:
                    await connection.execute(
                        """
                        UPDATE incentive_configuration_history
                        SET observed_at = GREATEST(observed_at, %s),
                            source_timestamp = GREATEST(source_timestamp, %s)
                        WHERE id = %s
                        """,
                        (observed_at, source_timestamp, latest["id"]),
                    )
                else:
                    exact = await (
                        await connection.execute(
                            """
                            SELECT id, content_hash, declared_effective_to
                            FROM incentive_configuration_history
                            WHERE exchange = %s AND scope_type = %s
                              AND scope_external_id = %s AND incentive_type = %s
                              AND effective_from = %s
                            """,
                            (
                                exchange,
                                scope_type,
                                scope_external_id,
                                incentive_type,
                                effective_from,
                            ),
                        )
                    ).fetchone()
                    changed = exact is None or (
                        exact["content_hash"] != digest
                        or exact["declared_effective_to"] != effective_to
                    )
                    if changed:
                        await connection.execute(
                            """
                            INSERT INTO incentive_configuration_history
                                (exchange, scope_type, scope_external_id,
                                 series_id, event_id, market_id, incentive_type,
                                 maker_rebate_rate, reward_rate, reward_amount,
                                 reward_currency, minimum_size, maximum_spread, multiplier,
                                 effective_from, declared_effective_to, effective_to,
                                 observed_at, source_timestamp, content_hash,
                                 configuration, raw_data)
                            VALUES
                                (%s, %s, %s,
                                 CASE WHEN %s = 'series' THEN
                                     (SELECT id FROM series WHERE exchange = %s AND external_id = %s)
                                 END,
                                 CASE WHEN %s = 'event' THEN
                                     (SELECT id FROM events WHERE exchange = %s AND external_id = %s)
                                 END,
                                 CASE WHEN %s = 'market' THEN
                                     (SELECT id FROM markets WHERE exchange = %s
                                      AND (external_id = %s OR condition_id = %s OR ticker = %s
                                           OR raw_data->>'id' = %s)
                                      ORDER BY CASE WHEN external_id = %s THEN 0 ELSE 1 END
                                      LIMIT 1)
                                 END,
                                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s)
                            ON CONFLICT
                                (exchange, scope_type, scope_external_id, incentive_type,
                                 effective_from)
                            DO UPDATE SET
                                series_id = COALESCE(EXCLUDED.series_id,
                                                     incentive_configuration_history.series_id),
                                event_id = COALESCE(EXCLUDED.event_id,
                                                    incentive_configuration_history.event_id),
                                market_id = COALESCE(EXCLUDED.market_id,
                                                     incentive_configuration_history.market_id),
                                maker_rebate_rate = EXCLUDED.maker_rebate_rate,
                                reward_rate = EXCLUDED.reward_rate,
                                reward_amount = EXCLUDED.reward_amount,
                                reward_currency = EXCLUDED.reward_currency,
                                minimum_size = EXCLUDED.minimum_size,
                                maximum_spread = EXCLUDED.maximum_spread,
                                multiplier = EXCLUDED.multiplier,
                                declared_effective_to = EXCLUDED.declared_effective_to,
                                effective_to = EXCLUDED.effective_to,
                                observed_at = GREATEST(
                                    incentive_configuration_history.observed_at,
                                    EXCLUDED.observed_at
                                ),
                                source_timestamp = GREATEST(
                                    incentive_configuration_history.source_timestamp,
                                    EXCLUDED.source_timestamp
                                ),
                                content_hash = EXCLUDED.content_hash,
                                configuration = EXCLUDED.configuration,
                                raw_data = EXCLUDED.raw_data
                            """,
                            (
                                exchange,
                                scope_type,
                                scope_external_id,
                                scope_type,
                                exchange,
                                scope_external_id,
                                scope_type,
                                exchange,
                                scope_external_id,
                                scope_type,
                                exchange,
                                scope_external_id,
                                scope_external_id,
                                scope_external_id,
                                scope_external_id,
                                scope_external_id,
                                incentive_type,
                                maker_rebate_rate,
                                reward_rate,
                                reward_amount,
                                reward_currency,
                                minimum_size,
                                maximum_spread,
                                multiplier,
                                effective_from,
                                effective_to,
                                effective_to,
                                observed_at,
                                source_timestamp,
                                digest,
                                _json(configuration),
                                _json(configuration),
                            ),
                        )
                    elif exact is not None:
                        await connection.execute(
                            """
                            UPDATE incentive_configuration_history
                            SET observed_at = GREATEST(observed_at, %s),
                                source_timestamp = GREATEST(source_timestamp, %s)
                            WHERE id = %s
                            """,
                            (observed_at, source_timestamp, exact["id"]),
                        )
                stitched_rows = await _stitch_configuration_intervals(
                    connection,
                    table="incentive_configuration_history",
                    type_column="incentive_type",
                    exchange=exchange,
                    scope_type=scope_type,
                    scope_external_id=scope_external_id,
                    configuration_type=incentive_type,
                )
        if changed:
            await self.metrics.rows("incentive_configuration_history")
        if stitched_rows:
            await self.metrics.rows("incentive_configuration_history", stitched_rows)
        return changed

    async def upsert_outcome(self, market_id: int, value: Mapping[str, Any]) -> int:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO outcomes
                        (market_id, exchange, external_id, token_id, name, outcome_index,
                         last_price, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, external_id) WHERE external_id IS NOT NULL DO UPDATE SET
                        market_id = EXCLUDED.market_id, token_id = EXCLUDED.token_id,
                        name = EXCLUDED.name, outcome_index = EXCLUDED.outcome_index,
                        last_price = EXCLUDED.last_price, raw_data = EXCLUDED.raw_data,
                        updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    (
                        market_id,
                        value["exchange"],
                        value.get("external_id"),
                        value.get("token_id"),
                        value["name"],
                        value.get("outcome_index"),
                        value.get("last_price"),
                        _json(value.get("raw_data")),
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("outcomes")
        return int(row["id"])

    async def upsert_tag(self, exchange: str, raw: Mapping[str, Any]) -> int:
        external_id = raw.get("id")
        name = str(raw.get("label") or raw.get("name") or raw.get("slug") or external_id or "")
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO tags (exchange, external_id, name, slug, raw_data)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, name) DO UPDATE SET
                        external_id = COALESCE(EXCLUDED.external_id, tags.external_id),
                        slug = EXCLUDED.slug, raw_data = EXCLUDED.raw_data,
                        updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    (exchange, str(external_id) if external_id is not None else None, name, raw.get("slug"), _json(raw)),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("tags")
        return int(row["id"])

    async def store_raw_rest(
        self,
        *,
        exchange: str,
        source: str,
        endpoint: str,
        requested_at: datetime,
        received_at: datetime,
        response_timestamp: datetime | None,
        http_status: int,
        payload: Any,
        parameters: Mapping[str, Any] | None = None,
        entity_type: str | None = None,
        external_key: str | None = None,
    ) -> bool:
        digest = content_hash(payload)
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO raw_rest_payloads
                    (exchange, source, endpoint, entity_type, external_key, requested_at,
                     received_at, response_timestamp, parameters, http_status, content_hash, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT raw_rest_payloads_version_key DO NOTHING
                """,
                (
                    exchange,
                    source,
                    endpoint,
                    entity_type,
                    external_key,
                    requested_at,
                    received_at,
                    response_timestamp,
                    _json(dict(parameters or {})),
                    http_status,
                    digest,
                    _json(payload),
                ),
            )
            inserted = cursor.rowcount > 0
        if inserted:
            await self.metrics.rows("raw_rest_payloads")
        return inserted

    async def checkpoint(
        self,
        exchange: str,
        job: str,
        *,
        checkpoint_key: str = "default",
        cursor: str | None = None,
        timestamp: datetime | None = None,
        last_external_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO collector_checkpoints
                    (exchange, job, checkpoint_key, cursor, checkpoint_timestamp,
                     last_external_id, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (exchange, job, checkpoint_key) DO UPDATE SET
                    cursor = EXCLUDED.cursor,
                    checkpoint_timestamp = EXCLUDED.checkpoint_timestamp,
                    last_external_id = EXCLUDED.last_external_id,
                    metadata = EXCLUDED.metadata,
                    updated_at = clock_timestamp()
                """,
                (
                    exchange,
                    job,
                    checkpoint_key,
                    cursor,
                    timestamp,
                    last_external_id,
                    _json(dict(metadata or {})),
                ),
            )
        if cursor.rowcount:
            await self.metrics.rows(
                "collector_checkpoints", max(int(cursor.rowcount), 0)
            )

    async def live_candidates(self, exchange: str | None = None) -> list[MarketCandidate]:
        params: tuple[Any, ...] = () if exchange is None else (exchange,)
        where = "" if exchange is None else "WHERE m.exchange = %s"
        query = f"""
            SELECT m.exchange, m.external_id, m.ticker, m.status, m.is_active,
                   m.is_tradable, m.volume, m.volume_24h, m.liquidity, m.raw_data,
                   COALESCE(array_agg(o.token_id) FILTER (WHERE o.token_id IS NOT NULL), ARRAY[]::TEXT[]) AS token_ids
            FROM markets m
            LEFT JOIN outcomes o ON o.market_id = m.id
            {where}
            GROUP BY m.id
            ORDER BY m.exchange, m.external_id
        """
        async with self.pool.connection() as connection:
            rows = await (await connection.execute(query, params)).fetchall()
        return [
            MarketCandidate(
                exchange=row["exchange"],
                external_id=row["external_id"],
                ticker=row["ticker"],
                status=row["status"],
                active=row["is_active"],
                tradable=row["is_tradable"],
                volume=row["volume"],
                volume_24h=row["volume_24h"],
                liquidity=row["liquidity"],
                outcome_token_ids=tuple(row["token_ids"]),
                raw_data=row["raw_data"],
            )
            for row in rows
        ]

    async def create_connection(
        self,
        *,
        run_id: int | None,
        exchange: str,
        channel: str,
        endpoint: str,
        reconnect_attempt: int,
        subscribed_market_count: int,
        subscribed_market_external_ids: Iterable[str] = (),
        pending_subscription: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> int:
        market_external_ids = list(dict.fromkeys(subscribed_market_external_ids))
        async with self.pool.connection() as connection:
            async with connection.transaction():
                row = await (
                    await connection.execute(
                        """
                        INSERT INTO collector_connections
                            (connection_uuid, collector_run_id, exchange, channel, endpoint,
                             status, connected_at, reconnect_attempt, subscribed_market_count, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, clock_timestamp(), %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            uuid.uuid4(),
                            run_id,
                            exchange,
                            channel,
                            endpoint,
                            "connecting" if pending_subscription else "connected",
                            reconnect_attempt,
                            subscribed_market_count,
                            _json(dict(metadata or {})),
                        ),
                    )
                ).fetchone()
                assert row is not None
                connection_id = int(row["id"])
                for external_id in (() if pending_subscription else market_external_ids):
                    await connection.execute(
                        """
                        INSERT INTO collector_connection_markets
                            (connection_id, exchange, channel, market_id,
                             market_external_id, subscription_payload)
                        VALUES
                            (%s, %s, %s,
                             (SELECT id FROM markets
                              WHERE exchange = %s
                                AND (external_id = %s OR condition_id = %s OR ticker = %s)
                              ORDER BY CASE WHEN external_id = %s THEN 0 ELSE 1 END
                              LIMIT 1),
                             %s, %s)
                        """,
                        (
                            connection_id,
                            exchange,
                            channel,
                            exchange,
                            external_id,
                            external_id,
                            external_id,
                            external_id,
                            external_id,
                            _json(dict(metadata or {})),
                        ),
                    )
        await self.metrics.rows("collector_connections")
        if market_external_ids and not pending_subscription:
            await self.metrics.rows(
                "collector_connection_markets", len(market_external_ids)
            )
        return connection_id

    async def confirm_subscription(
        self,
        connection_id: int,
        *,
        exchange: str,
        channel: str,
        market_external_ids: Iterable[str],
        acknowledgement: Mapping[str, Any] | None = None,
    ) -> int:
        """Mark an exchange-acknowledged subscription and retain exact markets."""
        identifiers = list(dict.fromkeys(market_external_ids))
        connection_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    UPDATE collector_connections
                    SET status = 'connected',
                        metadata = metadata || %s,
                        updated_at = clock_timestamp()
                    WHERE id = %s
                    """,
                    (_json({"subscription_ack": dict(acknowledgement or {})}), connection_id),
                )
                connection_rows = max(int(cursor.rowcount or 0), 0)
                inserted = 0
                for external_id in identifiers:
                    cursor = await connection.execute(
                        """
                        INSERT INTO collector_connection_markets
                            (connection_id, exchange, channel, market_id,
                             market_external_id, subscription_payload)
                        VALUES
                            (%s, %s, %s,
                             (SELECT id FROM markets
                              WHERE exchange = %s
                                AND (external_id = %s OR condition_id = %s OR ticker = %s)
                              ORDER BY CASE WHEN external_id = %s THEN 0 ELSE 1 END
                              LIMIT 1),
                             %s, %s)
                        """,
                        (
                            connection_id,
                            exchange,
                            channel,
                            exchange,
                            external_id,
                            external_id,
                            external_id,
                            external_id,
                            external_id,
                            _json(dict(acknowledgement or {})),
                        ),
                    )
                    inserted += max(cursor.rowcount, 0)
        if connection_rows:
            await self.metrics.rows("collector_connections", connection_rows)
        if inserted:
            await self.metrics.rows("collector_connection_markets", inserted)
        return inserted

    async def close_connection(
        self,
        connection_id: int,
        *,
        reason: str | None,
        failed: bool = False,
    ) -> None:
        connection_rows = 0
        membership_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    UPDATE collector_connections
                    SET status = %s, disconnected_at = clock_timestamp(),
                        disconnect_reason = %s, updated_at = clock_timestamp()
                    WHERE id = %s
                    """,
                    ("failed" if failed else "closed", reason, connection_id),
                )
                connection_rows = max(int(cursor.rowcount or 0), 0)
                cursor = await connection.execute(
                    """
                    UPDATE collector_connection_markets
                    SET unsubscribed_at = clock_timestamp(), unsubscribe_reason = %s
                    WHERE connection_id = %s AND unsubscribed_at IS NULL
                    """,
                    (reason, connection_id),
                )
                membership_rows = max(int(cursor.rowcount or 0), 0)
        if connection_rows:
            await self.metrics.rows("collector_connections", connection_rows)
        if membership_rows:
            await self.metrics.rows("collector_connection_markets", membership_rows)

    async def active_subscribed_market_count(self, run_id: int | None) -> int:
        """Count exchange-confirmed, currently open market subscriptions."""
        if run_id is None:
            return 0
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    SELECT COUNT(DISTINCT (cc.exchange, ccm.market_external_id)) AS count
                    FROM collector_connections cc
                    JOIN collector_connection_markets ccm ON ccm.connection_id = cc.id
                    WHERE cc.collector_run_id = %s
                      AND cc.status = 'connected'
                      AND cc.disconnected_at IS NULL
                      AND ccm.unsubscribed_at IS NULL
                    """,
                    (run_id,),
                )
            ).fetchone()
        return int(row["count"] if row else 0)

    async def active_subscribed_market_ids(
        self, run_id: int | None
    ) -> set[tuple[str, str]]:
        """Return exact exchange-confirmed memberships for the current run."""
        if run_id is None:
            return set()
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT DISTINCT cc.exchange, ccm.market_external_id
                    FROM collector_connections cc
                    JOIN collector_connection_markets ccm
                      ON ccm.connection_id = cc.id
                    WHERE cc.collector_run_id = %s
                      AND cc.status = 'connected'
                      AND cc.disconnected_at IS NULL
                      AND ccm.unsubscribed_at IS NULL
                    """,
                    (run_id,),
                )
            ).fetchall()
        return {
            (str(row["exchange"]), str(row["market_external_id"]))
            for row in rows
        }

    async def update_connection_stats(
        self,
        connection_id: int,
        *,
        messages_received: int,
        messages_dropped: int = 0,
        first_sequence: int | None = None,
        last_sequence: int | None = None,
        first_message_at: datetime | None = None,
        last_message_at: datetime | None = None,
    ) -> None:
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE collector_connections
                SET messages_received = %s, messages_dropped = %s,
                    first_sequence = COALESCE(first_sequence, %s),
                    last_sequence = COALESCE(%s, last_sequence),
                    first_message_at = COALESCE(first_message_at, %s),
                    last_message_at = COALESCE(%s, last_message_at),
                    updated_at = clock_timestamp()
                WHERE id = %s
                """,
                (
                    messages_received,
                    messages_dropped,
                    first_sequence,
                    last_sequence,
                    first_message_at,
                    last_message_at,
                    connection_id,
                ),
            )
        if cursor.rowcount:
            await self.metrics.rows(
                "collector_connections", max(int(cursor.rowcount), 0)
            )

    async def record_gap(
        self,
        *,
        run_id: int | None,
        connection_id: int | None,
        exchange: str,
        channel: str,
        market_external_id: str | None,
        outcome_external_id: str | None,
        gap_type: str,
        last_sequence: int | None = None,
        expected_sequence: int | None = None,
        actual_sequence: int | None = None,
        reconnect_reason: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> int:
        is_forward_gap = (
            expected_sequence is not None
            and actual_sequence is not None
            and actual_sequence > expected_sequence
        )
        missing_start = expected_sequence if is_forward_gap else None
        missing_end = actual_sequence - 1 if is_forward_gap and actual_sequence is not None else None
        messages_missing = (
            actual_sequence - expected_sequence
            if is_forward_gap
            and actual_sequence is not None
            and expected_sequence is not None
            else None
        )
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO data_gaps
                        (collector_run_id, collector_connection_id, exchange, channel,
                         market_id, outcome_id, gap_type, status, detected_at,
                         gap_started_at,
                         last_sequence_before_gap, expected_sequence, first_sequence_after_gap,
                         missing_sequence_start, missing_sequence_end, messages_missing,
                         reconnect_reason, details)
                    SELECT %s, %s, %s, %s, m.id, o.id, %s, 'open', clock_timestamp(),
                           clock_timestamp(),
                           %s, %s, %s, %s, %s, %s, %s, %s
                    FROM (SELECT 1) seed
                    LEFT JOIN markets m
                      ON m.exchange = %s
                     AND (m.external_id = %s OR m.condition_id = %s OR m.ticker = %s)
                    LEFT JOIN outcomes o
                      ON o.market_id = m.id
                     AND (o.external_id = %s OR o.token_id = %s)
                    ORDER BY m.id NULLS LAST LIMIT 1
                    RETURNING id
                    """,
                    (
                        run_id,
                        connection_id,
                        exchange,
                        channel,
                        gap_type,
                        last_sequence,
                        expected_sequence,
                        actual_sequence,
                        missing_start,
                        missing_end,
                        messages_missing,
                        reconnect_reason,
                        _json(dict(details or {})),
                        exchange,
                        market_external_id,
                        market_external_id,
                        market_external_id,
                        outcome_external_id,
                        outcome_external_id,
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("data_gaps")
        return int(row["id"])

    async def resolve_gap(
        self, gap_id: int, *, action: str, recovery_snapshot_id: int | None = None
    ) -> None:
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE data_gaps
                SET status = 'resolved', resolved_at = clock_timestamp(),
                    gap_ended_at = clock_timestamp(),
                    resolution_action = %s, recovery_snapshot_id = %s,
                    updated_at = clock_timestamp()
                WHERE id = %s
                """,
                (action, recovery_snapshot_id, gap_id),
            )
        if cursor.rowcount:
            await self.metrics.rows("data_gaps", max(int(cursor.rowcount), 0))

    async def record_live_selection(
        self,
        run_id: int | None,
        markets: Iterable[MarketCandidate],
        subscribed_ids: set[tuple[str, str]],
        reasons: Mapping[tuple[str, str], str],
    ) -> None:
        config = {
            "max_live_markets": self.settings.max_live_markets,
            "min_live_market_volume": str(self.settings.min_live_market_volume),
            "min_live_market_liquidity": str(self.settings.min_live_market_liquidity),
            "allowlist": sorted(self.settings.live_market_allowlist),
            "blocklist": sorted(self.settings.live_market_blocklist),
        }
        market_list = list(markets)
        eligible = {
            (market.exchange, market.external_id)
            for market in market_list
            if market.active
            and market.tradable
            and reasons.get((market.exchange, market.external_id))
            in {None, "max_live_markets_cap"}
        }
        ranked = sorted(
            (market for market in market_list if (market.exchange, market.external_id) in eligible),
            key=lambda market: (
                -(market.liquidity or Decimal("0")),
                -(market.volume_24h or Decimal("0")),
                -(market.volume or Decimal("0")),
                market.exchange,
                market.external_id,
            ),
        )
        rank_positions = {
            (market.exchange, market.external_id): position
            for position, market in enumerate(ranked, 1)
        }
        inserted = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                for market in market_list:
                    key = (market.exchange, market.external_id)
                    subscribed = key in subscribed_ids
                    cursor = await connection.execute(
                        """
                        INSERT INTO live_market_subscription_decisions
                            (collector_run_id, exchange, market_id, market_external_id,
                             is_active, is_tradable, is_eligible, is_subscribed,
                             exclusion_reason, ranking_position, observed_volume,
                             observed_liquidity, config_snapshot)
                        VALUES
                            (%s, %s,
                             (SELECT id FROM markets WHERE exchange = %s AND external_id = %s),
                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            run_id,
                            market.exchange,
                            market.exchange,
                            market.external_id,
                            market.external_id,
                            market.active,
                            market.tradable,
                            key in eligible,
                            subscribed,
                            reasons.get(key),
                            rank_positions.get(key),
                            market.volume,
                            market.liquidity,
                            _json(config),
                        ),
                    )
                    inserted += max(int(cursor.rowcount or 0), 0)
        if inserted:
            await self.metrics.rows("live_market_subscription_decisions", inserted)

    async def record_metrics(
        self,
        *,
        run_id: int | None,
        interval_start: datetime,
        interval_seconds: int,
        snapshot: Mapping[str, Any],
        coverage: Mapping[str, int] | None = None,
    ) -> None:
        coverage = coverage or {}
        message_rates = snapshot.get("websocket_messages_per_minute", {})
        row_rates = snapshot.get("database_rows_per_minute", {})
        websocket_messages = int(sum(message_rates.values()) * interval_seconds / 60)
        database_rows = int(sum(row_rates.values()) * interval_seconds / 60)
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO collector_metrics
                    (collector_run_id, interval_start, interval_seconds,
                     websocket_messages, database_rows_written,
                     markets_discovered, markets_active, markets_tradable,
                     markets_subscribed, markets_excluded, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    interval_start,
                    interval_seconds,
                    websocket_messages,
                    database_rows,
                    coverage.get("discovered", 0),
                    coverage.get("active", 0),
                    coverage.get("tradable", 0),
                    coverage.get("subscribed", 0),
                    coverage.get("excluded", 0),
                    _json(dict(snapshot)),
                ),
            )
        if cursor.rowcount:
            await self.metrics.rows("collector_metrics", max(int(cursor.rowcount), 0))

    async def database_row_write_deltas(self) -> dict[str, int]:
        """Return committed insert/update/delete deltas from PostgreSQL stats.

        Application counters remain useful for mapping writes to logical item
        kinds, but server table statistics also capture direct metadata,
        coverage, relationship, and control-row writes that do not pass through
        the batch writer.
        """
        async with self._database_write_baseline_lock:
            async with self.pool.connection() as connection:
                rows = await (
                    await connection.execute(
                        """
                        SELECT relname,
                               n_tup_ins + n_tup_upd + n_tup_del AS row_writes
                        FROM pg_stat_user_tables
                        WHERE schemaname = current_schema()
                        ORDER BY relname
                        """
                    )
                ).fetchall()
            current = {
                str(row["relname"]): int(row["row_writes"] or 0)
                for row in rows
            }
            previous = self._database_write_baseline
            self._database_write_baseline = current
            if previous is None:
                return {}
            return {
                table: value - previous.get(table, 0)
                for table, value in current.items()
                if value > previous.get(table, 0)
            }

    async def write_items(self, items: list["WriteItem"]) -> dict[str, int]:
        if not items:
            return {}
        counts: defaultdict[str, int] = defaultdict(int)
        connection_counts: defaultdict[int, int] = defaultdict(int)
        connection_stat_rows = 0
        async with self.pool.connection() as connection:
            async with connection.transaction():
                for item in items:
                    row_count = await self._write_item(connection, item.kind, item.data)
                    counts[item.kind] += row_count
                    connection_id = item.data.get("connection_id")
                    if row_count and connection_id is not None:
                        connection_counts[int(connection_id)] += row_count
                for connection_id, row_count in connection_counts.items():
                    cursor = await connection.execute(
                        """
                        UPDATE collector_connections
                        SET rows_written = rows_written + %s,
                            updated_at = clock_timestamp()
                        WHERE id = %s
                        """,
                        (row_count, connection_id),
                    )
                    connection_stat_rows += max(int(cursor.rowcount or 0), 0)
        for table, count in counts.items():
            if count:
                await self.metrics.rows(table, count)
        if connection_stat_rows:
            await self.metrics.rows("collector_connections", connection_stat_rows)
        return dict(counts)

    async def record_write_failure(
        self, item: "WriteItem", error: BaseException, *, run_id: int | None = None
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO collector_write_failures
                    (collector_run_id, item_kind, error_type, error_message, payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    item.kind,
                    type(error).__name__,
                    str(error)[:8000],
                    _json(json.loads(canonical_json(item.data))),
                ),
            )
        await self.metrics.rows("collector_write_failures")

    async def _write_item(self, connection: Any, kind: str, data: Mapping[str, Any]) -> int:
        query, params = _write_query(kind, data)
        cursor = await connection.execute(query, params)
        return max(cursor.rowcount, 0)

    async def status(self) -> dict[str, Any]:
        tables = [
            "series",
            "events",
            "markets",
            "outcomes",
            "trades",
            "orderbook_snapshots",
            "orderbook_updates",
            "market_snapshots",
            "raw_ws_messages",
            "reference_price_updates",
            "data_gaps",
            "collector_write_failures",
        ]
        result: dict[str, Any] = {"database_connected": await self.ping(), "counts": {}}
        async with self.pool.connection() as connection:
            for table in tables:
                row = await (await connection.execute(f"SELECT count(*) AS count FROM {table}")).fetchone()
                result["counts"][table] = row["count"] if row else 0
            latest = await (
                await connection.execute(
                    """
                    SELECT
                        (SELECT max(executed_at) FROM trades) AS latest_trade,
                        (SELECT max(received_at) FROM raw_ws_messages) AS latest_ws_message,
                        (SELECT max(finished_at) FROM collector_runs WHERE status = 'completed') AS latest_successful_run
                    """
                )
            ).fetchone()
            result["latest"] = dict(latest or {})
            checkpoints = await (
                await connection.execute(
                    """
                    SELECT exchange, job, checkpoint_key, cursor, checkpoint_timestamp,
                           last_external_id, updated_at
                    FROM collector_checkpoints ORDER BY updated_at DESC LIMIT 20
                    """
                )
            ).fetchall()
            result["checkpoints"] = [dict(row) for row in checkpoints]
        return result


def _lookup_prefix() -> str:
    return """
        WITH resolved AS (
            SELECT m.id AS market_id, o.id AS outcome_id
            FROM markets m
            LEFT JOIN outcomes o
              ON o.market_id = m.id
             AND (%(outcome_external_id)s IS NOT NULL)
             AND (o.external_id = %(outcome_external_id)s OR o.token_id = %(outcome_external_id)s)
            WHERE m.exchange = %(exchange)s AND
                  (m.external_id = %(market_external_id)s OR m.condition_id = %(market_external_id)s OR m.ticker = %(market_external_id)s)
            ORDER BY CASE WHEN m.external_id = %(market_external_id)s THEN 0 ELSE 1 END
            LIMIT 1
        )
    """


def _write_query(kind: str, value: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    data = dict(value)
    data.setdefault("source_timestamp_raw", None)
    data.setdefault("exchange_timestamp_raw", None)
    for key in ("raw_data", "payload", "details", "state"):
        if key in data:
            data[key] = _json(data[key])
    prefix = _lookup_prefix()
    if kind == "trades":
        return (
            prefix
            + """
            INSERT INTO trades
                (exchange, external_trade_id, dedup_hash, market_id, outcome_id,
                 collector_connection_id, executed_at, source_timestamp, exchange_timestamp,
                 source_timestamp_raw, exchange_timestamp_raw,
                 received_at, received_monotonic_ns, sequence_number, price, size, side,
                 transaction_hash, raw_data)
            SELECT %(exchange)s, %(external_trade_id)s, %(dedup_hash)s, market_id, outcome_id,
                   %(connection_id)s, %(executed_at)s, %(source_timestamp)s,
                   %(exchange_timestamp)s, %(source_timestamp_raw)s,
                   %(exchange_timestamp_raw)s, %(received_at)s,
                   %(received_monotonic_ns)s,
                   %(sequence_number)s, %(price)s, %(size)s, %(side)s,
                   %(transaction_hash)s, %(raw_data)s
            FROM resolved
            ON CONFLICT (exchange, dedup_hash) DO NOTHING
            """,
            data,
        )
    if kind == "orderbook_snapshots":
        data["bids"] = _json(data.get("bids", []))
        data["asks"] = _json(data.get("asks", []))
        return (
            prefix
            + """
            INSERT INTO orderbook_snapshots
                (exchange, market_id, outcome_id, collector_connection_id, snapshot_type,
                 source_timestamp, exchange_timestamp, source_timestamp_raw,
                 exchange_timestamp_raw, received_at, received_monotonic_ns,
                 sequence_number, book_hash, bids, asks, best_bid, best_ask,
                 is_reconciliation, raw_data)
            SELECT %(exchange)s, market_id, outcome_id, %(connection_id)s,
                   %(snapshot_type)s, %(source_timestamp)s, %(exchange_timestamp)s,
                   %(source_timestamp_raw)s, %(exchange_timestamp_raw)s,
                   %(received_at)s, %(received_monotonic_ns)s, %(sequence_number)s,
                   %(book_hash)s, %(bids)s, %(asks)s, %(best_bid)s, %(best_ask)s,
                   %(is_reconciliation)s, %(raw_data)s
            FROM resolved
            """,
            data,
        )
    if kind == "orderbook_updates":
        return (
            prefix
            + """
            INSERT INTO orderbook_updates
                (exchange, market_id, outcome_id, collector_connection_id,
                 source_timestamp, exchange_timestamp, source_timestamp_raw,
                 exchange_timestamp_raw, received_at, received_monotonic_ns,
                 sequence_number, book_hash, side, price, size, size_delta,
                 operation, event_type, raw_data)
            SELECT %(exchange)s, market_id, outcome_id, %(connection_id)s,
                   %(source_timestamp)s, %(exchange_timestamp)s,
                   %(source_timestamp_raw)s, %(exchange_timestamp_raw)s,
                   %(received_at)s, %(received_monotonic_ns)s,
                   %(sequence_number)s, %(book_hash)s,
                   %(side)s, %(price)s, %(size)s, %(size_delta)s, %(operation)s,
                   %(event_type)s, %(raw_data)s
            FROM resolved
            """,
            data,
        )
    if kind == "market_snapshots":
        return (
            prefix
            + """
            INSERT INTO market_snapshots
                (exchange, market_id, outcome_id, collector_connection_id, observed_at,
                 source_timestamp, exchange_timestamp, received_at, received_monotonic_ns,
                 sequence_number, book_hash, best_bid, best_ask, midpoint, spread,
                 last_trade_price, bid_depth, ask_depth, volume, open_interest, liquidity,
                 raw_data)
            SELECT %(exchange)s, market_id, outcome_id, %(connection_id)s, %(observed_at)s,
                   %(source_timestamp)s, %(exchange_timestamp)s, %(received_at)s,
                   %(received_monotonic_ns)s, %(sequence_number)s, %(book_hash)s,
                   %(best_bid)s, %(best_ask)s, %(midpoint)s, %(spread)s,
                   %(last_trade_price)s, %(bid_depth)s, %(ask_depth)s, %(volume)s,
                   %(open_interest)s, %(liquidity)s, %(raw_data)s
            FROM resolved
            """,
            data,
        )
    if kind == "raw_ws_messages":
        return (
            prefix
            + """
            INSERT INTO raw_ws_messages
                (exchange, collector_connection_id, channel, market_id, outcome_id,
                 market_external_id, outcome_external_id, message_type,
                 source_timestamp, exchange_timestamp, source_timestamp_raw,
                 exchange_timestamp_raw, received_at, received_monotonic_ns,
                 sequence_number, book_hash, payload)
            SELECT %(exchange)s, %(connection_id)s, %(channel)s, market_id, outcome_id,
                   %(market_external_id)s, %(outcome_external_id)s, %(message_type)s,
                   %(source_timestamp)s, %(exchange_timestamp)s,
                   %(source_timestamp_raw)s, %(exchange_timestamp_raw)s, %(received_at)s,
                   %(received_monotonic_ns)s, %(sequence_number)s, %(book_hash)s, %(payload)s
            FROM resolved
            UNION ALL
            SELECT %(exchange)s, %(connection_id)s, %(channel)s, NULL, NULL,
                   %(market_external_id)s, %(outcome_external_id)s, %(message_type)s,
                   %(source_timestamp)s, %(exchange_timestamp)s,
                   %(source_timestamp_raw)s, %(exchange_timestamp_raw)s, %(received_at)s,
                   %(received_monotonic_ns)s, %(sequence_number)s, %(book_hash)s, %(payload)s
            WHERE NOT EXISTS (SELECT 1 FROM resolved)
            """,
            data,
        )
    if kind == "reference_price_updates":
        return (
            """
            WITH inserted_instrument AS (
                INSERT INTO reference_instruments
                    (delivery_exchange, provider, external_id, symbol, status, raw_data)
                VALUES
                    (%(delivery_exchange)s, %(provider)s,
                     %(external_instrument_id)s, %(external_instrument_id)s,
                     %(source_status)s, %(raw_data)s)
                ON CONFLICT (delivery_exchange, provider, external_id) DO NOTHING
                RETURNING id
            ), instrument AS (
                SELECT id FROM inserted_instrument
                UNION ALL
                SELECT id FROM reference_instruments
                WHERE delivery_exchange = %(delivery_exchange)s
                  AND provider = %(provider)s
                  AND external_id = %(external_instrument_id)s
                LIMIT 1
            )
            INSERT INTO reference_price_updates
                (delivery_exchange, provider, reference_instrument_id,
                 external_instrument_id, external_update_id,
                 collector_connection_id, source_timestamp, exchange_timestamp,
                 source_timestamp_raw, exchange_timestamp_raw,
                 received_at, received_monotonic_ns, sequence_number, price, bid, ask,
                 confidence_interval, publish_slot, source_status, raw_data)
            SELECT %(delivery_exchange)s, %(provider)s, instrument.id,
                 %(external_instrument_id)s,
                 %(external_update_id)s, %(connection_id)s, %(source_timestamp)s,
                 %(exchange_timestamp)s, %(source_timestamp_raw)s,
                 %(exchange_timestamp_raw)s, %(received_at)s, %(received_monotonic_ns)s,
                 %(sequence_number)s, %(price)s, %(bid)s, %(ask)s,
                 %(confidence_interval)s, %(publish_slot)s, %(source_status)s, %(raw_data)s
            FROM instrument
            ON CONFLICT (delivery_exchange, provider, external_update_id)
                WHERE external_update_id IS NOT NULL DO NOTHING
            """,
            data,
        )
    if kind == "sports_feed_updates":
        return (
            """
            WITH inserted_event AS (
                INSERT INTO sports_events
                    (delivery_exchange, provider, external_id, sport, league,
                     home_participant, away_participant, scheduled_at, status, raw_data)
                VALUES
                    (%(delivery_exchange)s, %(provider)s, %(external_event_id)s,
                     COALESCE(%(state)s->>'sport', %(raw_data)s->>'sport'),
                     COALESCE(%(state)s->>'league', %(raw_data)s->>'league'),
                     COALESCE(%(state)s->>'home', %(raw_data)s->>'home'),
                     COALESCE(%(state)s->>'away', %(raw_data)s->>'away'),
                     NULL,
                     %(status)s, %(raw_data)s)
                ON CONFLICT (delivery_exchange, provider, external_id) DO NOTHING
                RETURNING id
            ), sports_event AS (
                SELECT id FROM inserted_event
                UNION ALL
                SELECT id FROM sports_events
                WHERE delivery_exchange = %(delivery_exchange)s
                  AND provider = %(provider)s
                  AND external_id = %(external_event_id)s
                LIMIT 1
            )
            INSERT INTO sports_feed_updates
                (delivery_exchange, provider, sports_event_id,
                 external_event_id, external_update_id,
                 collector_connection_id, update_type, status, period, clock,
                 home_score, away_score, source_timestamp, exchange_timestamp,
                 source_timestamp_raw, exchange_timestamp_raw,
                 received_at, received_monotonic_ns, sequence_number, state, raw_data)
            SELECT %(delivery_exchange)s, %(provider)s, sports_event.id,
                 %(external_event_id)s,
                 %(external_update_id)s, %(connection_id)s, %(update_type)s,
                 %(status)s, %(period)s, %(clock)s, %(home_score)s, %(away_score)s,
                 %(source_timestamp)s, %(exchange_timestamp)s,
                 %(source_timestamp_raw)s, %(exchange_timestamp_raw)s, %(received_at)s,
                 %(received_monotonic_ns)s, %(sequence_number)s, %(state)s, %(raw_data)s
            FROM sports_event
            ON CONFLICT (delivery_exchange, provider, external_update_id)
                WHERE external_update_id IS NOT NULL DO NOTHING
            """,
            data,
        )
    if kind == "market_lifecycle_events":
        return (
            prefix
            + """
            INSERT INTO market_lifecycle_events
                (exchange, market_id, market_external_id,
                 collector_connection_id, external_event_id,
                 dedup_hash, event_type, previous_status, new_status,
                 source_timestamp, exchange_timestamp, source_timestamp_raw,
                 exchange_timestamp_raw, received_at,
                 received_monotonic_ns, sequence_number, details, raw_data)
            SELECT %(exchange)s, (SELECT market_id FROM resolved),
                   %(market_external_id)s, %(connection_id)s, %(external_event_id)s,
                   %(dedup_hash)s, %(event_type)s, %(previous_status)s, %(new_status)s,
                   %(source_timestamp)s, %(exchange_timestamp)s,
                   %(source_timestamp_raw)s, %(exchange_timestamp_raw)s, %(received_at)s,
                   %(received_monotonic_ns)s, %(sequence_number)s, %(details)s, %(raw_data)s
            ON CONFLICT (exchange, dedup_hash) DO NOTHING
            """,
            data,
        )
    if kind == "event_lifecycle_events":
        return (
            """
            INSERT INTO event_lifecycle_events
                (exchange, event_id, event_external_id,
                 collector_connection_id, external_update_id, dedup_hash,
                 event_type, source_timestamp, exchange_timestamp,
                 source_timestamp_raw, exchange_timestamp_raw, received_at,
                 received_monotonic_ns, sequence_number, details, raw_data)
            VALUES
                (%(exchange)s,
                 (SELECT id FROM events
                  WHERE exchange = %(exchange)s
                    AND (external_id = %(event_external_id)s
                         OR ticker = %(event_external_id)s)
                  ORDER BY CASE WHEN external_id = %(event_external_id)s THEN 0 ELSE 1 END
                  LIMIT 1),
                 %(event_external_id)s, %(connection_id)s,
                 %(external_update_id)s, %(dedup_hash)s, %(event_type)s,
                 %(source_timestamp)s, %(exchange_timestamp)s,
                 %(source_timestamp_raw)s, %(exchange_timestamp_raw)s,
                 %(received_at)s, %(received_monotonic_ns)s,
                 %(sequence_number)s, %(details)s, %(raw_data)s)
            ON CONFLICT (exchange, dedup_hash) DO NOTHING
            """,
            data,
        )
    if kind == "comments":
        return (
            """
            INSERT INTO comments
                (exchange, external_comment_id, dedup_hash, event_id, market_id,
                 parent_entity_type, parent_entity_id,
                 parent_external_comment_id, public_identifier, profile_name, body,
                 source_created_at, source_updated_at, source_timestamp,
                 exchange_timestamp, received_at, received_monotonic_ns, raw_data)
            VALUES
                (%(exchange)s, %(external_comment_id)s, %(dedup_hash)s,
                 CASE WHEN %(parent_entity_type)s = 'event' THEN
                    (SELECT id FROM events WHERE exchange = %(exchange)s
                     AND (external_id = %(parent_entity_id)s
                          OR raw_data->>'id' = %(parent_entity_id)s)
                     LIMIT 1) END,
                 CASE WHEN %(parent_entity_type)s = 'market' THEN
                    (SELECT id FROM markets WHERE exchange = %(exchange)s
                     AND (external_id = %(parent_entity_id)s
                          OR condition_id = %(parent_entity_id)s
                          OR ticker = %(parent_entity_id)s
                          OR raw_data->>'id' = %(parent_entity_id)s)
                     LIMIT 1) END,
                 %(parent_entity_type)s, %(parent_entity_id)s,
                 %(parent_external_comment_id)s, %(public_identifier)s,
                 %(profile_name)s, %(body)s, %(source_created_at)s,
                 %(source_updated_at)s, %(source_timestamp)s,
                 %(exchange_timestamp)s, %(received_at)s,
                 %(received_monotonic_ns)s, %(raw_data)s)
            ON CONFLICT (exchange, dedup_hash) DO UPDATE SET
                body = EXCLUDED.body, source_updated_at = EXCLUDED.source_updated_at,
                raw_data = EXCLUDED.raw_data
            """,
            data,
        )
    if kind == "candlesticks":
        return (
            prefix
            + """
            INSERT INTO candlesticks
                (exchange, market_id, outcome_id, interval_seconds, period_start,
                 period_end, open, high, low, close, bid_open, bid_high, bid_low,
                 bid_close, ask_open, ask_high, ask_low, ask_close, volume,
                 open_interest, source_timestamp, retrieved_at, raw_data)
            SELECT %(exchange)s, market_id, outcome_id, %(interval_seconds)s,
                   %(period_start)s, %(period_end)s, %(open)s, %(high)s, %(low)s,
                   %(close)s, %(bid_open)s, %(bid_high)s, %(bid_low)s, %(bid_close)s,
                   %(ask_open)s, %(ask_high)s, %(ask_low)s, %(ask_close)s,
                   %(volume)s, %(open_interest)s, %(source_timestamp)s,
                   %(retrieved_at)s, %(raw_data)s
            FROM resolved
            ON CONFLICT (exchange, market_id, outcome_id, interval_seconds, period_start)
            DO UPDATE SET
                period_end = EXCLUDED.period_end, open = EXCLUDED.open,
                high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
                bid_open = EXCLUDED.bid_open, bid_high = EXCLUDED.bid_high,
                bid_low = EXCLUDED.bid_low, bid_close = EXCLUDED.bid_close,
                ask_open = EXCLUDED.ask_open, ask_high = EXCLUDED.ask_high,
                ask_low = EXCLUDED.ask_low, ask_close = EXCLUDED.ask_close,
                volume = EXCLUDED.volume, open_interest = EXCLUDED.open_interest,
                source_timestamp = EXCLUDED.source_timestamp,
                retrieved_at = EXCLUDED.retrieved_at, raw_data = EXCLUDED.raw_data
            """,
            data,
        )
    if kind == "holder_snapshots":
        return (
            prefix
            + """
            INSERT INTO holder_snapshots
                (exchange, market_id, outcome_id, public_identifier, profile_name,
                 amount, rank, observed_at, source_timestamp, received_at, raw_data)
            SELECT %(exchange)s, market_id, outcome_id, %(public_identifier)s,
                   %(profile_name)s, %(amount)s, %(rank)s, %(observed_at)s,
                   %(source_timestamp)s, %(received_at)s, %(raw_data)s
            FROM resolved
            ON CONFLICT (exchange, market_id, outcome_id, public_identifier, observed_at)
            DO NOTHING
            """,
            data,
        )
    raise ValueError(f"unsupported write item kind: {kind}")
