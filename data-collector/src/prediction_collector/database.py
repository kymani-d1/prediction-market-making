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
from prediction_collector.migrations import migrate_database, verify_database_migrations


LOGGER = logging.getLogger(__name__)


_CURRENT_TIER_STATUS_SQL = """
    WITH latest_cohort AS (
        SELECT max(evaluated_at) AS evaluated_at
        FROM market_collection_tiers
    )
    SELECT tier, count(*) AS markets,
           count(*) FILTER (WHERE ceiling_binding) AS ceiling_binding,
           max(tier.evaluated_at) AS last_evaluated_at
    FROM market_collection_tiers tier
    CROSS JOIN latest_cohort latest
    WHERE tier.evaluated_at = latest.evaluated_at
    GROUP BY tier
"""


def _discovery_state(
    *, latest_complete_discovery: datetime | None, open_refresh_failures: int
) -> str:
    if open_refresh_failures:
        return "retrying"
    if latest_complete_discovery is not None:
        return "ready"
    return "discovering"


def _discovery_status(
    *,
    latest_complete_discovery: datetime | None,
    open_refresh_failures: int,
    open_coverage_warnings: int,
    open_metadata_schema_warnings: int,
) -> dict[str, Any]:
    return {
        "discovery_state": _discovery_state(
            latest_complete_discovery=latest_complete_discovery,
            open_refresh_failures=open_refresh_failures,
        ),
        "discovery_warnings": {
            "open_total": open_coverage_warnings,
            "market_metadata_schema_failure": open_metadata_schema_warnings,
        },
    }

if TYPE_CHECKING:
    from prediction_collector.writer import WriteItem


@dataclass(slots=True)
class MetadataSyncDiagnostics:
    stale_lifecycle_states_preserved: int = 0

    def as_log_fields(self) -> dict[str, int]:
        return {
            "stale_lifecycle_states_preserved": (self.stale_lifecycle_states_preserved),
        }


def _json(value: Any) -> Jsonb:
    return Jsonb(value if value is not None else {})


def _compact_raw(value: Any, keys: Iterable[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {key: value[key] for key in keys if key in value and value[key] is not None}


def _compact_event_raw(value: Any) -> dict[str, Any]:
    return _compact_raw(value, ("id", "ticker", "slug", "updatedAt"))


def _compact_market_raw(value: Any) -> dict[str, Any]:
    return _compact_raw(
        value,
        (
            "id",
            "conditionId",
            "questionID",
            "clobTokenIds",
            "outcomes",
            "negRisk",
            "negativeRisk",
            "marketGroup",
            "groupItemTitle",
            "groupItemThreshold",
            "rewardsMinSize",
            "rewardsMaxSpread",
            "feesEnabled",
            "feeSchedule",
            "makerBaseFee",
            "takerBaseFee",
            "active",
            "closed",
            "archived",
            "acceptingOrders",
            "enableOrderBook",
            "updatedAt",
        ),
    )


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
        "marketGroup",
        "groupItemTitle",
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
                    "archived",
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
    if current_upstream is None and not current.get(
        "metadata_exchange_timestamp_is_transport"
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
        incoming_raw["_latest_lifecycle_event"] = current_raw["_latest_lifecycle_event"]
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
    def __init__(
        self, settings: Settings, metrics: ThroughputMetrics | None = None
    ) -> None:
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

    async def verify_migrations(self) -> dict[str, object]:
        return await verify_database_migrations(self.settings.database_dsn)

    async def ping(self) -> bool:
        try:
            async with self.pool.connection() as connection:
                row = await (await connection.execute("SELECT 1 AS ok")).fetchone()
            return bool(row and row["ok"] == 1)
        except Exception:
            return False

    async def start_run(self, job_type: str, exchange: str | None) -> int:
        async with self.pool.connection() as connection:
            superseded = 0
            if job_type == "live":
                # Deployments may briefly overlap while the old process drains,
                # but a newly starting single-replica live worker is the sole
                # authoritative run. Serialise concurrent starts, then mark
                # older running rows cancelled without touching the old process.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext("
                    "'prediction_collector_single_live_run'))"
                )
                cursor = await connection.execute(
                    """
                    UPDATE collector_runs
                    SET status = 'cancelled',
                        finished_at = clock_timestamp(),
                        error_summary = COALESCE(
                            error_summary,
                            'superseded by a newer single-replica live run'
                        ),
                        metadata = metadata || jsonb_build_object(
                            'superseded_by_live_startup', true,
                            'superseded_at', clock_timestamp()
                        )
                    WHERE job_type = 'live'
                      AND status = 'running'
                      AND exchange IS NOT DISTINCT FROM %s
                    """,
                    (exchange,),
                )
                superseded = max(int(cursor.rowcount or 0), 0)
            row = await (
                await connection.execute(
                    """
                    INSERT INTO collector_runs
                        (run_uuid, job_type, exchange, status, metadata)
                    VALUES (%s, %s, %s, 'running', %s)
                    RETURNING id
                    """,
                    (
                        uuid.uuid4(),
                        job_type,
                        exchange,
                        _json(self.settings.safe_summary()),
                    ),
                )
            ).fetchone()
        assert row is not None
        if superseded:
            LOGGER.info(
                "Reconciled superseded live collector runs",
                extra={"superseded_runs": superseded, "exchange": exchange},
            )
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
                WHERE id = %s AND status = 'running'
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
                        _json(
                            _compact_raw(
                                value.get("raw_data"), ("id", "ticker", "slug")
                            )
                        ),
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
                        _json(_compact_event_raw(value.get("raw_data"))),
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
                        _json({}),
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
                            _json(value.get("price_level_structure"))
                            if value.get("price_level_structure")
                            else None,
                            _json(value.get("structural_metadata")),
                            _json(_compact_market_raw(raw)),
                        ),
                    )
                ).fetchone()
                assert row is not None
                market_id = int(row["id"])
                await connection.execute(
                    "UPDATE markets SET archived = %s WHERE id = %s",
                    (bool(value.get("archived", raw.get("archived"))), market_id),
                )
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
            raise ValueError(
                f"unsupported lifecycle market fields: {sorted(unexpected)}"
            )
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

                current_raw = (
                    row["raw_data"] if isinstance(row["raw_data"], dict) else {}
                )
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
                        "latest_lifecycle_type": lifecycle_payload.get("event_type")
                        or lifecycle_payload.get("type"),
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
                        _json(_compact_market_raw(value["raw_data"])),
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
                    _json({}),
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
                        WHEN CAST(%s AS timestamptz) IS NOT NULL
                         AND (source_timestamp IS NULL OR %s > source_timestamp)
                        THEN %s ELSE source_timestamp
                    END,
                    exchange_timestamp = CASE
                        WHEN NOT %s AND CAST(%s AS timestamptz) IS NOT NULL
                         AND (exchange_timestamp IS NULL
                              OR exchange_timestamp_is_transport
                              OR %s > exchange_timestamp)
                        THEN %s ELSE exchange_timestamp
                    END,
                    exchange_timestamp_is_transport = CASE
                        WHEN NOT %s AND CAST(%s AS timestamptz) IS NOT NULL
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
                "fee_rate": str(value.get("fee_rate"))
                if value.get("fee_rate") is not None
                else None,
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
                        _json({"source": "market_metadata"}),
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
            first_reward = (
                rewards[0] if rewards and isinstance(rewards[0], dict) else {}
            )
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
                        _json({"source": "market_metadata"}),
                    ),
                )

        event_external_id = value.get("event_external_id")
        group_external_id: str | None = None
        group_type: str | None = None
        if value.get("negative_risk") and event_external_id:
            group_external_id = f"negative-risk:{event_external_id}"
            group_type = "negative_risk"
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
        if group_external_id and group_type:
            group_event_external_id = event_external_id
            group_name = raw.get("marketGroup") or event_external_id
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
                            }
                        ),
                        _json({}),
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
                        _json({}),
                        group_row["id"],
                        market_id,
                        membership_role,
                    ),
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
                if (
                    version_current
                    and latest is not None
                    and latest["content_hash"] == digest
                ):
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
                                _json({}),
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
                if (
                    version_current
                    and latest is not None
                    and latest["content_hash"] == digest
                ):
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
                                _json({}),
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
                        _json(
                            _compact_raw(
                                value.get("raw_data"),
                                ("name", "token_id", "outcome_index"),
                            )
                        ),
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("outcomes")
        return int(row["id"])

    async def upsert_tag(self, exchange: str, raw: Mapping[str, Any]) -> int:
        external_id = raw.get("id")
        name = str(
            raw.get("label") or raw.get("name") or raw.get("slug") or external_id or ""
        )
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
                    (
                        exchange,
                        str(external_id) if external_id is not None else None,
                        name,
                        raw.get("slug"),
                        _json(_compact_raw(raw, ("id", "label", "name", "slug"))),
                    ),
                )
            ).fetchone()
        assert row is not None
        await self.metrics.rows("tags")
        return int(row["id"])

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

    async def live_candidates(
        self, exchange: str | None = None
    ) -> list[MarketCandidate]:
        params: tuple[Any, ...] = () if exchange is None else (exchange,)
        where = "" if exchange is None else "WHERE m.exchange = %s"
        query = f"""
            SELECT m.exchange, m.external_id, m.ticker, m.status, m.is_active,
                   m.is_tradable, m.archived, m.accepting_orders,
                   m.enable_order_book, m.close_time,
                   m.volume, m.volume_24h, m.liquidity, m.raw_data,
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
                closed=str(row["status"] or "").lower()
                in {"closed", "resolved", "settled", "finalized"},
                archived=bool(row["archived"]),
                accepting_orders=bool(row["accepting_orders"]),
                enable_order_book=bool(row["enable_order_book"]),
                volume=row["volume"],
                volume_24h=row["volume_24h"],
                liquidity=row["liquidity"],
                has_maker_rewards=bool(
                    isinstance(row["raw_data"], Mapping)
                    and (
                        row["raw_data"].get("rewardsMinSize") is not None
                        or row["raw_data"].get("rewardsMaxSpread") is not None
                    )
                ),
                close_time=row["close_time"],
                outcome_token_ids=tuple(row["token_ids"]),
                raw_data=row["raw_data"],
            )
            for row in rows
        ]

    async def collection_tier_market_ids(self, tiers: Iterable[str]) -> set[str]:
        values = list(dict.fromkeys(tiers))
        if not values:
            return set()
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT m.external_id
                    FROM market_collection_tiers tier
                    JOIN markets m ON m.id = tier.market_id
                    WHERE m.exchange = 'polymarket'
                      AND tier.tier = ANY(%s::TEXT[])
                    """,
                    (values,),
                )
            ).fetchall()
        return {str(row["external_id"]) for row in rows}

    async def load_tier_state(self) -> list[tuple[str, str, datetime]]:
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT market.external_id, tier.tier,
                           COALESCE(tier.promoted_at, tier.demoted_at,
                                    tier.first_assigned_at) AS assigned_at
                    FROM market_collection_tiers tier
                    JOIN markets market ON market.id = tier.market_id
                    WHERE market.exchange = 'polymarket'
                    """
                )
            ).fetchall()
        return [
            (str(row["external_id"]), str(row["tier"]), row["assigned_at"])
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
                for external_id in () if pending_subscription else market_external_ids:
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
                    (
                        _json({"subscription_ack": dict(acknowledgement or {})}),
                        connection_id,
                    ),
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
        return {(str(row["exchange"]), str(row["market_external_id"])) for row in rows}

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
        missing_end = (
            actual_sequence - 1
            if is_forward_gap and actual_sequence is not None
            else None
        )
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
            "full_l2_max_markets": self.settings.full_l2_max_markets,
            "sampled_max_markets": self.settings.sampled_max_markets,
            "full_l2_min_score": str(self.settings.full_l2_min_score),
            "full_l2_min_liquidity": str(self.settings.full_l2_min_liquidity),
            "full_l2_allowlist": sorted(self.settings.full_l2_market_allowlist),
            "blocklist": sorted(self.settings.live_market_blocklist),
        }
        market_list = list(markets)
        payload = [
            {
                "external_id": market.external_id,
                "is_active": market.active,
                "is_tradable": market.tradable,
                "is_subscribed": (market.exchange, market.external_id)
                in subscribed_ids,
                "exclusion_reason": reasons.get((market.exchange, market.external_id)),
                "observed_volume": str(market.volume)
                if market.volume is not None
                else None,
                "observed_liquidity": (
                    str(market.liquidity) if market.liquidity is not None else None
                ),
            }
            for market in market_list
        ]
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                """
                WITH input AS (
                    SELECT *
                    FROM jsonb_to_recordset(%s::JSONB) AS x(
                        external_id TEXT, is_active BOOLEAN, is_tradable BOOLEAN,
                        is_subscribed BOOLEAN, exclusion_reason TEXT,
                        observed_volume NUMERIC, observed_liquidity NUMERIC
                    )
                ), resolved AS (
                    SELECT i.*, m.id AS market_id, t.tier, t.score,
                           t.tier IN ('full_l2', 'sampled') AS is_eligible
                    FROM input i
                    JOIN markets m
                      ON m.exchange = 'polymarket' AND m.external_id = i.external_id
                    LEFT JOIN market_collection_tiers t ON t.market_id = m.id
                ), ranked AS (
                    SELECT r.*,
                           CASE WHEN is_eligible THEN row_number() OVER (
                               PARTITION BY is_eligible
                               ORDER BY score DESC NULLS LAST, external_id
                           ) END AS ranking_position
                    FROM resolved r
                )
                INSERT INTO live_market_subscription_decisions
                    (collector_run_id, exchange, market_id, market_external_id,
                     is_active, is_tradable, is_eligible, is_subscribed,
                     exclusion_reason, ranking_criterion, ranking_position,
                     observed_volume, observed_liquidity, config_snapshot, details)
                SELECT %s, 'polymarket', market_id, external_id, is_active,
                       is_tradable, COALESCE(is_eligible, FALSE), is_subscribed,
                       exclusion_reason, 'tier_score_desc_external_id_asc',
                       ranking_position, observed_volume, observed_liquidity,
                       %s, jsonb_build_object('tier', tier, 'score', score)
                FROM ranked
                """,
                (
                    _json(json.loads(canonical_json(payload))),
                    run_id,
                    _json(config),
                ),
            )
            inserted = max(int(cursor.rowcount or 0), 0)
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
            current = {str(row["relname"]): int(row["row_writes"] or 0) for row in rows}
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

    async def _write_item(
        self, connection: Any, kind: str, data: Mapping[str, Any]
    ) -> int:
        query, params = _write_query(kind, data)
        cursor = await connection.execute(query, params)
        return max(cursor.rowcount, 0)

    async def record_tier_assignments(self, assignments: Iterable[Any]) -> int:
        values = list(assignments)
        if not values:
            return 0
        now = utc_now()
        payload = [
            {
                "external_id": item.market.external_id,
                "tier": item.tier.value,
                "score": str(item.score),
                "reason_codes": list(item.reasons),
                "ceiling_binding": item.ceiling_binding,
            }
            for item in values
        ]
        encoded = _json(json.loads(canonical_json(payload)))
        async with self.pool.connection() as connection:
            async with connection.transaction():
                changed_row = await (
                    await connection.execute(
                        """
                        WITH input AS (
                            SELECT * FROM jsonb_to_recordset(%s::JSONB) AS x(
                                external_id TEXT, tier TEXT, score NUMERIC,
                                reason_codes TEXT[], ceiling_binding BOOLEAN
                            )
                        )
                        INSERT INTO market_collection_tier_history
                            (market_id, previous_tier, tier, score, reason_codes,
                             ceiling_binding, evaluated_at)
                        SELECT m.id, t.tier, i.tier, i.score, i.reason_codes,
                               i.ceiling_binding, %s
                        FROM input i
                        JOIN markets m
                          ON m.exchange = 'polymarket' AND m.external_id = i.external_id
                        LEFT JOIN market_collection_tiers t ON t.market_id = m.id
                        WHERE t.tier IS DISTINCT FROM i.tier
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """,
                        (encoded, now),
                    )
                ).fetchall()
                await connection.execute(
                    """
                    WITH input AS (
                        SELECT * FROM jsonb_to_recordset(%s::JSONB) AS x(
                            external_id TEXT, tier TEXT, score NUMERIC,
                            reason_codes TEXT[], ceiling_binding BOOLEAN
                        )
                    )
                    INSERT INTO market_collection_tiers
                        (market_id, tier, score, reason_codes, signals,
                         ceiling_binding, first_assigned_at, evaluated_at,
                         promoted_at, demoted_at)
                    SELECT m.id, i.tier, i.score, i.reason_codes,
                           jsonb_build_object('policy_reasons', i.reason_codes),
                           i.ceiling_binding, %s, %s,
                           CASE WHEN i.tier = 'full_l2' THEN %s END,
                           NULL
                    FROM input i
                    JOIN markets m
                      ON m.exchange = 'polymarket' AND m.external_id = i.external_id
                    ON CONFLICT (market_id) DO UPDATE SET
                        tier = EXCLUDED.tier,
                        score = EXCLUDED.score,
                        reason_codes = EXCLUDED.reason_codes,
                        signals = EXCLUDED.signals,
                        ceiling_binding = EXCLUDED.ceiling_binding,
                        evaluated_at = EXCLUDED.evaluated_at,
                        promoted_at = CASE
                            WHEN EXCLUDED.tier = 'full_l2'
                             AND market_collection_tiers.tier <> 'full_l2'
                            THEN EXCLUDED.evaluated_at
                            ELSE market_collection_tiers.promoted_at END,
                        demoted_at = CASE
                            WHEN EXCLUDED.tier <> 'full_l2'
                             AND market_collection_tiers.tier = 'full_l2'
                            THEN EXCLUDED.evaluated_at
                            ELSE market_collection_tiers.demoted_at END,
                        updated_at = clock_timestamp()
                    """,
                    (encoded, now, now, now),
                )
        changed = len(changed_row)
        if changed:
            await self.metrics.rows("market_collection_tier_history", changed)
        await self.metrics.rows("market_collection_tiers", len(values))
        return changed

    async def register_archive_object(self, **value: Any) -> int:
        value.setdefault("payload_content_hash", None)
        value.setdefault("object_role", "data")
        value.setdefault("compaction_generation", 0)
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO archive_objects
                        (stream, schema_version, object_key, content_hash, compression,
                         row_count, uncompressed_bytes, compressed_bytes,
                         min_source_timestamp, max_source_timestamp,
                         min_received_at, max_received_at, partition_date,
                         partition_hour, status, local_spool_path,
                         payload_content_hash, object_role, compaction_generation)
                    VALUES
                        (%(stream)s, %(schema_version)s, %(object_key)s,
                         %(content_hash)s, %(compression)s, %(row_count)s,
                         %(uncompressed_bytes)s, %(compressed_bytes)s,
                         %(min_source_timestamp)s, %(max_source_timestamp)s,
                         %(min_received_at)s, %(max_received_at)s,
                         %(partition_date)s, %(partition_hour)s, 'prepared',
                          %(local_spool_path)s, %(payload_content_hash)s,
                          %(object_role)s, %(compaction_generation)s)
                    ON CONFLICT (content_hash) DO UPDATE SET
                        local_spool_path = COALESCE(
                            archive_objects.local_spool_path,
                            EXCLUDED.local_spool_path
                        ),
                        updated_at = clock_timestamp()
                    RETURNING id
                    """,
                    value,
                )
            ).fetchone()
        assert row is not None
        return int(row["id"])

    async def mark_archive_upload_attempt(self, object_id: int, attempt: int) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_objects
                SET status = 'uploading', upload_attempts = GREATEST(upload_attempts, %s),
                    updated_at = clock_timestamp()
                WHERE id = %s AND status <> 'uploaded'
                """,
                (attempt, object_id),
            )

    async def archive_object_state(self, object_id: int) -> Mapping[str, Any]:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    SELECT id, object_key, content_hash, compressed_bytes,
                           status, local_spool_path
                    FROM archive_objects WHERE id = %s
                    """,
                    (object_id,),
                )
            ).fetchone()
        if row is None:
            raise LookupError(f"archive object {object_id} disappeared")
        return row

    async def archive_object_by_content_hash(
        self, digest: str
    ) -> Mapping[str, Any] | None:
        async with self.pool.connection() as connection:
            return await (
                await connection.execute(
                    """
                    SELECT id, object_key, content_hash, status
                    FROM archive_objects WHERE content_hash = %s
                    """,
                    (digest,),
                )
            ).fetchone()

    async def mark_archive_uploaded(self, object_id: int) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_objects
                SET status = 'uploaded', uploaded_at = COALESCE(uploaded_at, clock_timestamp()),
                    local_spool_path = NULL, last_error = NULL,
                    updated_at = clock_timestamp()
                WHERE id = %s
                """,
                (object_id,),
            )

    async def mark_archive_retrying(self, object_id: int, error: str) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_objects SET status = 'retrying', last_error = %s,
                    updated_at = clock_timestamp() WHERE id = %s AND status <> 'uploaded'
                """,
                (error[:8000], object_id),
            )

    async def mark_archive_failed(self, object_id: int, error: str) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_objects SET status = 'failed', last_error = %s,
                    updated_at = clock_timestamp()
                WHERE id = %s AND status <> 'uploaded'
                """,
                (error[:8000], object_id),
            )

    async def abandon_archive_object(self, object_id: int, error: str) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_objects SET status = 'failed', last_error = %s,
                    local_spool_path = NULL, updated_at = clock_timestamp()
                WHERE id = %s AND status <> 'uploaded'
                """,
                (error[:8000], object_id),
            )

    async def archive_object_counts(self, object_id: int) -> Mapping[str, Any]:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    SELECT stream, row_count, uncompressed_bytes, compressed_bytes
                    FROM archive_objects WHERE id = %s
                    """,
                    (object_id,),
                )
            ).fetchone()
        if row is None:
            raise LookupError(f"archive object {object_id} disappeared")
        return row

    async def ensure_archive_identifier(
        self,
        *,
        entity_kind: str,
        archive_key: int,
        external_id: str,
        parent_archive_key: int | None,
    ) -> bool:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO archive_id_dictionary
                        (entity_kind, archive_key, exchange, external_id,
                         parent_archive_key)
                    VALUES (%s, %s, 'polymarket', %s, %s)
                    ON CONFLICT (entity_kind, archive_key) DO UPDATE SET
                        last_observed_at = clock_timestamp()
                    RETURNING external_id, (xmax = 0) AS inserted
                    """,
                    (entity_kind, archive_key, external_id, parent_archive_key),
                )
            ).fetchone()
        if row is None or str(row["external_id"]) != external_id:
            raise RuntimeError(
                f"archive identifier collision for {entity_kind}:{archive_key}"
            )
        return bool(row["inserted"])

    async def raw_rest_archive_by_content_hash(
        self, content_hash: str
    ) -> Mapping[str, Any] | None:
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    SELECT id, object_key, content_hash, compressed_bytes
                    FROM archive_objects
                    WHERE payload_content_hash = %s AND status = 'uploaded'
                      AND superseded_at IS NULL
                    LIMIT 1
                    """,
                    (content_hash,),
                )
            ).fetchone()
        return row

    async def archive_manifest_keys(self) -> list[str]:
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT object_key FROM archive_objects
                    WHERE status = 'uploaded' AND superseded_at IS NULL
                    """
                )
            ).fetchall()
        return [str(row["object_key"]) for row in rows]

    async def mark_market_final_snapshot_archived(
        self, *, market_external_id: str, archive_object_id: int
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO market_archive_finalizations
                    (market_id, final_snapshot_object_id)
                SELECT id, %s FROM markets
                WHERE exchange = 'polymarket' AND external_id = %s
                ON CONFLICT (market_id) DO UPDATE SET
                    final_snapshot_object_id = EXCLUDED.final_snapshot_object_id,
                    archived_at = clock_timestamp()
                """,
                (archive_object_id, market_external_id),
            )

    async def pending_archive_objects(
        self,
        *,
        limit: int,
        local_content_hashes: list[str] | None = None,
    ) -> list[Mapping[str, Any]]:
        # Manifests are shared by collector services, but spool files are not.
        # Prefer objects materialized in this process's spool so foreign rows
        # cannot starve local crash recovery behind the bounded result set.
        local_hashes = local_content_hashes or []
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT id, object_key, content_hash, compressed_bytes,
                           status, local_spool_path
                    FROM archive_objects
                    WHERE status <> 'uploaded'
                      AND local_spool_path IS NOT NULL
                    ORDER BY (content_hash = ANY(%s::TEXT[])) DESC, created_at
                    LIMIT %s
                    """,
                    (local_hashes, limit),
                )
            ).fetchall()
        return list(rows)

    async def archive_compaction_candidates(
        self,
        *,
        min_age_seconds: int,
        min_objects: int,
        target_bytes: int,
    ) -> list[Mapping[str, Any]]:
        async with self.pool.connection() as connection:
            partition = await (
                await connection.execute(
                    """
                    SELECT stream, partition_date, partition_hour
                    FROM archive_objects
                    WHERE status = 'uploaded' AND superseded_at IS NULL
                      AND object_role IN ('data', 'compacted')
                      AND stream NOT IN ('raw_rest', 'archive_dictionary')
                      AND uploaded_at < clock_timestamp() - make_interval(secs => %s)
                    GROUP BY stream, partition_date, partition_hour
                    HAVING count(*) >= %s
                    ORDER BY partition_date, partition_hour, stream
                    LIMIT 1
                    """,
                    (min_age_seconds, min_objects),
                )
            ).fetchone()
            if partition is None:
                return []
            rows = await (
                await connection.execute(
                    """
                    SELECT id, stream, schema_version, object_key, content_hash,
                           row_count, uncompressed_bytes, compressed_bytes,
                           min_source_timestamp, max_source_timestamp,
                           min_received_at, max_received_at, partition_date,
                           partition_hour, compaction_generation
                    FROM archive_objects
                    WHERE stream = %s AND partition_date = %s
                      AND partition_hour = %s AND status = 'uploaded'
                      AND superseded_at IS NULL
                      AND object_role IN ('data', 'compacted')
                    ORDER BY compressed_bytes, id
                    """,
                    (
                        partition["stream"],
                        partition["partition_date"],
                        partition["partition_hour"],
                    ),
                )
            ).fetchall()
        selected: list[Mapping[str, Any]] = []
        total = 0
        for row in rows:
            if selected and total >= target_bytes:
                break
            selected.append(row)
            total += int(row["compressed_bytes"])
        return selected if len(selected) >= min_objects else []

    async def running_archive_compactions(self) -> list[Mapping[str, Any]]:
        async with self.pool.connection() as connection:
            rows = await (
                await connection.execute(
                    """
                    SELECT c.id, c.source_object_ids,
                           c.replacement_object_id,
                           replacement.status AS replacement_status,
                           ARRAY(
                               SELECT source.object_key
                               FROM archive_objects source
                               WHERE source.id = ANY(c.source_object_ids)
                               ORDER BY source.id
                           ) AS source_object_keys
                    FROM archive_compactions c
                    LEFT JOIN archive_objects replacement
                      ON replacement.id = c.replacement_object_id
                    WHERE c.status = 'running'
                    ORDER BY c.started_at
                    """
                )
            ).fetchall()
        return list(rows)

    async def begin_archive_compaction(
        self, candidates: list[Mapping[str, Any]]
    ) -> int:
        first = candidates[0]
        async with self.pool.connection() as connection:
            row = await (
                await connection.execute(
                    """
                    INSERT INTO archive_compactions
                        (stream, partition_date, partition_hour, status,
                         source_object_ids, objects_before, bytes_before, row_count)
                    VALUES (%s, %s, %s, 'running', %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        first["stream"],
                        first["partition_date"],
                        first["partition_hour"],
                        [int(value["id"]) for value in candidates],
                        len(candidates),
                        sum(int(value["compressed_bytes"]) for value in candidates),
                        sum(int(value["row_count"]) for value in candidates),
                    ),
                )
            ).fetchone()
        assert row is not None
        return int(row["id"])

    async def set_archive_compaction_replacement(
        self, compaction_id: int, object_id: int
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_compactions SET replacement_object_id = %s
                WHERE id = %s AND status = 'running'
                """,
                (object_id, compaction_id),
            )

    async def complete_archive_compaction(
        self, compaction_id: int, replacement_id: int, source_ids: list[int]
    ) -> None:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    UPDATE archive_objects SET
                        status = 'uploaded',
                        uploaded_at = COALESCE(uploaded_at, clock_timestamp()),
                        local_spool_path = NULL, last_error = NULL,
                        updated_at = clock_timestamp()
                    WHERE id = %s
                    """,
                    (replacement_id,),
                )
                await connection.execute(
                    """
                    UPDATE archive_objects SET
                        superseded_by_object_id = %s,
                        superseded_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE id = ANY(%s::BIGINT[]) AND superseded_at IS NULL
                    """,
                    (replacement_id, source_ids),
                )
                await connection.execute(
                    """
                    UPDATE archive_compactions SET status = 'completed',
                        objects_after = 1,
                        bytes_after = (SELECT compressed_bytes FROM archive_objects WHERE id = %s),
                        replacement_object_id = %s,
                        completed_at = clock_timestamp()
                    WHERE id = %s
                    """,
                    (replacement_id, replacement_id, compaction_id),
                )

    async def fail_archive_compaction(self, compaction_id: int, error: str) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_compactions SET status = 'failed', error = %s,
                    completed_at = clock_timestamp() WHERE id = %s
                """,
                (error[:8000], compaction_id),
            )

    async def record_raw_rest_provenance(
        self, *, archive_object_id: int, object_key: str, value: Mapping[str, Any]
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO raw_rest_payloads
                    (source, endpoint, entity_type, external_key, requested_at,
                     received_at, response_timestamp, response_timestamp_raw,
                     parameters, http_status, content_hash, response_bytes,
                     record_count, archive_object_id, object_key)
                VALUES
                    (%(source)s, %(endpoint)s, %(entity_type)s, %(external_key)s,
                     %(requested_at)s, %(received_at)s, %(response_timestamp)s,
                     %(response_timestamp_raw)s, %(parameters)s, %(http_status)s,
                     %(content_hash)s, %(response_bytes)s, %(record_count)s,
                     %(archive_object_id)s, %(object_key)s)
                ON CONFLICT ON CONSTRAINT raw_rest_payloads_request_key DO NOTHING
                """,
                {
                    **value,
                    "parameters": _json(value.get("parameters")),
                    "archive_object_id": archive_object_id,
                    "object_key": object_key,
                },
            )

    async def record_archive_degradation(
        self,
        *,
        run_id: int | None,
        stream: str,
        priority: int,
        reason: str,
        rows_affected: int,
        bytes_affected: int,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO archive_degradation_events
                    (collector_run_id, stream, priority, reason, rows_affected,
                     bytes_affected, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    stream,
                    priority,
                    reason,
                    rows_affected,
                    bytes_affected,
                    _json(dict(details or {})),
                ),
            )

    async def resolve_archive_record_degradations(
        self, record_ids: list[str]
    ) -> None:
        if not record_ids:
            return
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_degradation_events
                SET resolved_at = clock_timestamp()
                WHERE resolved_at IS NULL
                  AND reason = 'bounded_queue_timeout'
                  AND details->>'record_id' = ANY(%s::TEXT[])
                """,
                (record_ids,),
            )

    async def resolve_transient_archive_degradations(
        self, *, run_id: int | None
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_degradation_events event
                SET resolved_at = clock_timestamp()
                WHERE event.resolved_at IS NULL
                  AND event.reason IN (
                      'bounded_queue_timeout',
                      'archive_spool_capacity_exceeded'
                  )
                  AND (
                      event.collector_run_id IS NOT DISTINCT FROM %s
                      OR EXISTS (
                          SELECT 1
                          FROM collector_runs current_run
                          JOIN collector_runs previous_run
                            ON previous_run.id = event.collector_run_id
                          WHERE current_run.id = %s
                            AND previous_run.job_type = current_run.job_type
                            AND previous_run.status <> 'running'
                      )
                  )
                """,
                (run_id, run_id),
            )

    async def resolve_optional_hot_write_degradations(self) -> None:
        """Resolve only pressure-driven optional PostgreSQL shedding events."""
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                UPDATE archive_degradation_events
                SET resolved_at = clock_timestamp()
                WHERE resolved_at IS NULL
                  AND reason IN (
                      'storage_critical_optional_hot_write_shed',
                      'postgres_critical_optional_hot_write_shed'
                  )
                """
            )

    async def storage_snapshot(self) -> dict[str, Any]:
        async with self.pool.connection() as connection:
            database_row = await (
                await connection.execute(
                    "SELECT pg_database_size(current_database()) AS bytes"
                )
            ).fetchone()
            table_rows = await (
                await connection.execute(
                    """
                    SELECT relname, pg_total_relation_size(relid) AS bytes
                    FROM pg_catalog.pg_statio_user_tables
                    ORDER BY bytes DESC LIMIT 15
                    """
                )
            ).fetchall()
            previous = await (
                await connection.execute(
                    """
                    SELECT observed_at, postgres_database_bytes,
                           archive_compressed_bytes
                    FROM storage_metrics ORDER BY observed_at DESC LIMIT 1
                    """
                )
            ).fetchone()
        return {
            "observed_at": utc_now(),
            "postgres_database_bytes": int(
                database_row["bytes"] if database_row else 0
            ),
            "major_table_bytes": {
                str(row["relname"]): int(row["bytes"]) for row in table_rows
            },
            "previous": dict(previous) if previous else None,
        }

    async def record_storage_metrics(
        self,
        *,
        run_id: int | None,
        postgres: Mapping[str, Any],
        archive: Mapping[str, Any],
        pressure_state: str,
    ) -> None:
        observed_at = postgres["observed_at"]
        previous = postgres.get("previous") or {}
        elapsed_hours = (
            max(
                (observed_at - previous.get("observed_at")).total_seconds() / 3600,
                1 / 3600,
            )
            if previous.get("observed_at")
            else None
        )
        postgres_growth = (
            (postgres["postgres_database_bytes"] - previous["postgres_database_bytes"])
            / elapsed_hours
            if elapsed_hours
            else None
        )
        archive_growth = (
            (
                archive.get("compressed_bytes_uploaded", 0)
                - int(previous.get("archive_compressed_bytes") or 0)
            )
            / elapsed_hours
            if elapsed_hours
            else None
        )
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO storage_metrics
                    (collector_run_id, observed_at, postgres_database_bytes,
                     postgres_growth_bytes_per_hour, major_table_bytes,
                     archive_queue_rows, archive_queue_bytes,
                     archive_oldest_queued_age_seconds, archive_objects_uploaded,
                     archive_rows_uploaded, archive_uncompressed_bytes,
                     archive_compressed_bytes, archive_upload_failures,
                     archive_growth_bytes_per_hour, spool_bytes, pressure_state, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    observed_at,
                    postgres["postgres_database_bytes"],
                    postgres_growth,
                    _json(postgres["major_table_bytes"]),
                    archive.get("queue_depth", 0),
                    archive.get("queue_bytes", 0),
                    archive.get("oldest_queued_seconds"),
                    archive.get("objects_uploaded", 0),
                    archive.get("rows_uploaded", 0),
                    archive.get("uncompressed_bytes_uploaded", 0),
                    archive.get("compressed_bytes_uploaded", 0),
                    archive.get("upload_failures", 0),
                    archive_growth,
                    archive.get("spool_bytes", 0),
                    pressure_state,
                    _json(
                        {
                            "archive_healthy": archive.get("healthy", False),
                            "streams": archive.get("streams", {}),
                            "compaction": archive.get("compaction", {}),
                            "raw_rest_objects_reused": archive.get(
                                "raw_rest_objects_reused", 0
                            ),
                        }
                    ),
                ),
            )

    async def apply_retention(self) -> dict[str, int]:
        deleted: dict[str, int] = {}
        async with self.pool.connection() as connection:
            for table, column, interval in (
                (
                    "reference_price_updates",
                    "received_at",
                    f"{self.settings.postgres_reference_retention_hours} hours",
                ),
                (
                    "microstructure_observations",
                    "observed_at",
                    f"{self.settings.postgres_observation_retention_hours} hours",
                ),
            ):
                cursor = await connection.execute(
                    f"DELETE FROM {table} WHERE {column} < clock_timestamp() - %s::INTERVAL",
                    (interval,),
                )
                deleted[table] = max(int(cursor.rowcount or 0), 0)
            grace = f"{self.settings.closed_market_hot_state_grace_hours} hours"
            evicted = await connection.execute(
                """
                WITH stale_markets AS (
                    SELECT market.id FROM markets market
                    JOIN market_archive_finalizations final
                      ON final.market_id = market.id
                    WHERE market.exchange = 'polymarket'
                      AND (NOT market.is_active OR NOT market.is_tradable)
                      AND COALESCE(market.settlement_time, market.close_time,
                                   market.updated_at)
                          < clock_timestamp() - %s::INTERVAL
                )
                DELETE FROM current_orderbooks books
                USING stale_markets stale
                WHERE books.market_id = stale.id
                """,
                (grace,),
            )
            deleted["closed_market_current_orderbooks"] = max(
                int(evicted.rowcount or 0), 0
            )
            tiers = await connection.execute(
                """
                DELETE FROM market_collection_tiers tier
                USING markets market
                WHERE tier.market_id = market.id
                  AND market.exchange = 'polymarket'
                  AND (NOT market.is_active OR NOT market.is_tradable)
                  AND COALESCE(market.settlement_time, market.close_time,
                               market.updated_at)
                      < clock_timestamp() - %s::INTERVAL
                  AND (
                      EXISTS (
                          SELECT 1 FROM market_archive_finalizations final
                          WHERE final.market_id = market.id
                      )
                      OR NOT EXISTS (
                          SELECT 1 FROM current_orderbooks books
                          WHERE books.market_id = market.id
                      )
                  )
                """,
                (grace,),
            )
            deleted["closed_market_tiers"] = max(int(tiers.rowcount or 0), 0)
        return deleted

    async def status(
        self, *, migration_status: Mapping[str, object] | None = None
    ) -> dict[str, Any]:
        tables = [
            "series",
            "events",
            "markets",
            "outcomes",
            "trades",
            "current_orderbooks",
            "current_orderbook_levels",
            "microstructure_observations",
            "reference_price_updates",
            "sports_feed_updates",
            "raw_rest_payloads",
            "archive_objects",
            "data_gaps",
            "archive_degradation_events",
            "collector_write_failures",
        ]
        result: dict[str, Any] = {
            "scope": "polymarket_only",
            "database_connected": await self.ping(),
            "migrations": dict(migration_status or {}),
            "counts": {},
        }
        async with self.pool.connection() as connection:
            for table in tables:
                row = await (
                    await connection.execute(f"SELECT count(*) AS count FROM {table}")
                ).fetchone()
                result["counts"][table] = row["count"] if row else 0
            latest = await (
                await connection.execute(
                    """
                    SELECT
                        (SELECT max(executed_at) FROM trades) AS latest_trade,
                        (SELECT max(received_at) FROM current_orderbooks)
                            AS latest_orderbook,
                        (SELECT max(observed_at) FROM microstructure_observations)
                            AS latest_microstructure_observation,
                        (SELECT max(received_at) FROM reference_price_updates)
                            AS latest_reference_price,
                        (SELECT max(finished_at) FROM collector_runs
                         WHERE status = 'completed') AS latest_successful_run,
                        (SELECT max(uploaded_at) FROM archive_objects
                         WHERE status = 'uploaded') AS latest_archive_upload
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

            tier_rows = await (
                await connection.execute(_CURRENT_TIER_STATUS_SQL)
            ).fetchall()
            result["tiers"] = {
                tier: {"markets": 0, "ceiling_binding": 0, "last_evaluated_at": None}
                for tier in ("full_l2", "sampled", "metadata_only")
            }
            for row in tier_rows:
                result["tiers"][str(row["tier"])] = {
                    "markets": int(row["markets"]),
                    "ceiling_binding": int(row["ceiling_binding"]),
                    "last_evaluated_at": row["last_evaluated_at"],
                }

            archive_row = await (
                await connection.execute(
                    """
                    SELECT
                        count(*) FILTER (WHERE status = 'uploaded') AS objects_uploaded,
                        count(*) FILTER (WHERE status <> 'uploaded') AS objects_pending,
                        count(*) FILTER (WHERE status = 'failed') AS objects_failed,
                        COALESCE(sum(row_count) FILTER (WHERE status = 'uploaded'), 0)
                            AS rows_uploaded,
                        COALESCE(sum(uncompressed_bytes)
                            FILTER (WHERE status = 'uploaded'), 0)
                            AS uncompressed_bytes_uploaded,
                        COALESCE(sum(compressed_bytes)
                            FILTER (WHERE status = 'uploaded'), 0)
                            AS compressed_bytes_uploaded,
                        max(uploaded_at) AS latest_upload
                    FROM archive_objects
                    WHERE superseded_at IS NULL
                    """
                )
            ).fetchone()
            archive = dict(archive_row or {})
            compressed = int(archive.get("compressed_bytes_uploaded") or 0)
            uncompressed = int(archive.get("uncompressed_bytes_uploaded") or 0)
            archive["compression_ratio"] = (
                uncompressed / compressed if compressed else None
            )
            stream_rows = await (
                await connection.execute(
                    """
                    SELECT stream, count(*) AS objects,
                           COALESCE(sum(row_count), 0) AS rows,
                           COALESCE(sum(uncompressed_bytes), 0) AS uncompressed_bytes,
                           COALESCE(sum(compressed_bytes), 0) AS compressed_bytes,
                           COALESCE(sum(compressed_bytes) FILTER (
                               WHERE uploaded_at >= clock_timestamp() - INTERVAL '1 hour'
                           ), 0) AS bytes_last_hour
                    FROM archive_objects
                    WHERE status = 'uploaded' AND superseded_at IS NULL
                    GROUP BY stream ORDER BY stream
                    """
                )
            ).fetchall()
            archive["streams"] = {
                str(row["stream"]): {
                    "objects_total": int(row["objects"]),
                    "rows_total": int(row["rows"]),
                    "uncompressed_bytes_total": int(row["uncompressed_bytes"]),
                    "compressed_bytes_total": int(row["compressed_bytes"]),
                    "bytes_last_hour": int(row["bytes_last_hour"]),
                }
                for row in stream_rows
            }
            open_degradation = await (
                await connection.execute(
                    """
                    SELECT count(*) AS count
                    FROM archive_degradation_events WHERE resolved_at IS NULL
                    """
                )
            ).fetchone()
            archive["open_degradation_events"] = int(
                open_degradation["count"] if open_degradation else 0
            )

            storage = await (
                await connection.execute(
                    """
                    SELECT observed_at, postgres_database_bytes,
                           postgres_growth_bytes_per_hour, major_table_bytes,
                           archive_queue_rows, archive_queue_bytes,
                           archive_oldest_queued_age_seconds,
                           archive_growth_bytes_per_hour, spool_bytes,
                           pressure_state
                    FROM storage_metrics ORDER BY observed_at DESC LIMIT 1
                    """
                )
            ).fetchone()
            if storage is None:
                snapshot = await self.storage_snapshot()
                result["postgres"] = {
                    "observed_at": snapshot["observed_at"],
                    "database_bytes": snapshot["postgres_database_bytes"],
                    "estimated_growth_bytes_per_hour": None,
                    "major_table_bytes": snapshot["major_table_bytes"],
                    "pressure_state": "unknown",
                }
                archive.update(
                    {
                        "queue_depth": None,
                        "queue_bytes": None,
                        "oldest_queued_seconds": None,
                        "spool_bytes": None,
                        "estimated_growth_bytes_per_hour": None,
                    }
                )
            else:
                result["postgres"] = {
                    "observed_at": storage["observed_at"],
                    "database_bytes": int(storage["postgres_database_bytes"]),
                    "estimated_growth_bytes_per_hour": storage[
                        "postgres_growth_bytes_per_hour"
                    ],
                    "major_table_bytes": storage["major_table_bytes"],
                    "pressure_state": storage["pressure_state"],
                }
                archive.update(
                    {
                        "queue_depth": int(storage["archive_queue_rows"]),
                        "queue_bytes": int(storage["archive_queue_bytes"]),
                        "oldest_queued_seconds": storage[
                            "archive_oldest_queued_age_seconds"
                        ],
                        "spool_bytes": int(storage["spool_bytes"]),
                        "estimated_growth_bytes_per_hour": storage[
                            "archive_growth_bytes_per_hour"
                        ],
                    }
                )
            archive["healthy"] = bool(
                int(archive.get("objects_failed") or 0) == 0
                and int(archive.get("open_degradation_events") or 0) == 0
            )
            result["archive"] = archive

            live_run = await (
                await connection.execute(
                    """
                    SELECT id, started_at, metadata
                    FROM collector_runs
                    WHERE job_type = 'live' AND status = 'running'
                    ORDER BY started_at DESC LIMIT 1
                    """
                )
            ).fetchone()
            live: dict[str, Any] = {"polymarket": {"discovery_state": "stopped"}}
            if live_run:
                row = await (
                    await connection.execute(
                        """
                        WITH latest_decision AS (
                            SELECT max(evaluated_at) AS evaluated_at
                            FROM live_market_subscription_decisions
                            WHERE collector_run_id = %s AND exchange = 'polymarket'
                        )
                        SELECT
                            (SELECT count(*) FROM collector_connections
                             WHERE collector_run_id = %s AND exchange = 'polymarket'
                               AND status = 'connected' AND disconnected_at IS NULL)
                                AS connections_active,
                            (SELECT count(DISTINCT ccm.market_external_id)
                             FROM collector_connections cc
                             JOIN collector_connection_markets ccm
                               ON ccm.connection_id = cc.id
                             WHERE cc.collector_run_id = %s
                               AND cc.exchange = 'polymarket'
                               AND cc.status = 'connected'
                               AND cc.disconnected_at IS NULL
                               AND ccm.unsubscribed_at IS NULL)
                                AS markets_confirmed_subscribed,
                            (SELECT count(*)
                             FROM live_market_subscription_decisions d,
                                  latest_decision latest
                             WHERE d.collector_run_id = %s
                               AND d.exchange = 'polymarket'
                               AND d.evaluated_at = latest.evaluated_at
                               AND d.is_eligible) AS markets_selected,
                            (SELECT max(co.received_at)
                             FROM current_orderbooks co
                             JOIN markets m ON m.id = co.market_id
                             WHERE m.exchange = 'polymarket') AS latest_ws_message,
                            (SELECT max(d.evaluated_at)
                             FROM live_market_subscription_decisions d
                             WHERE d.collector_run_id = %s
                               AND d.exchange = 'polymarket')
                                AS latest_complete_discovery,
                            (SELECT count(*) FROM data_gaps g
                             WHERE g.collector_run_id = %s
                               AND g.exchange = 'polymarket'
                               AND g.channel = 'rest:market_discovery'
                               AND g.gap_type IN (
                                   'discovery_refresh_failed',
                                   'market_discovery_refresh_failed'
                               )
                               AND g.status IN ('open', 'reconciling'))
                                AS open_discovery_refresh_failures,
                            (SELECT count(*) FROM data_gaps g
                             WHERE g.collector_run_id = %s
                               AND g.exchange = 'polymarket'
                               AND g.channel = 'rest:market_discovery'
                               AND g.gap_type = 'market_metadata_schema_failure'
                               AND g.status IN ('open', 'reconciling'))
                                AS open_metadata_schema_warnings,
                            (SELECT count(*) FROM data_gaps g
                             WHERE g.collector_run_id = %s
                               AND g.exchange = 'polymarket'
                               AND g.channel = 'rest:market_discovery'
                               AND g.gap_type NOT IN (
                                   'discovery_refresh_failed',
                                   'market_discovery_refresh_failed'
                               )
                               AND g.status IN ('open', 'reconciling'))
                                AS open_discovery_coverage_warnings
                        """,
                        tuple(live_run["id"] for _ in range(8)),
                    )
                ).fetchone()
                state = dict(row or {})
                state.update(
                    _discovery_status(
                        latest_complete_discovery=state.get(
                            "latest_complete_discovery"
                        ),
                        open_refresh_failures=int(
                            state.get("open_discovery_refresh_failures") or 0
                        ),
                        open_coverage_warnings=int(
                            state.get("open_discovery_coverage_warnings") or 0
                        ),
                        open_metadata_schema_warnings=int(
                            state.get("open_metadata_schema_warnings") or 0
                        ),
                    )
                )
                state["healthy"] = bool(
                    state["discovery_state"] == "ready"
                    and int(state.get("connections_active") or 0) > 0
                    and int(state.get("markets_confirmed_subscribed") or 0) > 0
                    and state.get("latest_ws_message") is not None
                )
                live["polymarket"] = state
                result["live_run"] = {
                    "id": live_run["id"],
                    "started_at": live_run["started_at"],
                }
            result["live"] = live
            result["healthy"] = bool(
                result["database_connected"]
                and bool((migration_status or {}).get("current", True))
                and live["polymarket"].get("healthy")
                and archive["healthy"]
                and result["postgres"].get("pressure_state") != "critical"
            )
        return result


def _lookup_prefix() -> str:
    return """
        WITH resolved AS (
            SELECT m.id AS market_id, o.id AS outcome_id
            FROM markets m
            LEFT JOIN outcomes o
              ON o.market_id = m.id
             AND (%(outcome_external_id)s::TEXT IS NOT NULL)
             AND (o.external_id = %(outcome_external_id)s::TEXT
                  OR o.token_id = %(outcome_external_id)s::TEXT)
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
    data.setdefault("observation_kind", "change")
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
    if kind == "current_orderbook_snapshots":
        data["bids"] = _json(data.get("bids", []))
        data["asks"] = _json(data.get("asks", []))
        return (
            prefix
            + """
            , header AS (
                INSERT INTO current_orderbooks
                    (outcome_id, market_id, collector_connection_id, valid,
                     source_timestamp, exchange_timestamp, received_at,
                     received_monotonic_ns, sequence_number, book_hash,
                     best_bid, best_ask, midpoint, spread, bid_depth, ask_depth,
                     level_count)
                SELECT outcome_id, market_id, %(connection_id)s, TRUE,
                       %(source_timestamp)s, %(exchange_timestamp)s, %(received_at)s,
                       %(received_monotonic_ns)s, %(sequence_number)s, %(book_hash)s,
                       %(best_bid)s, %(best_ask)s, %(midpoint)s, %(spread)s,
                       %(bid_depth)s, %(ask_depth)s, %(level_count)s
                FROM resolved WHERE outcome_id IS NOT NULL
                ON CONFLICT (outcome_id) DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    collector_connection_id = EXCLUDED.collector_connection_id,
                    valid = TRUE,
                    source_timestamp = EXCLUDED.source_timestamp,
                    exchange_timestamp = EXCLUDED.exchange_timestamp,
                    received_at = EXCLUDED.received_at,
                    received_monotonic_ns = EXCLUDED.received_monotonic_ns,
                    sequence_number = EXCLUDED.sequence_number,
                    book_hash = EXCLUDED.book_hash,
                    best_bid = EXCLUDED.best_bid, best_ask = EXCLUDED.best_ask,
                    midpoint = EXCLUDED.midpoint, spread = EXCLUDED.spread,
                    bid_depth = EXCLUDED.bid_depth, ask_depth = EXCLUDED.ask_depth,
                    level_count = EXCLUDED.level_count,
                    updated_at = clock_timestamp()
                WHERE current_orderbooks.received_monotonic_ns
                      <= EXCLUDED.received_monotonic_ns
                RETURNING outcome_id
            ), removed AS (
                DELETE FROM current_orderbook_levels levels
                USING header WHERE levels.outcome_id = header.outcome_id
            ), levels AS (
                INSERT INTO current_orderbook_levels
                    (outcome_id, side, price, size, received_at, received_monotonic_ns)
                SELECT header.outcome_id, side_name, level.price::NUMERIC,
                       level.size::NUMERIC, %(received_at)s,
                       %(received_monotonic_ns)s
                FROM header
                CROSS JOIN LATERAL (
                    SELECT DISTINCT ON (side_name, price::NUMERIC)
                           side_name, price::NUMERIC AS price, size
                    FROM (
                        SELECT 'buy'::TEXT AS side_name,
                               value->>0 AS price, value->>1 AS size, ordinal
                        FROM jsonb_array_elements(%(bids)s)
                             WITH ORDINALITY AS bid(value, ordinal)
                        UNION ALL
                        SELECT 'sell'::TEXT, value->>0, value->>1, ordinal
                        FROM jsonb_array_elements(%(asks)s)
                             WITH ORDINALITY AS ask(value, ordinal)
                    ) raw_level
                    WHERE size::NUMERIC > 0
                    ORDER BY side_name, price::NUMERIC, ordinal DESC
                ) level
                ON CONFLICT (outcome_id, side, price) DO UPDATE SET
                    size = EXCLUDED.size,
                    received_at = EXCLUDED.received_at,
                    received_monotonic_ns = EXCLUDED.received_monotonic_ns
                RETURNING outcome_id
            )
            SELECT count(*) FROM header
            """,
            data,
        )
    if kind == "current_orderbook_updates":
        return (
            prefix
            + """
            , header AS (
                INSERT INTO current_orderbooks
                    (outcome_id, market_id, collector_connection_id, valid,
                     source_timestamp, exchange_timestamp, received_at,
                     received_monotonic_ns, sequence_number, book_hash,
                     best_bid, best_ask, midpoint, spread, bid_depth, ask_depth,
                     level_count)
                SELECT outcome_id, market_id, %(connection_id)s, TRUE,
                       %(source_timestamp)s, %(exchange_timestamp)s, %(received_at)s,
                       %(received_monotonic_ns)s, %(sequence_number)s, %(book_hash)s,
                       %(best_bid)s, %(best_ask)s, %(midpoint)s, %(spread)s,
                       %(bid_depth)s, %(ask_depth)s, %(level_count)s
                FROM resolved WHERE outcome_id IS NOT NULL
                ON CONFLICT (outcome_id) DO UPDATE SET
                    collector_connection_id = EXCLUDED.collector_connection_id,
                    valid = TRUE,
                    source_timestamp = EXCLUDED.source_timestamp,
                    exchange_timestamp = EXCLUDED.exchange_timestamp,
                    received_at = EXCLUDED.received_at,
                    received_monotonic_ns = EXCLUDED.received_monotonic_ns,
                    sequence_number = EXCLUDED.sequence_number,
                    book_hash = EXCLUDED.book_hash,
                    best_bid = EXCLUDED.best_bid, best_ask = EXCLUDED.best_ask,
                    midpoint = EXCLUDED.midpoint, spread = EXCLUDED.spread,
                    bid_depth = EXCLUDED.bid_depth, ask_depth = EXCLUDED.ask_depth,
                    level_count = EXCLUDED.level_count,
                    updated_at = clock_timestamp()
                WHERE current_orderbooks.received_monotonic_ns
                      <= EXCLUDED.received_monotonic_ns
                RETURNING outcome_id
            ), changed AS (
                INSERT INTO current_orderbook_levels
                    (outcome_id, side, price, size, received_at, received_monotonic_ns)
                SELECT outcome_id, %(side)s, %(price)s, %(size)s,
                       %(received_at)s, %(received_monotonic_ns)s
                FROM header WHERE %(size)s > 0
                ON CONFLICT (outcome_id, side, price) DO UPDATE SET
                    size = EXCLUDED.size, received_at = EXCLUDED.received_at,
                    received_monotonic_ns = EXCLUDED.received_monotonic_ns
                WHERE current_orderbook_levels.size IS DISTINCT FROM EXCLUDED.size
            ), removed AS (
                DELETE FROM current_orderbook_levels levels USING header
                WHERE levels.outcome_id = header.outcome_id
                  AND levels.side = %(side)s AND levels.price = %(price)s
                  AND %(size)s <= 0
            )
            SELECT count(*) FROM header
            """,
            data,
        )
    if kind == "microstructure_observations":
        return (
            prefix
            + """
            INSERT INTO microstructure_observations
                (market_id, outcome_id, tier, observed_at, source_timestamp,
                 received_at, best_bid, best_ask, midpoint, spread, spread_bps,
                 bid_depth_top, ask_depth_top, bid_depth_1pct, ask_depth_1pct,
                 bid_depth_total, ask_depth_total, book_imbalance,
                 last_trade_price, recent_trade_count, recent_trade_volume,
                  recent_update_count, observation_kind)
            SELECT market_id, outcome_id, %(tier)s, %(observed_at)s,
                   %(source_timestamp)s, %(received_at)s, %(best_bid)s,
                   %(best_ask)s, %(midpoint)s, %(spread)s, %(spread_bps)s,
                   %(bid_depth_top)s, %(ask_depth_top)s, %(bid_depth_1pct)s,
                   %(ask_depth_1pct)s, %(bid_depth_total)s, %(ask_depth_total)s,
                   %(book_imbalance)s, %(last_trade_price)s,
                   %(recent_trade_count)s, %(recent_trade_volume)s,
                   %(recent_update_count)s, %(observation_kind)s
            FROM resolved WHERE outcome_id IS NOT NULL
            ON CONFLICT (outcome_id, tier, observed_at) DO NOTHING
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
