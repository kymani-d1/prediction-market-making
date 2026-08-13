from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping
from urllib.parse import quote_plus

from dotenv import load_dotenv


DEFAULT_POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_POLYMARKET_DATA_URL = "https://data-api.polymarket.com"
DEFAULT_POLYMARKET_CLOB_URL = "https://clob.polymarket.com"
DEFAULT_POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_POLYMARKET_RTDS_URL = "wss://ws-live-data.polymarket.com"
DEFAULT_POLYMARKET_SPORTS_WS_URL = "wss://sports-api.polymarket.com/ws"
DEFAULT_POLYMARKET_EQUITY_SYMBOLS = frozenset(
    {
        "AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "NFLX",
        "PLTR", "OPEN", "RKLB", "ABNB", "COIN", "HOOD", "QQQ", "SPY",
        "EWY", "VXX", "EURUSD", "GBPUSD", "USDCAD", "USDJPY", "USDKRW",
        "XAUUSD", "XAGUSD", "WTI", "CC", "NGD",
    }
)


class ConfigurationError(ValueError):
    pass


def _bool(value: str | bool | None, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"Expected a boolean, got {value!r}")


def _int(
    value: str | int | None, default: int, *, name: str, minimum: int = 0
) -> int:
    try:
        parsed = default if value is None or value == "" else int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if parsed < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}")
    return parsed


def _float(
    value: str | float | None, default: float, *, name: str, minimum: float = 0
) -> float:
    try:
        parsed = default if value is None or value == "" else float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if parsed < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}")
    return parsed


def _decimal(value: str | Decimal | None, default: Decimal, *, name: str) -> Decimal:
    try:
        parsed = default if value is None or value == "" else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ConfigurationError(f"{name} must be a decimal number") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ConfigurationError(f"{name} must be a finite value >= 0")
    return parsed


def _csv(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    postgres_user: str = "prediction_collector"
    postgres_password: str = field(default="prediction_collector", repr=False)
    postgres_db: str = "prediction_markets"
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    database_url: str | None = field(default=None, repr=False)

    polymarket_rtds_enabled: bool = True
    polymarket_sports_enabled: bool = True
    polymarket_comments_enabled: bool = False

    metadata_sync_interval_seconds: int = 300
    economics_sync_interval_seconds: int = 3600
    polymarket_fee_rate_sync_interval_seconds: int = 21_600
    # The complete tick stream for FULL_L2 lives in Parquet. PostgreSQL keeps
    # only coarse research-ready observations, so these conservative defaults
    # do not recreate the old unbounded hot-store growth through derived rows.
    full_l2_observation_interval_seconds: int = 300
    sampled_snapshot_interval_seconds: int = 60
    orderbook_reconcile_interval_seconds: int = 300
    tier_reevaluation_interval_seconds: int = 300
    tier_activity_window_seconds: int = 900
    metrics_log_interval_seconds: int = 60
    storage_metrics_interval_seconds: int = 300
    retention_interval_seconds: int = 3600
    http_concurrency: int = 8
    http_timeout_seconds: int = 30
    http_max_attempts: int = 6
    database_pool_min_size: int = 1
    database_pool_max_size: int = 8
    database_batch_size: int = 500
    database_flush_interval_seconds: float = 2
    database_queue_size: int = 50_000
    polymarket_ws_subscription_chunk_size: int = 500

    full_l2_max_markets: int = 500
    sampled_max_markets: int = 1_000
    full_l2_min_score: Decimal = Decimal("55")
    full_l2_min_liquidity: Decimal = Decimal("1000")
    full_l2_min_recent_trades: int = 2
    full_l2_min_book_updates: int = 100
    full_l2_market_allowlist: frozenset[str] = field(default_factory=frozenset)
    live_market_blocklist: frozenset[str] = field(default_factory=frozenset)

    s3_endpoint_url: str | None = None
    s3_bucket: str | None = None
    s3_region: str = "auto"
    s3_access_key_id: str | None = field(default=None, repr=False)
    s3_secret_access_key: str | None = field(default=None, repr=False)
    s3_prefix: str = "prediction-market-archive"
    s3_url_style: str = "virtual"
    archive_batch_rows: int = 25_000
    archive_batch_bytes: int = 32 * 1024 * 1024
    archive_flush_seconds: float = 15
    archive_compression: str = "zstd"
    archive_queue_max_rows: int = 50_000
    archive_queue_max_bytes: int = 64 * 1024 * 1024
    archive_queue_warn_rows: int = 35_000
    archive_queue_critical_rows: int = 47_500
    archive_queue_warn_bytes: int = 48 * 1024 * 1024
    archive_queue_critical_bytes: int = 60 * 1024 * 1024
    archive_enqueue_timeout_seconds: float = 5
    archive_spool_directory: Path = Path("/tmp/prediction-collector-archive")
    archive_spool_max_bytes: int = 2 * 1024 * 1024 * 1024
    archive_upload_max_attempts: int = 6
    raw_ws_policy: str = "errors"

    postgres_storage_warn_gb: Decimal = Decimal("70")
    postgres_storage_critical_gb: Decimal = Decimal("85")
    postgres_reference_retention_hours: int = 6
    postgres_observation_retention_hours: int = 24

    polymarket_equity_symbols: frozenset[str] = DEFAULT_POLYMARKET_EQUITY_SYMBOLS
    log_level: str = "INFO"
    json_logs: bool = True
    polymarket_gamma_url: str = DEFAULT_POLYMARKET_GAMMA_URL
    polymarket_data_url: str = DEFAULT_POLYMARKET_DATA_URL
    polymarket_clob_url: str = DEFAULT_POLYMARKET_CLOB_URL
    polymarket_ws_url: str = DEFAULT_POLYMARKET_WS_URL
    polymarket_rtds_url: str = DEFAULT_POLYMARKET_RTDS_URL
    polymarket_sports_ws_url: str = DEFAULT_POLYMARKET_SPORTS_WS_URL

    @property
    def database_dsn(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql://{quote_plus(self.postgres_user)}:"
            f"{quote_plus(self.postgres_password)}@{self.postgres_host}:"
            f"{self.postgres_port}/{quote_plus(self.postgres_db)}"
        )

    @property
    def archive_configured(self) -> bool:
        return bool(
            self.s3_endpoint_url
            and self.s3_bucket
            and self.s3_access_key_id
            and self.s3_secret_access_key
        )

    def require_archive(self) -> None:
        if not self.archive_configured:
            raise ConfigurationError(
                "Continuous collection requires S3_ENDPOINT_URL, S3_BUCKET, "
                "S3_ACCESS_KEY_ID, and S3_SECRET_ACCESS_KEY"
            )

    def safe_summary(self) -> dict[str, object]:
        return {
            "scope": "polymarket_only",
            "archive_configured": self.archive_configured,
            "s3_bucket": self.s3_bucket,
            "s3_prefix": self.s3_prefix,
            "raw_ws_policy": self.raw_ws_policy,
            "full_l2_max_markets": self.full_l2_max_markets,
            "sampled_max_markets": self.sampled_max_markets,
            "full_l2_min_score": str(self.full_l2_min_score),
            "full_l2_allowlist_entries": len(self.full_l2_market_allowlist),
            "blocklist_entries": len(self.live_market_blocklist),
            "archive_queue_max_rows": self.archive_queue_max_rows,
            "archive_queue_max_bytes": self.archive_queue_max_bytes,
            "postgres_storage_warn_gb": str(self.postgres_storage_warn_gb),
            "postgres_storage_critical_gb": str(self.postgres_storage_critical_gb),
            "http_concurrency": self.http_concurrency,
            "log_level": self.log_level,
            "json_logs": self.json_logs,
        }

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, *, load_dotenv_file: bool = True
    ) -> "Settings":
        if load_dotenv_file and env is None:
            load_dotenv(override=False)
        source: Mapping[str, str] = os.environ if env is None else env

        def get(name: str, default: str | None = None) -> str | None:
            return source.get(name, default)

        settings = cls(
            postgres_user=get("POSTGRES_USER", "prediction_collector") or "prediction_collector",
            postgres_password=get("POSTGRES_PASSWORD", "prediction_collector") or "prediction_collector",
            postgres_db=get("POSTGRES_DB", "prediction_markets") or "prediction_markets",
            postgres_host=get("POSTGRES_HOST", "127.0.0.1") or "127.0.0.1",
            postgres_port=_int(get("POSTGRES_PORT"), 5432, name="POSTGRES_PORT", minimum=1),
            database_url=get("DATABASE_URL"),
            polymarket_rtds_enabled=_bool(get("POLYMARKET_RTDS_ENABLED"), True),
            polymarket_sports_enabled=_bool(get("POLYMARKET_SPORTS_ENABLED"), True),
            polymarket_comments_enabled=_bool(get("POLYMARKET_COMMENTS_ENABLED"), False),
            metadata_sync_interval_seconds=_int(get("METADATA_SYNC_INTERVAL_SECONDS"), 300, name="METADATA_SYNC_INTERVAL_SECONDS", minimum=5),
            economics_sync_interval_seconds=_int(get("ECONOMICS_SYNC_INTERVAL_SECONDS"), 3600, name="ECONOMICS_SYNC_INTERVAL_SECONDS", minimum=60),
            polymarket_fee_rate_sync_interval_seconds=_int(get("POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS"), 21_600, name="POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS", minimum=900),
            full_l2_observation_interval_seconds=_int(get("FULL_L2_OBSERVATION_INTERVAL_SECONDS"), 300, name="FULL_L2_OBSERVATION_INTERVAL_SECONDS", minimum=1),
            sampled_snapshot_interval_seconds=_int(get("SAMPLED_SNAPSHOT_INTERVAL_SECONDS"), 60, name="SAMPLED_SNAPSHOT_INTERVAL_SECONDS", minimum=1),
            orderbook_reconcile_interval_seconds=_int(get("ORDERBOOK_RECONCILE_INTERVAL_SECONDS"), 300, name="ORDERBOOK_RECONCILE_INTERVAL_SECONDS", minimum=5),
            tier_reevaluation_interval_seconds=_int(get("TIER_REEVALUATION_INTERVAL_SECONDS"), 300, name="TIER_REEVALUATION_INTERVAL_SECONDS", minimum=10),
            tier_activity_window_seconds=_int(get("TIER_ACTIVITY_WINDOW_SECONDS"), 900, name="TIER_ACTIVITY_WINDOW_SECONDS", minimum=60),
            metrics_log_interval_seconds=_int(get("METRICS_LOG_INTERVAL_SECONDS"), 60, name="METRICS_LOG_INTERVAL_SECONDS", minimum=10),
            storage_metrics_interval_seconds=_int(get("STORAGE_METRICS_INTERVAL_SECONDS"), 300, name="STORAGE_METRICS_INTERVAL_SECONDS", minimum=60),
            retention_interval_seconds=_int(get("RETENTION_INTERVAL_SECONDS"), 3600, name="RETENTION_INTERVAL_SECONDS", minimum=300),
            http_concurrency=_int(get("HTTP_CONCURRENCY"), 8, name="HTTP_CONCURRENCY", minimum=1),
            http_timeout_seconds=_int(get("HTTP_TIMEOUT_SECONDS"), 30, name="HTTP_TIMEOUT_SECONDS", minimum=1),
            http_max_attempts=_int(get("HTTP_MAX_ATTEMPTS"), 6, name="HTTP_MAX_ATTEMPTS", minimum=1),
            database_pool_min_size=_int(get("DATABASE_POOL_MIN_SIZE"), 1, name="DATABASE_POOL_MIN_SIZE", minimum=1),
            database_pool_max_size=_int(get("DATABASE_POOL_MAX_SIZE"), 8, name="DATABASE_POOL_MAX_SIZE", minimum=1),
            database_batch_size=_int(get("DATABASE_BATCH_SIZE"), 500, name="DATABASE_BATCH_SIZE", minimum=1),
            database_flush_interval_seconds=_float(get("DATABASE_FLUSH_INTERVAL_SECONDS"), 2, name="DATABASE_FLUSH_INTERVAL_SECONDS", minimum=0.1),
            database_queue_size=_int(get("DATABASE_QUEUE_SIZE"), 50_000, name="DATABASE_QUEUE_SIZE", minimum=100),
            polymarket_ws_subscription_chunk_size=_int(get("POLYMARKET_WS_SUBSCRIPTION_CHUNK_SIZE"), 500, name="POLYMARKET_WS_SUBSCRIPTION_CHUNK_SIZE", minimum=1),
            full_l2_max_markets=_int(get("FULL_L2_MAX_MARKETS"), 500, name="FULL_L2_MAX_MARKETS"),
            sampled_max_markets=_int(get("SAMPLED_MAX_MARKETS"), 1_000, name="SAMPLED_MAX_MARKETS"),
            full_l2_min_score=_decimal(get("FULL_L2_MIN_SCORE"), Decimal("55"), name="FULL_L2_MIN_SCORE"),
            full_l2_min_liquidity=_decimal(get("FULL_L2_MIN_LIQUIDITY"), Decimal("1000"), name="FULL_L2_MIN_LIQUIDITY"),
            full_l2_min_recent_trades=_int(get("FULL_L2_MIN_RECENT_TRADES"), 2, name="FULL_L2_MIN_RECENT_TRADES"),
            full_l2_min_book_updates=_int(get("FULL_L2_MIN_BOOK_UPDATES"), 100, name="FULL_L2_MIN_BOOK_UPDATES"),
            full_l2_market_allowlist=_csv(get("FULL_L2_MARKET_ALLOWLIST")),
            live_market_blocklist=_csv(get("LIVE_MARKET_BLOCKLIST")),
            s3_endpoint_url=get("S3_ENDPOINT_URL") or get("AWS_ENDPOINT_URL"),
            s3_bucket=get("S3_BUCKET") or get("AWS_S3_BUCKET_NAME"),
            s3_region=get("S3_REGION") or get("AWS_DEFAULT_REGION") or "auto",
            s3_access_key_id=get("S3_ACCESS_KEY_ID") or get("AWS_ACCESS_KEY_ID"),
            s3_secret_access_key=get("S3_SECRET_ACCESS_KEY") or get("AWS_SECRET_ACCESS_KEY"),
            s3_prefix=(get("S3_PREFIX", "prediction-market-archive") or "prediction-market-archive").strip("/"),
            s3_url_style=(get("S3_URL_STYLE") or get("AWS_S3_URL_STYLE") or "virtual").lower(),
            archive_batch_rows=_int(get("ARCHIVE_BATCH_ROWS"), 25_000, name="ARCHIVE_BATCH_ROWS", minimum=1),
            archive_batch_bytes=_int(get("ARCHIVE_BATCH_BYTES"), 32 * 1024 * 1024, name="ARCHIVE_BATCH_BYTES", minimum=1024),
            archive_flush_seconds=_float(get("ARCHIVE_FLUSH_SECONDS"), 15, name="ARCHIVE_FLUSH_SECONDS", minimum=0.1),
            archive_compression=(get("ARCHIVE_COMPRESSION", "zstd") or "zstd").lower(),
            archive_queue_max_rows=_int(get("ARCHIVE_QUEUE_MAX_ROWS"), 50_000, name="ARCHIVE_QUEUE_MAX_ROWS", minimum=100),
            archive_queue_max_bytes=_int(get("ARCHIVE_QUEUE_MAX_BYTES"), 64 * 1024 * 1024, name="ARCHIVE_QUEUE_MAX_BYTES", minimum=1024),
            archive_queue_warn_rows=_int(get("ARCHIVE_QUEUE_WARN_ROWS"), 35_000, name="ARCHIVE_QUEUE_WARN_ROWS", minimum=1),
            archive_queue_critical_rows=_int(get("ARCHIVE_QUEUE_CRITICAL_ROWS"), 47_500, name="ARCHIVE_QUEUE_CRITICAL_ROWS", minimum=1),
            archive_queue_warn_bytes=_int(get("ARCHIVE_QUEUE_WARN_BYTES"), 48 * 1024 * 1024, name="ARCHIVE_QUEUE_WARN_BYTES", minimum=1024),
            archive_queue_critical_bytes=_int(get("ARCHIVE_QUEUE_CRITICAL_BYTES"), 60 * 1024 * 1024, name="ARCHIVE_QUEUE_CRITICAL_BYTES", minimum=1024),
            archive_enqueue_timeout_seconds=_float(get("ARCHIVE_ENQUEUE_TIMEOUT_SECONDS"), 5, name="ARCHIVE_ENQUEUE_TIMEOUT_SECONDS", minimum=0.1),
            archive_spool_directory=Path(get("ARCHIVE_SPOOL_DIRECTORY", "/tmp/prediction-collector-archive") or "/tmp/prediction-collector-archive"),
            archive_spool_max_bytes=_int(get("ARCHIVE_SPOOL_MAX_BYTES"), 2 * 1024 * 1024 * 1024, name="ARCHIVE_SPOOL_MAX_BYTES", minimum=1024),
            archive_upload_max_attempts=_int(get("ARCHIVE_UPLOAD_MAX_ATTEMPTS"), 6, name="ARCHIVE_UPLOAD_MAX_ATTEMPTS", minimum=1),
            raw_ws_policy=(get("RAW_WS_POLICY", "errors") or "errors").lower(),
            postgres_storage_warn_gb=_decimal(get("POSTGRES_STORAGE_WARN_GB"), Decimal("70"), name="POSTGRES_STORAGE_WARN_GB"),
            postgres_storage_critical_gb=_decimal(get("POSTGRES_STORAGE_CRITICAL_GB"), Decimal("85"), name="POSTGRES_STORAGE_CRITICAL_GB"),
            postgres_reference_retention_hours=_int(get("POSTGRES_REFERENCE_RETENTION_HOURS"), 6, name="POSTGRES_REFERENCE_RETENTION_HOURS", minimum=1),
            postgres_observation_retention_hours=_int(get("POSTGRES_OBSERVATION_RETENTION_HOURS"), 24, name="POSTGRES_OBSERVATION_RETENTION_HOURS", minimum=1),
            polymarket_equity_symbols=(_csv(get("POLYMARKET_EQUITY_SYMBOLS")) if get("POLYMARKET_EQUITY_SYMBOLS") is not None else DEFAULT_POLYMARKET_EQUITY_SYMBOLS),
            log_level=(get("LOG_LEVEL", "INFO") or "INFO").upper(),
            json_logs=_bool(get("JSON_LOGS"), True),
            polymarket_gamma_url=(get("POLYMARKET_GAMMA_URL", DEFAULT_POLYMARKET_GAMMA_URL) or DEFAULT_POLYMARKET_GAMMA_URL).rstrip("/"),
            polymarket_data_url=(get("POLYMARKET_DATA_URL", DEFAULT_POLYMARKET_DATA_URL) or DEFAULT_POLYMARKET_DATA_URL).rstrip("/"),
            polymarket_clob_url=(get("POLYMARKET_CLOB_URL", DEFAULT_POLYMARKET_CLOB_URL) or DEFAULT_POLYMARKET_CLOB_URL).rstrip("/"),
            polymarket_ws_url=get("POLYMARKET_WS_URL", DEFAULT_POLYMARKET_WS_URL) or DEFAULT_POLYMARKET_WS_URL,
            polymarket_rtds_url=get("POLYMARKET_RTDS_URL", DEFAULT_POLYMARKET_RTDS_URL) or DEFAULT_POLYMARKET_RTDS_URL,
            polymarket_sports_ws_url=get("POLYMARKET_SPORTS_WS_URL", DEFAULT_POLYMARKET_SPORTS_WS_URL) or DEFAULT_POLYMARKET_SPORTS_WS_URL,
        )
        if settings.database_pool_min_size > settings.database_pool_max_size:
            raise ConfigurationError("DATABASE_POOL_MIN_SIZE cannot exceed DATABASE_POOL_MAX_SIZE")
        if settings.postgres_storage_critical_gb <= settings.postgres_storage_warn_gb:
            raise ConfigurationError("POSTGRES_STORAGE_CRITICAL_GB must exceed POSTGRES_STORAGE_WARN_GB")
        if settings.raw_ws_policy not in {"none", "errors", "full_l2", "all"}:
            raise ConfigurationError("RAW_WS_POLICY must be none, errors, full_l2, or all")
        if settings.archive_compression != "zstd":
            raise ConfigurationError("ARCHIVE_COMPRESSION currently supports only zstd")
        if settings.s3_url_style not in {"virtual", "path", "auto"}:
            raise ConfigurationError("S3_URL_STYLE must be virtual, path, or auto")
        if settings.full_l2_market_allowlist & settings.live_market_blocklist:
            raise ConfigurationError("FULL_L2_MARKET_ALLOWLIST and LIVE_MARKET_BLOCKLIST overlap")
        if settings.archive_queue_warn_rows >= settings.archive_queue_max_rows:
            raise ConfigurationError("ARCHIVE_QUEUE_WARN_ROWS must be below ARCHIVE_QUEUE_MAX_ROWS")
        if settings.archive_queue_critical_rows > settings.archive_queue_max_rows:
            raise ConfigurationError("ARCHIVE_QUEUE_CRITICAL_ROWS cannot exceed ARCHIVE_QUEUE_MAX_ROWS")
        if settings.archive_queue_warn_bytes >= settings.archive_queue_max_bytes:
            raise ConfigurationError("ARCHIVE_QUEUE_WARN_BYTES must be below ARCHIVE_QUEUE_MAX_BYTES")
        if settings.archive_queue_critical_bytes > settings.archive_queue_max_bytes:
            raise ConfigurationError("ARCHIVE_QUEUE_CRITICAL_BYTES cannot exceed ARCHIVE_QUEUE_MAX_BYTES")
        return settings
