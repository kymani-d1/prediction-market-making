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
DEFAULT_KALSHI_API_URL = "https://external-api.kalshi.com/trade-api/v2"
DEFAULT_KALSHI_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
DEFAULT_POLYMARKET_EQUITY_SYMBOLS = frozenset(
    {
        "AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "NFLX",
        "PLTR", "OPEN", "RKLB", "ABNB", "COIN", "HOOD", "QQQ", "SPY",
        "EWY", "VXX", "EURUSD", "GBPUSD", "USDCAD", "USDJPY", "USDKRW",
        "XAUUSD", "XAGUSD", "WTI", "CC", "NGD",
    }
)


class ConfigurationError(ValueError):
    """Raised when configuration is unsafe or internally inconsistent."""


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
    value: str | int | None,
    default: int,
    *,
    name: str,
    minimum: int = 0,
) -> int:
    try:
        parsed = default if value is None or value == "" else int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
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

    polymarket_enabled: bool = True
    kalshi_enabled: bool = True
    kalshi_api_key_id: str | None = field(default=None, repr=False)
    kalshi_private_key_path: Path | None = field(default=None, repr=False)

    store_raw_ws: bool = True
    store_raw_rest: bool = True
    polymarket_rtds_enabled: bool = True
    polymarket_sports_enabled: bool = True
    polymarket_comments_enabled: bool = True
    kalshi_reference_feeds_enabled: bool = True

    metadata_sync_interval_seconds: int = 300
    economics_sync_interval_seconds: int = 3600
    polymarket_fee_rate_sync_interval_seconds: int = 21_600
    market_snapshot_interval_seconds: int = 60
    orderbook_reconcile_interval_seconds: int = 300
    metrics_log_interval_seconds: int = 60
    http_concurrency: int = 8
    http_timeout_seconds: int = 30
    http_max_attempts: int = 6
    database_pool_min_size: int = 1
    database_pool_max_size: int = 8
    database_batch_size: int = 500
    database_flush_interval_seconds: int = 2
    database_queue_size: int = 50_000
    polymarket_ws_subscription_chunk_size: int = 500
    kalshi_ws_subscription_chunk_size: int = 100

    max_live_markets: int = 0
    min_live_market_volume: Decimal = Decimal("0")
    min_live_market_liquidity: Decimal = Decimal("0")
    live_market_allowlist: frozenset[str] = field(default_factory=frozenset)
    live_market_blocklist: frozenset[str] = field(default_factory=frozenset)
    polymarket_equity_symbols: frozenset[str] = DEFAULT_POLYMARKET_EQUITY_SYMBOLS

    log_level: str = "INFO"
    json_logs: bool = True
    polymarket_gamma_url: str = DEFAULT_POLYMARKET_GAMMA_URL
    polymarket_data_url: str = DEFAULT_POLYMARKET_DATA_URL
    polymarket_clob_url: str = DEFAULT_POLYMARKET_CLOB_URL
    polymarket_ws_url: str = DEFAULT_POLYMARKET_WS_URL
    polymarket_rtds_url: str = DEFAULT_POLYMARKET_RTDS_URL
    polymarket_sports_ws_url: str = DEFAULT_POLYMARKET_SPORTS_WS_URL
    kalshi_api_url: str = DEFAULT_KALSHI_API_URL
    kalshi_ws_url: str = DEFAULT_KALSHI_WS_URL

    @property
    def database_dsn(self) -> str:
        if self.database_url:
            return self.database_url
        user = quote_plus(self.postgres_user)
        password = quote_plus(self.postgres_password)
        database = quote_plus(self.postgres_db)
        return (
            f"postgresql://{user}:{password}@{self.postgres_host}:"
            f"{self.postgres_port}/{database}"
        )

    @property
    def kalshi_websocket_configured(self) -> bool:
        return bool(
            self.kalshi_api_key_id
            and self.kalshi_private_key_path
            and self.kalshi_private_key_path.is_file()
        )

    def safe_summary(self) -> dict[str, object]:
        return {
            "polymarket_enabled": self.polymarket_enabled,
            "kalshi_enabled": self.kalshi_enabled,
            "kalshi_websocket_configured": self.kalshi_websocket_configured,
            "store_raw_ws": self.store_raw_ws,
            "economics_sync_interval_seconds": self.economics_sync_interval_seconds,
            "polymarket_fee_rate_sync_interval_seconds": (
                self.polymarket_fee_rate_sync_interval_seconds
            ),
            "max_live_markets": self.max_live_markets,
            "min_live_market_volume": str(self.min_live_market_volume),
            "min_live_market_liquidity": str(self.min_live_market_liquidity),
            "allowlist_entries": len(self.live_market_allowlist),
            "blocklist_entries": len(self.live_market_blocklist),
            "http_concurrency": self.http_concurrency,
            "log_level": self.log_level,
            "json_logs": self.json_logs,
        }

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        load_dotenv_file: bool = True,
    ) -> "Settings":
        if load_dotenv_file and env is None:
            load_dotenv(override=False)
        source: Mapping[str, str] = os.environ if env is None else env

        def get(name: str, default: str | None = None) -> str | None:
            return source.get(name, default)

        private_key = get("KALSHI_PRIVATE_KEY_PATH")
        settings = cls(
            postgres_user=get("POSTGRES_USER", "prediction_collector") or "prediction_collector",
            postgres_password=get("POSTGRES_PASSWORD", "prediction_collector") or "prediction_collector",
            postgres_db=get("POSTGRES_DB", "prediction_markets") or "prediction_markets",
            postgres_host=get("POSTGRES_HOST", "127.0.0.1") or "127.0.0.1",
            postgres_port=_int(get("POSTGRES_PORT"), 5432, name="POSTGRES_PORT", minimum=1),
            database_url=get("DATABASE_URL"),
            polymarket_enabled=_bool(get("POLYMARKET_ENABLED"), True),
            kalshi_enabled=_bool(get("KALSHI_ENABLED"), True),
            kalshi_api_key_id=get("KALSHI_API_KEY_ID"),
            kalshi_private_key_path=Path(private_key).expanduser() if private_key else None,
            store_raw_ws=_bool(get("STORE_RAW_WS"), True),
            store_raw_rest=_bool(get("STORE_RAW_REST"), True),
            polymarket_rtds_enabled=_bool(get("POLYMARKET_RTDS_ENABLED"), True),
            polymarket_sports_enabled=_bool(get("POLYMARKET_SPORTS_ENABLED"), True),
            polymarket_comments_enabled=_bool(get("POLYMARKET_COMMENTS_ENABLED"), True),
            kalshi_reference_feeds_enabled=_bool(
                get("KALSHI_REFERENCE_FEEDS_ENABLED"), True
            ),
            metadata_sync_interval_seconds=_int(
                get("METADATA_SYNC_INTERVAL_SECONDS"),
                300,
                name="METADATA_SYNC_INTERVAL_SECONDS",
                minimum=5,
            ),
            economics_sync_interval_seconds=_int(
                get("ECONOMICS_SYNC_INTERVAL_SECONDS"),
                3600,
                name="ECONOMICS_SYNC_INTERVAL_SECONDS",
                minimum=60,
            ),
            polymarket_fee_rate_sync_interval_seconds=_int(
                get("POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS"),
                21_600,
                name="POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS",
                minimum=900,
            ),
            market_snapshot_interval_seconds=_int(
                get("MARKET_SNAPSHOT_INTERVAL_SECONDS"),
                60,
                name="MARKET_SNAPSHOT_INTERVAL_SECONDS",
                minimum=1,
            ),
            orderbook_reconcile_interval_seconds=_int(
                get("ORDERBOOK_RECONCILE_INTERVAL_SECONDS"),
                300,
                name="ORDERBOOK_RECONCILE_INTERVAL_SECONDS",
                minimum=5,
            ),
            metrics_log_interval_seconds=_int(
                get("METRICS_LOG_INTERVAL_SECONDS"),
                60,
                name="METRICS_LOG_INTERVAL_SECONDS",
                minimum=10,
            ),
            http_concurrency=_int(
                get("HTTP_CONCURRENCY"), 8, name="HTTP_CONCURRENCY", minimum=1
            ),
            http_timeout_seconds=_int(
                get("HTTP_TIMEOUT_SECONDS"), 30, name="HTTP_TIMEOUT_SECONDS", minimum=1
            ),
            http_max_attempts=_int(
                get("HTTP_MAX_ATTEMPTS"), 6, name="HTTP_MAX_ATTEMPTS", minimum=1
            ),
            database_pool_min_size=_int(
                get("DATABASE_POOL_MIN_SIZE"),
                1,
                name="DATABASE_POOL_MIN_SIZE",
                minimum=1,
            ),
            database_pool_max_size=_int(
                get("DATABASE_POOL_MAX_SIZE"),
                8,
                name="DATABASE_POOL_MAX_SIZE",
                minimum=1,
            ),
            database_batch_size=_int(
                get("DATABASE_BATCH_SIZE"), 500, name="DATABASE_BATCH_SIZE", minimum=1
            ),
            database_flush_interval_seconds=_int(
                get("DATABASE_FLUSH_INTERVAL_SECONDS"),
                2,
                name="DATABASE_FLUSH_INTERVAL_SECONDS",
                minimum=1,
            ),
            database_queue_size=_int(
                get("DATABASE_QUEUE_SIZE"),
                50_000,
                name="DATABASE_QUEUE_SIZE",
                minimum=100,
            ),
            polymarket_ws_subscription_chunk_size=_int(
                get("POLYMARKET_WS_SUBSCRIPTION_CHUNK_SIZE"),
                500,
                name="POLYMARKET_WS_SUBSCRIPTION_CHUNK_SIZE",
                minimum=1,
            ),
            kalshi_ws_subscription_chunk_size=_int(
                get("KALSHI_WS_SUBSCRIPTION_CHUNK_SIZE"),
                100,
                name="KALSHI_WS_SUBSCRIPTION_CHUNK_SIZE",
                minimum=1,
            ),
            max_live_markets=_int(
                get("MAX_LIVE_MARKETS"), 0, name="MAX_LIVE_MARKETS", minimum=0
            ),
            min_live_market_volume=_decimal(
                get("MIN_LIVE_MARKET_VOLUME"),
                Decimal("0"),
                name="MIN_LIVE_MARKET_VOLUME",
            ),
            min_live_market_liquidity=_decimal(
                get("MIN_LIVE_MARKET_LIQUIDITY"),
                Decimal("0"),
                name="MIN_LIVE_MARKET_LIQUIDITY",
            ),
            live_market_allowlist=_csv(get("LIVE_MARKET_ALLOWLIST")),
            live_market_blocklist=_csv(get("LIVE_MARKET_BLOCKLIST")),
            polymarket_equity_symbols=(
                _csv(get("POLYMARKET_EQUITY_SYMBOLS"))
                if get("POLYMARKET_EQUITY_SYMBOLS") is not None
                else DEFAULT_POLYMARKET_EQUITY_SYMBOLS
            ),
            log_level=(get("LOG_LEVEL", "INFO") or "INFO").upper(),
            json_logs=_bool(get("JSON_LOGS"), True),
            polymarket_gamma_url=(
                get("POLYMARKET_GAMMA_URL", DEFAULT_POLYMARKET_GAMMA_URL)
                or DEFAULT_POLYMARKET_GAMMA_URL
            ).rstrip("/"),
            polymarket_data_url=(
                get("POLYMARKET_DATA_URL", DEFAULT_POLYMARKET_DATA_URL)
                or DEFAULT_POLYMARKET_DATA_URL
            ).rstrip("/"),
            polymarket_clob_url=(
                get("POLYMARKET_CLOB_URL", DEFAULT_POLYMARKET_CLOB_URL)
                or DEFAULT_POLYMARKET_CLOB_URL
            ).rstrip("/"),
            polymarket_ws_url=get("POLYMARKET_WS_URL", DEFAULT_POLYMARKET_WS_URL)
            or DEFAULT_POLYMARKET_WS_URL,
            polymarket_rtds_url=get("POLYMARKET_RTDS_URL", DEFAULT_POLYMARKET_RTDS_URL)
            or DEFAULT_POLYMARKET_RTDS_URL,
            polymarket_sports_ws_url=get(
                "POLYMARKET_SPORTS_WS_URL", DEFAULT_POLYMARKET_SPORTS_WS_URL
            )
            or DEFAULT_POLYMARKET_SPORTS_WS_URL,
            kalshi_api_url=(get("KALSHI_API_URL", DEFAULT_KALSHI_API_URL) or DEFAULT_KALSHI_API_URL).rstrip(
                "/"
            ),
            kalshi_ws_url=get("KALSHI_WS_URL", DEFAULT_KALSHI_WS_URL) or DEFAULT_KALSHI_WS_URL,
        )
        if settings.database_pool_min_size > settings.database_pool_max_size:
            raise ConfigurationError(
                "DATABASE_POOL_MIN_SIZE cannot exceed DATABASE_POOL_MAX_SIZE"
            )
        overlap = settings.live_market_allowlist & settings.live_market_blocklist
        if overlap:
            raise ConfigurationError(
                "LIVE_MARKET_ALLOWLIST and LIVE_MARKET_BLOCKLIST overlap: "
                + ", ".join(sorted(overlap))
            )
        return settings
