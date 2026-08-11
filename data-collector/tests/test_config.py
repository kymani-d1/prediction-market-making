from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from prediction_collector.config import ConfigurationError, Settings


def settings(env: dict[str, str] | None = None) -> Settings:
    return Settings.from_env(env or {}, load_dotenv_file=False)


def test_live_coverage_defaults_are_unrestricted() -> None:
    value = settings()

    assert value.economics_sync_interval_seconds == 3600
    assert value.polymarket_fee_rate_sync_interval_seconds == 21_600
    assert value.max_live_markets == 0
    assert value.min_live_market_volume == Decimal("0")
    assert value.min_live_market_liquidity == Decimal("0")
    assert value.live_market_allowlist == frozenset()
    assert value.live_market_blocklist == frozenset()


def test_env_parses_coverage_booleans_csv_decimals_and_urls() -> None:
    value = settings(
        {
            "MAX_LIVE_MARKETS": "17",
            "MIN_LIVE_MARKET_VOLUME": "1000.2500",
            "MIN_LIVE_MARKET_LIQUIDITY": "50.125",
            "LIVE_MARKET_ALLOWLIST": " poly:one, two,poly:one, ",
            "LIVE_MARKET_BLOCKLIST": "three",
            "POLYMARKET_ENABLED": "off",
            "STORE_RAW_WS": "YES",
            "POLYMARKET_GAMMA_URL": "https://example.test/gamma///",
            "LOG_LEVEL": "debug",
            "ECONOMICS_SYNC_INTERVAL_SECONDS": "900",
            "POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS": "7200",
        }
    )

    assert value.max_live_markets == 17
    assert value.min_live_market_volume == Decimal("1000.2500")
    assert value.min_live_market_liquidity == Decimal("50.125")
    assert value.live_market_allowlist == frozenset({"poly:one", "two"})
    assert value.live_market_blocklist == frozenset({"three"})
    assert value.polymarket_enabled is False
    assert value.store_raw_ws is True
    assert value.polymarket_gamma_url == "https://example.test/gamma"
    assert value.log_level == "DEBUG"
    assert value.economics_sync_interval_seconds == 900
    assert value.polymarket_fee_rate_sync_interval_seconds == 7200


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MAX_LIVE_MARKETS", "-1"),
        ("MAX_LIVE_MARKETS", "1.5"),
        ("MIN_LIVE_MARKET_VOLUME", "-0.01"),
        ("MIN_LIVE_MARKET_LIQUIDITY", "NaN"),
        ("MIN_LIVE_MARKET_LIQUIDITY", "Infinity"),
        ("POLYMARKET_ENABLED", "sometimes"),
        ("ECONOMICS_SYNC_INTERVAL_SECONDS", "59"),
        ("POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS", "899"),
    ],
)
def test_invalid_configuration_is_rejected(name: str, value: str) -> None:
    with pytest.raises(ConfigurationError):
        settings({name: value})


def test_pool_bounds_and_allow_block_overlap_are_rejected() -> None:
    with pytest.raises(ConfigurationError, match="MIN_SIZE cannot exceed"):
        settings({"DATABASE_POOL_MIN_SIZE": "9", "DATABASE_POOL_MAX_SIZE": "8"})

    with pytest.raises(ConfigurationError, match="overlap"):
        settings(
            {
                "LIVE_MARKET_ALLOWLIST": "polymarket:123,kalshi:ABC",
                "LIVE_MARKET_BLOCKLIST": "polymarket:123",
            }
        )


def test_database_dsn_escapes_credentials_and_safe_summary_omits_secrets(
    workspace_tmp_path: Path,
) -> None:
    key_path = workspace_tmp_path / "exists.pem"
    key_path.write_text("ephemeral test placeholder", encoding="utf-8")
    value = settings(
        {
            "POSTGRES_USER": "user@example",
            "POSTGRES_PASSWORD": "p@ss:/word",
            "POSTGRES_DB": "market data",
            "KALSHI_API_KEY_ID": "test-key-id",
            "KALSHI_PRIVATE_KEY_PATH": str(key_path),
        }
    )

    assert value.database_dsn == (
        "postgresql://user%40example:p%40ss%3A%2Fword@127.0.0.1:5432/market+data"
    )
    summary = value.safe_summary()
    assert summary["kalshi_websocket_configured"] is True
    assert "postgres_password" not in summary
    assert "kalshi_api_key_id" not in summary
    assert "kalshi_private_key_path" not in summary


def test_kalshi_websocket_requires_both_key_id_and_existing_key_path(
    workspace_tmp_path: Path,
) -> None:
    key_path = workspace_tmp_path / "exists.pem"
    key_path.write_text("ephemeral test placeholder", encoding="utf-8")
    assert not settings({"KALSHI_API_KEY_ID": "id-only"}).kalshi_websocket_configured
    assert not settings({"KALSHI_PRIVATE_KEY_PATH": str(key_path)}).kalshi_websocket_configured
    assert not settings(
        {
            "KALSHI_API_KEY_ID": "id",
            "KALSHI_PRIVATE_KEY_PATH": str(workspace_tmp_path / "missing.pem"),
        }
    ).kalshi_websocket_configured
    assert settings(
        {"KALSHI_API_KEY_ID": "id", "KALSHI_PRIVATE_KEY_PATH": str(key_path)}
    ).kalshi_websocket_configured
