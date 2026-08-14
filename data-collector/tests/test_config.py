from __future__ import annotations

from pathlib import Path

import pytest

from prediction_collector.config import ConfigurationError, Settings


def settings(env: dict[str, str] | None = None) -> Settings:
    return Settings.from_env(env or {}, load_dotenv_file=False)


def test_defaults_are_polymarket_only_and_bounded() -> None:
    value = settings()
    assert value.full_l2_max_markets == 10
    assert value.sampled_max_markets == 50
    assert value.metadata_sync_interval_seconds == 900
    assert value.sampled_snapshot_interval_seconds == 30
    assert value.sampled_heartbeat_interval_seconds == 900
    assert value.postgres_reference_retention_hours == 6
    assert value.postgres_observation_retention_hours == 24
    assert value.raw_ws_policy == "errors_sample"
    assert str(value.raw_ws_valid_sample_rate) == "0.001"
    assert value.safe_summary()["scope"] == "polymarket_only"
    assert not any("kalshi" in key.lower() for key in value.safe_summary())


def test_railway_aws_aliases_configure_archive_without_exposing_credentials() -> None:
    value = settings(
        {
            "AWS_ENDPOINT_URL": "https://storage.railway.app",
            "AWS_S3_BUCKET_NAME": "collector",
            "AWS_DEFAULT_REGION": "auto",
            "AWS_ACCESS_KEY_ID": "access",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_S3_URL_STYLE": "virtual",
            "FULL_L2_MARKET_ALLOWLIST": "abc, polymarket:def",
            "LIVE_MARKET_BLOCKLIST": "blocked",
        }
    )
    assert value.archive_configured
    assert value.s3_bucket == "collector"
    assert value.full_l2_market_allowlist == {"abc", "polymarket:def"}
    summary = str(value.safe_summary())
    assert "access" not in summary
    assert "secret" not in summary


def test_run_requires_complete_archive_credentials() -> None:
    with pytest.raises(ConfigurationError, match="Continuous collection requires"):
        settings({"S3_BUCKET": "only-a-bucket"}).require_archive()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("RAW_WS_POLICY", "forever"),
        ("ARCHIVE_COMPRESSION", "gzip"),
        ("S3_URL_STYLE", "invalid"),
        ("FULL_L2_MAX_MARKETS", "-1"),
        ("POSTGRES_STORAGE_WARN_GB", "-1"),
    ],
)
def test_invalid_configuration_fails_closed(name: str, value: str) -> None:
    with pytest.raises(ConfigurationError):
        settings({name: value})


def test_allowlist_and_blocklist_cannot_overlap() -> None:
    with pytest.raises(ConfigurationError, match="overlap"):
        settings(
            {
                "FULL_L2_MARKET_ALLOWLIST": "same",
                "LIVE_MARKET_BLOCKLIST": "same",
            }
        )


def test_env_example_is_polymarket_only_and_parses() -> None:
    values = {
        key: value
        for line in (Path(__file__).parents[1] / ".env.example").read_text(
            encoding="utf-8"
        ).splitlines()
        if line and not line.startswith("#") and "=" in line
        for key, value in [line.split("=", 1)]
    }
    assert not any(key.startswith("KALSHI_") for key in values)
    parsed = Settings.from_env(values, load_dotenv_file=False)
    assert parsed.archive_configured
    assert parsed.s3_url_style == "path"
