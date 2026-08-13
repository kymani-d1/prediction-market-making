from __future__ import annotations

from datetime import UTC, datetime, timedelta

from prediction_collector.database import (
    _market_metadata_is_stale,
    _preserve_newer_market_state,
)
from prediction_collector.jobs.live import _subscription_fingerprints
from prediction_collector.common.types import MarketCandidate


NOW = datetime(2026, 8, 13, tzinfo=UTC)


def test_subscription_fingerprint_detects_augmented_token_changes() -> None:
    first = MarketCandidate(
        "polymarket", "market", None, "active", True, True,
        outcome_token_ids=("yes", "no"),
    )
    second = MarketCandidate(
        "polymarket", "market", None, "active", True, True,
        outcome_token_ids=("yes", "no", "clarified"),
    )
    assert _subscription_fingerprints([first]) != _subscription_fingerprints([second])


def test_stale_rest_metadata_cannot_regress_newer_lifecycle_state() -> None:
    incoming = {
        "status": "active",
        "is_active": True,
        "is_tradable": True,
        "observed_at": NOW,
        "raw_data": {},
    }
    current = {
        "status": "resolved",
        "is_active": False,
        "is_tradable": False,
        "metadata_observation_timestamp": NOW + timedelta(seconds=1),
        "metadata_source_timestamp": None,
        "metadata_exchange_timestamp": None,
        "metadata_exchange_timestamp_is_transport": False,
        "metadata_resolution_source": "oracle",
        "raw_data": {},
    }
    assert _market_metadata_is_stale(incoming, current)
    merged, preserved = _preserve_newer_market_state(incoming, current)
    assert preserved
    assert merged["status"] == "resolved"
    assert not merged["is_active"]
