from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from prediction_collector.config import Settings
from prediction_collector.database import Database


DATABASE_URL = os.getenv("RESEARCH_INTEGRATION_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="set RESEARCH_INTEGRATION_DATABASE_URL to a disposable PostgreSQL database",
)


def _criteria() -> dict[str, object]:
    return {
        "method": "stratified_deterministic_sample",
        "method_version": 2,
        "horizon_days": 730,
        "dimensions": [
            "category",
            "volume_bucket",
            "liquidity_bucket",
            "duration_bucket",
            "lifecycle_bucket",
            "calendar_quarter",
        ],
        "volume_buckets_usdc": [0, 1_000, 100_000],
        "liquidity_buckets_usdc": [0, 1_000, 10_000],
        "duration_buckets_days": [1, 7, 30],
        "category_source": "event_metadata_then_question_taxonomy_v1",
        "category_balance": "round_robin_before_full_strata_depth",
        "requires_outcome_token": True,
        "active_markets_included": True,
    }


@pytest.mark.asyncio
async def test_research_schema_selection_resume_and_view_contract() -> None:
    assert DATABASE_URL is not None
    settings = Settings.from_env(
        {"DATABASE_URL": DATABASE_URL}, load_dotenv_file=False
    )
    database = Database(settings)
    migrations = await database.migrate()
    assert "004_research_backfill.sql" in migrations or not migrations
    assert "005_research_category_view.sql" in migrations or not migrations
    await database.open()
    now = datetime.now(UTC)
    try:
        async with database.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "DELETE FROM research_cohorts WHERE version LIKE 'integration-v%'"
                )
                market_rows = await (
                    await connection.execute(
                        """
                        SELECT id FROM markets
                        WHERE external_id LIKE 'research-integration-%'
                        """
                    )
                ).fetchall()
                market_ids = [int(row["id"]) for row in market_rows]
                if market_ids:
                    await connection.execute(
                        "DELETE FROM outcomes WHERE market_id = ANY(%s)",
                        (market_ids,),
                    )
                    await connection.execute(
                        "DELETE FROM markets WHERE id = ANY(%s)",
                        (market_ids,),
                    )
                await connection.execute(
                    "DELETE FROM events WHERE external_id = 'research-integration-event'"
                )
                event = await (
                    await connection.execute(
                        """
                        INSERT INTO events
                            (exchange, external_id, title, category, status)
                        VALUES
                            ('polymarket', 'research-integration-event',
                             'Research integration event', NULL, 'closed')
                        RETURNING id
                        """
                    )
                ).fetchone()
                assert event is not None
                event_id = int(event["id"])
                questions = [
                    "Will an NBA team win the final?",
                    "Will Bitcoin trade above its target?",
                    "Will the presidential election be called?",
                    "Will the Federal Reserve cut its interest rate?",
                    "Will a Ukraine ceasefire be announced?",
                    "Will OpenAI announce new artificial intelligence research?",
                    "Will the highest temperature exceed 30 C?",
                    "Will a movie win an Oscar?",
                    "Will an uncategorized outcome occur?",
                ]
                inserted_market_ids: list[int] = []
                for index in range(9):
                    active = index == 8
                    opened = None if active else now - timedelta(days=40 + index * 45)
                    closed = None if active else opened + timedelta(days=2 + index)
                    market = await (
                        await connection.execute(
                            """
                            INSERT INTO markets
                                (exchange, external_id, event_id, question, status,
                                 is_active, is_tradable, open_time, close_time,
                                 settlement_time, volume, liquidity, raw_data)
                            VALUES
                                ('polymarket', %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s, %s, %s,
                                 '{"feesEnabled": false}'::jsonb)
                            RETURNING id
                            """,
                            (
                                f"research-integration-{index}",
                                event_id,
                                questions[index],
                                "active" if active else "resolved",
                                active,
                                active,
                                opened,
                                closed,
                                closed,
                                [0, 500, 5_000, 500_000][index % 4],
                                [0, 500, 5_000, 50_000][index % 4],
                            ),
                        )
                    ).fetchone()
                    assert market is not None
                    market_id = int(market["id"])
                    inserted_market_ids.append(market_id)
                    for outcome_index, name in enumerate(("Yes", "No")):
                        await connection.execute(
                            """
                            INSERT INTO outcomes
                                (market_id, exchange, external_id, token_id, name,
                                 outcome_index, last_price)
                            VALUES (%s, 'polymarket', %s, %s, %s, %s, %s)
                            """,
                            (
                                market_id,
                                f"research-integration-{index}:{outcome_index}",
                                f"research-token-{index}-{outcome_index}",
                                name,
                                outcome_index,
                                1 if outcome_index == 0 and not active else 0,
                            ),
                        )

                stale = await (
                    await connection.execute(
                        """
                        INSERT INTO markets
                            (exchange, external_id, event_id, question, status,
                             is_active, is_tradable)
                        VALUES
                            ('polymarket', 'research-integration-stale', %s,
                             'Undated inactive market?', 'closed', false, false)
                        RETURNING id
                        """,
                        (event_id,),
                    )
                ).fetchone()
                assert stale is not None
                stale_id = int(stale["id"])
                await connection.execute(
                    """
                    INSERT INTO outcomes
                        (market_id, exchange, external_id, token_id, name,
                         outcome_index, last_price)
                    VALUES
                        (%s, 'polymarket', 'research-integration-stale:0',
                         'research-token-stale-0', 'Yes', 0, 1)
                    """,
                    (stale_id,),
                )

        criteria = _criteria()
        cohort = await database.create_or_get_research_cohort(
            exchange="polymarket",
            version="integration-v1",
            seed="integration-seed",
            max_markets=9,
            horizon_start=now - timedelta(days=730),
            criteria=criteria,
        )
        assert int(cohort["selected_count"]) == 9
        repeated = await database.create_or_get_research_cohort(
            exchange="polymarket",
            version="integration-v1",
            seed="integration-seed",
            max_markets=9,
            horizon_start=now - timedelta(days=730),
            criteria=criteria,
        )
        assert int(repeated["id"]) == int(cohort["id"])

        selected = [
            market
            async for market in database.iter_research_markets(
                exchange="polymarket",
                cohort_version="integration-v1",
                batch_size=2,
            )
        ]
        assert len(selected) == 9
        assert [market.selection_rank for market in selected] == list(range(1, 10))
        assert stale_id not in {market.market_id for market in selected}
        assert {market.market_id for market in selected} == set(inserted_market_ids)

        first = selected[0]
        await database.set_research_phase_status(
            cohort_id=first.cohort_id,
            market_id=first.market_id,
            phase="price",
            status="completed",
            records=2,
            requests=1,
            provenance={
                "source": "integration",
                "retrieved_at": now,
                "fee_rate": Decimal("0.01"),
            },
        )
        await database.set_research_phase_status(
            cohort_id=first.cohort_id,
            market_id=first.market_id,
            phase="trade",
            status="partial",
            records=3,
            requests=2,
            partial_history=True,
        )
        await database.set_research_phase_status(
            cohort_id=first.cohort_id,
            market_id=first.market_id,
            phase="resolution",
            status="completed",
        )
        await database.set_research_phase_status(
            cohort_id=first.cohort_id,
            market_id=first.market_id,
            phase="economics",
            status="unavailable",
        )

        status = await database.research_cohort_status(
            exchange="polymarket", cohort_version="integration-v1"
        )
        assert int(status["completed_markets"]) == 1
        assert int(status["partial_markets"]) == 1
        assert int(status["price_records"]) == 2
        assert int(status["trade_records"]) == 3
        assert int(status["request_count"]) == 3

        async with database.pool.connection() as connection:
            view = await (
                await connection.execute(
                    """
                    SELECT count(*) AS rows,
                           count(DISTINCT category) AS distinct_categories,
                           count(*) FILTER (WHERE category = 'unknown') AS unknown,
                           count(*) FILTER (WHERE is_winner IS TRUE) AS winners
                    FROM research_cohort_dataset
                    WHERE cohort_version = 'integration-v1'
                    """
                )
            ).fetchone()
        assert view is not None
        assert int(view["rows"]) == 18
        assert int(view["distinct_categories"]) >= 8
        assert int(view["unknown"]) == 0
        assert int(view["winners"]) == 8
    finally:
        await database.close()
