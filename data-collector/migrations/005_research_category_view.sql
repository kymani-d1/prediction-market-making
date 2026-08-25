BEGIN;

CREATE OR REPLACE VIEW research_cohort_dataset AS
SELECT c.version AS cohort_version,
       c.selection_timestamp,
       c.horizon_start,
       cm.selection_rank,
       cm.selection_reason,
       cm.strata,
       cm.selection_metrics,
       m.id AS market_id,
       m.external_id AS condition_id,
       m.slug,
       m.question,
       COALESCE(cm.strata ->> 'category', e.category, 'unknown') AS category,
       m.status,
       m.open_time,
       m.close_time,
       m.settlement_time,
       m.result,
       m.volume,
       m.liquidity,
       m.fee_rate AS current_fee_rate,
       m.raw_data -> 'feesEnabled' AS current_fees_enabled,
       m.raw_data -> 'feeSchedule' AS current_fee_schedule,
       m.raw_data ->> 'rewardsMinSize' AS current_rewards_min_size,
       m.raw_data ->> 'rewardsMaxSpread' AS current_rewards_max_spread,
       o.id AS outcome_id,
       o.external_id AS outcome_external_id,
       o.token_id,
       o.name AS outcome_name,
       o.outcome_index,
       COALESCE(
           o.is_winner,
           CASE
               WHEN lower(m.status) IN ('closed', 'resolved', 'settled', 'finalized')
                    AND o.last_price = 1 THEN TRUE
               WHEN lower(m.status) IN ('closed', 'resolved', 'settled', 'finalized')
                    AND o.last_price = 0 THEN FALSE
               ELSE NULL
           END
       ) AS is_winner,
       cov.price_status,
       cov.trade_status,
       cov.resolution_status,
       cov.economics_status,
       cov.partial_history,
       cov.price_records,
       cov.trade_records,
       cov.request_count,
       cov.provenance,
       tags.tags
FROM research_cohorts c
JOIN research_cohort_markets cm ON cm.cohort_id = c.id
JOIN markets m ON m.id = cm.market_id
LEFT JOIN events e ON e.id = m.event_id
LEFT JOIN outcomes o ON o.market_id = m.id
JOIN research_market_coverage cov
  ON cov.cohort_id = cm.cohort_id AND cov.market_id = cm.market_id
LEFT JOIN LATERAL (
    SELECT array_agg(t.name ORDER BY t.name) AS tags
    FROM market_tags mt
    JOIN tags t ON t.id = mt.tag_id
    WHERE mt.market_id = m.id
) tags ON TRUE;

COMMIT;
