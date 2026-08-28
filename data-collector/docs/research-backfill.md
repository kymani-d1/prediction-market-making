# Bounded research backfill

## Product boundary

`collector-live` is permanent. It collects the L2/order-book evidence that
cannot be reconstructed later. `research-backfill` creates bounded,
reproducible historical context datasets for quantitative research. The legacy
`backfill` command remains available only as an exhaustive
production-engineering path; it is not the normal research workflow.

Research backfill deliberately excludes comments, holder snapshots, current
order books, live tier evaluation, universal historical fee lookups, the global
current-reward crawl, and exhaustive raw REST archival. None provides enough
historical market-making value to justify its cost. These exclusions are local
to the research mode and do not weaken live collection or archival guarantees.

## Catalogue policy

The default reuses the catalogue continuously maintained in PostgreSQL by the
live collector. Set `RESEARCH_BACKFILL_REFRESH_CATALOGUE=true` to stream an
active catalogue plus closed markets whose end date falls inside
`RESEARCH_BACKFILL_CATALOGUE_HORIZON_DAYS` (default 730 days). Gamma keyset
pagination and page-level checkpoints keep this bounded in Python memory. Raw
catalogue response bodies are archived only when
`RESEARCH_BACKFILL_ARCHIVE_RAW_REST=true`; normalized metadata and request
progress remain durable either way. With the default `false` value, research
mode does not start the S3/archive writer, inspect or replay an old spool, or
require archive credentials.

## Bootstrap cohort

Bootstrap is the explicit `--mode bootstrap` path and remains the default for
backward compatibility. The default cohort size is 100 markets. The configurable
hard ceiling defaults to 5,000 and a code-level absolute ceiling prevents any
higher value. Increasing the default requires measured production evidence; it
must not be inferred from the ceiling.

Selection is executed in PostgreSQL and persisted atomically. Eligible markets
must be active or inside the configured historical horizon and must have at
least one CLOB outcome token. Fixed, interpretable strata cover:

- event category when present, otherwise a fixed versioned question/event-title
  taxonomy (crypto, politics, weather/climate, sports/esports, macro/finance,
  geopolitics, science/technology, entertainment, or other);
- zero, quiet, medium, and high volume;
- zero, thin, medium, and deep liquidity;
- intraday, short, medium, long, and unknown duration;
- active, resolved, and inactive lifecycle state;
- calendar quarter.

Within each stratum, `md5(seed + ':' + condition_id)` provides a stable order.
The selector first round-robins category buckets, then depth within the complete
strata, until the bound is reached. This prevents both a top-volume-only sample
and a large category with many occupied sub-strata from taking every pilot slot.
The taxonomy is an explicit research heuristic, not exchange ground truth.
`research_cohorts` stores the version, seed, criteria, horizon, timestamp, and
bound; `research_cohort_markets` stores exact membership, rank, strata,
selection-time metrics, category source, and reason. Change the cohort version
when any selection input or method changes.

## Incremental batches

`--mode incremental` creates a new persisted batch; it never changes an existing
cohort. Eligible markets are inactive and have a normalized status of resolved,
closed, settled, or finalized, a settlement/close timestamp inside the horizon,
and at least one CLOB outcome token. Selection excludes every market already in
any persisted research cohort. Active markets are deliberately excluded because
`collector-live` owns ongoing data and a periodic historical job must not fetch
the same active market repeatedly.

The batch stores its selection timestamp/cutoff, the latest bootstrap selection
used as the lower baseline (or the horizon when no bootstrap exists), selection
method version, seed, requested/selected counts, exact membership, chronological
rank, reason, strata, and selection-time metrics. Keeping the bootstrap baseline
while excluding prior members prevents an over-cap backlog from being skipped;
successive incremental batches drain it in eligibility-time order. A version
rerun verifies the static inputs and reuses the same membership. Any seed,
method, policy, or bound drift under that version fails closed.

The configured incremental per-run limit defaults to 100. Code enforces an
independent absolute ceiling of 250 even if configuration is wrong. When
`--cohort-version` is omitted, incremental mode derives a deterministic UTC ISO
week version such as `research-incremental-2026-W35`. A rerun in the same week
reuses that immutable version. Before using the current week's version, the job
resumes the oldest incremental cohort whose markets are not all terminal. This
prevents a failed weekly run from being stranded when the calendar advances.

## Historical outputs

Prices use the official batch price-history endpoint (maximum 20 tokens per
request), `interval=max`, and a configurable primary fidelity that defaults to
60 minutes. Production diagnostics found closed-token histories omitted at
`max/60` that appeared at `max/720`; the single-token and batch endpoints agreed,
so batching was not the failure. For tokens with an omitted or empty primary
history, the collector performs exactly one additional batch request at the
configured fallback fidelity (default 720 minutes). It does not split windows or
fan out per token. Empty results after both explicit responses remain
`unavailable`; mixed token coverage is `partial`. The upstream documentation
does not define a retention guarantee or the exact span represented by `max`, so
the fallback is evidence-based rather than treated as a contractual guarantee.

Each observation is normalized into `candlesticks` with
`open=high=low=close`; this is a sampled probability series, not exchange OHLCV.
The primary hourly series and coarser fallback are intended for volatility,
regime, and time-to-resolution context. Neither substitutes for proprietary live
tick/L2 history.

Trades retain the existing market-scoped Data API pagination logic. Pages are
bounded to 10,000 rows and saturated windows are recursively bisected. A
saturated one-second window or the documented roughly three-year market-history
floor is persisted as explicit partial coverage rather than called complete.
Trade writes are naturally idempotent.

Resolution uses normalized market results when present and terminal 1/0 outcome
prices as a fallback label. Missing closed-market labels are explicit
`unavailable`. Economics uses `feesEnabled`, `feeSchedule`, maker/taker fee, and
reward-threshold fields already present in Gamma metadata. It does not pretend a
current token fee lookup reconstructs a historical schedule. Evidence is marked
current metadata, fee-free, or unknown with zero additional fee requests.

`research_market_coverage` is the durable per-market checkpoint. Each component
distinguishes `not_started`, `in_progress`, `completed`, `partial`,
`unavailable`, `retryable_failed`, and `not_applicable`. A component becomes
terminal only after normalized writer/database work has drained safely. A crash
leaves unfinished work retryable; completed markets are skipped on restart.

Every attempted market also persists a compact profile under coverage
provenance: identity/rank, phase state, records, request/page/window counts, API
time, normalization time, writer enqueue time, writer drain time, total time,
and error type. Writer drain includes the durable database batch/commit path but
can include concurrent markets sharing the writer; it is not falsely labelled as
exclusive SQL time.

## Commands

```text
# Use existing catalogue, select a 100-market bootstrap, and fetch its data.
python -m prediction_collector research-backfill --mode bootstrap --max-markets 100

# Explicit version for a bounded validation or manual batch.
python -m prediction_collector research-backfill --mode incremental --cohort-version research-incremental-20260825-v1 --max-markets 5

# Scheduled operation: automatic ISO-week version and unfinished-batch resume.
python -m prediction_collector research-backfill --mode incremental

# Independently resumable phases.
python -m prediction_collector research-backfill --mode bootstrap --phase catalogue
python -m prediction_collector research-backfill --mode bootstrap --phase cohort --max-markets 100
python -m prediction_collector research-backfill --mode bootstrap --phase data
```

The Railway research service must use `python -m prediction_collector
research-backfill --mode incremental` for scheduled operation, never bootstrap
or the legacy `backfill` command. `RESEARCH_BACKFILL_COHORT_VERSION` applies to
bootstrap; normal scheduled incremental runs use automatic ISO-week versions.
Never clear or rewrite a completed cohort to repurpose its version.

## Research access

`research_cohort_dataset` is the cohort/outcome spine for Pandas, Polars, SQL,
or PyArrow workflows. Join `candlesticks` and `trades` by its market/outcome IDs.

```sql
SELECT *
FROM research_cohort_dataset
WHERE cohort_version = 'pilot-v1'
ORDER BY selection_rank, outcome_index;

SELECT d.cohort_version, d.condition_id, d.token_id,
       c.period_start, c.close AS probability
FROM research_cohort_dataset d
JOIN candlesticks c
  ON c.market_id = d.market_id AND c.outcome_id = d.outcome_id
WHERE d.cohort_version = 'pilot-v1';
```

Historical limitations are structural: the price API is sampled probability
history, public market trade queries have a finite history floor and offset
budget, current book endpoints do not provide old books, and present-day fee or
reward responses cannot establish historical economics. The permanent live
collector exists specifically to accumulate the missing microstructure from now
on.
