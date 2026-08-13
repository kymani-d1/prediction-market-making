# Integration benchmark — 2026-08-13

This is a local disposable integration test, not a Railway production test. It
used public read-only Polymarket endpoints and placed no orders.

## Infrastructure

- PostgreSQL 18.6 official Windows portable distribution
- MinIO `RELEASE.2025-09-07T16-13-09Z`, S3 API on loopback
- Python 3.14
- fresh migrations `001_initial.sql` and `002_polymarket_hot_archive.sql`
- 5 FULL_L2 and 5 SAMPLED markets, zero score/activity thresholds
- 500-row / 1 MiB / 5-second benchmark archive batches
- `RAW_WS_POLICY=errors`

The real-server migration caught and corrected two static-SQL misses: a generated
constraint-name collision and an index-name collision retained when the legacy
table was renamed. A live snapshot then exposed numerically duplicate book-price
levels. Current-book replacement now deduplicates by numeric price and has an
idempotent conflict update. The accepted follow-ups added zero new quarantine
rows.

## Accepted 63-second live run

The database was warm from the comprehensive-discovery phase, so this is closer
to recurring metadata-refresh behavior than a fresh-install measurement. The
crawl was intentionally stopped before complete discovery.

| Measurement | Result |
|---|---:|
| Discovered during run | 4,855 markets |
| Trade-ready | 4,798 markets |
| Confirmed live selection | 5 FULL_L2 + 5 SAMPLED |
| WebSocket messages | 2,183 (2,075/min) |
| PostgreSQL write activity | 54,762 row operations (52,039/min) |
| PostgreSQL physical growth | 2,777,088 bytes (2.64 MB/min) |
| New trades | 1 |
| New current books / levels | 4 / 38 |
| New observations | 20 |
| Raw REST provenance rows | 21 |
| Parquet objects | 72 |
| Archive rows | 3,991 |
| Uncompressed / compressed archive | 58,778,422 / 5,765,970 bytes |
| Compression ratio | 10.19:1 |
| Archive queue peak | 70 records / 7,069,152 bytes |
| Queue at shutdown | 0 records / 0 bytes |
| Upload failures | 0 |
| New database write failures | 0 |
| New archive degradation events | 0 |
| Average process CPU | 81.3% of one core |
| Peak working set | 277,737,472 bytes |

Rows/minute remains high because current metadata/current books are updated in
place. It is not equivalent to permanent storage growth. That distinction is the
point of the new architecture.

## Errors-only raw policy check

A separate 33-second live run verified the post-benchmark raw-policy correction:

- 1,032 WebSocket messages were received;
- only 2 raw WebSocket records were archived (malformed/unknown evidence);
- 821 normalized reference prices were archived;
- 54 normalized order-book updates and 50 initial snapshots were archived;
- no new database failure or archive-degradation row was created;
- queue and spool were empty at shutdown.

Known RTDS `update` envelopes and normalized sports events are therefore not
permanently duplicated as raw frames under the default policy.

## Old versus new PostgreSQL growth

The old production snapshot was 3,549 MB with 661,587 book-update rows and an
observed 40k–70k updates/minute. If those counts describe approximately the same
run interval, its duration was 9.45–16.54 minutes and the implied database growth
was roughly 215–376 MB/minute. The exact old start/end timestamps are unavailable,
so this is an inferred range, not a directly timed benchmark.

The warmed new run grew at 2.64 MB/minute while discovery was active. Against
that inferred old range, the short-sample physical-growth reduction is
98.8%–99.3%. The traffic universes are not identical (10 selected live markets in
the local run versus 34,180 old subscriptions), but FULL_L2 count no longer
causes append-only PostgreSQL tick growth: it changes S3 volume and current-state
write load instead.

## Capacity projection

The benchmark script’s naive 30-day extrapolation is intentionally not used as a
capacity claim. It assumes short-run metadata refresh/bloat and pre-retention
growth continue linearly. They do not.

For the production defaults (500 FULL_L2, 1,000 SAMPLED, 300-second Tier-A
observations, 60-second Tier-B observations, six hot hours of references, 24 hot
hours of observations), a reasonable first planning range is:

| Store | Planning range | Assumptions |
|---|---:|---|
| PostgreSQL after full metadata/backfill | 1.5–2.2 GB base | scaled from compact metadata/current rows; no full JSON bodies |
| Hot reference window | 0.3–0.6 GB | about 1.5k–1.8k normalized updates/min for six hours |
| Hot observation window | 1.2–2.2 GB | about 2.2k observations/min for 24 hours |
| Current books/trades/control/index headroom | 0.4–1.0 GB | depends on level count and trade activity |
| Expected PostgreSQL plateau | roughly 3.4–6.0 GB | hourly retention; optional hot writes shed at configured critical pressure |
| Archive per day | roughly 9–35 GB compressed | 5k–25k FULL_L2 updates/min at 400–800 bytes/row plus discovery/reference/observation streams |
| Archive per 30 days | roughly 0.27–1.05 TB compressed | permanent; strongly dependent on activity concentration and batch size |

The current Railway PostgreSQL volume is 100 GB. The configured 70 GB warning
and 85 GB critical thresholds leave substantial room above the modeled 3.4–6.0
GB plateau for WAL, autovacuum lag, indexes, backfill bursts, and model error.
That capacity is safety headroom, not a reason to restore permanent tick/raw
history to PostgreSQL. After 24 hours, replace these ranges with the collector’s
measured `storage_metrics` slopes and per-stream manifest bytes. If FULL_L2
updates are above the range, reduce tier ceilings or raise scoring thresholds;
metadata coverage remains complete.

## What this proves and does not prove

Proved with real services:

- both migrations execute on PostgreSQL 18;
- actual public Polymarket REST and WebSocket payloads parse;
- current books replace state without append history or duplicate-level failure;
- Parquet/Zstd objects upload through an S3-compatible API and are readable by
  PyArrow;
- manifest rows, compact raw REST provenance, retries, queue metrics, storage
  metrics, and read-only status SQL execute;
- the archive drained cleanly with no failed upload or sustained backlog.

Not proved:

- months-long growth, autovacuum/bloat equilibrium, or Railway-specific CPU/I/O;
- the complete 147k-market historical backfill in the same run;
- a production 500-market FULL_L2 traffic peak;
- Railway bucket disaster recovery (Railway Buckets currently lack native
  backups/versioning/object lock).
