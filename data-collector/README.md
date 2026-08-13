# Polymarket data collector

Read-only production collector for Polymarket market-making research. It performs
complete public market discovery, dynamically selects the expensive collection
level, keeps a bounded PostgreSQL hot store, and writes permanent high-volume
history as compressed Parquet to S3-compatible object storage. It has no trading
or wallet-signing code.

## Architecture

```text
Gamma / CLOB / Data REST -----> normalization + semantic metadata versions
Market / RTDS / Sports WS ----> timestamps + quality tracking + current state
                                      |
                         +------------+-------------+
                         |                          |
                  PostgreSQL hot store       bounded archive queue
                  - all metadata             - background Parquet
                  - normalized trades        - Zstd compression
                  - current L2 state          - retry + local spool
                  - recent observations      - HEAD/hash verification
                  - fees/rewards             - S3-compatible upload
                  - tiers/gaps/manifest             |
                         |                    permanent archive
                         +---- status/metrics       FULL_L2 deltas/snapshots
                                               sampled observations
                                               raw REST/reference evidence
```

The old design appended normalized deltas, full snapshots, derived snapshots,
raw WebSocket frames, and full REST JSON to PostgreSQL. Those streams have very
different value and retention needs. The combination caused PostgreSQL to fill
within minutes. Increasing the volume would only postpone the failure.

The new split is deliberate:

- PostgreSQL is the query-friendly hot/control store. Current books replace old
  state; reference prices and derived observations have automatic retention.
- Object storage is the permanent high-volume research warehouse. Objects are
  hourly partitioned Parquet, compressed with Zstd, and recorded in a manifest.
- Normalized trades stay permanently in PostgreSQL for now. Revisit only after
  long-duration measurements demonstrate that they dominate growth.
- Full raw REST bodies are archive-only. PostgreSQL keeps request provenance,
  content hashes, record counts, sizes, and the archive object key.
- Raw WebSocket retention defaults to malformed, unknown, and error messages.
  Normal known FULL_L2 messages already have a normalized permanent stream.

## Collection tiers

Every discovered market gets one auditable tier. Discovery, lifecycle metadata,
outcomes, tags, relationships, fees, rewards, resolution, and final result are
not capped.

| Tier | Continuous collection | Permanent history |
|---|---|---|
| `FULL_L2` | All book deltas/snapshots, trades, current books, periodic derived observations | Normalized L2 and observations in Parquet |
| `SAMPLED` | Current books, trades, lifecycle, and 60-second derived observations | Observations in Parquet; no permanent per-delta stream |
| `METADATA_ONLY` | No continuous market-book subscription | Metadata/lifecycle/economics only |

Eligibility requires a genuinely trade-ready market: active, not closed or
archived, accepting orders, order book enabled, and usable outcome tokens.
Ranking is deterministic. The score combines log-scaled liquidity and 24-hour
volume, recent trades/book updates, maker rewards, research-useful wide spreads,
and proximity to resolution. Ties use the exchange market ID. Tier reasons and
ceiling bindings are persisted in `market_collection_tiers` and its history.

Production defaults are `500` FULL_L2 and `1,000` SAMPLED markets. These are
resource safety ceilings, not arbitrary discovery limits. Set either to `0` for
no ceiling only after measuring CPU, archive backlog, PostgreSQL write activity,
and disk growth. `FULL_L2_MARKET_ALLOWLIST` forces named markets into FULL_L2;
`LIVE_MARKET_BLOCKLIST` forces named markets to metadata-only. Lists accept
comma-separated condition IDs, slugs, tickers, or outcome token IDs.

Tier A observations default to 300 seconds because its exact tick history is
already archived. Tier B defaults to 60 seconds because its observation stream
is its permanent microstructure history. PostgreSQL retains six hours of hot
reference prices and 24 hours of hot microstructure observations; the
same observations remain permanent in Parquet.

## Archive and failure semantics

Objects use keys such as:

```text
production/schema_version=1/exchange=polymarket/
  stream=orderbook_updates/date=2026-08-13/hour=14/
  part-0123456789abcdef01234567.parquet
```

Supported streams are `orderbook_updates`, `orderbook_snapshots`,
`microstructure_observations`, `raw_ws`, `raw_rest`, and `reference_prices`.
The writer batches by row count, estimated bytes, flush interval, stream, date,
and hour. Serialization runs off the WebSocket path. Queue rows and bytes are
both bounded. Uploads use retry/backoff/jitter, remain in a bounded local spool,
and are verified with object size plus SHA-256 metadata before the manifest is
marked uploaded.

Priority under pressure is:

1. raw REST and reconstruction-critical normalized L2;
2. current book state and normalized trades;
3. reference prices and derived observations;
4. optional raw WebSocket diagnostics.

The collector never silently claims success after data loss. Queue timeouts,
serialization quarantine, spool exhaustion, upload failures, and hot-store
shedding create `archive_degradation_events`; failed database rows create
`collector_write_failures`; partial backfills finish as `partial`. A fatal archive
writer failure is supervised and fails the run instead of leaving producers
blocked forever.

## Configuration

Copy `.env.example` to `.env`. `Settings.safe_summary()` never includes secrets.
The application also accepts Railway CLI/AWS aliases: `AWS_ENDPOINT_URL`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_BUCKET_NAME`,
`AWS_DEFAULT_REGION`, and `AWS_S3_URL_STYLE`.

Important groups:

- PostgreSQL: `DATABASE_URL`, or `POSTGRES_USER`, `POSTGRES_PASSWORD`,
  `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`.
- Archive: `S3_ENDPOINT_URL`, `S3_BUCKET`, `S3_REGION`,
  `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_PREFIX`, `S3_URL_STYLE`.
- Batching: `ARCHIVE_BATCH_ROWS`, `ARCHIVE_BATCH_BYTES`,
  `ARCHIVE_FLUSH_SECONDS`, queue/spool limits, and retry attempts.
- Tiering: `FULL_L2_MAX_MARKETS`, `SAMPLED_MAX_MARKETS`, score/activity
  thresholds, allowlist/blocklist, and reevaluation interval.
- Retention: `RAW_WS_POLICY`, `POSTGRES_REFERENCE_RETENTION_HOURS`,
`POSTGRES_OBSERVATION_RETENTION_HOURS`. Reference prices default to six hot
hours and observations to 24 hours; both remain permanent in Parquet.
- Guardrails: `POSTGRES_STORAGE_WARN_GB`,
  `POSTGRES_STORAGE_CRITICAL_GB`, archive warning/critical thresholds.

The six-hour live `/fee-rate` refresh is limited to current FULL_L2/SAMPLED
markets because it requires one REST request per token. The explicit backfill
remains comprehensive and is not affected by tier ceilings.

Removed variables are not aliases and are deliberately ignored: all
`KALSHI_*` variables (`KALSHI_ENABLED`, `KALSHI_API_KEY_ID`,
`KALSHI_PRIVATE_KEY_PATH`, `KALSHI_PRIVATE_KEY_HOST_PATH`,
`KALSHI_REFERENCE_FEEDS_ENABLED`, `KALSHI_WS_SUBSCRIPTION_CHUNK_SIZE`,
`KALSHI_API_URL`, `KALSHI_WS_URL`), plus the old `POLYMARKET_ENABLED`,
`STORE_RAW_WS`, `STORE_RAW_REST`, `MAX_LIVE_MARKETS`,
`MIN_LIVE_MARKET_VOLUME`, `MIN_LIVE_MARKET_LIQUIDITY`,
`LIVE_MARKET_ALLOWLIST`, and `MARKET_SNAPSHOT_INTERVAL_SECONDS` controls.

## Commands and operating order

```text
python -m prediction_collector migrate   apply pending migrations
python -m prediction_collector run       permanent live worker; migrates on startup
python -m prediction_collector backfill  one-shot uncapped Polymarket REST backfill
python -m prediction_collector status    strictly read-only health report
python -m prediction_collector smoke     read-only public API shape check
```

`run` and `backfill` require valid S3 configuration. `status` never applies a
migration. It verifies migration names/checksums and returns non-zero with a
pending/inconsistent report. Migration responsibility belongs to `migrate` and
the write-worker startup path.

The correct order after creating storage is:

1. run `migrate`;
2. start the permanent `run` worker immediately;
3. verify live subscription and archive uploads;
4. run `backfill` in a second terminal or one-shot container.

Do not reverse steps 2 and 4. Historical REST data is recoverable later;
missed WebSocket microstructure generally is not.

### Windows native setup

Install Python 3.14+ and PostgreSQL 18. PostgreSQL’s official Windows page
provides the EDB installer and portable zip: <https://www.postgresql.org/download/windows/>.
For local S3 compatibility, install the official MinIO Windows binary:
<https://min.io/docs/minio/windows/operations/install-deploy-manage/deploy-minio-single-node-single-drive.html>.

```powershell
cd data-collector
Copy-Item .env.example .env
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"

# In one separate terminal, start PostgreSQL 18.
# In another, start MinIO and create S3_BUCKET through its console/CLI.
# For native execution change S3_ENDPOINT_URL to http://127.0.0.1:9000.

python -m prediction_collector migrate
python -m prediction_collector run
```

Then, in a second terminal while live collection remains running:

```powershell
cd data-collector
.\.venv\Scripts\Activate.ps1
python -m prediction_collector backfill
```

### Docker Compose

Compose includes PostgreSQL 18, a development MinIO service, a bucket initializer,
a migration job, and the permanent collector.

```powershell
cd data-collector
Copy-Item .env.example .env
# Replace all local example passwords in .env.
docker compose up -d database object-store object-store-init migrate collector
docker compose logs -f collector
```

Keep that collector running. Start backfill separately:

```powershell
docker compose run --rm collector backfill
```

`docker compose down -v` destroys both local PostgreSQL and MinIO volumes. It is
not a normal reset command and must never be run against data you intend to keep.

## PostgreSQL schema

Migration `002_polymarket_hot_archive.sql` is forward-only and non-destructive.
It renames the former unbounded tables to `legacy_*` instead of dropping them,
then creates:

- `market_collection_tiers` and append-only tier changes;
- `current_orderbooks` and `current_orderbook_levels` (replacement state);
- `microstructure_observations` (short hot window);
- `archive_objects` (hash/size/time/status manifest);
- compact `raw_rest_payloads` provenance;
- `storage_metrics` and `archive_degradation_events`.

Metadata history hashes only semantic state/structure. Bid, ask, volume, and
transport timing changes update current fields without opening false metadata
versions. Full exchange payloads live in the raw REST archive. Outcome IDs remain
stable before token assignment; linked negative-risk memberships are deduplicated
and retired when authoritative metadata changes.

## Status and observability

`status` reports database/migration health, table counts, latest trades/books/
reference/archive times, tier counts and ceiling bindings, discovery/subscription
state, archive pending/failed objects, queue age/bytes, compression, spool size,
PostgreSQL database size/growth, largest tables, and overall health.

Normal INFO logs are aggregate or connection-level: connection/disconnection,
subscription confirmation, gaps/errors, tier summary, minute throughput, archive
queue/upload summary, and storage growth. Per-market lifecycle races and tier
reasons are DEBUG or persisted, not emitted once per record. The one-minute
throughput event includes WebSocket messages/minute and actual PostgreSQL row
write activity from `pg_stat_user_tables`.

Storage guardrails set the writer state to `warning` or `critical`. At critical
PostgreSQL size, optional hot observations/reference copies are shed only after
their permanent archive path is attempted, and degradation is explicit. Watch
Railway volume usage as well as row counts because `raw_ws`, normalized L2, and
indexes have very different bytes-per-row.

The current Railway PostgreSQL volume is 100 GB. Production defaults warn at
70 GB and enter critical protection at 85 GB, leaving filesystem/WAL/autovacuum
headroom. Keep the 6-hour reference and 24-hour observation windows anyway; the
extra capacity is a failure margin, not permanent tick storage.

## Research access

Download only the hour partitions and markets you need. `archive_reader.py`
builds partition prefixes and uses PyArrow predicate/column pushdown:

```python
from datetime import UTC, datetime
from prediction_collector.archive_reader import load_archive

table = load_archive(
    ["downloaded/part-001.parquet", "downloaded/part-002.parquet"],
    start=datetime(2026, 8, 13, 14, tzinfo=UTC),
    end=datetime(2026, 8, 13, 15, tzinfo=UTC),
    markets=["0x-condition-id"],
    columns=["received_at", "market_external_id", "price", "size"],
)
```

DuckDB can query the same objects directly after download or with an S3 extension:

```sql
SELECT market_external_id, received_at, side, price, size
FROM read_parquet('research/schema_version=1/exchange=polymarket/stream=orderbook_updates/date=2026-08-13/hour=14/*.parquet')
WHERE market_external_id = '0x-condition-id'
ORDER BY received_at;
```

## Railway production deployment

Railway Buckets are private S3-compatible object stores. Create one from the
project canvas with **Create -> Bucket**, choose the same region as the worker,
and name it `Archive`. The region cannot be changed later. Railway uses
virtual-hosted URLs; the credential tab states the correct style. Official docs:
<https://docs.railway.com/storage-buckets>.

On the live service’s Variables tab, use variable references—not copied secrets:

```text
S3_ENDPOINT_URL=${{Archive.ENDPOINT}}
S3_BUCKET=${{Archive.BUCKET}}
S3_REGION=${{Archive.REGION}}
S3_ACCESS_KEY_ID=${{Archive.ACCESS_KEY_ID}}
S3_SECRET_ACCESS_KEY=${{Archive.SECRET_ACCESS_KEY}}
S3_URL_STYLE=virtual
S3_PREFIX=production
DATABASE_URL=${{Postgres.DATABASE_URL}}
```

`Archive.BUCKET` is the real globally unique S3 name. Do not use
`RAILWAY_BUCKET_NAME`, which is only the display name.

CLI equivalent after `railway login` and `railway link`:

```powershell
railway bucket create Archive --region ams
railway bucket -b Archive -e production info --json
railway bucket -b Archive -e production credentials
```

Valid bucket regions are `sjc`, `iad`, `ams`, and `sin`. The credentials command
emits AWS-compatible aliases already accepted by the collector. See
<https://docs.railway.com/cli/bucket>.

Create two services from the same repository/Dockerfile:

- `collector-live`: start command `python -m prediction_collector run`, restart
  on failure, one replica, health command `python -m prediction_collector status`.
- `collector-backfill`: start command `python -m prediction_collector backfill`,
  restart policy `Never`; deploy/run only after live is confirmed.

Use the production defaults from `.env.example`; set `JSON_LOGS=true` and
`LOG_LEVEL=INFO`. Start `collector-live` first. Do not let a one-shot backfill
service restart forever.

Verify deployment:

```powershell
railway logs --service collector-live --environment production --since 15m
railway ssh --service collector-live --environment production -- python -m prediction_collector status
railway bucket -b Archive -e production info --json
```

To list actual uploaded keys from inside the configured live service:

```powershell
railway ssh --service collector-live --environment production -- python -c "import boto3,os; c=boto3.client('s3',endpoint_url=os.environ['S3_ENDPOINT_URL'],region_name=os.environ['S3_REGION'],aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY']); print([x['Key'] for x in c.list_objects_v2(Bucket=os.environ['S3_BUCKET'],Prefix=os.environ['S3_PREFIX'],MaxKeys=5).get('Contents',[])])"
```

Railway Buckets currently do not provide object versioning, object locks,
bucket lifecycle rules, or native bucket backups. The manifest plus immutable
content-addressed keys protects collector idempotency, but critical archives
still need a separate replication/export plan.

### Backups before irreplaceable collection

Before leaving the live worker unattended, open the Railway PostgreSQL service,
go to **Settings -> Backups**, and enable at least Daily plus Weekly scheduled
volume backups. Daily snapshots run every 24 hours and are retained six days;
weekly snapshots run every seven days and are retained one month. Railway warns
that wiping a volume deletes its backups, so do not treat same-volume backups as
protection from a deliberate wipe. Reference: <https://docs.railway.com/volumes/backups>.

For a valuable long-running database, enable PostgreSQL point-in-time recovery
as well. Railway’s PITR uses pgBackRest, daily incremental and weekly full base
backups, plus archived WAL for roughly a four-week restore window:
<https://docs.railway.com/volumes/point-in-time-recovery>.

## Safe replacement of the experimental Railway database

Do not wipe the current volume in place first. The safer reset is blue/green:

1. Stop/disable the backfill worker. Keep the existing live worker stopped only
   for the shortest cutover window.
2. Confirm the `pre-polymarket-only` tag exists and push it:
   `git push origin pre-polymarket-only`.
3. Create and lock a manual backup of the old PostgreSQL volume. Export a
   `pg_dump -Fc` as an independent copy if any experimental data matters.
4. Provision a new Railway PostgreSQL service named `PostgresV2`; do not delete
   or wipe the old one.
5. Set `DATABASE_URL=${{PostgresV2.DATABASE_URL}}` on `collector-live` and
   `collector-backfill`. Leave the archive bucket unchanged and use a new
   `S3_PREFIX=production-v2` if you want a clean namespace.
6. Deploy `collector-live`. Startup applies migrations. Confirm migration
   version, live subscription, current books, manifest uploads, object count,
   queue depth, database size, and no open degradation events.
7. Only then run `collector-backfill` once.
8. Retain the old PostgreSQL service through a rollback window. Delete it only
   after the new live/archive path has been verified and the independent backup
   is restorable.

If you deliberately use Railway’s **Wipe Volume** action instead, understand that
it destroys the database and all backups attached to that volume. Stop both
workers, type the exact confirmation in the dashboard yourself, wait for
PostgreSQL to reinitialize, then redeploy live first. This repository does not
perform destructive Railway actions automatically.

## Tests and integration benchmark

```powershell
python -m pytest -q -p no:cacheprovider
python -m compileall -q src scripts
```

The repository includes deterministic tests for Polymarket-only startup,
tradability, tier scoring/caps/promotions, current-book replacement, metadata
semantic deduplication, raw REST provenance, archive batching/partitioning,
Parquet readability, retry/backpressure/permanent failure, restart spool recovery,
status read-only behavior, and timestamp/feed integrity.

For a disposable real PostgreSQL 18 and S3-compatible endpoint:

```powershell
python scripts/integration_benchmark.py --seconds 180 --output integration-results.local.json
```

The script never provisions, deletes, or points itself at Railway. Supply a
throwaway `DATABASE_URL` and S3 bucket. It runs public read-only Polymarket
traffic, reports coverage, messages, actual PostgreSQL write activity, archive
compression/backlog/failures, CPU, memory, and short-sample projections.

The measured PostgreSQL 18/MinIO/public-Polymarket results and explicit capacity
assumptions are in [docs/integration-benchmark-2026-08-13.md](docs/integration-benchmark-2026-08-13.md).

## Remaining limitations

- Polymarket does not provide an order-book sequence number equivalent to some
  exchanges. Exact receive/exchange timestamps and full-book hashes are retained
  where available; reconnects require new snapshots.
- Short benchmarks are not long-duration capacity proofs. Initial comprehensive
  metadata discovery is write-heavy and must not be extrapolated as steady state.
- Tier scoring is intentionally deterministic but requires tuning from real
  maker-research outcomes and long-duration resource measurements.
- Railway Buckets lack versioning, object lock, lifecycle management, and native
  backups. Replicate valuable Parquet externally.
- Current PostgreSQL retention uses batched SQL deletes, not native partition
  detach/drop. If observation volume grows materially, time partitioning is the
  next hot-store optimization.
