# Storage and capacity validation - 2026-08-14

This report records the final Polymarket-only storage-minimisation pass. All
live traffic was public and read-only. PostgreSQL 18.6 and MinIO were disposable
local services. No Railway production database, volume, bucket, service, or
configuration was changed.

## Permanent representation

The permanent representation is intentionally asymmetric by tier.

| Tier | Permanent | PostgreSQL hot-only |
|---|---|---|
| `FULL_L2` | Initial/reset/recovery/closing snapshots, every normalized ordered L2 delta, normalized trades, relevant reference prices, metadata/lifecycle/economics, gap evidence, and the tiny raw WebSocket audit sample | Current book and periodic derived observations |
| `SAMPLED` | Change-driven microstructure observations plus a sparse heartbeat, normalized trades, metadata/lifecycle/economics, relevant reference prices, and audit evidence | Current book |
| `METADATA_ONLY` | Complete metadata/lifecycle/economics, resolution, and raw REST provenance/content reference | No continuous current book |

Ordinary FULL_L2 derived observations are not archived. They can be regenerated
from snapshot plus deltas, trades, and reference prices. REST reconciliation
snapshots are archived as quality evidence but are not applied during replay,
because an HTTP response has no ordering relationship with concurrent WebSocket
deltas. Sports/public-comment behavior is unchanged and is not a substitute for
the market-book representation.

The default snapshot policy is event-driven: initial book, reconnect/reset,
gap recovery, closing state, and an hourly REST reconciliation checkpoint.
`ORDERBOOK_RECONCILE_INTERVAL_SECONDS=3600` is configurable. Continuous full
book snapshots were removed.

## Compact Parquet schema

Archive schema version 2 uses coarse partitions:

```text
schema_version=2/exchange=polymarket/stream=<stream>/date=YYYY-MM-DD/hour=HH/
```

Objects mix markets. There is no per-market partition.

Important FULL_L2 delta columns are:

| Column | Arrow type | Meaning |
|---|---|---|
| `market_key` | `int64` | Stable deterministic positive 63-bit market key |
| `token_key` | `int64` | Stable deterministic positive 63-bit outcome-token key |
| `connection_id` | `int64` | Collector connection provenance |
| `source_ts_ns` | `int64` | Upstream timestamp when available |
| `exchange_ts_ns` | `int64` | Exchange/transport timestamp when available |
| `received_ts_ns` | `int64` | UTC process receive time |
| `received_monotonic_ns` | `int64` | Local ordering clock |
| `book_hash` | `string` | Authoritative hash only while it identifies the current state |
| `side` | `int8` | Buy/sell code |
| `action` | `int8` | Set/delete/delta code |
| `price_mantissa`, `size_mantissa` | `int64` | Exact decimal mantissa |
| `price_scale`, `size_scale` | `int8` | Exact decimal scale |

Snapshots use the same identity/timestamp fields and a compact list of level
structs containing `side:int8`, exact mantissa/scale price and size, plus a
snapshot-type code and reconciliation flag. Sampled observations use compact
mantissa/scale pairs for best prices, total depths and last trade. Reference
prices use `decimal256(76,36)` because RTDS TWAP precision can exceed a safe
`int64` mantissa.

The key is `SHA256("polymarket-archive-v2", kind, external_id)` truncated to a
positive 63-bit integer. PostgreSQL enforces uniqueness and persists the mapping;
the `archive_dictionary` stream makes it independently available in Parquet.
Assignment is deterministic, restart-safe, and idempotent. The reader resolves
ordinary condition/token IDs before applying Arrow predicate pushdown, so a
researcher does not need to know the integer keys.

Mantissa plus scale was chosen instead of float or a hard-coded tick scale.
It round-trips every source decimal exactly and does not assume Polymarket will
never change tick or size precision.

## Encoding benchmark

Input was 27,488 real order-book updates captured from public Polymarket
traffic. Logical verbose JSON occupied 16,797,338 bytes. Times are local
single-run wall-clock measurements and are useful for relative comparison, not
hardware-independent guarantees.

| Representation | Zstd | Row group | Dictionary pages | Parquet bytes | Bytes/row | Write | Full read | Filtered read |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| Verbose schema | 3 | 100,000 | yes | 339,064 | 12.335 | 31.06 ms | 6.88 ms | 8.27 ms |
| Compact numeric | 3 | 100,000 | yes | 371,725 | 13.523 | 22.66 ms | 3.47 ms | 4.13 ms |
| Verbose schema | 3 | 100,000 | no | 233,700 | 8.502 | 30.03 ms | 4.98 ms | 7.37 ms |
| **Compact numeric (chosen)** | **3** | **100,000** | **no** | **223,513** | **8.131** | **11.53 ms** | **3.93 ms** | **3.82 ms** |
| Compact numeric | 6 | 100,000 | yes | 343,695 | 12.503 | 31.51 ms | 3.74 ms | 4.02 ms |

At the same chosen encoding, the compact schema was 4.36% smaller than verbose,
2.61 times faster to write, 1.27 times faster for a full read, and 1.93 times
faster for the filtered read. The small size difference is real: repeated source
strings compress well. Compact primitives still win decisively on CPU, filtering,
precision semantics, and avoiding high-cardinality strings in analytical plans.
Dictionary pages made dense numeric streams larger, so they are disabled for
books/observations and retained only for genuinely categorical reference fields.
Zstd 3 is the production trade-off; Zstd 6 cost substantially more write CPU in
this test without beating the chosen layout.

## Live scaling benchmark

Discovery used a compact snapshot from one complete public crawl: 207 pages,
20,632 active events, 204,292 nested markets, and 158,407 trade-ready markets.
Each stage hydrated authoritative selected-market details before subscription.
Startup and shutdown were outside the exact 60-second rate window.

| FULL_L2 / SAMPLED | Status | Confirmed / desired | CPU avg | CPU peak | Peak RSS | WS msg/min | Archive rows/min | Compressed archive/min | Queue peak | PostgreSQL growth/min | WAL/min | Failures |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 / 50 | valid | 53 / 60 | 47.4% | 149.6% | 480 MB | 12,517 | 10,034 | 0.199 MB | 15.0 MB estimated | 1.76 MB | 17.0 MB | 0 |
| 25 / 100 | valid | 122 / 125 | 97.4% | 136.8% | 545 MB | 27,036 | 29,988 | 0.565 MB* | 46.5 MB estimated | 4.47 MB | 25.3 MB | 0 |
| 50 / 200 | valid but unsafe | 247 / 250 | 97.1% | 135.5% | 638 MB | 46,022 | 26,845 | 0.577 MB* | 40.5 MB estimated | 3.33 MB | 57.5 MB | 0 |
| 100 / 500 | invalid, stopped | not accepted | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | quarantined crossed derived rows; shutdown exceeded 420 s |

`*` The 25/100 and 50/200 runs preceded the final numeric dictionary-page
correction, so their measured files are conservative overestimates. Re-encoding
the 25/100 L2 rows at the chosen setting reduces that minute by about 148 KB.

The 100/500 attempt was not treated as data. It exposed a derived-observation
bug during transiently crossed multi-change frames and could not drain promptly.
The exact source-ordered deltas remain archiveable, while invalid crossed
derived observations are now suppressed and permanent database errors are
isolated without six pointless retries. The stage is still unvalidated and is
not production-safe.

A second 310-second 10/50 run crossed a normal five-minute archive flush:
56.3% average CPU, 676 MB peak RSS, 15.1 ms average event-loop lag with one
4.89-second serialization/host scheduling spike, 93,175 WebSocket messages,
zero database/archive failures, zero reconnects, and a fully drained queue.
That peak lag means scale-up requires a longer Railway pilot even at 10/50.

## Small-file result and compaction

The old run wrote 72 objects in 63 seconds: 68.6 objects/minute, 5,765,970
compressed bytes total, and about 80 KB/object.

The 310-second new run wrote 72 objects, but the composition matters:

- 60 were immutable content-addressed REST bodies from a metadata refresh on a
  fresh manifest;
- only 12 were batched stream files, or 2.32 stream objects/minute, a 96.6%
  reduction before compaction;
- those 12 non-REST objects averaged about 78 KB because 10/50 traffic is too
  small to reach a 16-64 MB compressed target inside one hour and shutdown
  forces final partial batches;
- the normal writer waits for 250,000 rows, about 48 MiB estimated input, or
  five minutes, whichever occurs first;
- hourly compaction merges compatible completed objects within the same
  stream/date/hour. At 10/50 the observed total flow implies only a few MB per
  compacted active stream-hour; at 25/100, L2 alone was about 13 MB/hour after
  the final encoding correction. Crossing hour partitions solely to force a
  larger file was rejected because it weakens partition pruning and durability.

Compaction verifies source manifest state, schemas, row counts, object size and
hash; uploads the replacement; atomically records replacement/supersession; and
only then deletes source objects. Tests cover success and partial failure. The
manifest keeps compact audit rows but does not duplicate large JSON metadata.

## Duplication removed

- FULL_L2 ordinary derived observations: 100% removed from permanent archive;
  the bounded PostgreSQL convenience window remains.
- Continuous FULL_L2 snapshots: removed. Only reconstruction/recovery/closing
  anchors and explicit hourly reconciliation evidence remain.
- SAMPLED static states: suppressed until the 900-second heartbeat. A static
  market no longer produces 1,440 identical daily rows.
- Raw WebSocket: 100% malformed/unknown/parser-error evidence plus a deterministic
  SHA-256-selected 0.1% known-valid sample. In the 10/50 minute, 13 raw audit
  rows represented 12,517 WebSocket messages.
- Reference values: consecutive identical semantic values are suppressed; a
  configurable 300-second unchanged heartbeat proves liveness. Raw payloads are
  not duplicated beside normalized reference values.
- Raw REST: one immutable body per SHA-256. A warm repeated full discovery crawl
  reused 197 of 207 bodies (95.17%); only 10 changed pages were uploaded,
  3,408,751 bytes. All 207 request/provenance observations were still recorded.

## Replay and research reader

Replay orders snapshots/deltas by receive timestamp and local monotonic order,
resets on reconstruction snapshots, applies exact set/delete/delta semantics,
deletes zero-size levels, and rejects a delta before an anchor. REST
reconciliations are intentionally excluded from mutation replay. Tests compare
the reconstructed levels, best bid/ask, empty-level deletion, reset boundaries,
and book hash behavior with a later known snapshot.

`load_archive()` accepts ordinary market/token IDs, time range and projected
columns. It reads the compact dictionary, resolves keys, prunes hour partitions,
and applies Arrow column projection and predicates. In the encoding benchmark,
the compact filtered read took 3.82 ms versus 7.37 ms for verbose at the same
compression setting.

## Tier correctness

Promotion does not depend solely on already-subscribed L2 activity. The complete
Gamma/CLOB discovery supplies public liquidity, 24-hour volume, spread/top-book,
maker reward, close-time and status signals. Those can promote a
`METADATA_ONLY` market directly to `SAMPLED` or `FULL_L2`. Recent live trades and
updates refine the score only after subscription. Tests cover both promotion
paths.

Promotion and demotion thresholds differ; minimum dwell defaults to 1,800
seconds; demotion has hysteresis; exact tier reasons/history are persisted; and
socket replacement is shard-specific. FULL_L2 reserves five places for a
deterministic wide-spread/low-liquidity research bucket plus explicit allowlist
entries, so the tier is not simply the highest-volume list.

## Capacity model

This is a planning model, not measured production capacity. It uses conservative
per-minute coefficients derived from the accepted runs and final encoding:

- L2: 9,350 bytes/FULL_L2 market/minute;
- sampled observations: 176 bytes/SAMPLED market/minute;
- reference feeds: 126,659 bytes/minute for the current explicit feed list;
- raw WebSocket sample: 155 bytes/selected market/minute;
- reconstruction snapshots/tier churn: 410 bytes/FULL_L2 market/minute;
- full raw REST discovery: 3,408,751 new bytes per 15-minute crawl after 95.17%
  content reuse;
- other/dictionary allowance: 1,000 bytes/minute.

The L2 and snapshot coefficients deliberately use burstier one-minute evidence,
not the lowest long-run interval. Actual markets are highly skewed and tier
composition can dominate a simple market-count model.

| Configuration | L2 MB/day | Sampled MB/day | Reference MB/day | Raw WS MB/day | Snapshots MB/day | Raw REST MB/day | Total archive GB/day | Archive GB/30d |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 / 50 | 134.6 | 12.7 | 182.4 | 13.4 | 5.9 | 327.2 | 0.678 | 20.3 |
| 25 / 200 | 336.6 | 50.7 | 182.4 | 50.2 | 14.8 | 327.2 | 0.963 | 28.9 |
| 50 / 200 | 673.2 | 50.7 | 182.4 | 55.8 | 29.5 | 327.2 | 1.320 | 39.6 |
| 50 / 250 | 673.2 | 63.4 | 182.4 | 67.0 | 29.5 | 327.2 | 1.344 | 40.3 |
| 100 / 500 | 1,346.4 | 126.7 | 182.4 | 133.9 | 59.0 | 327.2 | 2.177 | 65.3 |
| 500 / 1,000 | 6,732.0 | 253.4 | 182.4 | 334.8 | 295.2 | 327.2 | 8.127 | 243.8 |

The 500/1,000 row is a linear storage sensitivity only. It is not credible as
a single-worker deployment projection because even 50/200 saturated a core and
100/500 did not complete safely.

PostgreSQL is bounded for reference prices and observations, but normalized
trades remain permanent. At the measured central rate of roughly 0.4
trades/selected-market/minute and a deliberately conservative 700 physical
bytes/row including indexes, trade growth alone is approximately 0.73, 2.72,
3.63, 7.26 and 18.14 GB/month for 10/50, 25/200, 50/250, 100/500 and 500/1,000.
Those figures are highly activity-sensitive. With complete metadata/backfill,
hot windows, indexes and bloat headroom, plan roughly 4-8 GB occupied after one
month at 10/50 and 7-15 GB at 25/200, not a strict plateau. Larger tiers are not
capacity-approved. The 100 GB Railway volume, 70 GB warning and 85 GB critical
threshold leave necessary WAL/autovacuum/failure headroom.

The old 9-35 GB/day estimate used verbose 400-800-byte L2 rows and duplicated
snapshots/observations/raw evidence. The old 63-second run itself implies about
7.9 GB/day, close to that range's lower edge, but was dominated by startup and
tiny objects. The final L2 encoding measured 8.13 bytes/row, FULL_L2 derived
archive rows were removed, raw WebSocket retention fell to 0.1%, and raw REST is
content-addressed. The new 310-second 10/50 run physically produced 0.392 GB/day
at its observed rate; the conservative model including a complete warm discovery
every 15 minutes is 0.678 GB/day. These are quantitatively consistent once
startup and stream composition are separated.

## PostgreSQL retention and write amplification

The accelerated PostgreSQL 18 harness inserted 500,000 total rows across
reference prices and observations, performed four retention cycles, then a
post-vacuum reuse cycle.

| Measurement | Result |
|---|---:|
| Initial database | 12,867,263 bytes |
| Before vacuum after four fast cycles | 172,226,239 bytes |
| Dead tuples before vacuum | 250,000 observations; 249,979 references |
| Autovacuum count during accelerated burst | 0 (test completed before naptime) |
| Dead tuples after `VACUUM ANALYZE` | 0 |
| Heap reuse after another 100,000 inserts | heap sizes unchanged before retention |
| Final database after second vacuum/retention | 116,233,919 bytes |
| Total WAL | 458,803,968 bytes |

Ordinary vacuum does not promise filesystem shrink; the important result is
that heap free space was reused and live rows returned to 50,000 per table.
Indexes remained the largest concern. Migration 003 therefore sets 5,000-row
plus 2% vacuum triggers on both short-retention tables. Native time partitioning
was not added: the accelerated test does not justify that complexity yet.
Monitor index growth and autovacuum on a 24-hour Railway pilot.

The 310-second live run made 407,125 PostgreSQL row operations, generated
97,975,312 WAL bytes, and invoked autovacuum five times on current books/levels.
Current levels are changed incrementally; the collector does not append every
book version or rewrite every level after each delta.

## Failure, backpressure, and crash safety

Deterministic stress tests covered:

- temporary upload outage: two failures, third retry success, one logical object;
- prolonged outage with one-attempt runs: durable spool retained, writer marked
  degraded, queue stayed within configured row/byte limits; a healthy restart
  uploaded one manifest object and removed the spool;
- slow object store: 200 unique rows, bounded queue, zero loss;
- journal before serialization and crash after complete local file;
- upload/manifest idempotency and hash verification;
- manifest committed before local cleanup: restart verified the remote hash and
  removed the local residue;
- compaction partial failure: sources remained authoritative until a verified
  replacement was atomically committed;
- poison database rows: isolated/quarantined without killing the writer or
  deadlocking `queue.join()`.

Within configured queue/spool capacity, records lost were zero and duplicate
logical objects were zero. RAM is bounded by queue bytes, not just row count.
Beyond hard spool/queue capacity, the writer records an explicit degradation/gap
and fails rather than silently claiming completeness. A multi-hour wall-clock
outage under Railway disk pressure was not run; this remains an operational
limit.

## Conservative Railway configuration

The measured starting point is 10 FULL_L2 / 50 SAMPLED, not 25/200. The latter
would contradict the 97.4% CPU result at only 25/100.

Start the single live worker with a 2 vCPU / 2 GB RAM ceiling. The long 10/50
sample peaked at 676 MB and briefly used more than one core; a 1 vCPU / 1 GB
ceiling leaves inadequate failure/backlog headroom even though average use was
lower.

```dotenv
FULL_L2_MAX_MARKETS=10
SAMPLED_MAX_MARKETS=50
FULL_L2_RESEARCH_RESERVE=5
METADATA_SYNC_INTERVAL_SECONDS=900
TIER_REEVALUATION_INTERVAL_SECONDS=300
TIER_MIN_DWELL_SECONDS=1800
FULL_L2_OBSERVATION_INTERVAL_SECONDS=300
SAMPLED_SNAPSHOT_INTERVAL_SECONDS=30
SAMPLED_HEARTBEAT_INTERVAL_SECONDS=900
ORDERBOOK_RECONCILE_INTERVAL_SECONDS=3600
ARCHIVE_BATCH_ROWS=250000
ARCHIVE_BATCH_BYTES=50331648
ARCHIVE_FLUSH_SECONDS=300
ARCHIVE_ZSTD_LEVEL=3
ARCHIVE_ROW_GROUP_ROWS=100000
ARCHIVE_COMPACTION_ENABLED=true
ARCHIVE_COMPACTION_INTERVAL_SECONDS=3600
RAW_WS_POLICY=errors_sample
RAW_WS_VALID_SAMPLE_RATE=0.001
REFERENCE_UNCHANGED_HEARTBEAT_SECONDS=300
POSTGRES_REFERENCE_RETENTION_HOURS=6
POSTGRES_OBSERVATION_RETENTION_HOURS=24
CLOSED_MARKET_HOT_STATE_GRACE_HOURS=24
POSTGRES_STORAGE_WARN_GB=70
POSTGRES_STORAGE_CRITICAL_GB=85
LOG_LEVEL=INFO
JSON_LOGS=true
```

Run 10/50 for at least 24 hours. Increase to 15/75, then 20/100, then 25/100;
do not jump to 25/200. A stage passes only if sustained CPU is below 70%, peak
RSS stays below 70% of the service limit, p99 event-loop lag is below 100 ms,
archive queue remains below the warning threshold and drains, no unexplained
gaps/write failures occur, WAL/checkpoints do not cause sustained I/O pressure,
and measured archive growth remains within the budget for 24 hours. Roll back a
stage on any failed condition. 50/200 and 100/500 are specifically not approved.

## Railway costs and backups

As verified on 2026-08-14, Railway Storage Buckets cost $0.015/GB-month and S3
operations/bucket egress are free. Uploads from a Railway service to its bucket
still incur service egress at $0.05/GB because buckets use the public network.
See [Storage Buckets Billing](https://docs.railway.com/storage-buckets/billing)
and [Railway pricing](https://railway.com/pricing).

| Stored for a full month | Bucket storage | One-time service egress to upload that volume |
|---:|---:|---:|
| 100 GB | $1.50 | $5.00 |
| 250 GB | $3.75 | $12.50 |
| 500 GB | $7.50 | $25.00 |
| 1,000 GB | $15.00 | $50.00 |

These exclude plan, CPU, RAM, PostgreSQL volume, PITR, and ongoing new upload
egress. Railway volume storage is separately $0.15/GB-month based on actual
usage; a completely full 100 GB database volume would be about $15/month.

Before collecting irreplaceable data, enable Daily and Weekly scheduled volume
backups in the PostgreSQL service. Railway documents six-day retention for
daily and one-month retention for weekly schedules in
[Volume Backups](https://docs.railway.com/volumes/backups). Enable
[PostgreSQL PITR](https://docs.railway.com/volumes/point-in-time-recovery) once
the dataset matters; it uses daily incremental/weekly full pgBackRest backups
and WAL with roughly a four-week restore window, billed through bucket storage
and service egress. None of these production actions were performed by this
task.

## Remaining limits

- Polymarket market books do not expose a universal sequence number. Replay
  relies on exact receive/monotonic ordering within connection boundaries and
  a fresh snapshot after gaps/reconnects.
- No benchmark ran for 24 hours or on Railway hardware. CPU, storage and network
  performance can differ materially from this Windows host.
- 50/200 saturated a core; 100/500 was invalid. 500/1,000 is capacity arithmetic,
  not an operational claim.
- The benchmark's confirmed count can be below desired when illiquid selected
  tokens do not emit an initial book during the bounded warm-up. Coverage and
  open gaps remain explicit rather than being marked complete.
- Hourly compaction is unit/integration tested but was not left running against
  a day of real S3 objects.
- The outage suite is deterministic; it did not fill a real 2 GiB spool over a
  multi-hour outage or measure Railway-volume RSS/I/O under that condition.
- Raw REST reuse varies because Gamma pages include volatile values. The 95.17%
  figure is one repeated-crawl observation, not a guaranteed ratio.
- Normalized trades remain permanent in PostgreSQL and therefore prevent a
  strict size plateau. Move them to a compact archive only after a real 24-hour
  growth study justifies another migration.
