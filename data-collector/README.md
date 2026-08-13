# Prediction-market data collector

Read-only Polymarket and Kalshi ingestion for market-making research. The
collector writes normalized records and optional raw source payloads to
PostgreSQL. It never submits, cancels, or modifies exchange orders.

## What is collected

- Polymarket Gamma metadata for current and closed events/markets, outcomes,
  tags, negative-risk grouping, fees, rewards, and metadata versions.
- Polymarket historical trades using explicit per-market `start`/`end` epoch
  windows, public comments, order books, price history, holder snapshots, and
  raw REST responses. Saturated trade windows are recursively bisected instead
  of silently stopping at the Data API offset ceiling.
- Polymarket CLOB live books, price changes, trades, market state, tick-size
  changes, exchange timestamps, and book hashes.
- Polymarket RTDS Binance, Chainlink, TWAP, Pyth-backed equity/ETF/FX/metal/
  commodity reference prices, plus public comment events.
- Polymarket's live sports feed.
- Kalshi current and historical series/events/markets, fixed-point prices and
  quantities, categorical results, numeric settlement values, price-level
  structures, trades, books, candlesticks, fee changes, incentive programs,
  multivariate REST collections/selected-leg relationships, and raw REST
  responses.
- Kalshi authenticated live books, trades, tickers, lifecycle streams,
  multivariate lifecycle events, event fee overrides, CF Benchmarks indices,
  and Pyth underlying prices.
- Connection history, exchange-acknowledged subscribed markets, sequence ranges,
  reconnect reasons, dropped-message counters, explicit data gaps, archived
  reconciliation snapshots, per-minute throughput, and every live-market
  inclusion/exclusion decision.

## Live coverage is unrestricted by default

The production defaults are:

```dotenv
MAX_LIVE_MARKETS=0
MIN_LIVE_MARKET_VOLUME=0
MIN_LIVE_MARKET_LIQUIDITY=0
LIVE_MARKET_ALLOWLIST=
LIVE_MARKET_BLOCKLIST=
```

Zero means no restriction. With those values, every discovered market that is
currently active and tradable is eligible for continuous live subscription.
There is no hidden or hard-coded market cap.

Coverage counters are deliberately separate:

- `discovered` counts every candidate returned by enabled exchange discovery;
- `active` counts candidates whose exchange active flag is true, whether or not
  their tradable flag is also true;
- `tradable` counts candidates whose exchange tradable flag is true, whether or
  not their active flag is also true;
- `markets_selected_for_subscription` is the eligible subscription plan after
  filters and an optional cap;
- `subscribed` is the distinct market count on currently open, exchange-
  confirmed subscription rows. It is not incremented merely because the client
  sent a subscription request.

A market must be both active and tradable to enter the subscription plan.

`POLYMARKET_WS_SUBSCRIPTION_CHUNK_SIZE` and
`KALSHI_WS_SUBSCRIPTION_CHUNK_SIZE` only divide a complete subscription set
across connections. They do not exclude markets.

### Selection and exclusion order

The collector evaluates every discovered market in this order:

1. Exclude inactive markets as `inactive`.
2. Exclude markets that are not currently tradable as `not_tradable`.
3. If an exchange requires credentials that are not configured, exclude its
   live markets as `credentials_missing`; public REST collection remains on.
4. Apply `LIVE_MARKET_BLOCKLIST` as `blocklist`.
5. If an allowlist is non-empty, exclude unmatched markets as
   `not_in_allowlist`.
6. Apply total-volume and liquidity minimums as `below_min_volume` or
   `below_min_liquidity`.
7. Deterministically sort remaining markets by liquidity descending, 24-hour
   volume descending, total volume descending, exchange ascending, and external
   market ID ascending.
8. If `MAX_LIVE_MARKETS` is non-zero, keep the first N and mark the remainder
   `max_live_markets_cap`.

Allow/block entries are comma-separated selectors. Discovery exposes external
market IDs, tickers, slugs, Polymarket condition IDs, and outcome token IDs as
selectors. A blocklist match wins. Configuration fails fast if the same literal
selector appears in both lists.

Each market connection starts in `connecting` state without market-membership
rows. Polymarket confirms it only after the initial book dump has arrived for
every requested outcome token; Kalshi confirms only after every requested
channel has acknowledged the subscription. Confirmation changes the connection
to `connected` and inserts the exact `collector_connection_markets` rows. The
periodic throughput record queries these confirmed, still-open rows to report
the actual subscribed-market count. Closed, rejected, timed-out, or not-yet-
acknowledged requests do not inflate it.

Periodic discovery compares the actual socket definition, not just market IDs.
For Polymarket that fingerprint includes the sorted outcome-token set. If an
existing market gains, loses, or changes a token while the selected market IDs
remain identical, the collector still rebuilds the affected subscription
connections so the new token set is covered.

Kalshi's legacy `liquidity_dollars` field is deprecated and no longer provides
usable liquidity. The collector records Kalshi candidate liquidity as unknown
instead of pretending zero is a measurement. Consequently, any non-zero
`MIN_LIVE_MARKET_LIQUIDITY` currently excludes Kalshi markets. Use volume or an
allowlist for Kalshi until computed order-book liquidity is implemented.

### Historical work is never live-capped

`MAX_LIVE_MARKETS`, live minimums, allowlists, and blocklists apply only to
continuous market WebSocket subscriptions and their periodic reconciliation.
They do **not** affect metadata history, historical trades, comments, fees,
incentives, books, candlesticks, holders, or raw REST backfills. The backfill
commands do not call the live-selection function.

### Polymarket trade-window completeness

Polymarket's Data API rejects trade offsets beyond 10,000. The backfill avoids a
false “complete” result by querying each market with explicit inclusive
`start`/`end` epoch-second windows. If both 10,000-row pages are full, the window
is saturated and is recursively bisected into disjoint older/newer halves. This
continues down to a one-second window.

If a one-second window still saturates the offset budget, the retrieved rows are
kept but `data_gaps` records `rest_pagination_limit`; the run is not silently
certified complete. Market-scoped queries also honor Polymarket's documented
approximately three-year history floor. If the known market open time predates
that floor, the collector starts at the floor and records `upstream_history_floor`.
These are upstream coverage constraints, not `MAX_LIVE_MARKETS` behavior.

## Architecture

```text
Polymarket REST/Gamma/CLOB -------> exchange services ----+
Polymarket CLOB/RTDS/sports WS --> frame-time parsers -----+--> bounded batch writer --> PostgreSQL 18
Kalshi REST ----------------------> exchange services -----+
Kalshi market/lifecycle/ref WS --> sequence-aware parsers -+
                                             |
                                             +--> connection, gap, coverage and throughput telemetry
```

Each WebSocket frame is timestamped immediately on receipt with wall-clock and
monotonic clocks before JSON parsing. Normalized records and optional raw
payloads flow through a bounded queue and batched PostgreSQL writer. Periodic
metadata rediscovery updates the subscription set without introducing a market
coverage cap.

## Requirements

- Python 3.14 or Docker with Compose v2.
- PostgreSQL 18 for native execution. Docker Compose provisions it.
- Internet access to the configured exchange REST and WebSocket endpoints.
- A newly rotated Kalshi API key ID and RSA private-key file for Kalshi live
  WebSockets, lifecycle, CF Benchmarks, and Pyth. Public Kalshi REST does not
  require the key.

## Native setup on Windows PowerShell

Run from the repository root:

```powershell
Set-Location data-collector
Copy-Item .env.example .env
py -3.14 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -e ".[test]"
```

Edit `.env`. Set `POSTGRES_*` for the PostgreSQL instance. For authenticated
Kalshi streams, set the key ID and an absolute path to a **rotated** PEM file:

```dotenv
KALSHI_API_KEY_ID=your-new-key-id
KALSHI_PRIVATE_KEY_PATH=C:/Users/your-user/.secrets/kalshi-collector.pem
```

Restrict the Windows file ACL. Substitute the real path:

```powershell
icacls C:\Users\your-user\.secrets\kalshi-collector.pem /inheritance:r
icacls C:\Users\your-user\.secrets\kalshi-collector.pem /grant:r "$($env:USERNAME):(R)"
```

Then validate public APIs, migrate, and start live collection immediately:

```powershell
& .\.venv\Scripts\python.exe -m prediction_collector smoke --exchange all
& .\.venv\Scripts\python.exe -m prediction_collector migrate
& .\.venv\Scripts\python.exe -m prediction_collector run --exchange all
```

Run the historical backfill in another terminal after the permanent live
collector is connected. REST history can be recovered later; missed WebSocket
microstructure generally cannot:

```powershell
& .\.venv\Scripts\python.exe -m prediction_collector backfill --exchange all
```

Use another terminal for status:

```powershell
& .\.venv\Scripts\python.exe -m prediction_collector status
```

`status` exits non-zero when migrations are pending/inconsistent or when an
enabled live exchange is degraded. Its `live.<exchange>` object reports
`discovery_state`, active connections, selected markets, exchange-confirmed
memberships, latest WebSocket receipt time, and open discovery gaps. A populated
catalog with zero live connections/messages is therefore not reported healthy.
The command performs only reads; it never creates the migration table or
applies schema changes.

Stop live collection with `Ctrl+C`; shutdown drains queued database writes.

## Native setup on Linux

Run from the repository root:

```bash
cd data-collector
cp .env.example .env
python3.14 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[test]'
```

Place a newly rotated key outside the repository, restrict it, and configure its
absolute path in `.env`:

```bash
install -d -m 700 "$HOME/.secrets"
# Securely transfer the rotated PEM to the path below before changing its mode.
chmod 600 "$HOME/.secrets/kalshi-collector.pem"
```

Do not use the shell to echo or interpolate the private-key contents. Transfer
an already-created PEM file using your normal secret-delivery process.

Run the collector:

```bash
.venv/bin/python -m prediction_collector smoke --exchange all
.venv/bin/python -m prediction_collector migrate
.venv/bin/python -m prediction_collector run --exchange all
```

In another terminal, start the recoverable historical work only after the live
collector is running, and inspect status independently:

```bash
.venv/bin/python -m prediction_collector backfill --exchange all
.venv/bin/python -m prediction_collector status
```

## Docker Compose

Copy the example configuration and replace the local database password:

```powershell
Set-Location data-collector
Copy-Item .env.example .env
```

### Full authenticated collection

Set these values in `.env`:

```dotenv
KALSHI_API_KEY_ID=your-new-key-id
KALSHI_PRIVATE_KEY_HOST_PATH=C:/Users/your-user/.secrets/kalshi-collector.pem
```

`KALSHI_PRIVATE_KEY_HOST_PATH` is interpreted by Docker Compose. The authenticated
service bind-mounts that file read-only at
`/run/collector-secrets/kalshi-private-key.pem` and overrides the application's
`KALSHI_PRIVATE_KEY_PATH` accordingly. The key is excluded from the build
context.

On Linux, a mode-0600 bind mount remains subject to numeric ownership inside
the container. Get the owner IDs of the rotated PEM and put them in `.env`:

```bash
id -u
id -g
```

```dotenv
COLLECTOR_UID=1000
COLLECTOR_GID=1000
```

Replace `1000` with the command outputs. The authenticated service runs with
those IDs so it can read the key without making the PEM group/world-readable.
Docker Desktop on Windows maps host ACLs into its Linux VM and normally uses the
example defaults.

Start PostgreSQL 18, run the one-shot migration, and start the full collector:

```powershell
docker compose --profile authenticated up --build -d collector
docker compose ps -a
docker compose logs -f collector
```

The database health check must pass and the migration service must complete
successfully before the collector starts. The collector health check runs the
database `status` command. `status` is strictly read-only: it verifies migration
checksums and reports pending migrations but never applies them. Use the
one-shot `migrate` service or normal collector startup to change schema state.

### Public-only fallback

This mode needs no key:

```powershell
docker compose --profile public up --build -d collector-public
docker compose logs -f collector-public
```

It is not comprehensive Kalshi collection. It intentionally disables
authenticated Kalshi market, lifecycle, CF Benchmarks, and Pyth WebSockets while
retaining Polymarket live feeds and both exchanges' public REST collection.

### Compose operations

```powershell
# JSON database/table status
docker compose exec collector python -m prediction_collector status

# Run an uncapped historical backfill in a one-shot container
docker compose run --rm collector backfill --exchange all

# Public REST smoke check without database writes
docker compose run --rm collector smoke --exchange all

# Re-run migrations; already-applied files are checksum-verified and skipped
docker compose run --rm migrate

# Stop containers while preserving PostgreSQL data
docker compose down
```

Replace `collector` with `collector-public` for a public-only deployment.

## CLI reference

All commands load `.env` from the working directory.


| Command                                                         | Effect                                                                                                                                                                                  |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `python -m prediction_collector smoke --exchange all`           | Calls current public REST discovery endpoints without database writes.                                                                                                                  |
| `python -m prediction_collector migrate`                        | Applies immutable SQL migrations and records checksums in`schema_migrations`.                                                                                                           |
| `python -m prediction_collector backfill --exchange polymarket` | Uncapped Polymarket metadata, recursive start/end-window trade history, comments, fees/rewards, books/history/holders, and raw REST backfill.                                           |
| `python -m prediction_collector backfill --exchange kalshi`     | Uncapped Kalshi current/historical metadata, fee/incentive history, trades, books/candles, and raw REST backfill.                                                                       |
| `python -m prediction_collector backfill --exchange all`        | Runs each exchange backfill sequentially. Live coverage controls are ignored.                                                                                                           |
| `python -m prediction_collector run --exchange all`             | Continuous live collection, periodic discovery, ack-confirmed coverage metrics, archived REST reconciliation evidence, hourly economics, six-hour Polymarket fee rates, and throughput. |
| `python -m prediction_collector status`                         | Tests the database and prints selected table counts plus recent collector state.                                                                                                        |

`POLYMARKET_ENABLED=false` or `KALSHI_ENABLED=false` disables that exchange even
if `--exchange all` is supplied. Requesting an exchange configuration in which
no exchange remains enabled fails instead of silently doing nothing.

## Configuration

[`data-collector/.env.example`](.env.example) is the canonical complete list.
The code validates integers, non-negative decimal thresholds, booleans, pool
sizes, and allow/block overlap at startup.

### Database

- `DATABASE_URL`: preferred complete PostgreSQL DSN when non-empty. Railway
  should use this.
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`,
  `POSTGRES_PORT`: used when `DATABASE_URL` is empty.
- `DATABASE_POOL_MIN_SIZE`, `DATABASE_POOL_MAX_SIZE`, `DATABASE_BATCH_SIZE`,
  `DATABASE_FLUSH_INTERVAL_SECONDS`, `DATABASE_QUEUE_SIZE`: database backpressure
  and batching controls.

`POSTGRES_BIND_HOST`, `KALSHI_PRIVATE_KEY_HOST_PATH`, `COLLECTOR_IMAGE`,
`COLLECTOR_UID`, and `COLLECTOR_GID` are Docker Compose inputs rather than
application settings.

### Exchange and retention switches

- `POLYMARKET_ENABLED`, `KALSHI_ENABLED`
- `STORE_RAW_WS`, `STORE_RAW_REST`
- `POLYMARKET_RTDS_ENABLED`, `POLYMARKET_SPORTS_ENABLED`,
  `POLYMARKET_COMMENTS_ENABLED`
- `KALSHI_REFERENCE_FEEDS_ENABLED`
- `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`

### Coverage and scheduling

- `MAX_LIVE_MARKETS`, `MIN_LIVE_MARKET_VOLUME`,
  `MIN_LIVE_MARKET_LIQUIDITY`
- `LIVE_MARKET_ALLOWLIST`, `LIVE_MARKET_BLOCKLIST`
- `METADATA_SYNC_INTERVAL_SECONDS`
- `ECONOMICS_SYNC_INTERVAL_SECONDS`: general live fee/reward/incentive refresh;
  default `3600` seconds (hourly), minimum 60 seconds.
- `POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS`: separate current-live-token fee
  refresh; default `21600` seconds (six hours), minimum 900 seconds.
- `MARKET_SNAPSHOT_INTERVAL_SECONDS`, `ORDERBOOK_RECONCILE_INTERVAL_SECONDS`,
  `METRICS_LOG_INTERVAL_SECONDS`
- `POLYMARKET_WS_SUBSCRIPTION_CHUNK_SIZE`,
  `KALSHI_WS_SUBSCRIPTION_CHUNK_SIZE`
- `POLYMARKET_EQUITY_SYMBOLS`

### HTTP, logs, and endpoints

- `HTTP_CONCURRENCY`, `HTTP_TIMEOUT_SECONDS`, `HTTP_MAX_ATTEMPTS`
- `LOG_LEVEL`; `JSON_LOGS=true` is the production default and preserves every
  structured coverage/throughput field in container logs.
- `POLYMARKET_GAMMA_URL`, `POLYMARKET_DATA_URL`, `POLYMARKET_CLOB_URL`,
  `POLYMARKET_WS_URL`, `POLYMARKET_RTDS_URL`,
  `POLYMARKET_SPORTS_WS_URL`, `KALSHI_API_URL`, `KALSHI_WS_URL`

## Live feeds and recovery behavior

### Polymarket

- CLOB market WebSockets are split across connections without dropping eligible
  markets. Snapshots and updates retain exchange timestamps and the full-book
  hash when supplied. A connection becomes confirmed only after every requested
  outcome token has produced its initial book dump.
- Metadata rediscovery fingerprints each selected market's current outcome-token
  set. A token-set change rotates only the affected stable subscription shard,
  even when no market ID was added or removed; unrelated shards stay connected.
- Polymarket does not expose a sequence number equivalent to Kalshi's order-book
  sequence. Disconnects are therefore recorded as unknown gaps rather than
  inventing precise missing ranges.
- Periodic CLOB REST order-book snapshots are archived with
  `is_reconciliation=true` as independent reconciliation evidence. They do not
  reset, replace, or otherwise mutate the in-memory live WebSocket book: the
  REST request races with WebSocket deltas and has no shared ordering boundary.
  Only ordered WebSocket book messages mutate live state.
- RTDS subscribes to Binance crypto, Chainlink crypto/TWAP, configured Pyth
  equity-style instruments, and comment events. Comment-created events are
  normalized; enabled raw WebSocket retention preserves the other comment and
  reaction event types.
- The sports socket runs independently so sports-feed failure cannot cap market
  subscriptions.

### Kalshi

- Authenticated market WebSockets subscribe to order-book deltas, trades, and
  tickers for every selected market. Market rows count as confirmed only after
  all requested channel acknowledgements have arrived.
- Order books use snapshot followed by sequence-aware deltas. A discontinuity
  creates a `data_gaps` record, marks the in-memory book invalid, requests a new
  WebSocket snapshot, and records the recovery used to resolve the gap. This
  protocol snapshot is ordered recovery; the separate periodic REST snapshot is
  archived evidence and does not mutate the live book.
- Lifecycle and multivariate lifecycle streams are global and are not subject to
  `MAX_LIVE_MARKETS`.
- Metadata sync calls the dedicated multivariate-event REST surface with nested
  markets and the multivariate collection surface. Collection constraints are
  stored in `market_groups`; selected legs become `market_group_members` and
  directional `multivariate_leg` rows in `market_relationships` pointing to the
  resolved target market/outcome. An authoritative leg-set change closes the
  removed membership/relationship validity intervals and retains unchanged
  edges, so there is only one coherent current constraint set. Event-only
  creation notifications are stored in `event_lifecycle_events` instead of
  fabricating a market identity.
- CF Benchmarks subscribes with `index_ids=["all"]`; Pyth subscribes with
  `underlying_tickers=["all"]`. These global reference feeds are also outside
  the market cap.

All connections reconnect with bounded exponential backoff and retain their
attempt/reason history.

### Failure-tolerant bootstrap and discovery

Live startup has no all-exchanges REST barrier. The writer, Polymarket RTDS,
Polymarket sports, Kalshi lifecycle/reference feeds, metrics, and other
discovery-independent supervisors start immediately. Polymarket and Kalshi each
have an independent market-discovery loop. A slow or failed Kalshi crawl cannot
delay Polymarket subscriptions, and vice versa.

Each failed discovery attempt logs the concrete exception and retry delay,
opens one `rest:market_discovery` data gap for the outage, and retries forever
with jittered exponential backoff capped at 60 seconds. A successful complete
crawl resets the backoff, resolves that gap, persists the complete coverage
decision, and schedules stale-market reconciliation. No Railway redeploy is
required after a transient Gamma or Kalshi REST outage.

Polymarket live discovery follows the documented active-event relation:
`/events/keyset?closed=false`, whose events include nested markets. Returned
events and markets are filtered by their authoritative `active`/`closed`
fields. This avoids traversing the much larger direct market relation before
subscribing. Cursor repetition, missing cursors on full pages, malformed
wrappers, and HTTP exhaustion fail visibly; isolated malformed nested objects
are retained in raw REST evidence and aggregated into a schema-quality gap.

When `MAX_LIVE_MARKETS=0`, no minimum thresholds are active, and there are no
allow/block lists, each completed REST page is merged into the desired universe
and its eligible markets are added to stable WebSocket shards immediately.
Complete pagination continues in the background and still converges on every
eligible market. Configurations requiring global ranking wait for the complete
exchange crawl before applying the cap. Kalshi discovers ordinary open markets
first, then its multivariate event relation, so the large MVE universe does not
block ordinary market capture while eventual unrestricted coverage is retained.

Exact status enrichment for markets absent from a complete open response runs
asynchronously, so a restored database with many stale active rows cannot delay
current live subscriptions.

### Live economics refresh

Economics history is not frozen at process startup:

- On startup and then every hour by default,
  `ECONOMICS_SYNC_INTERVAL_SECONDS=3600` refreshes Polymarket rewards/incentives
  and Kalshi fee/incentive configurations. This general pass deliberately omits
  per-token Polymarket CLOB fee calls.
- On startup and then every six hours by default,
  `POLYMARKET_FEE_RATE_SYNC_INTERVAL_SECONDS=21600` independently refreshes
  authoritative per-token fee rates for current live Polymarket markets.
- Explicit historical backfills still collect the comprehensive economics
  history and are unaffected by live market caps.
- Fee and incentive schedules are serialized per exchange/scope/type and
  stitched into non-overlapping effective intervals. Out-of-order historical
  schedule pages and concurrent lifecycle/REST observations cannot create
  multiple contradictory current rows.
- A failed scheduled refresh is logged and stored as a `data_gaps` event with
  its channel, cadence, and error; one exchange's failure does not stop the
  other exchange's refresh loop.

## Timestamp and ordering model

Timestamp columns are nullable when an upstream does not provide that clock.
Raw timestamp strings are retained where available so future parsing changes do
not destroy source evidence.


| Field                        | Meaning                                                                                                                            |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `source_timestamp`           | Original provider/underlying timestamp, such as Pyth or CF Benchmarks publication time.                                            |
| `exchange_timestamp`         | Timestamp applied by Polymarket or Kalshi to the exchange envelope/message.                                                        |
| `received_at`                | UTC wall-clock timestamp captured immediately after this process receives the WebSocket frame.                                     |
| `received_monotonic_ns`      | Local monotonic nanoseconds captured at frame receipt; use for ordering within one process lifetime, not across hosts or restarts. |
| `persisted_at`               | PostgreSQL server time when the row is written.                                                                                    |
| `sequence_number` / `cursor` | Exact upstream ordering marker when supplied.                                                                                      |
| `book_hash`                  | Exact Polymarket book hash when supplied; it is not treated as a sequence number.                                                  |

For latency research, compare `source_timestamp` or `exchange_timestamp` with
`received_at`. Do not compare `received_monotonic_ns` across collector instances.

## Data model

Migration [`001_initial.sql`](migrations/001_initial.sql) creates these groups:


| Area                             | Tables                                                                                                       | Purpose                                                                                                                                                                                             |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Catalog                          | `series`, `events`, `markets`, `outcomes`, `tags`, `event_tags`, `market_tags`                               | Current exchange metadata and normalized identifiers.`markets` keeps categorical `result` and numeric `settlement_value` separate.                                                                  |
| Runs and coverage                | `collector_runs`, `collector_metrics`, `live_market_subscription_decisions`, `collector_checkpoints`         | Job status, separate discovered/active/tradable/ack-confirmed-subscribed/excluded counts, exact reasons/config snapshots, throughput, and resumable cursors.                                        |
| Connections and quality          | `collector_connections`, `collector_connection_markets`, `data_gaps`, `collector_write_failures`             | Pending/confirmed connection state, confirmed market membership, first/last sequence, received/dropped counts, reconnect causes, missing ranges, resolution actions, and quarantined poison writes. |
| Core market data                 | `trades`, `orderbook_snapshots`, `orderbook_updates`, `market_snapshots`, `candlesticks`, `holder_snapshots` | Normalized trades, books, quotes, candles, and holder observations with multi-clock provenance.                                                                                                     |
| Public information flow          | `comments`                                                                                                   | Historical and live public Polymarket comments with source timestamps and public profile identifiers.                                                                                               |
| Versioned metadata and lifecycle | `market_metadata_history`, `market_lifecycle_events`, `event_lifecycle_events`                               | Append-only market metadata versions, market activation/deactivation/close-date/determination/settlement/price-structure events, and event-level creation history that has no market ticker.        |
| Economics                        | `fee_configuration_history`, `incentive_configuration_history`                                               | Time-varying global/series/event/market fees, maker rebates, rewards, sizing, and spread constraints.                                                                                               |
| Linked markets                   | `market_groups`, `market_group_members`, `market_relationships`                                              | Negative-risk/augmented structures plus Kalshi multivariate collections, selected-leg membership validity, and directional market/outcome constraints.                                              |
| Reference feeds                  | `reference_instruments`, `market_reference_instruments`, `reference_price_updates`                           | Binance, Chainlink, Pyth, and CF Benchmarks instruments/updates plus optional market mappings.                                                                                                      |
| Sports feeds                     | `sports_events`, `market_sports_events`, `sports_feed_updates`                                               | Exchange-delivered sports state and optional prediction-market links.                                                                                                                               |
| Source evidence                  | `raw_rest_payloads`, `raw_ws_messages`                                                                       | Deduplicated raw REST payloads and frame-level WebSocket evidence with receipt time, sequence, cursor, and hash.                                                                                    |

Prices, probabilities, sizes, rates, and money use PostgreSQL `NUMERIC`, and the
Python collector parses them with `Decimal`; binary floating-point is not used
for normalized financial values.

`markets.result` and `market_metadata_history.result` hold categorical outcomes
such as `yes`, `no`, or an exchange result label. Their separate
`settlement_value` columns hold a numeric payout/settlement value. The collector
does not overload the categorical result with a dollar value or infer one field
from the other. Kalshi multivariate selected-leg settlement values remain in the
relationship constraint payload as well.

## Logs and load measurement

With `JSON_LOGS=true`, each line is machine-readable JSON. The live collector
logs and persists:

- discovered market count;
- active market count and tradable market count as separate values;
- markets selected for subscription after filtering/capping;
- actual subscribed market count derived from currently open,
  exchange-acknowledged connection-market rows;
- excluded count and counts by exclusion reason;
- WebSocket messages per minute by source;
- database rows written per minute by table, measured from PostgreSQL's
  committed insert/update/delete counters. Application-accounted per-kind
  counters are logged alongside the server measurement for diagnosis.

The server-side value comes from `pg_stat_user_tables`, so it includes every
writer using the same database, not only this process. The supplied Compose
deployment uses a dedicated database, where this is the most accurate measure
of actual ingestion write load and captures direct metadata/control writes as
well as the batch writer.

The default interval is 60 seconds. Change `METRICS_LOG_INTERVAL_SECONDS` only if
the observation window needs to differ. Measure these values before introducing
a cap; setting an arbitrary cap first defeats the point of the instrumentation.

Normal full-market metadata collection keeps INFO logging bounded by syncs, not
by records or REST pages. Each completed metadata sync emits one INFO summary
with `stale_lifecycle_states_preserved`, `unresolved_multivariate_legs`,
`unresolved_multivariate_leg_markets`, and
`unresolved_multivariate_leg_outcomes`. The corresponding per-market and
per-leg diagnostics, as well as pagination progress, are DEBUG-only. WebSocket
connects/disconnects, exchange-confirmed subscriptions, sequence gaps, errors,
and periodic throughput metrics remain visible at INFO or WARN as appropriate.
This prevents ordinary comprehensive discovery from producing log volume that
grows linearly with the number of markets or multivariate legs; genuine faults
remain intentionally visible and can still produce incident-time bursts.

Useful PostgreSQL queries:

```sql
-- Most recent coverage decision summary.
SELECT exclusion_reason, is_subscribed, count(*)
FROM live_market_subscription_decisions
WHERE evaluated_at >= now() - interval '10 minutes'
GROUP BY exclusion_reason, is_subscribed
ORDER BY is_subscribed DESC, exclusion_reason NULLS FIRST;

-- Recent throughput and coverage buckets.
SELECT interval_start, interval_seconds, websocket_messages,
       database_rows_written, messages_dropped,
       markets_discovered, markets_active, markets_tradable,
       markets_subscribed, markets_excluded
FROM collector_metrics
ORDER BY interval_start DESC
LIMIT 120;

-- Open or unresolved data-quality gaps.
SELECT exchange, channel, market_external_id, gap_type,
       missing_sequence_start, missing_sequence_end, detected_at,
       reconnect_reason, resolution_action
FROM data_gaps
WHERE resolved_at IS NULL
ORDER BY detected_at DESC;

-- Recent connection state.
SELECT exchange, channel, status, connected_at, disconnected_at,
       first_sequence, last_sequence, messages_received, messages_dropped,
       reconnect_attempt, reconnect_reason, disconnect_reason
FROM collector_connections
ORDER BY created_at DESC
LIMIT 100;
```

## Railway deployment

Use a separate Railway PostgreSQL service and one collector worker. Importing
the local Compose file is possible, but separating the managed database and
worker is easier to operate.

1. Create a Railway project and add a PostgreSQL service.
2. Create a service from this Git repository. Set its Root Directory to
   `/data-collector`. Railway will detect the `Dockerfile` at that root.
3. Do not generate a public domain; this is a worker, not an HTTP service.
4. Set `DATABASE_URL` on the collector service to the Railway reference variable
   `${{Postgres.DATABASE_URL}}`, adjusting `Postgres` if the database service has
   a different name.
5. Copy application variables from `.env.example`, but leave `POSTGRES_*` out
   when `DATABASE_URL` is set. Keep all three live limits at zero for complete
   default coverage. Set `JSON_LOGS=true`.
6. Revoke the exposed Kalshi key and create a new one. Attach a small Railway
   volume to the collector at `/run/collector-secrets`, transfer the rotated PEM
   into that volume with `railway volume browse /` or `railway volume files upload /secure/local/kalshi-collector.pem /kalshi-private-key.pem`, and set
   `KALSHI_PRIVATE_KEY_PATH=/run/collector-secrets/kalshi-private-key.pem` plus
   the new `KALSHI_API_KEY_ID`. Do not commit the PEM or put its contents in a
   Railway variable.
7. Railway mounts volumes as root while this image normally runs as a non-root
   user. Set `RAILWAY_RUN_UID=0` on this service so it can read the key volume,
   as Railway's volume documentation requires. This reduces container-level
   isolation; restrict Railway project/service access accordingly.
8. Keep one collector replica. Multiple replicas would duplicate full-market
   subscriptions and raw ingestion unless intentional sharding is implemented.
9. Use the Dockerfile default start command (`run --exchange all`) and an
   `On Failure` or `Always` restart policy appropriate to the Railway plan.
10. Set a deployment draining window long enough for graceful shutdown, for
    example `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=45`.

The `run` command applies pending migrations before opening the live collector,
so a separate release command is not required. This worker has no HTTP endpoint,
therefore a Railway HTTP health-check path is inappropriate. Use deployment
logs, collector metrics, and an interactive `python -m prediction_collector status` check instead.

Run historical ingestion as a separate one-shot Railway service from the same
root/image with start command `backfill --exchange all`, then disable or remove
that service after it exits successfully. Do not configure it with an always-on
restart policy.

From a linked Railway project, deploy and verify the permanent worker with:

```bash
railway up ./data-collector --service <live-service> --environment production
railway logs --service <live-service> --environment production --since 15m
railway ssh --service <live-service> --environment production -- \
  python -m prediction_collector status
```

`railway up` streams the deployment by default and returns a nonzero exit code
if the deployment fails. Use `--detach` only when another process will poll the
deployment status. The SSH status command is read-only and should report each
enabled exchange as ready only after complete discovery and confirmed live
subscriptions.

Current Railway references:

- [Deploying a monorepo](https://docs.railway.com/deployments/monorepo)
- [Dockerfile builds](https://docs.railway.com/builds/dockerfiles)
- [Railway PostgreSQL](https://docs.railway.com/databases/postgresql)
- [Reference variables](https://docs.railway.com/variables/reference)
- [Volumes](https://docs.railway.com/volumes/reference)
- [Restart policies](https://docs.railway.com/deployments/restart-policy)

## Troubleshooting

### `Live discovery returned no markets`

At least one enabled exchange failed discovery or returned no eligible source
records. Run `smoke --exchange polymarket` and `smoke --exchange kalshi`
separately. Check DNS, TLS interception, egress firewalls, endpoint overrides,
and the exchange enable switches. Do not fix this by inventing static market
IDs; live discovery is the coverage source of truth.

### Kalshi REST works but WebSockets are disabled

Both `KALSHI_API_KEY_ID` and an existing regular file at
`KALSHI_PRIVATE_KEY_PATH` are required. The public collector profile disables
them intentionally. In Docker, check the authenticated profile, host bind path,
read permission, and in-container target:

```powershell
docker compose exec collector ls -l /run/collector-secrets/kalshi-private-key.pem
```

Do not print the file. A signature/authentication failure after the leaked key
is revoked means the new key ID and PEM do not match, or the container clock is
wrong.

### A non-zero liquidity threshold removes every Kalshi market

This is expected with the current API. Kalshi's deprecated liquidity field is
unusable, so candidate liquidity is unknown and the threshold treats it as zero.
Set `MIN_LIVE_MARKET_LIQUIDITY=0` and use volume/allowlist controls, or implement
computed book liquidity before using that threshold for Kalshi.

### Markets are unexpectedly excluded

Inspect `live_market_subscription_decisions`, not just logs. It records selector
configuration, observed metrics, deterministic rank, and exact reason. Common
causes are stale allowlist identifiers, a broader-than-intended token blocklist,
or interpreting `MIN_LIVE_MARKET_VOLUME` as 24-hour volume when it actually uses
the discovered total-volume field. Kalshi markets also receive
`credentials_missing` when the REST service is enabled but authenticated
WebSocket signing is not configured.

### Selected markets exceed subscribed markets

This is meaningful, not cosmetic. “Selected” is the intended plan;
“subscribed” is queried from open connection-market rows confirmed by the
exchange. Check the `Polymarket WebSocket subscription confirmed` and `Kalshi WebSocket subscription confirmed` log events, then inspect
`collector_connections` and `collector_connection_markets`. Polymarket waits for
an initial book for every token; Kalshi waits for every requested channel ack.
A missing ack, timeout, protocol rejection, or disconnect leaves the count below
the selected plan and must not be counted as coverage.

### Migration checksum error

An already-applied SQL migration was edited. Do not bypass the check or modify
`schema_migrations`. Restore the applied file and add a new numbered migration
for the change.

### Database connection failure

For native execution, confirm `DATABASE_URL` is either empty or correct and that
the `POSTGRES_*` fields point to a reachable PostgreSQL 18 server. In Compose,
the collector host is forced to `database` and the internal port to `5432`; use
`docker compose ps -a` and database logs. Special characters in the local
password are safe because the application constructs and encodes the DSN.

### Queue pressure or database lag

Compare WebSocket messages/minute with rows/minute and watch container memory.
Increase database I/O capacity first. Then tune batch size, flush interval, pool
size, and queue size. Only introduce a live market cap after measuring sustained
load and confirming the database, not network parsing, is the bottleneck.

### Sequence gaps keep reopening

Check `collector_connections` for reconnect churn and `data_gaps` for channel,
expected/actual sequences, and resolution actions. Kalshi should request a new
snapshot after a book gap. Polymarket cannot provide a precise sequence range,
so a disconnect remains an unknown interval. Periodic REST books preserve
independent post-gap evidence but cannot reconstruct missing deltas or prove the
interval complete.

## Backup and reset safety

`docker compose down` removes containers and the network but preserves the named
`postgres_data` volume. It is the normal stop command.

Back up before schema changes or any reset:

```powershell
docker compose exec -T database pg_dump -U prediction_collector -d prediction_markets -Fc > prediction-markets.dump
```

If the configured user/database names differ, substitute them. Verify the dump
exists and is non-empty before proceeding.

The following command is destructive and is shown only so it cannot be confused
with the safe command above:

```text
docker compose down --volumes
```

It permanently removes the local PostgreSQL named volume and all collected data.
Do not run it unless a verified backup exists and a full reset is explicitly
intended. Never delete PostgreSQL's data directory manually.

PostgreSQL major-version upgrades are not image-tag edits. Back up, read the
official upgrade notes, and use `pg_upgrade` or logical dump/restore. The Compose
volume is mounted at `/var/lib/postgresql` to match PostgreSQL 18+ image layout.

## Tests

Windows PowerShell:

```powershell
$testTemp = Join-Path (Get-Location) 'tests\.pytest-tmp'
New-Item -ItemType Directory -Path $testTemp -Force | Out-Null
$env:TEMP = $testTemp
$env:TMP = $testTemp
& .\.venv\Scripts\python.exe -m pytest -q
```

Linux:

```bash
.venv/bin/python -m pytest -q
```

The offline suite covers configuration, deterministic market selection,
fingerprints/deduplication, pagination/retry behavior, authentication signing,
order-book state, and representative Polymarket/Kalshi payload parsers. A passing
offline suite does not prove that external APIs, authenticated WebSockets, or a
production PostgreSQL deployment are currently reachable; run smoke and staged
live checks in the deployment environment.
