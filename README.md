# Prediction-market making research

This repository contains a read-only exchange data collector for market-making,
latency, adverse-selection, and cross-market research. It collects public data
from Polymarket and Kalshi and writes normalized plus raw records to PostgreSQL.
It does not submit, cancel, or modify orders.

## Security warning

The Kalshi private key pasted into the implementation request is compromised and
must be revoked in Kalshi. Create a replacement before running authenticated
collection. Never copy a private key into `.env`, source control, a Docker image,
a command line, or logs. The collector accepts only a key **file path**; the
Docker configuration mounts that file read-only.

## Components

- [`data-collector/`](data-collector/) — Python 3.14 collector, PostgreSQL 18
  schema, Docker packaging, backfills, live order books/trades, reference feeds,
  event/market lifecycle history, linked Kalshi multivariate markets,
  time-varying economics, coverage telemetry, and operational documentation.

## Coverage contract

Production defaults select every discovered market that is both active and
tradable:

```dotenv
MAX_LIVE_MARKETS=0
MIN_LIVE_MARKET_VOLUME=0
MIN_LIVE_MARKET_LIQUIDITY=0
```

Zero means unrestricted. Optional allowlists, blocklists, thresholds, and a cap
apply only to resource-intensive continuous market subscriptions. Historical
metadata, trades, comments, fees/incentives, and market-data backfills are never
capped by `MAX_LIVE_MARKETS`.

If a non-zero cap is configured, the collector ranks eligible markets
deterministically by liquidity descending, 24-hour volume descending, total
volume descending, then exchange and external market ID ascending. Discovery,
active-flag, tradable-flag, selected, exchange-confirmed subscribed, and excluded
counts are distinct. Every exclusion reason is stored and logged; sending a
subscription request alone does not count as coverage.

Polymarket token-set changes rotate only the affected stable subscription shard
even when the selected market IDs do not change. Periodic REST books are
archived as reconciliation evidence without overwriting live WebSocket book
state. Current economics refresh hourly, with authoritative per-token
Polymarket fee rates on a separate six-hour cadence.

## Start here

The complete setup, CLI, Docker Compose, Railway, schema, monitoring, recovery,
and credential instructions are in the
[`data-collector` README](data-collector/README.md).

Minimal local setup with Docker:

```powershell
Set-Location data-collector
Copy-Item .env.example .env
# Edit .env and point KALSHI_PRIVATE_KEY_HOST_PATH at a newly rotated PEM file.
docker compose --profile authenticated up --build -d collector
docker compose logs -f collector
```

For public-only operation without a Kalshi private key:

```powershell
docker compose --profile public up --build -d collector-public
docker compose logs -f collector-public
```

Public-only mode is deliberately incomplete: authenticated Kalshi market,
lifecycle, CF Benchmarks, and Pyth WebSocket feeds are unavailable. It still
collects Polymarket live feeds and both exchanges' public REST data.
