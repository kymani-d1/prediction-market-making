# Prediction-market making research

This repository currently contains a read-only, Polymarket-only data collector.
It does not place orders, hold signing credentials, or execute trading logic.

The collector is designed for continuous market-microstructure research without
using PostgreSQL as an unlimited tick warehouse:

```text
Polymarket REST + WebSockets
            |
            +--> PostgreSQL hot store
            |    metadata, trades, current books, recent observations,
            |    tier decisions, quality events and archive manifests
            |
            +--> S3-compatible Parquet/Zstd archive
                 FULL_L2 history, sampled observations, raw REST evidence,
                 selected raw WebSocket evidence and reference prices
```

All active/tradable markets are discovered and retained as metadata. Expensive
continuous collection is dynamically assigned to `FULL_L2`, `SAMPLED`, or
`METADATA_ONLY` tiers. The default ceilings are explicit safety controls, not
discovery limits.

Start with [the collector runbook](data-collector/README.md). The critical
operational rule is: after migrations, start the permanent live worker first;
run historical backfill separately afterward. REST history is recoverable;
missed WebSocket microstructure usually is not.

Before the refactor, the repository state was preserved locally as the annotated
Git tag `pre-polymarket-only`. Push it when you are ready:

```powershell
git push origin pre-polymarket-only
```
