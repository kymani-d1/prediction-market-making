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

The production system has two deliberately different jobs. `collector-live` is
the permanent, high-value microstructure collector: it discovers all currently
tradeable markets and assigns bounded `FULL_L2`, `SAMPLED`, or `METADATA_ONLY`
tiers. `research-backfill` is a bounded historical dataset bootstrap: it
persists a deterministic stratified cohort, then collects historical prices and
trades only for that cohort. It is not an archival replica of Polymarket.

Start with [the collector runbook](data-collector/README.md). The critical
operational rule is: protect the permanent live worker first; run bounded
historical research separately afterward. REST history is partly recoverable;
missed WebSocket L2 microstructure is not.

Before the refactor, the repository state was preserved locally as the annotated
Git tag `pre-polymarket-only`. Push it when you are ready:

```powershell
git push origin pre-polymarket-only
```
