# Northflank 0.1 vCPU / 256 MB profile

Measured locally on 2026-10-25 with Python 3.14, the same public Ethereum RPC, one-block backfill, two confirmations,
SQLite on local disk, and no buyers. These are process measurements, not Northflank billing metrics. Network and block
contents vary, so repeat them on the deployed container before treating them as capacity guarantees.

## Results

- Import-only maximum RSS: 94,916 KiB before; 69,644 KiB after.
- 25-second live-run maximum RSS: 197,228 KiB before; 147,616 KiB after.
- Live-run user plus system CPU: 2.39 seconds before; 1.80 seconds after.
- Events persisted: 3,202 before; 6,559 after.
- CPU time per persisted event: 0.746 ms before; 0.274 ms after.
- Event queue drops with no buyers: zero before and after.

The live samples covered different consecutive Ethereum blocks; event-count throughput is therefore not a controlled
source benchmark. CPU-per-event is included as an indicative measurement, not a guaranteed 63% improvement. The
repeatable facts are the lower resident footprint, disabled exchange tasks, fixed-size rate windows, fixed-size buyer
queues, and bounded SQLite/export settings.

While the optimized collector was ingesting, `/health` and `/catalog/listings` both returned HTTP 200. The catalog
contained exactly four wallet feeds. At the observation point 2,655 events were committed, all feeds were `ONLINE`,
and the no-buyer drop counters were zero.

## Reproduce

```bash
SUPPLIER_DATABASE_PATH=/tmp/ume-profile.sqlite3 \
SUPPLIER_ETHEREUM_BACKFILL_BLOCKS=1 \
/usr/bin/time -v timeout 25 \
  .venv/bin/uvicorn supplier.app:app --host 127.0.0.1 --port 18092

curl -fsS http://127.0.0.1:18092/health
curl -fsS http://127.0.0.1:18092/catalog/listings
```

For Northflank keep `SUPPLIER_ENABLE_EXCHANGE_FEEDS=false`, mount durable storage at `/app/data`, set
`SUPPLIER_DATABASE_PATH=/app/data/supplier.sqlite3`, and retain the default 64-event buyer queue. A slow buyer loses
the oldest queued event intentionally; `dropped_events` exposes that backpressure loss. SQLite remains the replay
source, so reconnecting buyers can receive the bounded recent replay without keeping history in RAM.

Parquet generation loads PyArrow on demand. It is no longer resident during ordinary indexing, but a large export can
temporarily increase memory. The production default is therefore capped at 10,000 rows per request.
