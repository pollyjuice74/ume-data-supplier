# Production revenue launch deployment gate

No production deployment is performed by this branch. The current public contract and token are Sepolia testnet
artifacts, not real revenue.

## Northflank durable rollout

Before changing the service, open its Northflank volume settings and verify a persistent volume is mounted exactly
at `/app/data`. The repository cannot prove a console-only volume attachment. In the running container, back up and
validate the existing database before any mount change:

```bash
sqlite3 /app/data/supplier.sqlite3 'PRAGMA wal_checkpoint(FULL); PRAGMA integrity_check;'
cp -p /app/data/supplier.sqlite3 /app/data/supplier.pre-revenue-launch.sqlite3
sqlite3 /app/data/supplier.pre-revenue-launch.sqlite3 'PRAGMA integrity_check;'
```

Set these secret/runtime values:

```text
SUPPLIER_DATABASE_PATH=/app/data/supplier.sqlite3
SUPPLIER_ETHEREUM_RPC_URL=https://eth-mainnet.PROVIDER/v2/PRIMARY_KEY
SUPPLIER_ETHEREUM_FALLBACK_RPC_URLS=https://mainnet.SECOND_PROVIDER/v3/FALLBACK_KEY
SUPPLIER_RPC_URL=https://ethereum-sepolia-rpc.publicnode.com
SUPPLIER_FEED_ACCESS_ADDRESS=0x1e917c8a8bc40bc529f59103c1c9b2e293d2a743
SUPPLIER_CHAIN_ID=11155111
SUPPLIER_ENABLE_EXCHANGE_FEEDS=false
```

RPC credentials must be created in the selected providers' consoles and stored as Northflank secrets. This is the
one unavoidable manual account step; publicnode worked from the development host but returned 403 from Northflank,
so a provider has not been accepted until it advances blocks from the deployed service.

Deploy this branch only after approval. Verify twice, at least one block apart:

```bash
curl -fsS https://SUPPLIER/health | jq '.feeds.ethereum_mirror_candidates_v1 |
  {status,collector_running,error_category,current_chain_head,last_successfully_ingested_block,indexer_lag_blocks}'
curl -fsS https://SUPPLIER/feeds/ethereum_mirror_candidates_v1/sample | jq .verification_basis
```

Expected: `ONLINE`, `collector_running=true`, null error, advancing indexed block, bounded lag and
`LIVE_BOUNDED_SAMPLE`. Restart the Northflank service and confirm checkpoint/event counts remain and advance. If they
reset, stop the rollout and restore the backup to a correctly mounted volume.

Render remains the control plane and must not proxy SSE. After the supplier rollout, deploy the matching marketplace
branch, confirm `/v1/health/runtime`, and require `purchase-check` to return available only while the source is fresh.
