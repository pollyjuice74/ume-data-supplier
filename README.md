# UME Data Supplier

An independent FastAPI service that collects a bounded, restart-safe Ethereum wallet event feed and fans out existing exchange feeds. Delivery is protected with wallet authentication plus the existing UME `FeedAccess` entitlement contract.

The production-oriented feed is `ethereum-wallet-trading-v1`: canonical ERC-20 transfers, Uniswap v2/v3 swaps, and window-relative inventory deltas stored incrementally in SQLite WAL. It exposes public metadata/schema/sample/health, an entitled SSE stream, and entitled JSON/CSV/Parquet history. It never accepts buyer private keys and does not execute trades.

## Safety and commercial status

Every bundled listing is `TEST_ONLY_RIGHTS_UNVERIFIED` and `purchasable: false`. Public WebSocket availability is not permission to resell market data. Do not make a listing purchasable until the exchange has granted the necessary redistribution rights and UME has configured its on-chain feed.

The current UME `FeedAccess` source supports an explicit treasury and protocol fee. `/payments/status` reads the deployed contract instead of assuming its configuration. A 250 bps result is the requested 2.5%; test-USDC receipts are not real revenue.

## Run locally

```bash
cp .env.example .env
# For a host-run supplier, set SUPPLIER_RPC_URL=http://127.0.0.1:8545 and
# SUPPLIER_FEED_ACCESS_ADDRESS to the existing local deployment.
./scripts/start_supplier.sh
```

Open `http://localhost:8090/docs`, `http://localhost:8090/health`, or `http://localhost:8090/catalog/listings`. Run UME separately; this process never starts or resets Anvil, UME, or its database.

See [Oracle deployment and KPM purchase instructions](docs/oracle-production.md) for the 24/7 configuration.

## Northflank

The root `Dockerfile` is deployment-ready for a Northflank combined service: it runs unprivileged, honors the injected
`PORT`, includes an HTTP healthcheck, and owns `/app/data`. Configure a persistent volume at `/app/data`, expose the
injected HTTP port, set `/health` as the readiness path, and copy the required values from `.env.example` into secret or
runtime variables. Do not add `.env` to the image. A service without a persistent `/app/data` volume will still run, but
its bounded SQLite history and checkpoint will disappear on replacement.

For hosted UME, run `./scripts/start_supplier_tunnel.sh`. It downloads a pinned, checksum-verified Cloudflare Tunnel client into the local cache when needed, prints the temporary HTTPS supplier URL, and starts the existing supplier behind it. The Sell Data page pairs that exact URL with the connected seller wallet; the supplier does not need the wallet private key. Keep the command running while the listing should be live. If the Python supplier stops, the helper keeps the tunnel URL alive; press Enter in the same terminal to restart the supplier at that URL, or Ctrl+C to stop everything.

Buyer flow: request a challenge, sign its exact text with the buyer wallet, exchange it for a session, and request a short-lived stream ticket. Connect to `ws://localhost:8090/feeds/{feed_id}/stream?ticket=...`. The supplier checks `FeedAccess.expiresAt` before issuing the ticket and during the connection; a transaction hash is never accepted as proof.

## Tests

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
ruff check .
```

## Marketplace integration status

UME currently has a catalog adapter for its built-in Hyperliquid service, but no authenticated, persistent API for an ordinary seller to register an externally delivered live listing without uploading a file. The next marketplace PR must add that minimal capability with server-side URL allowlisting/SSRF controls, persistent service identity, and idempotent seller-scoped keys. Until that API exists, this supplier exposes accurate catalog metadata but does not pretend that five durable UME listings were registered.

See [docs/architecture.md](docs/architecture.md), [docs/integration-status.md](docs/integration-status.md),
and [docs/upstream-sources-and-rights.md](docs/upstream-sources-and-rights.md).
