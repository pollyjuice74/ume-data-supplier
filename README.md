# UME Data Supplier

An independent FastAPI service that collects a bounded, restart-safe Ethereum wallet event feed and fans out existing exchange feeds. Delivery is protected with wallet authentication plus the existing UME `FeedAccess` entitlement contract.

The production adapter exposes four products from the same bounded, restart-safe store: `ethereum_wallet_trades_v1`, `ethereum_wallet_inventory_deltas_v1`, `ethereum_wallet_behavior_v1`, and `ethereum_mirror_candidates_v1`. They expose public metadata/schema/sample/health plus entitled SSE and JSON/CSV/Parquet history. Inventory is explicitly window-relative; missing executable quote evidence produces `MISSED_NO_QUOTE` with null cost and PnL. The service never accepts buyer private keys or executes trades.

Binance, Coinbase, Kraken, and Hyperliquid collectors are disabled by default. Set
`SUPPLIER_ENABLE_EXCHANGE_FEEDS=true` only in a deployment that intentionally serves those unrelated feeds. The public
supplier catalog contains only the four wallet products. Per-buyer queues, delivery-rate windows, SQLite history, and
exports are bounded; PyArrow is loaded lazily only when a buyer requests Parquet.
The 256 MB deployment defaults to at most 10,000 rows per export and a 4 MiB SQLite page cache per connection.

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

Buyer flow: request a challenge, sign its exact text with the buyer wallet, exchange it for a session, and request a short-lived stream ticket. Connect an `EventSource` to `http://localhost:8090/feeds/{feed_id}/sse?ticket=...`. The supplier checks `FeedAccess.expiresAt` before issuing the single-use ticket and during the connection; a transaction hash is never accepted as proof.

## Tests

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
ruff check .
```

## Marketplace integration status

KPM now has an authenticated external supplier workflow with exact-origin allowlisting, SSRF controls, cryptographic origin ownership, idempotent seller-scoped listings, and testnet entitlement checks. Publishing still requires the seller to pair the origin and sign the listing in MetaMask. Each new feed must also be configured in the Sepolia `FeedAccess` contract by its owner; test USDC is not revenue.

See [docs/architecture.md](docs/architecture.md), [docs/integration-status.md](docs/integration-status.md),
and [docs/upstream-sources-and-rights.md](docs/upstream-sources-and-rights.md).
