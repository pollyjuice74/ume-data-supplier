# UME Data Supplier

An independent FastAPI service that fans out five real exchange WebSocket feeds and protects delivery with wallet authentication plus the existing UME `FeedAccess` entitlement contract.

## Safety and commercial status

Every bundled listing is `TEST_ONLY_RIGHTS_UNVERIFIED` and `purchasable: false`. Public WebSocket availability is not permission to resell market data. Do not make a listing purchasable until the exchange has granted the necessary redistribution rights and UME has configured its on-chain feed.

The current UME `FeedAccess` contract pays the seller directly and has no treasury-fee mechanism. This service does not invent or bypass settlement. Production UME is also currently configured in intake-only mode. The requested seller/treasury split therefore requires a separately reviewed marketplace change and contract deployment approval.

## Run locally

```bash
cp .env.example .env
# Set SUPPLIER_FEED_ACCESS_ADDRESS to the existing local deployment.
./scripts/start_supplier.sh
```

Open `http://localhost:8090/docs`, `http://localhost:8090/health`, or `http://localhost:8090/catalog/listings`. Run UME separately; this process never starts or resets Anvil, UME, or its database.

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
