# UME Data Supplier

An independent FastAPI service that fans out five real exchange WebSocket feeds and protects delivery with wallet authentication plus the existing UME `FeedAccess` entitlement contract.

## Safety and commercial status

Every bundled listing is `TEST_ONLY_RIGHTS_UNVERIFIED` and `purchasable: false`. Public WebSocket availability is not permission to resell market data. Do not make a listing purchasable until the exchange has granted the necessary redistribution rights and UME has configured its on-chain feed.

The paid-delivery feature branch pairs this service with a revised UME
`FeedAccess` contract that atomically splits payment between seller and treasury.
That contract has been verified only with disposable local Anvil wallets and
`LocalUSDC`; it has not been deployed to production or a public testnet.

## Run locally

```bash
cp .env.example .env
# For a host-run supplier, set SUPPLIER_RPC_URL=http://127.0.0.1:8545 and
# SUPPLIER_FEED_ACCESS_ADDRESS to the existing local deployment.
./scripts/start_supplier.sh
```

Open `http://localhost:8090/docs`, `http://localhost:8090/health`, or `http://localhost:8090/catalog/listings`. Run UME separately; this process never starts or resets Anvil, UME, or its database.

For hosted UME, run `./scripts/start_supplier_tunnel.sh`. It downloads a pinned, checksum-verified Cloudflare Tunnel client into the local cache when needed, prints the temporary HTTPS supplier URL, and starts the existing supplier behind it. The Sell Data page pairs that exact URL with the connected seller wallet; the supplier does not need the wallet private key. Keep the command running while the listing should be live. If the Python supplier stops, the helper keeps the tunnel URL alive; press Enter in the same terminal to restart the supplier at that URL, or Ctrl+C to stop everything.

Buyer flow: request a challenge, sign its exact text with the buyer wallet, exchange it for a session, and request a short-lived stream ticket. Connect to `ws://localhost:8090/feeds/{feed_id}/stream?ticket=...`. The supplier checks `FeedAccess.expiresAt` before issuing the ticket and during the connection; a transaction hash is never accepted as proof.

The challenge and session are bound to the wallet, feed, chain, FeedAccess
contract, and observed entitlement expiry. Challenges are single-use. Active
connections recheck chain time and entitlement every five seconds, even while
events are flowing, so expiry closes delivery without relying on the host clock.

## Tests

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
ruff check .
```

## Marketplace integration status

UME has an authenticated, persistent external-listing API with server-side URL
validation, supplier ownership proof, health monitoring, and seller-scoped
idempotency. Paid delivery additionally requires that the listing feed, price,
duration, seller, payment token, chain, and FeedAccess contract agree with
on-chain configuration. Production remains test-only/non-purchasable until a
separately reviewed public-testnet or production deployment is configured.

See [docs/architecture.md](docs/architecture.md), [docs/integration-status.md](docs/integration-status.md),
and [docs/upstream-sources-and-rights.md](docs/upstream-sources-and-rights.md).
