# UME integration status

## Implemented here

- Five separately described, normalized live feeds.
- Independent one-command process and container deployment.
- Shared upstream connections, bounded queues, health state, exponential reconnect, and graceful cancellation.
- Wallet signature authentication, audience-bound single-use delivery tickets, direct on-chain entitlement verification, and active-session expiry checks.
- Test-only catalog metadata with 1 test USDC / one-hour proposed terms.
- Restart-safe Ethereum wallet collection with deterministic deduplication, bounded SQLite WAL retention,
  reorg rewind, SSE, public schema/sample/health, and protected JSON/CSV/Parquet history.
- Existing KPM external supplier discovery, seller origin pairing, listing publication, testnet purchase and
  FeedAccess entitlement interfaces were verified in the marketplace repository.

## Marketplace gaps (not bypassed)

- A production marketplace may still be in intake-only mode, so real paid checkout must be verified per deployment.
- `FeedAccess` source supports a configurable treasury fee; `/payments/status` must observe 250 bps and the intended
  treasury on the deployed contract. No verified real-USDC production deployment was found.
- Each feed must be configured by the existing `FeedAccess` owner before it can be purchased.
- Commercial redistribution permission has not been established for the bundled third-party market data. Listings remain test-only.

No contracts, chain state, UME database, production resources, or secrets are changed by this repository.
