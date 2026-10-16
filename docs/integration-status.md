# UME integration status

## Implemented here

- Five separately described, normalized live feeds.
- Independent one-command process and container deployment.
- Shared upstream connections, bounded queues, health state, exponential reconnect, and graceful cancellation.
- Wallet signature authentication, audience-bound single-use delivery tickets, direct on-chain entitlement verification, and active-session expiry checks.
- Test-only catalog metadata with 1 test USDC / one-hour proposed terms.

## Marketplace gaps (not bypassed)

- UME lacks an external-live-listing registration endpoint and persistent service identity. Its generic external URL validator intentionally leaves URLs non-purchasable until an adapter exists.
- `FeedAccess` supports seller payout and timed entitlement but no configurable protocol treasury fee.
- The hosted UME service is intake-only, so paid checkout is disabled.
- Each feed must be configured by the existing `FeedAccess` owner before it can be purchased.
- Commercial redistribution permission has not been established for the bundled third-party market data. Listings remain test-only.

No contracts, chain state, UME database, production resources, or secrets were changed by this repository.
