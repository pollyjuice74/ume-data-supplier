# FeedAccess Account 2 cutover

Do not change the production entitlement contract before the replacement is deployed and all four wallet feeds are receipt-backed and read-back verified on Ethereum Sepolia.

After verification, set the Northflank service variable `SUPPLIER_FEED_ACCESS_ADDRESS` to the new contract, retain `SUPPLIER_CHAIN_ID=11155111`, retain the existing Sepolia RPC URL, and restart the service. The supplier always treats the configured contract's `expiresAt(buyer, keccak256(feed_id))` as authoritative.

Compatibility impact: entitlements and ERC-20 allowances belong to a particular FeedAccess address. Legacy purchases do not migrate, and buyers must approve the new FeedAccess before buying. The legacy contract remains untouched and is the rollback address: `0xd9e96d29b580F0b9fb8C5310D6EF3ABb76F1DaE1`.

Post-cutover checks:

1. `/health` and discovery remain responsive.
2. An unauthenticated or non-entitled wallet receives `403` from protected delivery.
3. A different buyer wallet approves the new contract and completes a TEST USDC purchase.
4. The purchase receipt is successful and `expiresAt` is in the future.
5. The authenticated buyer receives a real event through protected SSE.

Never place a deployer key, buyer key, seed phrase, or supplier session secret in Git.
