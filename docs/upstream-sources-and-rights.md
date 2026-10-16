# Upstream sources and commercial-rights status

Technical integration uses only official documented endpoints:

- Binance Spot market-data-only WebSocket (`data-stream.binance.vision`) and `<symbol>@trade`: <https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md> and <https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md>
- Coinbase Exchange WebSocket ticker: <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels>
- Kraken Spot WebSocket v2 book: <https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book>
- Hyperliquid mainnet WebSocket trades: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket> and <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions>

These sources document access and message formats; that is not an affirmative commercial redistribution license. No source reviewed for this sprint affirmatively grants this supplier the right to resell its data. Consequently all five catalog entries are `TEST_ONLY_RIGHTS_UNVERIFIED`, return `purchasable: false`, and must remain so until the operator has written permission or counsel-confirmed terms for the intended product and jurisdiction.

The runtime contains no exchange credentials. If a future licensed adapter needs credentials, keep them in the supplier environment and never return them in catalog, health, event, or error payloads.
