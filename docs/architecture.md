# Architecture

`FeedHub` owns exactly one reconnecting upstream task for each configured feed. All authorized buyers receive fan-out copies through bounded per-subscriber queues; when a consumer falls behind, its oldest queued event is dropped instead of allowing unbounded memory growth. No market events are written to disk.

Adapters normalize upstream messages into versioned JSON schemas with source and ingestion timestamps. `/health` reports connectivity, subscriber counts, last-event time, and reconnect totals.

Wallet challenges are single-use and expire after five minutes. Sessions and stream tickets are HMAC-authenticated; tickets are feed-audience-bound, short-lived, and single-use. Delivery checks the configured chain and exact `FeedAccess` contract using `eth_call`. Active sockets are rechecked on heartbeat, so expired access is closed. Use a high-entropy `SUPPLIER_SESSION_SECRET` in every non-development environment.

The five upstreams are Binance Spot trade streams, Coinbase Exchange ticker, Kraken Spot WebSocket v2 book, and Hyperliquid trades. Reconnect uses exponential backoff with jitter.

