from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Feed:
    id: str
    name: str
    description: str
    source: str
    symbol: str
    kind: str
    upstream_url: str
    subscribe: dict | None
    schema: dict

    def public(self, public_url: str, owner_wallet: str) -> dict:
        value = asdict(self)
        value.pop("upstream_url")
        value.pop("subscribe")
        value.update(
            {
                "title": self.name,
                "category": "Market data",
                "format": "JSON",
                "bytes": None,
                "rows": None,
                "seller": owner_wallet.lower(),
                "supplier_owner_wallet": owner_wallet.lower(),
                "version": 1,
                "protocol": self.source,
                "delivery_type": "LIVE_STREAM",
                "delivery_method": "LIVE_STREAM",
                "delivery_url": f"{public_url}/feeds/{self.id}/stream",
                "price": "1.00",
                "currency": "test USDC",
                "access_duration_seconds": 3600,
                "commercial_status": "TEST_ONLY_RIGHTS_UNVERIFIED",
                "purchasable": False,
                "tags": [self.source.lower(), self.symbol.lower(), "live", self.kind],
            }
        )
        return value


TRADE_SCHEMA = {
    "version": "1.0",
    "fields": {
        "feed_id": "string",
        "source": "string",
        "symbol": "string",
        "event_type": "trade",
        "price": "decimal-string",
        "size": "decimal-string",
        "side": "buy|sell|unknown",
        "source_timestamp": "RFC3339|null",
        "ingested_at": "RFC3339",
        "trade_id": "string|null",
    },
}

FEEDS = {
    f.id: f
    for f in [
        Feed(
            "binance-btc-usdt-trades",
            "Binance BTC/USDT trades",
            "Real-time spot trades.",
            "Binance",
            "BTC/USDT",
            "trades",
            "wss://data-stream.binance.vision/ws/btcusdt@trade",
            None,
            TRADE_SCHEMA,
        ),
        Feed(
            "binance-eth-usdt-trades",
            "Binance ETH/USDT trades",
            "Real-time spot trades.",
            "Binance",
            "ETH/USDT",
            "trades",
            "wss://data-stream.binance.vision/ws/ethusdt@trade",
            None,
            TRADE_SCHEMA,
        ),
        Feed(
            "coinbase-btc-usd-ticker",
            "Coinbase BTC/USD ticker",
            "Real-time Exchange ticker updates.",
            "Coinbase",
            "BTC/USD",
            "ticker",
            "wss://ws-feed.exchange.coinbase.com",
            {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]},
            {
                "version": "1.0",
                "fields": {
                    "feed_id": "string",
                    "source": "string",
                    "symbol": "string",
                    "event_type": "ticker",
                    "price": "decimal-string",
                    "best_bid": "decimal-string|null",
                    "best_ask": "decimal-string|null",
                    "volume_24h": "decimal-string|null",
                    "source_timestamp": "RFC3339|null",
                    "ingested_at": "RFC3339",
                },
            },
        ),
        Feed(
            "kraken-btc-usd-book",
            "Kraken BTC/USD order book",
            "Level-10 spot order-book updates.",
            "Kraken",
            "BTC/USD",
            "order_book",
            "wss://ws.kraken.com/v2",
            {
                "method": "subscribe",
                "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10, "snapshot": True},
            },
            {
                "version": "1.0",
                "fields": {
                    "feed_id": "string",
                    "source": "string",
                    "symbol": "string",
                    "event_type": "book_snapshot|book_update",
                    "bids": "array<{price,qty}>",
                    "asks": "array<{price,qty}>",
                    "checksum": "integer|null",
                    "source_timestamp": "RFC3339|null",
                    "ingested_at": "RFC3339",
                },
            },
        ),
        Feed(
            "hyperliquid-btc-perp-trades",
            "Hyperliquid BTC perpetual trades",
            "Real-time BTC perpetual trades.",
            "Hyperliquid",
            "BTC-PERP",
            "trades",
            "wss://api.hyperliquid.xyz/ws",
            {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}},
            TRADE_SCHEMA,
        ),
    ]
}
