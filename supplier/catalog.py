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


def wallet_schema(schema_id: str, fields: dict) -> dict:
    return {"$id": schema_id, "version": "1.0", "type": "object", "fields": fields}


COMMON = {"event_id": "string", "chain_id": "integer", "provenance": "object"}
WALLET_FEEDS = [
    Feed(
        "ethereum_wallet_trades_v1",
        "Ethereum wallet trades v1",
        "Observed Uniswap v2/v3 swap logs. Token pair fields remain null when pool metadata is unavailable.",
        "Ethereum",
        "wallet-trades",
        "wallet_trades",
        "",
        None,
        wallet_schema(
            "ume.ethereum.wallet-trades.v1",
            COMMON
            | {
                "block_number": "integer",
                "block_timestamp": "RFC3339",
                "tx_hash": "0x-hash",
                "log_index": "integer",
                "wallet": "0x-address|null",
                "pool": "0x-address",
                "token_in": "0x-address|null",
                "token_out": "0x-address|null",
                "amount_in_raw": "decimal-string|null",
                "amount_out_raw": "decimal-string|null",
            },
        ),
    ),
    Feed(
        "ethereum_wallet_inventory_deltas_v1",
        "Ethereum wallet inventory deltas v1",
        "Exact raw-unit ERC-20 deltas and window-relative inventory; not complete wallet balances.",
        "Ethereum",
        "inventory-deltas",
        "wallet_inventory",
        "",
        None,
        wallet_schema(
            "ume.ethereum.wallet-inventory-deltas.v1",
            COMMON
            | {
                "wallet": "0x-address",
                "token_address": "0x-address",
                "block_number": "integer",
                "tx_hash": "0x-hash",
                "delta_raw": "signed decimal-string",
                "relative_inventory_raw": "signed decimal-string",
                "balance_scope": "WINDOW_RELATIVE",
            },
        ),
    ),
    Feed(
        "ethereum_wallet_behavior_v1",
        "Ethereum wallet behavior v1",
        "Bounded-window activity aggregates derived only from observed events; no profitability claim.",
        "Ethereum",
        "wallet-behavior",
        "wallet_behavior",
        "",
        None,
        wallet_schema(
            "ume.ethereum.wallet-behavior.v1",
            COMMON
            | {
                "wallet": "0x-address",
                "observation_window": "object",
                "trade_count": "integer",
                "transfer_count": "integer",
                "activity_frequency": "number|null",
                "accumulation_metrics": "object",
                "relationship_metrics": "object|null",
                "lead_lag_metrics": "object|null",
                "quality_flags": "array<string>",
            },
        ),
    ),
    Feed(
        "ethereum_mirror_candidates_v1",
        "Ethereum mirror candidates v1",
        "Observed swap actions prepared for independent evaluation. Missing quotes are never assigned PnL.",
        "Ethereum",
        "mirror-candidates",
        "mirror_candidates",
        "",
        None,
        wallet_schema(
            "ume.ethereum.mirror-candidates.v1",
            COMMON
            | {
                "source_wallet": "0x-address|null",
                "source_tx_hash": "0x-hash",
                "observed_action": "string",
                "token_in": "0x-address|null",
                "token_out": "0x-address|null",
                "amount_in_raw": "decimal-string|null",
                "amount_out_raw": "decimal-string|null",
                "observation_timestamp": "RFC3339",
                "replication_delay_ms": "number|null",
                "replication_delay_measurement": (
                    "WEBSOCKET_PROVIDER_RECEIPT_TO_PUBLICATION_MEASURED|"
                    "HTTP_COLLECTOR_RECEIPT_TO_PUBLICATION_MEASURED"
                ),
                "execution_status": "OBSERVED_CONFIRMED_NO_QUOTE",
                "signal_basis": "OBSERVED_CONFIRMED_CHAIN_FACT",
                "usd_estimate": "null",
                "available_liquidity": "null",
                "estimated_slippage_bps": "null",
                "prediction": "null",
                "estimated_cost": "null",
                "estimated_pnl": "null",
                "missing_reason": "string",
                "block_timestamp": "RFC3339",
                "provider_received_at": "RFC3339",
                "collector_received_at": "RFC3339",
                "normalized_at": "RFC3339",
                "published_at": "RFC3339",
            },
        ),
    ),
]
for wallet_feed in WALLET_FEEDS:
    FEEDS[wallet_feed.id] = wallet_feed
