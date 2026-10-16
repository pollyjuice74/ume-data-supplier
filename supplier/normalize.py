from datetime import UTC, datetime
from typing import Any

from .catalog import Feed


def iso_ms(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str) and "T" in value:
        return value
    return datetime.fromtimestamp(float(value) / 1000, UTC).isoformat()


def normalize(feed: Feed, message: dict) -> list[dict]:
    now = datetime.now(UTC).isoformat()
    base = {"feed_id": feed.id, "source": feed.source, "symbol": feed.symbol, "ingested_at": now}
    if feed.source == "Binance" and message.get("e") == "trade":
        return [
            {
                **base,
                "event_type": "trade",
                "price": message["p"],
                "size": message["q"],
                "side": "sell" if message.get("m") else "buy",
                "trade_id": str(message.get("t")),
                "source_timestamp": iso_ms(message.get("T")),
            }
        ]
    if feed.source == "Coinbase" and message.get("type") == "ticker":
        return [
            {
                **base,
                "event_type": "ticker",
                "price": message["price"],
                "best_bid": message.get("best_bid"),
                "best_ask": message.get("best_ask"),
                "volume_24h": message.get("volume_24h"),
                "source_timestamp": message.get("time"),
            }
        ]
    if feed.source == "Kraken" and message.get("channel") == "book":
        result = []
        for row in message.get("data", []):
            bids = [{"price": item["price"], "qty": item["qty"]} for item in row.get("bids", [])]
            asks = [{"price": item["price"], "qty": item["qty"]} for item in row.get("asks", [])]
            result.append(
                {
                    **base,
                    "event_type": "book_snapshot" if message.get("type") == "snapshot" else "book_update",
                    "bids": bids,
                    "asks": asks,
                    "checksum": row.get("checksum"),
                    "source_timestamp": row.get("timestamp"),
                }
            )
        return result
    if feed.source == "Hyperliquid" and message.get("channel") == "trades":
        return [
            {
                **base,
                "event_type": "trade",
                "price": row["px"],
                "size": row["sz"],
                "side": {"B": "buy", "A": "sell"}.get(row.get("side"), "unknown"),
                "trade_id": str(row.get("tid")),
                "source_timestamp": iso_ms(row.get("time")),
            }
            for row in message.get("data", [])
        ]
    return []
