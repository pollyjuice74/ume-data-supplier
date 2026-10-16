from supplier.catalog import FEEDS
from supplier.normalize import normalize


def test_binance_trade():
    event = normalize(
        FEEDS["binance-btc-usdt-trades"],
        {"e": "trade", "p": "60000", "q": "0.1", "m": False, "t": 7, "T": 1700000000000},
    )[0]
    assert event["side"] == "buy"
    assert event["price"] == "60000"


def test_hyperliquid_trade():
    event = normalize(
        FEEDS["hyperliquid-btc-perp-trades"],
        {"channel": "trades", "data": [{"px": "60001", "sz": "0.2", "side": "A", "tid": 8, "time": 1700000000000}]},
    )[0]
    assert event["side"] == "sell"


def test_kraken_snapshot():
    event = normalize(
        FEEDS["kraken-btc-usd-book"],
        {
            "channel": "book",
            "type": "snapshot",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": 1, "qty": 2}], "asks": []}],
        },
    )[0]
    assert event["event_type"] == "book_snapshot"
    assert event["bids"] == [{"price": 1, "qty": 2}]
