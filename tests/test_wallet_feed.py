from datetime import UTC, datetime

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from supplier import app as supplier_app
from supplier.wallet_collector import TRANSFER, EthereumWalletCollector
from supplier.wallet_store import WalletStore


class Hub:
    connected = {}
    published = []

    def publish(self, feed_id, event):
        self.published.append((feed_id, event))


def block(number=10):
    return {
        "number": number,
        "hash": "0x" + "ab" * 32,
        "parent_hash": "0x" + "cd" * 32,
        "source_timestamp": "2026-01-01T00:00:00+00:00",
        "observed_at": "2026-01-01T00:00:01+00:00",
    }


def transfer_log():
    return {
        "blockNumber": "0xa",
        "blockHash": "0x" + "ab" * 32,
        "transactionHash": "0x" + "ef" * 32,
        "logIndex": "0x1",
        "address": "0x" + "11" * 20,
        "topics": [TRANSFER, "0x" + "00" * 12 + "22" * 20, "0x" + "00" * 12 + "33" * 20],
        "data": "0x" + hex(25)[2:].zfill(64),
    }


def test_normalize_deduplicate_inventory_and_exports(tmp_path):
    store = WalletStore(str(tmp_path / "events.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum-wallet-trading-v1", 1, 0, 1)
    rows = collector.normalize(transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00")
    assert [row["event_type"] for row in rows] == ["TOKEN_TRANSFER", "INVENTORY_CHANGE", "INVENTORY_CHANGE"]
    assert len(store.ingest_block(block(), rows)) == 3
    assert len(store.ingest_block(block(), rows)) == 0
    with store.connect() as db:
        balances = {
            row["wallet"]: row["balance_raw"] for row in db.execute("SELECT wallet,balance_raw FROM wallet_inventory")
        }
    assert balances["0x" + "22" * 20] == "-25"
    assert balances["0x" + "33" * 20] == "25"
    assert b"TOKEN_TRANSFER" in store.export("csv", 100)
    assert len(pq.read_table(__import__("io").BytesIO(store.export("parquet", 100)))) == 3


def test_rewind_rebuilds_inventory(tmp_path):
    store = WalletStore(str(tmp_path / "events.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum-wallet-trading-v1", 1, 0, 1)
    rows = collector.normalize(transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00")
    store.ingest_block(block(), rows)
    store.rewind(10)
    assert store.coverage()["events"] == 0
    assert store.coverage()["inventory_states"] == 0
    assert store.checkpoint() is None


def test_erc721_transfer_signature_is_not_mislabelled_as_erc20(tmp_path):
    store = WalletStore(str(tmp_path / "events.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum-wallet-trading-v1", 1, 0, 1)
    log = transfer_log() | {"topics": transfer_log()["topics"] + ["0x" + "01".zfill(64)], "data": "0x"}
    assert collector.normalize(log, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00") == []


def test_public_metadata_health_and_protected_history(monkeypatch, tmp_path):
    store = WalletStore(str(tmp_path / "events.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum-wallet-trading-v1", 1, 0, 1)
    store.ingest_block(
        block(), collector.normalize(transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00")
    )
    monkeypatch.setattr(supplier_app, "wallet_store", store)
    monkeypatch.setattr(supplier_app.cfg, "enable_live", False)

    async def active(wallet, feed):
        return True

    monkeypatch.setattr(supplier_app.entitlements, "active", active)
    session = supplier_app.issuer.issue("0x" + "44" * 20, "session", 60)
    with TestClient(supplier_app.app) as client:
        assert client.get("/feeds/ethereum-wallet-trading-v1/schema").status_code == 200
        assert client.get("/feeds/ethereum-wallet-trading-v1/sample").status_code == 200
        assert client.get("/feeds/ethereum-wallet-trading-v1/history.csv").status_code == 401
        response = client.get(
            "/feeds/ethereum-wallet-trading-v1/history.csv", headers={"Authorization": f"Bearer {session}"}
        )
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")


def test_delivery_receipt_is_explicit_about_clock_quality(tmp_path):
    store = WalletStore(str(tmp_path / "events.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum-wallet-trading-v1", 1, 0, 1)
    rows = collector.normalize(transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00")
    store.ingest_block(block(), rows)
    result = store.receipt(rows[0]["event_id"], "0x" + "44" * 20, datetime.now(UTC).isoformat())
    assert result["measurement_status"] == "MEASURED_BUYER_REPORTED_CLOCKS_UNVERIFIED"
