import asyncio
import threading
from datetime import UTC, datetime

import httpx
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from supplier import app as supplier_app
from supplier.wallet_collector import SWAP_V3, TRANSFER, EthereumWalletCollector
from supplier.wallet_products import WALLET_FEED_IDS, project
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
    coverage = store.coverage()
    assert coverage["events"] == 3
    assert coverage["inventory_states"] == 2
    assert coverage["first_block"] == coverage["last_block"] == 10
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
        catalog = client.get("/catalog/listings").json()
        assert {item["id"] for item in catalog} == set(WALLET_FEED_IDS)
        feed = "ethereum_wallet_inventory_deltas_v1"
        assert client.get(f"/feeds/{feed}/schema").status_code == 200
        assert client.get(f"/feeds/{feed}/sample").status_code == 200
        assert client.get(f"/feeds/{feed}/history.csv").status_code == 401
        response = client.get(f"/feeds/{feed}/history.csv", headers={"Authorization": f"Bearer {session}"})
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")


def test_sse_reports_expired_cursor_instead_of_silent_gap(monkeypatch, tmp_path):
    store = WalletStore(str(tmp_path / "cursor-sse.sqlite3"))
    monkeypatch.setattr(supplier_app, "wallet_store", store)
    monkeypatch.setattr(supplier_app.cfg, "enable_live", False)

    async def active(wallet, feed):
        return True

    monkeypatch.setattr(supplier_app.entitlements, "active", active)
    feed = "ethereum_mirror_candidates_v1"
    ticket = supplier_app.issuer.issue("0x" + "44" * 20, f"stream:{feed}", 60)
    with TestClient(supplier_app.app) as client:
        response = client.get(f"/feeds/{feed}/sse", params={"ticket": ticket, "cursor": "pruned-event"})
    assert response.status_code == 200
    assert "event: cursor_expired" in response.text
    assert "CURSOR_EXPIRED" in response.text


def test_delivery_receipt_is_explicit_about_clock_quality(tmp_path):
    store = WalletStore(str(tmp_path / "events.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum-wallet-trading-v1", 1, 0, 1)
    rows = collector.normalize(transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00")
    store.ingest_block(block(), rows)
    result = store.receipt(rows[0]["event_id"], "0x" + "44" * 20, datetime.now(UTC).isoformat())
    assert result["measurement_status"] == "MEASURED_BUYER_REPORTED_CLOCKS_UNVERIFIED"


def test_supplier_ownership_proof_survives_store_restart(tmp_path):
    path = str(tmp_path / "ownership.sqlite3")
    proof = {"owner_wallet": "0x" + "44" * 20, "signature": "0xsigned", "supplier_url": "https://supplier"}
    WalletStore(path).set_setting("ownership_proof", proof)
    assert WalletStore(path).setting("ownership_proof") == proof


def test_four_products_keep_missing_execution_evidence_null(tmp_path):
    store = WalletStore(str(tmp_path / "products.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum_wallet_trades_v1", 1, 0, 1)
    log = transfer_log() | {
        "topics": [SWAP_V3, "0x" + "00" * 12 + "22" * 20, "0x" + "00" * 12 + "33" * 20],
        "data": "0x" + (25).to_bytes(32, "big").hex() + ((1 << 256) - 20).to_bytes(32, "big").hex(),
    }
    event = collector.normalize(log, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00")[0]
    trade = project("ethereum_wallet_trades_v1", event, store)
    mirror = project("ethereum_mirror_candidates_v1", event, store)
    assert trade["amount_in_raw"] == "25" and trade["amount_out_raw"] == "20"
    assert mirror["execution_status"] == "MISSED_NO_QUOTE"
    assert mirror["estimated_pnl"] is None and mirror["estimated_cost"] is None
    assert len(WALLET_FEED_IDS) == 4


def test_events_after_cursor_is_ordered_and_rejects_unknown_cursor(tmp_path):
    store = WalletStore(str(tmp_path / "cursor.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum_wallet_trades_v1", 1, 0, 1)
    events = collector.normalize(
        transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"
    )
    store.ingest_block(block(), events)
    expected = sorted(events, key=lambda item: (item["log_index"], item["event_id"]))
    assert [row["event_id"] for row in store.events_after(expected[0]["event_id"])] == [
        row["event_id"] for row in expected[1:]
    ]
    with pytest.raises(KeyError, match="CURSOR_EXPIRED"):
        store.events_after("not-retained")


def test_pruned_inventory_baseline_survives_reorg_rebuild(tmp_path):
    store = WalletStore(str(tmp_path / "baseline.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum_wallet_trades_v1", 1, 0, 1)
    first = collector.normalize(
        transfer_log(), "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"
    )
    store.ingest_block(block(), first)
    second_log = transfer_log() | {
        "blockNumber": hex(11),
        "blockHash": "0x" + "bc" * 32,
        "transactionHash": "0x" + "cd" * 32,
    }
    second = collector.normalize(
        second_log, "2026-01-01T00:01:00+00:00", "2026-01-01T00:01:01+00:00"
    )
    second_block = block(11) | {"hash": "0x" + "bc" * 32, "parent_hash": block()["hash"]}
    store.ingest_block(second_block, second)
    store.prune(retention_days=365, max_events=3)
    store.rewind(11)
    with store.connect() as db:
        balances = {row["wallet"]: row["balance_raw"] for row in db.execute("SELECT * FROM wallet_inventory")}
    assert balances["0x" + "22" * 20] == "-25"
    assert balances["0x" + "33" * 20] == "25"


async def test_collector_falls_back_after_forbidden_rpc(tmp_path):
    store = WalletStore(str(tmp_path / "fallback.sqlite3"))
    collector = EthereumWalletCollector(
        ["https://primary.invalid", "https://fallback.invalid"],
        store,
        Hub(),
        "ethereum_wallet_trades_v1",
        1,
        0,
        1,
    )

    async def handler(request):
        if request.url.host == "primary.invalid":
            return httpx.Response(403)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 2, "result": "0x2a"})

    await collector.client.aclose()
    collector.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    assert await collector.rpc("eth_blockNumber", []) == "0x2a"
    assert collector.active_provider_index == 1
    await collector.close()


async def test_sqlite_ingestion_does_not_block_health_and_sse_event_loop(monkeypatch, tmp_path):
    store = WalletStore(str(tmp_path / "nonblocking.sqlite3"))
    collector = EthereumWalletCollector("http://unused", store, Hub(), "ethereum_wallet_trades_v1", 1, 0, 1)
    started, release = threading.Event(), threading.Event()
    original_ingest = store.ingest_block

    def blocked_ingest(block_row, events):
        started.set()
        release.wait(timeout=2)
        return original_ingest(block_row, events)

    async def rpc(method, _params):
        if method == "eth_getBlockByNumber":
            return {
                "hash": "0x" + "ab" * 32,
                "parentHash": "0x" + "cd" * 32,
                "timestamp": hex(1_767_225_600),
            }
        return []

    monkeypatch.setattr(store, "ingest_block", blocked_ingest)
    monkeypatch.setattr(collector, "rpc", rpc)
    task = asyncio.create_task(collector.collect_block(10))
    try:
        await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)
        # This models the scheduler work needed by /health and SSE heartbeat.
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=0.1)
    finally:
        release.set()
        await task
