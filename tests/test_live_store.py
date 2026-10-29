from supplier.live_store import LiveWalletStore


def event(number: int, event_id: str, event_type: str = "TOKEN_TRANSFER") -> dict:
    return {
        "event_id": event_id,
        "chain_id": 1,
        "block_number": number,
        "block_hash": f"0x{number:064x}",
        "transaction_hash": f"0x{number:064x}",
        "log_index": number,
        "event_type": event_type,
        "wallet": "0x" + "1" * 40,
        "counterparty": "0x" + "2" * 40,
        "asset": "0x" + "3" * 40,
        "amount_raw": "1",
        "source_timestamp": "2026-01-01T00:00:00+00:00",
        "supplier_observed_at": "2026-01-01T00:00:01+00:00",
        "normalized_at": "2026-01-01T00:00:01+00:00",
        "published_at": "2026-01-01T00:00:01+00:00",
        "provenance": {},
    }


def block(number: int) -> dict:
    return {
        "number": number,
        "hash": f"0x{number:064x}",
        "parent_hash": f"0x{number - 1:064x}",
        "source_timestamp": "2026-01-01T00:00:00+00:00",
        "observed_at": "2026-01-01T00:00:01+00:00",
    }


def test_live_store_is_bounded_deduplicated_and_explicitly_ephemeral():
    store = LiveWalletStore(max_events=100)
    for number in range(1, 102):
        assert store.ingest_block(block(number), [event(number, str(number))])
    assert len(store.latest(1000)) == 100
    assert not store.ingest_block(block(101), [event(101, "101")])
    coverage = store.coverage()
    assert coverage["mode"] == "LIVE_ONLY_EPHEMERAL"
    assert coverage["historical_storage"] is False
    assert coverage["restart_resets_window"] is True
    assert coverage["first_block"] == 2
    assert store.checkpoint()["block_number"] == 101


def test_live_store_cursor_and_reorg_rebuild_window_state():
    store = LiveWalletStore(max_events=100)
    store.ingest_block(block(10), [event(10, "a", "INVENTORY_CHANGE")])
    store.ingest_block(block(11), [event(11, "b", "INVENTORY_CHANGE")])
    assert [row["event_id"] for row in store.events_after("a")] == ["b"]
    assert store.latest()[-1]["relative_inventory_raw"] == "2"
    store.rewind(11)
    assert [row["event_id"] for row in store.latest()] == ["a"]
    assert store.coverage()["inventory_states"] == 1


def test_live_store_restart_has_no_historical_state():
    first = LiveWalletStore(max_events=100)
    first.ingest_block(block(12), [event(12, "before-restart")])
    restarted = LiveWalletStore(max_events=100)
    assert restarted.latest() == []
    assert restarted.checkpoint() is None
