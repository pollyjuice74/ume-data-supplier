import hashlib
from datetime import datetime

WALLET_FEED_IDS = (
    "ethereum_wallet_trades_v1",
    "ethereum_wallet_inventory_deltas_v1",
    "ethereum_wallet_behavior_v1",
    "ethereum_mirror_candidates_v1",
)
SWAPS = {"UNISWAP_V2_SWAP", "UNISWAP_V3_SWAP"}


def _provenance(event: dict, derivation: str, limitations: list[str] | None = None) -> dict:
    return event["provenance"] | {
        "source_event_id": event["event_id"],
        "derivation": derivation,
        "limitations": limitations or [],
    }


def trade(event: dict) -> dict | None:
    if event["event_type"] not in SWAPS:
        return None
    amount0, amount1 = int(event["amount0_raw"]), int(event["amount1_raw"])
    incoming = next((abs(x) for x in (amount0, amount1) if x > 0), None)
    outgoing = next((abs(x) for x in (amount0, amount1) if x < 0), None)
    return {
        "event_id": event["event_id"],
        "chain_id": event["chain_id"],
        "block_number": event["block_number"],
        "block_timestamp": event["source_timestamp"],
        "tx_hash": event["transaction_hash"],
        "log_index": event["log_index"],
        "wallet": event.get("wallet"),
        "pool": event["pool"],
        "token_in": None,
        "token_out": None,
        "amount_in_raw": str(incoming) if incoming is not None else None,
        "amount_out_raw": str(outgoing) if outgoing is not None else None,
        "provenance": _provenance(event, "observed_swap_log", ["POOL_TOKEN_METADATA_NOT_RESOLVED"]),
    }


def inventory(event: dict) -> dict | None:
    if event["event_type"] != "INVENTORY_CHANGE":
        return None
    return {
        "event_id": event["event_id"],
        "chain_id": event["chain_id"],
        "wallet": event["wallet"],
        "token_address": event["asset"],
        "block_number": event["block_number"],
        "tx_hash": event["transaction_hash"],
        "delta_raw": event["amount_raw"],
        "relative_inventory_raw": event.get("relative_inventory_raw"),
        "balance_scope": "WINDOW_RELATIVE",
        "provenance": _provenance(event, "exact_transfer_delta", ["INDEXED_WINDOW_NOT_COMPLETE_BALANCE"]),
    }


def behavior(event: dict, aggregate: dict) -> dict | None:
    wallet = event.get("wallet")
    if not wallet:
        return None
    start, end = aggregate["from"], aggregate["to"]
    seconds = (
        max((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(), 1) if start and end else 0
    )
    event_id = hashlib.sha256(f"behavior:{wallet}:{event['event_id']}".encode()).hexdigest()
    return {
        "event_id": event_id,
        "chain_id": event["chain_id"],
        "wallet": wallet,
        "observation_window": {"from": start, "to": end, "scope": "BOUNDED_INDEXED_WINDOW"},
        "trade_count": aggregate["trade_count"],
        "transfer_count": aggregate["transfer_count"],
        "activity_frequency": (aggregate["trade_count"] + aggregate["transfer_count"]) / seconds if seconds else None,
        "accumulation_metrics": {"active_blocks": aggregate["active_blocks"]},
        "relationship_metrics": None,
        "lead_lag_metrics": None,
        "quality_flags": ["WINDOW_RELATIVE", "RELATIONSHIPS_NOT_COMPUTED_LIVE", "NO_PNL_INFERENCE"],
        "provenance": _provenance(event, "bounded_sql_aggregate"),
    }


def mirror(event: dict) -> dict | None:
    item = trade(event)
    if not item:
        return None
    return {
        "event_id": hashlib.sha256(f"mirror:{event['event_id']}".encode()).hexdigest(),
        "chain_id": event["chain_id"],
        "source_wallet": event.get("wallet"),
        "source_tx_hash": event["transaction_hash"],
        "observed_action": event["event_type"],
        "token_in": item["token_in"],
        "token_out": item["token_out"],
        "observation_timestamp": event["supplier_observed_at"],
        "replication_delay_ms": None,
        "execution_status": "MISSED_NO_QUOTE",
        "estimated_cost": None,
        "estimated_pnl": None,
        "missing_reason": "NO_EXECUTABLE_QUOTE_EVIDENCE",
        "provenance": _provenance(event, "candidate_from_observed_swap", ["NOT_AN_EXECUTED_MIRROR_TRADE"]),
    }


def project(feed_id: str, event: dict, store=None) -> dict | None:
    if feed_id == WALLET_FEED_IDS[0]:
        return trade(event)
    if feed_id == WALLET_FEED_IDS[1]:
        return inventory(event)
    if feed_id == WALLET_FEED_IDS[2] and store is not None and event.get("wallet") and event["event_type"] in SWAPS:
        return behavior(event, store.behavior(event["wallet"]))
    if feed_id == WALLET_FEED_IDS[3]:
        return mirror(event)
    return None


def rows(feed_id: str, raw_events: list[dict], store) -> list[dict]:
    result = []
    seen = set()
    for event in raw_events:
        item = project(feed_id, event, store)
        if item and item["event_id"] not in seen:
            result.append(item)
            seen.add(item["event_id"])
    return result
