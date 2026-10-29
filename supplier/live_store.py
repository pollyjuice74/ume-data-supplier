import threading
from collections import OrderedDict, deque
from datetime import datetime

from .wallet_store import utc_now


class LiveWalletStore:
    """Bounded, process-local state for a live-only supplier.

    This deliberately provides no durable history. A restart starts a new window;
    the collector's HTTP backfill reconstructs recent canonical events.
    """

    ephemeral = True

    def __init__(self, max_events: int = 5000):
        self.max_events = max_events
        self.lock = threading.RLock()
        self._events: deque[dict] = deque(maxlen=max_events)
        self._ids: set[str] = set()
        self._blocks: OrderedDict[int, dict] = OrderedDict()
        self._checkpoint: dict | None = None
        self._inventory: dict[tuple[int, str, str], int] = {}
        self._settings: dict[str, dict] = {}
        self._receipts: OrderedDict[tuple[str, str], dict] = OrderedDict()

    def setting(self, key: str) -> dict | None:
        return self._settings.get(key)

    def set_setting(self, key: str, value: dict) -> None:
        self._settings[key] = value

    def checkpoint(self) -> dict | None:
        return dict(self._checkpoint) if self._checkpoint else None

    def _rebuild(self) -> None:
        self._ids = {item["event_id"] for item in self._events}
        self._inventory.clear()
        for event in self._events:
            if event["event_type"] == "INVENTORY_CHANGE":
                key = (event["chain_id"], event["wallet"], event["asset"])
                self._inventory[key] = self._inventory.get(key, 0) + int(event["amount_raw"])
                event["relative_inventory_raw"] = str(self._inventory[key])

    def ingest_block(self, block: dict, events: list[dict]) -> list[dict]:
        inserted: list[dict] = []
        with self.lock:
            self._blocks[block["number"]] = dict(block)
            while len(self._blocks) > max(128, self.max_events):
                self._blocks.popitem(last=False)
            for original in events:
                if original["event_id"] in self._ids:
                    continue
                event = dict(original)
                if event["event_type"] == "INVENTORY_CHANGE":
                    key = (event["chain_id"], event["wallet"], event["asset"])
                    balance = self._inventory.get(key, 0) + int(event["amount_raw"])
                    self._inventory[key] = balance
                    event["relative_inventory_raw"] = str(balance)
                if len(self._events) == self.max_events:
                    self._ids.discard(self._events[0]["event_id"])
                self._events.append(event)
                self._ids.add(event["event_id"])
                inserted.append(event)
            self._checkpoint = {
                "name": "ethereum",
                "block_number": block["number"],
                "block_hash": block["hash"],
                "updated_at": utc_now(),
            }
        return inserted

    def rewind(self, from_block: int) -> None:
        with self.lock:
            self._events = deque(
                (event for event in self._events if event["block_number"] < from_block),
                maxlen=self.max_events,
            )
            for number in list(self._blocks):
                if number >= from_block:
                    del self._blocks[number]
            self._rebuild()
            if self._blocks:
                number, block = next(reversed(self._blocks.items()))
                self._checkpoint = {
                    "name": "ethereum",
                    "block_number": number,
                    "block_hash": block["hash"],
                    "updated_at": utc_now(),
                }
            else:
                self._checkpoint = None

    def prune(self, *_args) -> None:
        return None

    def latest(self, limit: int = 100, public: bool = False, event_types=None) -> list[dict]:
        with self.lock:
            rows = [dict(row) for row in self._events if not event_types or row["event_type"] in event_types]
        rows = rows[-min(max(limit, 1), self.max_events) :]
        return rows[-5:] if public else rows

    def events_after(self, event_id: str, limit: int = 1000) -> list[dict]:
        with self.lock:
            rows = list(self._events)
        try:
            index = next(index for index, row in enumerate(rows) if row["event_id"] == event_id)
        except StopIteration as exc:
            raise KeyError("CURSOR_EXPIRED") from exc
        return [dict(row) for row in rows[index + 1 : index + 1 + limit]]

    def coverage(self) -> dict:
        with self.lock:
            rows = list(self._events)
            receipts = list(self._receipts.values())
        return {
            "mode": "LIVE_ONLY_EPHEMERAL",
            "historical_storage": False,
            "history_guarantee": "NONE",
            "reconnect_replay": "BEST_EFFORT_CURRENT_PROCESS_WINDOW",
            "restart_resets_window": True,
            "events": len(rows),
            "window_capacity_events": self.max_events,
            "first_block": rows[0]["block_number"] if rows else None,
            "last_block": rows[-1]["block_number"] if rows else None,
            "from": rows[0]["source_timestamp"] if rows else None,
            "to": rows[-1]["source_timestamp"] if rows else None,
            "inventory_states": len(self._inventory),
            "inventory_basis": "PROCESS_WINDOW_RELATIVE",
            "bounded": True,
            "buyer_receipts": len(receipts),
            "supplier_to_buyer_delay_ms_average": (
                sum(row["supplier_to_buyer_ms"] for row in receipts) / len(receipts) if receipts else None
            ),
            "last_buyer_receipt_at": receipts[-1]["receipt_at"] if receipts else None,
            "supplier_to_buyer_measurement": (
                "BUYER_REPORTED_CLOCKS_UNVERIFIED" if receipts else "UNAVAILABLE_NO_BUYER_ACK"
            ),
        }

    def behavior(self, wallet: str) -> dict:
        rows = [row for row in self.latest(self.max_events) if row.get("wallet") == wallet]
        swaps = {"UNISWAP_V2_SWAP", "UNISWAP_V3_SWAP"}
        return {
            "from": rows[0]["source_timestamp"] if rows else None,
            "to": rows[-1]["source_timestamp"] if rows else None,
            "trade_count": len({row["transaction_hash"] for row in rows if row["event_type"] in swaps}),
            "transfer_count": len(
                {row["transaction_hash"] for row in rows if row["event_type"] == "TOKEN_TRANSFER"}
            ),
            "active_blocks": len({row["block_number"] for row in rows}),
        }

    def receipt(self, event_id: str, wallet: str, receipt_at: str) -> dict:
        event = next((row for row in self.latest(self.max_events) if row["event_id"] == event_id), None)
        if not event:
            raise KeyError(event_id)
        latency = (
            datetime.fromisoformat(receipt_at.replace("Z", "+00:00"))
            - datetime.fromisoformat(event["published_at"])
        ).total_seconds() * 1000
        result = {
            "event_id": event_id,
            "receipt_at": receipt_at,
            "supplier_to_buyer_ms": latency,
            "measurement_status": "MEASURED_BUYER_REPORTED_CLOCKS_UNVERIFIED",
        }
        self._receipts[(event_id, wallet)] = result
        while len(self._receipts) > self.max_events:
            self._receipts.popitem(last=False)
        return result

    def export_rows(self, _rows: list[dict], _fmt: str) -> bytes:
        raise RuntimeError("Historical exports are disabled in live-only mode")
