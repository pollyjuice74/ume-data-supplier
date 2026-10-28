import csv
import io
import json
import sqlite3
import threading
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from pathlib import Path


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class WalletStore:
    """Small WAL-backed, restart-safe event store for the paid wallet feed."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.behavior_cache: OrderedDict[str, dict] = OrderedDict()
        self.behavior_cache_size = 2048
        self._migrate()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA cache_size=-4096")
        db.execute("PRAGMA temp_store=FILE")
        return db

    def _migrate(self) -> None:
        with self.lock, self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA wal_autocheckpoint=256")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS wallet_blocks(
                  block_number INTEGER PRIMARY KEY, block_hash TEXT NOT NULL, parent_hash TEXT NOT NULL,
                  source_timestamp TEXT NOT NULL, observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_events(
                  event_id TEXT PRIMARY KEY, block_number INTEGER NOT NULL, block_hash TEXT NOT NULL,
                  transaction_hash TEXT NOT NULL, log_index INTEGER NOT NULL, event_type TEXT NOT NULL,
                  wallet TEXT, counterparty TEXT, asset TEXT, amount_raw TEXT,
                  source_timestamp TEXT NOT NULL, supplier_observed_at TEXT NOT NULL,
                  normalized_at TEXT NOT NULL, published_at TEXT NOT NULL, payload_json TEXT NOT NULL,
                  UNIQUE(block_hash, transaction_hash, log_index, event_type, wallet)
                );
                CREATE INDEX IF NOT EXISTS wallet_events_block_idx ON wallet_events(block_number,log_index);
                CREATE TABLE IF NOT EXISTS wallet_inventory(
                  chain_id INTEGER NOT NULL, wallet TEXT NOT NULL, asset TEXT NOT NULL,
                  balance_raw TEXT NOT NULL, updated_event_id TEXT NOT NULL, updated_at TEXT NOT NULL,
                  PRIMARY KEY(chain_id,wallet,asset)
                );
                CREATE TABLE IF NOT EXISTS wallet_inventory_baseline(
                  chain_id INTEGER NOT NULL, wallet TEXT NOT NULL, asset TEXT NOT NULL,
                  balance_raw TEXT NOT NULL,
                  PRIMARY KEY(chain_id,wallet,asset)
                );
                CREATE TABLE IF NOT EXISTS wallet_checkpoint(
                  name TEXT PRIMARY KEY, block_number INTEGER NOT NULL,
                  block_hash TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS delivery_receipts(
                  event_id TEXT NOT NULL, buyer_wallet TEXT NOT NULL, receipt_at TEXT NOT NULL,
                  supplier_to_buyer_ms REAL, measurement_status TEXT NOT NULL,
                  PRIMARY KEY(event_id,buyer_wallet)
                );
                CREATE TABLE IF NOT EXISTS supplier_settings(
                  key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wallet_coverage_stats(
                  id INTEGER PRIMARY KEY CHECK(id=1), events INTEGER NOT NULL,
                  first_block INTEGER, last_block INTEGER,
                  first_timestamp TEXT, last_timestamp TEXT,
                  inventory_states INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO wallet_coverage_stats
                SELECT 1,COUNT(*),MIN(block_number),MAX(block_number),
                       MIN(source_timestamp),MAX(source_timestamp),
                       (SELECT COUNT(*) FROM wallet_inventory)
                FROM wallet_events;
                CREATE TRIGGER IF NOT EXISTS wallet_events_coverage_insert
                AFTER INSERT ON wallet_events BEGIN
                  UPDATE wallet_coverage_stats SET
                    events=events+1,
                    first_block=CASE
                      WHEN first_block IS NULL OR NEW.block_number<first_block THEN NEW.block_number
                      ELSE first_block END,
                    last_block=CASE
                      WHEN last_block IS NULL OR NEW.block_number>last_block THEN NEW.block_number
                      ELSE last_block END,
                    first_timestamp=CASE
                      WHEN first_timestamp IS NULL OR NEW.source_timestamp<first_timestamp THEN NEW.source_timestamp
                      ELSE first_timestamp END,
                    last_timestamp=CASE
                      WHEN last_timestamp IS NULL OR NEW.source_timestamp>last_timestamp THEN NEW.source_timestamp
                      ELSE last_timestamp END
                  WHERE id=1;
                END;
                """
            )

    def setting(self, key: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT value_json FROM supplier_settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def set_setting(self, key: str, value: dict) -> None:
        with self.lock, self.connect() as db:
            db.execute(
                """INSERT INTO supplier_settings VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
                (key, json.dumps(value, separators=(",", ":"), sort_keys=True), utc_now()),
            )

    @staticmethod
    def _refresh_coverage_stats(db: sqlite3.Connection) -> None:
        db.execute(
            """UPDATE wallet_coverage_stats SET
               events=(SELECT COUNT(*) FROM wallet_events),
               first_block=(SELECT MIN(block_number) FROM wallet_events),
               last_block=(SELECT MAX(block_number) FROM wallet_events),
               first_timestamp=(SELECT MIN(source_timestamp) FROM wallet_events),
               last_timestamp=(SELECT MAX(source_timestamp) FROM wallet_events),
               inventory_states=(SELECT COUNT(*) FROM wallet_inventory)
               WHERE id=1"""
        )

    def checkpoint(self) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM wallet_checkpoint WHERE name='ethereum'").fetchone()
        return dict(row) if row else None

    def ingest_block(self, block: dict, events: list[dict]) -> list[dict]:
        inserted: list[dict] = []
        with self.lock, self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT OR REPLACE INTO wallet_blocks VALUES(?,?,?,?,?)",
                (block["number"], block["hash"], block["parent_hash"], block["source_timestamp"], block["observed_at"]),
            )
            for event in events:
                if event.get("wallet"):
                    self.behavior_cache.pop(event["wallet"], None)
                if event["event_type"] == "INVENTORY_CHANGE":
                    current = db.execute(
                        "SELECT balance_raw FROM wallet_inventory WHERE chain_id=? AND wallet=? AND asset=?",
                        (event["chain_id"], event["wallet"], event["asset"]),
                    ).fetchone()
                    balance = (int(current[0]) if current else 0) + int(event["amount_raw"])
                    event["relative_inventory_raw"] = str(balance)
                values = (
                    event["event_id"],
                    event["block_number"],
                    event["block_hash"],
                    event["transaction_hash"],
                    event["log_index"],
                    event["event_type"],
                    event.get("wallet"),
                    event.get("counterparty"),
                    event.get("asset"),
                    event.get("amount_raw"),
                    event["source_timestamp"],
                    event["supplier_observed_at"],
                    event["normalized_at"],
                    event["published_at"],
                    json.dumps(event, separators=(",", ":"), sort_keys=True),
                )
                cursor = db.execute(
                    """INSERT OR IGNORE INTO wallet_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", values
                )
                if cursor.rowcount:
                    inserted.append(event)
                    if event["event_type"] == "INVENTORY_CHANGE":
                        db.execute(
                            """INSERT INTO wallet_inventory VALUES(?,?,?,?,?,?)
                               ON CONFLICT(chain_id,wallet,asset) DO UPDATE SET balance_raw=excluded.balance_raw,
                               updated_event_id=excluded.updated_event_id,updated_at=excluded.updated_at""",
                            (
                                event["chain_id"],
                                event["wallet"],
                                event["asset"],
                                str(balance),
                                event["event_id"],
                                event["normalized_at"],
                            ),
                        )
                        if current is None:
                            db.execute(
                                "UPDATE wallet_coverage_stats SET inventory_states=inventory_states+1 WHERE id=1"
                            )
            db.execute(
                """INSERT INTO wallet_checkpoint VALUES('ethereum',?,?,?)
                   ON CONFLICT(name) DO UPDATE SET block_number=excluded.block_number,
                   block_hash=excluded.block_hash,updated_at=excluded.updated_at""",
                (block["number"], block["hash"], utc_now()),
            )
        return inserted

    def rewind(self, from_block: int) -> None:
        with self.lock, self.connect() as db:
            self.behavior_cache.clear()
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM wallet_events WHERE block_number>=?", (from_block,))
            db.execute("DELETE FROM wallet_blocks WHERE block_number>=?", (from_block,))
            db.execute("DELETE FROM wallet_checkpoint")
            db.execute("DELETE FROM wallet_inventory")
            baseline_rows = db.execute(
                "SELECT wallet,asset,balance_raw FROM wallet_inventory_baseline"
            )
            balances: dict[tuple[str, str], tuple[int, str, str]] = {
                (row["wallet"], row["asset"]): (int(row["balance_raw"]), "baseline", utc_now())
                for row in baseline_rows
            }
            rows = db.execute(
                """SELECT wallet,asset,amount_raw,event_id,normalized_at FROM wallet_events
                   WHERE event_type='INVENTORY_CHANGE' ORDER BY block_number,log_index,event_id"""
            )
            for row in rows:
                key = (row["wallet"], row["asset"])
                value = balances.get(key, (0, "", ""))[0] + int(row["amount_raw"])
                balances[key] = (value, row["event_id"], row["normalized_at"])
            db.executemany(
                "INSERT INTO wallet_inventory VALUES(1,?,?,?,?,?)",
                [
                    (wallet, asset, str(value), event_id, updated)
                    for (wallet, asset), (value, event_id, updated) in balances.items()
                ],
            )
            last = db.execute(
                "SELECT block_number,block_hash FROM wallet_blocks ORDER BY block_number DESC LIMIT 1"
            ).fetchone()
            if last:
                db.execute("INSERT INTO wallet_checkpoint VALUES('ethereum',?,?,?)", (*last, utc_now()))
            self._refresh_coverage_stats(db)

    def prune(self, retention_days: int, max_events: int) -> None:
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        with self.lock, self.connect() as db:
            self.behavior_cache.clear()
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TEMP TABLE prune_event_ids(event_id TEXT PRIMARY KEY)")
            db.execute(
                "INSERT OR IGNORE INTO prune_event_ids SELECT event_id FROM wallet_events WHERE source_timestamp<?",
                (cutoff,),
            )
            db.execute(
                """INSERT OR IGNORE INTO prune_event_ids
                   SELECT event_id FROM wallet_events ORDER BY block_number DESC,log_index DESC LIMIT -1 OFFSET ?""",
                (max_events,),
            )
            removed_rows = db.execute(
                """SELECT wallet,asset,amount_raw FROM wallet_events
                   WHERE event_type='INVENTORY_CHANGE'
                     AND event_id IN (SELECT event_id FROM prune_event_ids)"""
            )
            removed: dict[tuple[str, str], int] = {}
            for row in removed_rows:
                key = (row["wallet"], row["asset"])
                removed[key] = removed.get(key, 0) + int(row["amount_raw"])
            for (wallet, asset), delta in removed.items():
                previous = db.execute(
                    "SELECT balance_raw FROM wallet_inventory_baseline WHERE chain_id=1 AND wallet=? AND asset=?",
                    (wallet, asset),
                ).fetchone()
                balance = (int(previous[0]) if previous else 0) + delta
                db.execute(
                    """INSERT INTO wallet_inventory_baseline VALUES(1,?,?,?)
                       ON CONFLICT(chain_id,wallet,asset) DO UPDATE SET balance_raw=excluded.balance_raw""",
                    (wallet, asset, str(balance)),
                )
            db.execute("DELETE FROM wallet_events WHERE event_id IN (SELECT event_id FROM prune_event_ids)")
            db.execute("DELETE FROM delivery_receipts WHERE event_id NOT IN (SELECT event_id FROM wallet_events)")
            db.execute(
                """DELETE FROM wallet_blocks WHERE source_timestamp<? AND block_number<
                   COALESCE((SELECT block_number FROM wallet_checkpoint WHERE name='ethereum'),0)""",
                (cutoff,),
            )
            self._refresh_coverage_stats(db)

    def latest(self, limit: int = 100, public: bool = False, event_types: tuple[str, ...] | None = None) -> list[dict]:
        limit = min(max(limit, 1), 50_000)
        with self.connect() as db:
            if event_types:
                placeholders = ",".join("?" for _ in event_types)
                rows = db.execute(
                    f"SELECT payload_json FROM wallet_events WHERE event_type IN ({placeholders}) "
                    "ORDER BY block_number DESC,log_index DESC LIMIT ?",
                    (*event_types, limit),
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT payload_json FROM wallet_events ORDER BY block_number DESC,log_index DESC LIMIT ?", (limit,)
                ).fetchall()
        events = [json.loads(row[0]) for row in reversed(rows)]
        return events[-5:] if public else events

    def events_after(self, event_id: str, limit: int = 1000) -> list[dict]:
        """Return retained events strictly after a stable event cursor."""
        limit = min(max(limit, 1), 50_000)
        with self.connect() as db:
            cursor = db.execute(
                "SELECT block_number,log_index,event_id FROM wallet_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if not cursor:
                raise KeyError("CURSOR_EXPIRED")
            rows = db.execute(
                """SELECT payload_json FROM wallet_events
                   WHERE (block_number,log_index,event_id)>(?,?,?)
                   ORDER BY block_number,log_index,event_id LIMIT ?""",
                (cursor["block_number"], cursor["log_index"], cursor["event_id"], limit),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def coverage(self) -> dict:
        with self.connect() as db:
            row = db.execute(
                """SELECT events,first_block,last_block,first_timestamp,last_timestamp,inventory_states
                   FROM wallet_coverage_stats WHERE id=1"""
            ).fetchone()
            delivery = db.execute(
                "SELECT COUNT(*),AVG(supplier_to_buyer_ms),MAX(receipt_at) FROM delivery_receipts"
            ).fetchone()
        return {
            "events": row[0],
            "first_block": row[1],
            "last_block": row[2],
            "from": row[3],
            "to": row[4],
            "inventory_states": row[5],
            "inventory_basis": "WINDOW_RELATIVE",
            "bounded": True,
            "buyer_receipts": delivery[0],
            "supplier_to_buyer_delay_ms_average": delivery[1],
            "last_buyer_receipt_at": delivery[2],
            "supplier_to_buyer_measurement": (
                "BUYER_REPORTED_CLOCKS_UNVERIFIED" if delivery[0] else "UNAVAILABLE_NO_BUYER_ACK"
            ),
        }

    def export_rows(self, rows: list[dict], fmt: str) -> bytes:
        if fmt == "json":
            return json.dumps(rows, separators=(",", ":")).encode()
        flat = [
            {k: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v for k, v in row.items()}
            for row in rows
        ]
        if fmt == "csv":
            output = io.StringIO()
            if flat:
                fieldnames = list(dict.fromkeys(key for row in flat for key in row))
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flat)
            return output.getvalue().encode()
        if fmt == "parquet":
            # PyArrow has a large native footprint. Import it only for an actual
            # Parquet request so the always-on collector does not pay that cost.
            import pyarrow as pa
            import pyarrow.parquet as pq

            output = io.BytesIO()
            pq.write_table(pa.Table.from_pylist(flat), output, compression="zstd")
            return output.getvalue()
        raise ValueError("unsupported format")

    def export(self, fmt: str, max_rows: int) -> bytes:
        """Backward-compatible raw export used by local operators and migrations."""
        return self.export_rows(self.latest(max_rows), fmt)

    def behavior(self, wallet: str) -> dict:
        with self.lock:
            cached = self.behavior_cache.get(wallet)
            if cached is not None:
                self.behavior_cache.move_to_end(wallet)
                return cached
        with self.connect() as db:
            row = db.execute(
                """SELECT MIN(source_timestamp),MAX(source_timestamp),
                   COUNT(DISTINCT CASE WHEN event_type LIKE 'UNISWAP_%' THEN transaction_hash END),
                   COUNT(DISTINCT CASE WHEN event_type='TOKEN_TRANSFER' THEN transaction_hash END),
                   COUNT(DISTINCT block_number)
                   FROM wallet_events WHERE wallet=?""",
                (wallet,),
            ).fetchone()
        result = {
            "from": row[0],
            "to": row[1],
            "trade_count": row[2],
            "transfer_count": row[3],
            "active_blocks": row[4],
        }
        with self.lock:
            self.behavior_cache[wallet] = result
            self.behavior_cache.move_to_end(wallet)
            while len(self.behavior_cache) > self.behavior_cache_size:
                self.behavior_cache.popitem(last=False)
        return result

    def receipt(self, event_id: str, wallet: str, receipt_at: str) -> dict:
        received = datetime.fromisoformat(receipt_at.replace("Z", "+00:00"))
        with self.lock, self.connect() as db:
            row = db.execute("SELECT published_at FROM wallet_events WHERE event_id=?", (event_id,)).fetchone()
            if not row:
                raise KeyError(event_id)
            published = datetime.fromisoformat(row[0])
            latency = (received - published).total_seconds() * 1000
            status = "MEASURED_BUYER_REPORTED_CLOCKS_UNVERIFIED"
            db.execute(
                "INSERT OR REPLACE INTO delivery_receipts VALUES(?,?,?,?,?)",
                (event_id, wallet, receipt_at, latency, status),
            )
        return {"event_id": event_id, "supplier_to_buyer_ms": latency, "measurement_status": status}
