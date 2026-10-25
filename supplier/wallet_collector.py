import asyncio
import hashlib
import time
from datetime import UTC, datetime

import httpx
from eth_utils import keccak

from .wallet_products import WALLET_FEED_IDS, project
from .wallet_store import WalletStore, utc_now

TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
SWAP_V2 = "0x" + keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
SWAP_V3 = "0x" + keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
ZERO = "0x" + "0" * 40


def quantity(value: str) -> int:
    return int(value, 16)


def topic_address(value: str) -> str:
    return "0x" + value[-40:].lower()


def words(value: str) -> list[int]:
    raw = value.removeprefix("0x")
    return [int(raw[i : i + 64], 16) for i in range(0, len(raw), 64) if len(raw[i : i + 64]) == 64]


def signed(value: int) -> int:
    return value - (1 << 256) if value >= 1 << 255 else value


class EthereumWalletCollector:
    def __init__(
        self,
        rpc_url: str,
        store: WalletStore,
        hub,
        feed_id: str,
        poll_seconds: float,
        confirmations: int,
        backfill: int,
    ):
        self.rpc_url, self.store, self.hub, self.feed_id = rpc_url, store, hub, feed_id
        self.poll_seconds, self.confirmations, self.backfill = poll_seconds, confirmations, backfill
        self.client = httpx.AsyncClient(timeout=30, headers={"User-Agent": "ume-data-supplier/0.2"})
        self.counter = 0
        self.running = False
        self.error: str | None = None
        self.reconnects = 0
        self.rpc_latency_ms: float | None = None
        self.last_prune = 0.0
        self.retention_days = 14
        self.max_events = 250_000

    async def close(self):
        await self.client.aclose()

    async def rpc(self, method: str, params: list):
        self.counter += 1
        started = time.monotonic()
        response = await self.client.post(
            self.rpc_url, json={"jsonrpc": "2.0", "id": self.counter, "method": method, "params": params}
        )
        self.rpc_latency_ms = (time.monotonic() - started) * 1000
        response.raise_for_status()
        body = response.json()
        if body.get("error"):
            raise RuntimeError(body["error"].get("message", str(body["error"])))
        return body.get("result")

    async def run(self):
        delay = 1.0
        while True:
            try:
                await self.collect_once()
                self.running, self.error, delay = True, None, 1.0
                await asyncio.sleep(self.poll_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.running, self.error = False, str(exc)
                self.reconnects += 1
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def collect_once(self):
        tip = quantity(await self.rpc("eth_blockNumber", [])) - self.confirmations
        checkpoint = self.store.checkpoint()
        if checkpoint:
            canonical = await self.rpc("eth_getBlockByNumber", [hex(checkpoint["block_number"]), False])
            if not canonical or canonical["hash"].lower() != checkpoint["block_hash"]:
                self.store.rewind(max(0, checkpoint["block_number"] - self.backfill))
                checkpoint = self.store.checkpoint()
        start = checkpoint["block_number"] + 1 if checkpoint else max(0, tip - self.backfill + 1)
        for number in range(start, tip + 1):
            await self.collect_block(number)
        if time.monotonic() - self.last_prune >= 3600:
            self.store.prune(self.retention_days, self.max_events)
            self.last_prune = time.monotonic()

    async def collect_block(self, number: int):
        observed = utc_now()
        block = await self.rpc("eth_getBlockByNumber", [hex(number), False])
        if not block:
            raise RuntimeError(f"Ethereum block {number} unavailable")
        query = {"fromBlock": hex(number), "toBlock": hex(number), "topics": [[TRANSFER, SWAP_V2, SWAP_V3]]}
        logs = await self.rpc("eth_getLogs", [query]) or []
        timestamp = datetime.fromtimestamp(quantity(block["timestamp"]), UTC).isoformat()
        events = []
        for log in logs:
            events.extend(self.normalize(log, timestamp, observed))
        inserted = self.store.ingest_block(
            {
                "number": number,
                "hash": block["hash"].lower(),
                "parent_hash": block["parentHash"].lower(),
                "source_timestamp": timestamp,
                "observed_at": observed,
            },
            events,
        )
        self.hub.connected[self.feed_id] = True
        for event in inserted:
            for feed_id in WALLET_FEED_IDS:
                item = project(feed_id, event, self.store)
                if item:
                    self.hub.connected[feed_id] = True
                    self.hub.publish(feed_id, item)

    def normalize(self, log: dict, source_timestamp: str, observed: str) -> list[dict]:
        topics = [item.lower() for item in log.get("topics", [])]
        head = topics[0] if topics else ""
        base = {
            "chain_id": 1,
            "block_number": quantity(log["blockNumber"]),
            "block_hash": log["blockHash"].lower(),
            "transaction_hash": log["transactionHash"].lower(),
            "log_index": quantity(log["logIndex"]),
            "source_timestamp": source_timestamp,
            "supplier_observed_at": observed,
            "normalized_at": utc_now(),
            "published_at": utc_now(),
            "provenance": {
                "source": "ethereum_rpc",
                "observation": "canonical_log",
                "contract": log["address"].lower(),
            },
        }

        def event(kind: str, wallet=None, counterparty=None, asset=None, amount=None, suffix=""):
            identity = f"1:{base['block_hash']}:{base['transaction_hash']}:{base['log_index']}:{kind}:{wallet}:{suffix}"
            return base | {
                "event_id": hashlib.sha256(identity.encode()).hexdigest(),
                "event_type": kind,
                "wallet": wallet,
                "counterparty": counterparty,
                "asset": asset,
                "amount_raw": amount,
            }

        transfer_words = words(log.get("data", "0x"))
        # ERC-721 reuses Transfer but normally indexes tokenId as a fourth topic.
        # This feed only emits fungible transfers with an explicit uint256 data word.
        if head == TRANSFER and len(topics) == 3 and transfer_words:
            source, target = topic_address(topics[1]), topic_address(topics[2])
            amount = str(transfer_words[0])
            result = [event("TOKEN_TRANSFER", source, target, log["address"].lower(), amount)]
            if source != ZERO:
                result.append(
                    event("INVENTORY_CHANGE", source, target, log["address"].lower(), str(-int(amount)), "out")
                )
            if target != ZERO:
                result.append(event("INVENTORY_CHANGE", target, source, log["address"].lower(), amount, "in"))
            return result
        data = words(log.get("data", "0x"))
        if head == SWAP_V2 and len(topics) >= 3 and len(data) >= 4:
            return [
                event("UNISWAP_V2_SWAP", topic_address(topics[1]), topic_address(topics[2]), suffix="swap")
                | {
                    "amount0_raw": str(data[2] - data[0]),
                    "amount1_raw": str(data[3] - data[1]),
                    "pool": log["address"].lower(),
                }
            ]
        if head == SWAP_V3 and len(topics) >= 3 and len(data) >= 2:
            return [
                event("UNISWAP_V3_SWAP", topic_address(topics[1]), topic_address(topics[2]), suffix="swap")
                | {
                    "amount0_raw": str(signed(data[0])),
                    "amount1_raw": str(signed(data[1])),
                    "pool": log["address"].lower(),
                }
            ]
        return []

    def health(self) -> dict:
        return {
            "collector_running": self.running,
            "error": self.error,
            "reconnects": self.reconnects,
            "rpc_latency_ms": self.rpc_latency_ms,
        }
