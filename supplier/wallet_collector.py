import asyncio
import hashlib
import random
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


class CollectorRpcError(RuntimeError):
    def __init__(self, category: str, detail: str):
        super().__init__(detail)
        self.category = category


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
        rpc_url: str | list[str],
        store: WalletStore,
        hub,
        feed_id: str,
        poll_seconds: float,
        confirmations: int,
        backfill: int,
    ):
        self.rpc_urls = [rpc_url] if isinstance(rpc_url, str) else list(rpc_url)
        if not self.rpc_urls:
            raise ValueError("at least one Ethereum ingestion RPC is required")
        self.store, self.hub, self.feed_id = store, hub, feed_id
        self.poll_seconds, self.confirmations, self.backfill = poll_seconds, confirmations, backfill
        self.client = httpx.AsyncClient(timeout=30, headers={"User-Agent": "ume-data-supplier/0.2"})
        self.counter = 0
        self.running = False
        self.error: str | None = None
        self.reconnects = 0
        self.rpc_latency_ms: float | None = None
        self.active_provider_index: int | None = None
        self.error_category: str | None = None
        self.chain_head: int | None = None
        self.last_success_at: str | None = None
        self.last_successful_block: int | None = None
        self.retry_in_seconds: float | None = None
        self.retry_max_seconds = 60.0

    async def close(self):
        await self.client.aclose()

    async def rpc(self, method: str, params: list):
        failures: list[CollectorRpcError] = []
        start_index = self.active_provider_index or 0
        for offset in range(len(self.rpc_urls)):
            index = (start_index + offset) % len(self.rpc_urls)
            self.counter += 1
            started = time.monotonic()
            try:
                response = await self.client.post(
                    self.rpc_urls[index],
                    json={"jsonrpc": "2.0", "id": self.counter, "method": method, "params": params},
                )
                self.rpc_latency_ms = (time.monotonic() - started) * 1000
                if response.status_code == 403:
                    raise CollectorRpcError("COLLECTOR_RPC_FORBIDDEN", "ingestion RPC rejected this service")
                if response.status_code == 429:
                    raise CollectorRpcError("COLLECTOR_RPC_RATE_LIMITED", "ingestion RPC rate limited this service")
                if response.status_code >= 500:
                    raise CollectorRpcError("COLLECTOR_RPC_UPSTREAM", "ingestion RPC is temporarily unavailable")
                response.raise_for_status()
                try:
                    body = response.json()
                except ValueError as exc:
                    raise CollectorRpcError("COLLECTOR_RPC_MALFORMED", "ingestion RPC returned invalid JSON") from exc
                if not isinstance(body, dict) or body.get("jsonrpc") != "2.0":
                    raise CollectorRpcError("COLLECTOR_RPC_MALFORMED", "ingestion RPC returned an invalid envelope")
                if body.get("error"):
                    raise CollectorRpcError("COLLECTOR_RPC_ERROR", "ingestion RPC returned a JSON-RPC error")
                if "result" not in body:
                    raise CollectorRpcError("COLLECTOR_RPC_MALFORMED", "ingestion RPC omitted result")
                self.active_provider_index = index
                return body["result"]
            except CollectorRpcError as exc:
                failures.append(exc)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                failures.append(CollectorRpcError("COLLECTOR_RPC_TIMEOUT", type(exc).__name__))
            except httpx.HTTPStatusError as exc:
                failures.append(CollectorRpcError("COLLECTOR_RPC_HTTP_ERROR", f"HTTP {exc.response.status_code}"))
        category = failures[-1].category if failures else "COLLECTOR_RPC_UNAVAILABLE"
        raise CollectorRpcError(category, f"all {len(self.rpc_urls)} configured ingestion RPC providers failed")

    async def run(self):
        delay = 1.0
        while True:
            try:
                await self.collect_once()
                self.running, self.error, self.error_category, delay = True, None, None, 1.0
                self.retry_in_seconds = None
                await asyncio.sleep(self.poll_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.running, self.error = False, str(exc)
                self.error_category = getattr(exc, "category", "COLLECTOR_STALLED")
                self.reconnects += 1
                wait = min(delay, self.retry_max_seconds) + random.uniform(0, min(1.0, delay / 4))
                self.retry_in_seconds = wait
                await asyncio.sleep(wait)
                delay = min(delay * 2, self.retry_max_seconds)

    async def collect_once(self):
        raw_head = quantity(await self.rpc("eth_blockNumber", []))
        self.chain_head = raw_head
        tip = raw_head - self.confirmations
        # SQLite work must not run on the ASGI event loop.  A busy Ethereum
        # block can contain thousands of transfers; on the production 0.1 vCPU
        # instance synchronous ingestion made health checks and SSE heartbeats
        # stop until the entire transaction/projection pass completed.
        checkpoint = await asyncio.to_thread(self.store.checkpoint)
        if checkpoint:
            canonical = await self.rpc("eth_getBlockByNumber", [hex(checkpoint["block_number"]), False])
            if not canonical or canonical["hash"].lower() != checkpoint["block_hash"]:
                await asyncio.to_thread(self.store.rewind, max(0, checkpoint["block_number"] - self.backfill))
                checkpoint = await asyncio.to_thread(self.store.checkpoint)
        start = checkpoint["block_number"] + 1 if checkpoint else max(0, tip - self.backfill + 1)
        for number in range(start, tip + 1):
            await self.collect_block(number)

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
        inserted, projected = await asyncio.to_thread(
            self._persist_and_project,
            {
                "number": number,
                "hash": block["hash"].lower(),
                "parent_hash": block["parentHash"].lower(),
                "source_timestamp": timestamp,
                "observed_at": observed,
            },
            events,
        )
        self.last_successful_block = number
        self.last_success_at = utc_now()
        self.hub.connected[self.feed_id] = True
        for feed_id, item in projected:
            self.hub.connected[feed_id] = True
            self.hub.publish(feed_id, item)
        return inserted

    def _persist_and_project(self, block: dict, events: list[dict]) -> tuple[list[dict], list[tuple[str, dict]]]:
        """Perform all blocking SQLite work in one worker-thread call."""
        inserted = self.store.ingest_block(block, events)
        projected = []
        for event in inserted:
            for feed_id in WALLET_FEED_IDS:
                item = project(feed_id, event, self.store)
                if item:
                    projected.append((feed_id, item))
        return inserted, projected

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
        checkpoint = self.store.checkpoint()
        indexed = checkpoint["block_number"] if checkpoint else self.last_successful_block
        return {
            "collector_running": self.running,
            "error_category": self.error_category,
            "error": self.error,
            "reconnects": self.reconnects,
            "rpc_latency_ms": self.rpc_latency_ms,
            "configured_rpc_providers": len(self.rpc_urls),
            "active_rpc_provider": self.active_provider_index,
            "current_chain_head": self.chain_head,
            "last_successfully_ingested_block": indexed,
            "indexer_lag_blocks": (
                max(0, self.chain_head - indexed) if self.chain_head is not None and indexed else None
            ),
            "last_success_at": self.last_success_at,
            "retry_in_seconds": self.retry_in_seconds,
        }
