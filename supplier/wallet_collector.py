import asyncio
import hashlib
import json
import logging
import random
import time
from collections import OrderedDict, deque
from datetime import UTC, datetime

import httpx
import websockets
from eth_utils import keccak

from .wallet_products import WALLET_FEED_IDS, project
from .wallet_store import WalletStore, utc_now

TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
SWAP_V2 = "0x" + keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
SWAP_V3 = "0x" + keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
ZERO = "0x" + "0" * 40
log = logging.getLogger("supplier.ethereum")


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
        websocket_urls: list[str] | None = None,
        reconcile_seconds: float = 30,
        subscribe_pending: bool = False,
    ):
        self.rpc_urls = [rpc_url] if isinstance(rpc_url, str) else list(rpc_url)
        if not self.rpc_urls:
            raise ValueError("at least one Ethereum ingestion RPC is required")
        self.store, self.hub, self.feed_id = store, hub, feed_id
        self.poll_seconds, self.confirmations, self.backfill = poll_seconds, confirmations, backfill
        self.websocket_urls = list(websocket_urls or [])
        self.reconcile_seconds = reconcile_seconds
        self.subscribe_pending = subscribe_pending
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
        self.ws_connected = False
        self.ws_provider_index: int | None = None
        self.ws_reconnects = 0
        self.ws_last_message_at: str | None = None
        self.pending_subscription_active = False
        self._collect_lock = asyncio.Lock()
        self._head_reconcile_task: asyncio.Task | None = None
        self._provider_receipts: dict[tuple[str, str, int], str] = {}
        self._pool_tokens: OrderedDict[str, tuple[str, str] | None] = OrderedDict()
        self._latency_observation_to_publication_ms: deque[float] = deque(maxlen=4096)
        self.reorgs = 0

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
        if self.websocket_urls:
            await self._run_event_driven()
            return
        await self._run_polling()

    async def _run_polling(self):
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

    async def _run_event_driven(self):
        """Use subscriptions as the low-latency trigger and HTTP as the authority."""
        subscription = asyncio.create_task(self._subscription_loop(), name="ethereum-subscriptions")
        try:
            # Reconcile immediately, then periodically even while the socket is healthy.
            while True:
                try:
                    await self.collect_once()
                    self.running, self.error, self.error_category = True, None, None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.running, self.error = False, str(exc)
                    self.error_category = getattr(exc, "category", "COLLECTOR_STALLED")
                await asyncio.sleep(self.reconcile_seconds)
        finally:
            subscription.cancel()
            await asyncio.gather(subscription, return_exceptions=True)

    async def _subscription_loop(self):
        delay = 1.0
        while True:
            for index, url in enumerate(self.websocket_urls):
                try:
                    async with websockets.connect(
                        url, ping_interval=20, ping_timeout=20, max_queue=128, open_timeout=15
                    ) as ws:
                        self.ws_connected, self.ws_provider_index = True, index
                        self.ws_last_message_at, delay = utc_now(), 1.0
                        subscriptions: dict[str, str] = {}
                        requests = [
                            (1, "newHeads", ["newHeads"]),
                            (2, "logs", ["logs", {"topics": [[TRANSFER, SWAP_V2, SWAP_V3]]}]),
                        ]
                        if self.subscribe_pending:
                            requests.append((3, "pending", ["newPendingTransactions"]))
                        for request_id, name, params in requests:
                            request = {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "method": "eth_subscribe",
                                "params": params,
                            }
                            await ws.send(json.dumps(request))
                            response = json.loads(await asyncio.wait_for(ws.recv(), 15))
                            if response.get("error"):
                                if name == "pending":
                                    self.pending_subscription_active = False
                                    continue
                                raise CollectorRpcError("COLLECTOR_WS_SUBSCRIBE", f"{name} subscription rejected")
                            subscriptions[str(response.get("result"))] = name
                            if name == "pending":
                                self.pending_subscription_active = True
                        async for raw in ws:
                            self.ws_last_message_at = utc_now()
                            message = json.loads(raw)
                            params = message.get("params") or {}
                            name = subscriptions.get(str(params.get("subscription")))
                            result = params.get("result")
                            if name == "logs" and isinstance(result, dict):
                                self._remember_provider_receipt(result)
                            elif name == "newHeads" and isinstance(result, dict):
                                raw_number = result.get("number")
                                if raw_number:
                                    self.chain_head = quantity(raw_number)
                                # Coalesce bursts. HTTP reconciliation supplies canonical logs,
                                # fills gaps, applies confirmations, and detects reorgs.
                                if not self._collect_lock.locked() and (
                                    self._head_reconcile_task is None or self._head_reconcile_task.done()
                                ):
                                    self._head_reconcile_task = asyncio.create_task(
                                        self._reconcile_from_head(), name="ethereum-head-reconcile"
                                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.ws_connected = False
                    self.ws_reconnects += 1
                    log.warning("ethereum_ws_disconnected provider=%s error=%s", index, type(exc).__name__)
                    continue
            self.ws_connected = False
            await asyncio.sleep(delay + random.uniform(0, min(1.0, delay / 4)))
            delay = min(delay * 2, self.retry_max_seconds)

    async def _reconcile_from_head(self) -> None:
        """Reconcile a subscription head without leaking background task errors."""
        try:
            await self.collect_once()
            self.running, self.error, self.error_category = True, None, None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.running, self.error = False, str(exc)
            self.error_category = getattr(exc, "category", "COLLECTOR_STALLED")
            log.warning("ethereum_head_reconciliation_failed category=%s", self.error_category)

    def _remember_provider_receipt(self, item: dict) -> None:
        try:
            key = (item["blockHash"].lower(), item["transactionHash"].lower(), quantity(item["logIndex"]))
        except (KeyError, TypeError, ValueError):
            return
        self._provider_receipts[key] = utc_now()
        if len(self._provider_receipts) > 20_000:
            for old in list(self._provider_receipts)[:5_000]:
                self._provider_receipts.pop(old, None)

    async def collect_once(self):
        async with self._collect_lock:
            return await self._collect_once_locked()

    async def _collect_once_locked(self):
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
                self.reorgs += 1
                self._publish_control(
                    "REORG_CORRECTION",
                    {"rewind_from_block": max(0, checkpoint["block_number"] - self.backfill)},
                )
                await asyncio.to_thread(self.store.rewind, max(0, checkpoint["block_number"] - self.backfill))
                checkpoint = await asyncio.to_thread(self.store.checkpoint)
        start = checkpoint["block_number"] + 1 if checkpoint else max(0, tip - self.backfill + 1)
        inserted_count = 0
        for number in range(start, tip + 1):
            inserted_count += len(await self.collect_block(number))
        return inserted_count

    def _publish_control(self, kind: str, details: dict) -> None:
        for feed_id in WALLET_FEED_IDS:
            self.hub.publish(feed_id, {"_ume_control": kind, "feed_id": feed_id, **details, "timestamp": utc_now()})

    async def collect_block(self, number: int):
        observed = utc_now()
        block = await self.rpc("eth_getBlockByNumber", [hex(number), True])
        if not block:
            raise RuntimeError(f"Ethereum block {number} unavailable")
        query = {"fromBlock": hex(number), "toBlock": hex(number), "topics": [[TRANSFER, SWAP_V2, SWAP_V3]]}
        logs = await self.rpc("eth_getLogs", [query]) or []
        swap_pools = {
            item["address"].lower()
            for item in logs
            if item.get("topics") and item["topics"][0].lower() in {SWAP_V2, SWAP_V3}
        }
        await self._resolve_pool_tokens(swap_pools, number)
        timestamp = datetime.fromtimestamp(quantity(block["timestamp"]), UTC).isoformat()
        transaction_senders = {
            item["hash"].lower(): item["from"].lower()
            for item in block.get("transactions", [])
            if isinstance(item, dict) and item.get("hash") and item.get("from")
        }
        events = []
        for log in logs:
            key = (log["blockHash"].lower(), log["transactionHash"].lower(), quantity(log["logIndex"]))
            provider_received_at = self._provider_receipts.pop(key, None)
            tokens = self._pool_tokens.get(log["address"].lower())
            if tokens:
                log = log | {"_token0": tokens[0], "_token1": tokens[1]}
            sender = transaction_senders.get(log["transactionHash"].lower())
            if sender:
                log = log | {"_transaction_sender": sender}
            events.extend(self.normalize(log, timestamp, observed, provider_received_at))
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
            item["delivery_class"] = (
                "LIVE_PROVIDER_OBSERVED" if item.get("provider_received_at") else "RECONCILED_CONFIRMED"
            )
            self.hub.connected[feed_id] = True
            self.hub.publish(feed_id, item)
            published = datetime.fromisoformat(item.get("published_at", utc_now()))
            if item.get("provider_received_at"):
                provider = datetime.fromisoformat(item["provider_received_at"])
                self._latency_observation_to_publication_ms.append(
                    max(0, (published - provider).total_seconds() * 1000)
                )
        return inserted

    async def _resolve_pool_tokens(self, pools: set[str], block_number: int) -> None:
        """Resolve pool assets without making enrichment a correctness dependency."""
        semaphore = asyncio.Semaphore(8)

        async def resolve(pool: str) -> None:
            if pool in self._pool_tokens:
                self._pool_tokens.move_to_end(pool)
                return
            value: tuple[str, str] | None = None
            try:
                async with semaphore:
                    token0, token1 = await asyncio.gather(
                        self.rpc("eth_call", [{"to": pool, "data": "0x0dfe1681"}, hex(block_number)]),
                        self.rpc("eth_call", [{"to": pool, "data": "0xd21220a7"}, hex(block_number)]),
                    )
                if len(token0) >= 42 and len(token1) >= 42:
                    value = ("0x" + token0[-40:].lower(), "0x" + token1[-40:].lower())
            except Exception as exc:
                log.info("pool_token_metadata_unavailable pool=%s error=%s", pool, type(exc).__name__)
            self._pool_tokens[pool] = value
            if len(self._pool_tokens) > 10_000:
                self._pool_tokens.popitem(last=False)

        await asyncio.gather(*(resolve(pool) for pool in pools))

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

    def normalize(
        self, log: dict, source_timestamp: str, observed: str, provider_received_at: str | None = None
    ) -> list[dict]:
        topics = [item.lower() for item in log.get("topics", [])]
        head = topics[0] if topics else ""
        base = {
            "chain_id": 1,
            "block_number": quantity(log["blockNumber"]),
            "block_hash": log["blockHash"].lower(),
            "transaction_hash": log["transactionHash"].lower(),
            "log_index": quantity(log["logIndex"]),
            "source_timestamp": source_timestamp,
            "provider_received_at": provider_received_at,
            "supplier_observed_at": observed,
            "normalized_at": utc_now(),
            "published_at": utc_now(),
            "provenance": {
                "source": "ethereum_rpc",
                "observation": "canonical_log",
                "transport": "websocket_trigger_http_verified" if provider_received_at else "http_reconciliation",
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
                event(
                    "UNISWAP_V2_SWAP",
                    log.get("_transaction_sender"),
                    topic_address(topics[2]),
                    suffix="swap",
                )
                | {
                    "amount0_raw": str(data[2] - data[0]),
                    "amount1_raw": str(data[3] - data[1]),
                    "pool": log["address"].lower(),
                    "token0": log.get("_token0"),
                    "token1": log.get("_token1"),
                }
            ]
        if head == SWAP_V3 and len(topics) >= 3 and len(data) >= 2:
            return [
                event(
                    "UNISWAP_V3_SWAP",
                    log.get("_transaction_sender"),
                    topic_address(topics[2]),
                    suffix="swap",
                )
                | {
                    # V3 logs report pool deltas. Negating them makes both V2
                    # and V3 fields wallet-relative: negative spent, positive received.
                    "amount0_raw": str(-signed(data[0])),
                    "amount1_raw": str(-signed(data[1])),
                    "pool": log["address"].lower(),
                    "token0": log.get("_token0"),
                    "token1": log.get("_token1"),
                }
            ]
        return []

    def health(self) -> dict:
        checkpoint = self.store.checkpoint()
        indexed = checkpoint["block_number"] if checkpoint else self.last_successful_block
        latency = sorted(self._latency_observation_to_publication_ms)

        def percentile(p: float) -> float | None:
            if not latency:
                return None
            return latency[min(len(latency) - 1, int((len(latency) - 1) * p))]

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
            "ingestion_mode": "WEBSOCKET_WITH_HTTP_RECONCILIATION" if self.websocket_urls else "HTTP_POLLING",
            "websocket_connected": self.ws_connected,
            "websocket_provider": self.ws_provider_index,
            "websocket_reconnects": self.ws_reconnects,
            "websocket_last_message_at": self.ws_last_message_at,
            "pending_subscription_active": self.pending_subscription_active,
            "reorgs_detected": self.reorgs,
            "observation_to_publication_latency_ms": {
                "sample_size": len(latency),
                "p50": percentile(0.50),
                "p95": percentile(0.95),
                "p99": percentile(0.99),
                "measurement": "WEBSOCKET_PROVIDER_RECEIPT_TO_SUPPLIER_PUBLICATION_MEASURED",
            },
        }
