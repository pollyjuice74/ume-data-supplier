import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime

from eth_account import Account
from eth_account.messages import encode_defunct
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from .catalog import FEEDS
from .config import settings
from .hub import FeedHub
from .live_store import LiveWalletStore
from .security import Entitlements, TokenIssuer, WalletAuth
from .wallet_collector import EthereumWalletCollector
from .wallet_products import WALLET_FEED_IDS
from .wallet_products import rows as product_rows
from .wallet_store import WalletStore, utc_now

cfg = settings()
logging.basicConfig(level=logging.INFO, format='{"level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}')
hub = FeedHub(cfg.queue_size, cfg.rate_window_max_events, cfg.enable_exchange_feeds)
issuer = TokenIssuer(cfg.session_secret)
wallet_auth = WalletAuth(issuer)
entitlements = Entitlements(cfg.rpc_url, cfg.feed_access_address)
wallet_store = (
    LiveWalletStore(cfg.live_window_events)
    if cfg.storage_mode.lower() == "memory"
    else WalletStore(cfg.database_path)
)
wallet_collector = EthereumWalletCollector(
    cfg.ethereum_rpc_urls,
    wallet_store,
    hub,
    "ethereum_wallet_trades_v1",
    cfg.ethereum_poll_seconds,
    cfg.ethereum_confirmations,
    cfg.ethereum_backfill_blocks,
    websocket_urls=cfg.ethereum_ws_urls,
    reconcile_seconds=cfg.ethereum_reconcile_seconds,
    subscribe_pending=cfg.ethereum_pending_transactions,
)
wallet_collector.retention_days = cfg.retention_days
wallet_collector.max_events = cfg.max_events
wallet_task: asyncio.Task | None = None
retention_task: asyncio.Task | None = None


async def retention_loop():
    while True:
        await asyncio.to_thread(wallet_store.prune, cfg.retention_days, cfg.max_events)
        await asyncio.sleep(3600)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global wallet_task, retention_task
    if cfg.enable_live:
        await hub.start()
        wallet_task = asyncio.create_task(wallet_collector.run(), name="ethereum-wallet-collector")
        if not getattr(wallet_store, "ephemeral", False):
            retention_task = asyncio.create_task(retention_loop(), name="wallet-retention")
    yield
    if wallet_task:
        wallet_task.cancel()
        await asyncio.gather(wallet_task, return_exceptions=True)
    if retention_task:
        retention_task.cancel()
        await asyncio.gather(retention_task, return_exceptions=True)
    await wallet_collector.close()
    await hub.stop()


app = FastAPI(title="UME Independent Data Supplier", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.allowed_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)


class WalletIn(BaseModel):
    wallet: str


class VerifyIn(WalletIn):
    signature: str


class OwnershipIn(BaseModel):
    owner_wallet: str
    supplier_url: str
    message: str
    signature: str


def ownership_message() -> str:
    return "\n".join(
        [
            "UME supplier origin ownership",
            "Version: 1",
            f"Supplier: {cfg.public_url.rstrip('/')}",
            f"Owner: {cfg.owner_wallet.lower()}",
        ]
    )


ownership_proof: dict | None = wallet_store.setting("ownership_proof")


def current_owner_wallet() -> str:
    return ownership_proof["owner_wallet"] if ownership_proof else cfg.owner_wallet


def session(authorization: str = Header("")) -> dict:
    try:
        return issuer.claim(authorization.removeprefix("Bearer "), "session")
    except Exception as exc:
        raise HTTPException(401, "Valid wallet session required") from exc


@app.get("/health")
def health():
    feed_health = {key: hub.health(key) for key in WALLET_FEED_IDS}
    coverage = wallet_store.coverage()
    wallet = feed_health["ethereum_wallet_trades_v1"] | wallet_collector.health() | coverage
    latest = wallet_store.latest(1)
    last = wallet.get("last_event_at") or (latest[-1]["supplier_observed_at"] if latest else None)
    age = time.time() - datetime.fromisoformat(last).timestamp() if last else None
    if latest:
        item = latest[-1]
        source = datetime.fromisoformat(item["source_timestamp"])
        observed = datetime.fromisoformat(item["supplier_observed_at"])
        wallet["source_to_supplier_delay_ms"] = (observed - source).total_seconds() * 1000
        wallet["source_to_supplier_measurement"] = "BLOCK_TIMESTAMP_TO_RPC_OBSERVATION"
    else:
        wallet["source_to_supplier_delay_ms"] = None
        wallet["source_to_supplier_measurement"] = "UNAVAILABLE_NO_EVENT"
    freshness_limit = max(cfg.stale_after_seconds, cfg.ethereum_poll_seconds * 2 + cfg.ethereum_confirmations * 15)
    wallet["status"] = (
        "ONLINE"
        if wallet["collector_running"] and age is not None and age <= freshness_limit
        else ("DELAYED" if wallet["collector_running"] else "OFFLINE")
    )
    wallet["rpc_connectivity"] = "OK" if wallet["collector_running"] else "FAILED"
    wallet["data_freshness"] = "FRESH" if wallet["status"] == "ONLINE" else "STALE"
    wallet["delivery_health"] = "READY" if wallet["status"] == "ONLINE" else "BLOCKED_STALE_SOURCE"
    wallet["states"] = {
        "INGESTING": wallet["collector_running"],
        "SUPPLIER_HEALTHY": True,
        "DELIVERABLE": wallet["status"] == "ONLINE",
    }
    wallet["freshness_threshold_seconds"] = freshness_limit
    for feed_id in WALLET_FEED_IDS:
        feed_health[feed_id] = (
            feed_health[feed_id]
            | wallet_collector.health()
            | coverage
            | {
                "status": wallet["status"],
                "last_event_at": feed_health[feed_id].get("last_event_at") or last,
                "freshness_threshold_seconds": freshness_limit,
                "source_to_supplier_delay_ms": wallet["source_to_supplier_delay_ms"],
                "source_to_supplier_measurement": wallet["source_to_supplier_measurement"],
                "states": wallet["states"],
            }
        )
    return {
        "status": "ok" if wallet["status"] != "OFFLINE" else "degraded",
        "live_enabled": cfg.enable_live,
        "chain_id": cfg.chain_id,
        "feed_access_address": cfg.feed_access_address.lower(),
        "public_url": cfg.public_url.rstrip("/"),
        "owner_wallet": current_owner_wallet().lower(),
        "feeds": feed_health,
    }


@app.get("/ownership")
def ownership():
    if (
        ownership_proof
        and ownership_proof.get("supplier_url") == cfg.public_url.rstrip("/")
    ):
        return ownership_proof
    if not cfg.seller_private_key:
        raise HTTPException(503, "Pair this supplier with the connected seller wallet first")
    account = Account.from_key(cfg.seller_private_key)
    if account.address.lower() != cfg.owner_wallet.lower():
        raise HTTPException(503, "Supplier owner wallet does not match the signing key")
    message = ownership_message()
    signature = Account.sign_message(encode_defunct(text=message), cfg.seller_private_key).signature.hex()
    return {
        "owner_wallet": account.address.lower(),
        "supplier_url": cfg.public_url.rstrip("/"),
        "message": message,
        "signature": "0x" + signature.removeprefix("0x"),
    }


@app.post("/ownership")
def pair_ownership(body: OwnershipIn):
    global ownership_proof
    expected_url = cfg.public_url.rstrip("/")
    expected = "\n".join(
        [
            "UME supplier origin ownership",
            "Version: 1",
            f"Supplier: {expected_url}",
            f"Owner: {body.owner_wallet.lower()}",
        ]
    )
    if body.supplier_url != expected_url or body.message != expected:
        raise HTTPException(422, "Ownership proof must bind this exact supplier origin")
    try:
        recovered = Account.recover_message(encode_defunct(text=body.message), signature=body.signature)
    except Exception as exc:
        raise HTTPException(401, "Ownership signature is invalid") from exc
    if recovered.lower() != body.owner_wallet.lower():
        raise HTTPException(401, "Ownership signature does not match the seller wallet")
    ownership_proof = body.model_dump() | {"owner_wallet": body.owner_wallet.lower()}
    wallet_store.set_setting("ownership_proof", ownership_proof)
    return ownership_proof


@app.get("/catalog/listings")
def catalog():
    return [
        FEEDS[feed_id].public(cfg.public_url, current_owner_wallet()) | {"health": health()["feeds"][feed_id]}
        for feed_id in WALLET_FEED_IDS
    ]


@app.get("/feeds/{feed_id}")
def feed_detail(feed_id: str):
    feed = FEEDS.get(feed_id)
    if not feed:
        raise HTTPException(404, "Unknown feed")
    coverage = (
        wallet_store.coverage()
        if feed_id in WALLET_FEED_IDS
        else {"mode": "live-only", "historical_storage": False, **hub.health(feed_id)}
    )
    return feed.public(cfg.public_url, current_owner_wallet()) | {
        "source": {"name": feed.source, "symbol": feed.symbol},
        "coverage": coverage,
        "available_formats": (
            ["SSE"] if getattr(wallet_store, "ephemeral", False) else ["SSE", "JSON", "CSV", "Parquet"]
        )
        if feed_id in WALLET_FEED_IDS else ["WebSocket"],
        "disclaimer": "On-chain observations only. Test-USDC marketplace terms are not real revenue.",
    }


@app.get("/feeds/{feed_id}/sample")
def feed_sample(feed_id: str):
    if feed_id not in FEEDS:
        raise HTTPException(404, "Unknown feed")
    projected = (
        product_rows(feed_id, wallet_store.latest(500, public=False), wallet_store)
        if feed_id in WALLET_FEED_IDS
        else []
    )
    event = hub.latest.get(feed_id) or (projected[-1] if projected else None)
    if not event:
        raise HTTPException(503, "No live sample is available yet")
    feed_health = health()["feeds"].get(feed_id, {})
    basis = "LIVE_BOUNDED_SAMPLE" if feed_health.get("status") == "ONLINE" else "STORED_STALE_SAMPLE"
    return {"verification_basis": basis, "freshness": feed_health.get("data_freshness"), "event": event}


@app.post("/auth/challenge")
def challenge(body: WalletIn):
    try:
        return {"message": wallet_auth.challenge(body.wallet)}
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/auth/verify")
def verify(body: VerifyIn):
    try:
        return {"token": wallet_auth.verify(body.wallet, body.signature), "wallet": body.wallet.lower()}
    except ValueError as exc:
        raise HTTPException(401, str(exc)) from exc


@app.post("/feeds/{feed_id}/stream-ticket")
async def stream_ticket(feed_id: str, auth: dict = Depends(session)):
    if feed_id not in FEEDS:
        raise HTTPException(404, "Unknown feed")
    expiry = await entitlements.expires_at(auth["sub"], feed_id)
    if expiry <= time.time():
        raise HTTPException(403, "Active on-chain entitlement required")
    lifetime = min(60, max(1, expiry - int(time.time())))
    return {"ticket": issuer.issue(auth["sub"], f"stream:{feed_id}", lifetime), "expires_at": expiry}


@app.get("/feeds/{feed_id}/schema")
def feed_schema(feed_id: str):
    feed = FEEDS.get(feed_id)
    if not feed:
        raise HTTPException(404, "Unknown feed")
    return feed.schema


@app.get("/feeds/{feed_id}/sse")
async def sse(request: Request, feed_id: str, ticket: str = Query(...), cursor: str | None = Query(None)):
    if feed_id not in FEEDS:
        raise HTTPException(404, "Unknown feed")
    try:
        claim = issuer.claim(ticket, f"stream:{feed_id}", single_use=True)
        if not await entitlements.active(claim["sub"], feed_id):
            raise ValueError("inactive")
    except Exception as exc:
        raise HTTPException(403, "Active on-chain entitlement and valid ticket required") from exc

    async def events():
        queue = hub.subscribe(feed_id)
        try:
            requested_cursor = cursor or request.headers.get("last-event-id")
            if requested_cursor and feed_id in WALLET_FEED_IDS:
                try:
                    raw_replay = wallet_store.events_after(requested_cursor, cfg.export_max_rows)
                except KeyError:
                    yield 'event: cursor_expired\ndata: {"code":"CURSOR_EXPIRED"}\n\n'
                    return
                replay = product_rows(feed_id, raw_replay, wallet_store)
            else:
                replay = (
                    product_rows(feed_id, wallet_store.latest(500), wallet_store)
                    if feed_id in WALLET_FEED_IDS
                    else []
                )
                replay = replay[-25:]
            delivered = {requested_cursor} if requested_cursor else set()
            for item in replay:
                if item["event_id"] in delivered:
                    continue
                delivered.add(item["event_id"])
                delivered_item = item | {
                    "supplier_delivery_at": utc_now(),
                    "delivery_class": "REPLAY_CURRENT_PROCESS_WINDOW",
                }
                yield f"id: {item['event_id']}\nevent: wallet_event\ndata: {hub_json(delivered_item)}\n\n"
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), 15)
                    control = item.get("_ume_control")
                    if control == "DELIVERY_GAP":
                        yield 'event: delivery_gap\ndata: {"code":"DELIVERY_GAP","reconnect_required":true}\n\n'
                        return
                    if control:
                        yield f"event: {control.lower()}\ndata: {hub_json(item)}\n\n"
                        continue
                    if item["event_id"] in delivered:
                        continue
                    delivered.add(item["event_id"])
                    if len(delivered) > cfg.live_window_events:
                        delivered = {item["event_id"]}
                    delivered_item = item | {"supplier_delivery_at": utc_now()}
                    yield f"id: {item['event_id']}\nevent: wallet_event\ndata: {hub_json(delivered_item)}\n\n"
                except TimeoutError:
                    if not await entitlements.active(claim["sub"], feed_id):
                        return
                    yield f'event: heartbeat\ndata: {{"timestamp":"{utc_now()}"}}\n\n'
        finally:
            hub.unsubscribe(feed_id, queue)

    return StreamingResponse(
        events(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"}
    )


def hub_json(value: dict) -> str:
    import json

    return json.dumps(value, separators=(",", ":"))


@app.get("/feeds/{feed_id}/history.{fmt}")
async def history(feed_id: str, fmt: str, auth: dict = Depends(session)):
    if feed_id not in WALLET_FEED_IDS or fmt not in {"json", "csv", "parquet"}:
        raise HTTPException(404, "Unknown export")
    if not await entitlements.active(auth["sub"], feed_id):
        raise HTTPException(403, "Active on-chain entitlement required")
    if getattr(wallet_store, "ephemeral", False):
        raise HTTPException(410, "Historical exports are unavailable: this is a live-only feed")
    media = {"json": "application/json", "csv": "text/csv", "parquet": "application/vnd.apache.parquet"}[fmt]
    return Response(
        wallet_store.export_rows(product_rows(feed_id, wallet_store.latest(cfg.export_max_rows), wallet_store), fmt),
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{feed_id}.{fmt}"'},
    )


class ReceiptIn(BaseModel):
    event_id: str
    buyer_receipt_at: str


@app.post("/feeds/{feed_id}/delivery-ack")
async def delivery_ack(feed_id: str, body: ReceiptIn, auth: dict = Depends(session)):
    if not await entitlements.active(auth["sub"], feed_id):
        raise HTTPException(403, "Active on-chain entitlement required")
    try:
        return wallet_store.receipt(body.event_id, auth["sub"], body.buyer_receipt_at)
    except KeyError as exc:
        raise HTTPException(404, "Unknown event") from exc


@app.get("/payments/status")
async def payments_status():
    try:
        status = await entitlements.contract_status()
        status["expected_protocol_fee_bps"] = cfg.expected_protocol_fee_bps
        status["fee_matches_expected"] = status.get("protocol_fee_bps") == cfg.expected_protocol_fee_bps
        status["revenue_classification"] = (
            "TEST_ONLY" if cfg.chain_id in {31337, 84532, 11155111} else "UNVERIFIED_PRODUCTION"
        )
        return status
    except Exception as exc:
        return {"configured": False, "reason": str(exc), "expected_protocol_fee_bps": cfg.expected_protocol_fee_bps}


@app.websocket("/feeds/{feed_id}/stream")
async def stream(websocket: WebSocket, feed_id: str, ticket: str):
    if feed_id not in FEEDS:
        await websocket.close(code=4404)
        return
    try:
        claim = issuer.claim(ticket, f"stream:{feed_id}", single_use=True)
        if not await entitlements.active(claim["sub"], feed_id):
            raise ValueError("entitlement inactive")
    except Exception:
        await websocket.close(code=4403)
        return
    await websocket.accept()
    queue = hub.subscribe(feed_id)
    try:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
                await websocket.send_json(event)
            except TimeoutError:
                if not await entitlements.active(claim["sub"], feed_id):
                    await websocket.close(code=4403, reason="entitlement expired")
                    return
                await websocket.send_json({"type": "heartbeat"})
    except WebSocketDisconnect:
        pass
    finally:
        hub.unsubscribe(feed_id, queue)
