import asyncio
import logging
import time
from contextlib import asynccontextmanager

from eth_account import Account
from eth_account.messages import encode_defunct
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .catalog import FEEDS
from .config import settings
from .hub import FeedHub
from .security import Entitlements, TokenIssuer, WalletAuth

cfg = settings()
logging.basicConfig(level=logging.INFO, format='{"level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}')
hub = FeedHub(cfg.queue_size)
issuer = TokenIssuer(cfg.session_secret)
wallet_auth = WalletAuth(issuer)
entitlements = Entitlements(cfg.rpc_url, cfg.feed_access_address)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if cfg.enable_live:
        await hub.start()
    yield
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
    feed_id: str


class VerifyIn(WalletIn):
    message: str
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


ownership_proof: dict | None = None


def current_owner_wallet() -> str:
    return ownership_proof["owner_wallet"] if ownership_proof else cfg.owner_wallet


def session(authorization: str = Header("")) -> dict:
    try:
        return issuer.claim(authorization.removeprefix("Bearer "), "session")
    except Exception as exc:
        raise HTTPException(401, "Valid wallet session required") from exc


@app.get("/health")
def health():
    return {"status": "ok", "live_enabled": cfg.enable_live, "feeds": {key: hub.health(key) for key in FEEDS}}


@app.get("/ownership")
def ownership():
    if ownership_proof:
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
    return ownership_proof


@app.get("/catalog/listings")
def catalog():
    return [
        feed.public(cfg.public_url, current_owner_wallet()) | {"health": hub.health(feed.id)} for feed in FEEDS.values()
    ]


@app.get("/feeds/{feed_id}")
def feed_detail(feed_id: str):
    feed = FEEDS.get(feed_id)
    if not feed:
        raise HTTPException(404, "Unknown feed")
    return feed.public(cfg.public_url, current_owner_wallet()) | {
        "source": {"name": feed.source, "symbol": feed.symbol},
        "coverage": {"mode": "live-only", "historical_storage": False, **hub.health(feed_id)},
        "disclaimer": "Test-only feed; commercial redistribution rights are not verified.",
    }


@app.get("/feeds/{feed_id}/sample")
def feed_sample(feed_id: str):
    if feed_id not in FEEDS:
        raise HTTPException(404, "Unknown feed")
    event = hub.latest.get(feed_id)
    if not event:
        raise HTTPException(503, "No live sample is available yet")
    return {"verification_basis": "LIVE_BOUNDED_SAMPLE", "event": event}


@app.post("/auth/challenge")
async def challenge(body: WalletIn):
    if body.feed_id not in FEEDS:
        raise HTTPException(404, "Unknown feed")
    try:
        expiry = await entitlements.expires_at(body.wallet, body.feed_id)
        return {
            "message": wallet_auth.challenge(body.wallet, body.feed_id, cfg.chain_id, cfg.feed_access_address, expiry)
        }
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/auth/verify")
def verify(body: VerifyIn):
    try:
        return {
            "token": wallet_auth.verify(body.wallet, body.message, body.signature),
            "wallet": body.wallet.lower(),
        }
    except ValueError as exc:
        raise HTTPException(401, str(exc)) from exc


@app.post("/feeds/{feed_id}/stream-ticket")
async def stream_ticket(feed_id: str, auth: dict = Depends(session)):
    if feed_id not in FEEDS:
        raise HTTPException(404, "Unknown feed")
    if (
        auth.get("feed_id") != feed_id
        or auth.get("chain_id") != cfg.chain_id
        or auth.get("contract") != cfg.feed_access_address.lower()
    ):
        raise HTTPException(403, "Wallet session is not bound to this feed entitlement")
    expiry, chain_time = await entitlements.status(auth["sub"], feed_id)
    if expiry <= chain_time:
        raise HTTPException(403, "Active on-chain entitlement required")
    lifetime = min(60, max(1, expiry - chain_time))
    return {"ticket": issuer.issue(auth["sub"], f"stream:{feed_id}", lifetime), "expires_at": expiry}


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
    next_entitlement_check = time.monotonic() + 5
    try:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
                if time.monotonic() >= next_entitlement_check:
                    if not await entitlements.active(claim["sub"], feed_id):
                        await websocket.close(code=4403, reason="entitlement expired")
                        return
                    next_entitlement_check = time.monotonic() + 5
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
