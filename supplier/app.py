import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
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


class WalletIn(BaseModel):
    wallet: str


class VerifyIn(WalletIn):
    signature: str


def session(authorization: str = Header("")) -> dict:
    try:
        return issuer.claim(authorization.removeprefix("Bearer "), "session")
    except Exception as exc:
        raise HTTPException(401, "Valid wallet session required") from exc


@app.get("/health")
def health():
    return {"status": "ok", "live_enabled": cfg.enable_live, "feeds": {key: hub.health(key) for key in FEEDS}}


@app.get("/catalog/listings")
def catalog():
    return [feed.public(cfg.public_url) | {"health": hub.health(feed.id)} for feed in FEEDS.values()]


@app.post("/auth/challenge")
def challenge(body: WalletIn):
    try:
        return {"message": wallet_auth.challenge(body.wallet)}
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/auth/verify")
def verify(body: VerifyIn):
    try:
        return {"token": wallet_auth.verify(body.wallet, body.signature)}
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
