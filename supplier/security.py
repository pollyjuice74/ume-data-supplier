import hashlib
import hmac
import json
import secrets
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import is_address, keccak, to_checksum_address


def _b64(data: bytes) -> str:
    return urlsafe_b64encode(data).decode().rstrip("=")


class TokenIssuer:
    def __init__(self, secret: str):
        self.secret = secret.encode()
        self.used: set[str] = set()

    def issue(self, wallet: str, audience: str, lifetime: int = 60, claims: dict | None = None) -> str:
        payload = {
            "sub": wallet.lower(),
            "aud": audience,
            "exp": int(time.time()) + lifetime,
            "jti": secrets.token_urlsafe(16),
        }
        payload.update(claims or {})
        body = _b64(json.dumps(payload, separators=(",", ":")).encode())
        return f"{body}.{_b64(hmac.new(self.secret, body.encode(), hashlib.sha256).digest())}"

    def claim(self, token: str, audience: str, single_use: bool = False) -> dict:
        body, signature = token.split(".", 1)
        expected = _b64(hmac.new(self.secret, body.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            raise ValueError("invalid signature")
        payload = json.loads(urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        if payload["aud"] != audience or payload["exp"] <= time.time() or payload["jti"] in self.used:
            raise ValueError("expired, reused, or wrong audience")
        if single_use:
            self.used.add(payload["jti"])
        return payload


class WalletAuth:
    def __init__(self, issuer: TokenIssuer):
        self.issuer = issuer
        self.challenges: dict[str, tuple[str, float, dict]] = {}

    def challenge(self, wallet: str, feed_id: str, chain_id: int, contract: str, entitlement_expiry: int) -> str:
        if not is_address(wallet):
            raise ValueError("invalid wallet")
        nonce = secrets.token_urlsafe(24)
        context = {
            "feed_id": feed_id,
            "chain_id": chain_id,
            "contract": contract.lower(),
            "entitlement_expiry": entitlement_expiry,
        }
        message = "\n".join(
            [
                "UME supplier authentication",
                f"Wallet: {wallet.lower()}",
                f"Chain ID: {chain_id}",
                f"Contract: {contract.lower()}",
                f"Feed ID: {feed_id}",
                f"Entitlement expiry: {entitlement_expiry}",
                f"Nonce: {nonce}",
            ]
        )
        self.challenges[wallet.lower()] = (message, time.time() + 300, context)
        return message

    def verify(self, wallet: str, message: str, signature: str) -> str:
        entry = self.challenges.pop(wallet.lower(), None)
        if not entry or entry[1] <= time.time():
            raise ValueError("challenge missing or expired")
        if message != entry[0]:
            raise ValueError("challenge message mismatch")
        recovered = Account.recover_message(encode_defunct(text=entry[0]), signature=signature)
        if recovered.lower() != wallet.lower():
            raise ValueError("signature does not match wallet")
        return self.issuer.issue(wallet, "session", 3600, entry[2])


class Entitlements:
    def __init__(self, rpc_url: str, contract: str):
        self.rpc_url, self.contract = rpc_url, contract

    async def expires_at(self, wallet: str, feed_id: str) -> int:
        if not self.contract or not is_address(self.contract):
            return 0
        selector = keccak(text="expiresAt(address,bytes32)")[:4]
        address_word = bytes.fromhex(to_checksum_address(wallet)[2:]).rjust(32, b"\0")
        feed_word = keccak(text=feed_id)
        data = "0x" + (selector + address_word + feed_word).hex()
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                self.rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_call",
                    "params": [{"to": self.contract, "data": data}, "latest"],
                },
            )
            response.raise_for_status()
            result = response.json()
        if "error" in result:
            raise RuntimeError(result["error"]["message"])
        return int(result["result"], 16)

    async def active(self, wallet: str, feed_id: str) -> bool:
        expiry, chain_time = await self.status(wallet, feed_id)
        return expiry > chain_time

    async def chain_time(self) -> int:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                self.rpc_url,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": ["latest", False]},
            )
            response.raise_for_status()
            result = response.json()
        if "error" in result:
            raise RuntimeError(result["error"]["message"])
        return int(result["result"]["timestamp"], 16)

    async def status(self, wallet: str, feed_id: str) -> tuple[int, int]:
        return await self.expires_at(wallet, feed_id), await self.chain_time()
