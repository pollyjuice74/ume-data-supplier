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
        self.used: dict[str, int] = {}
        self.max_used = 100_000

    def issue(self, wallet: str, audience: str, lifetime: int = 60) -> str:
        self._prune_used()
        if len(self.used) >= self.max_used:
            raise ValueError("single-use token capacity temporarily exhausted")
        payload = {
            "sub": wallet.lower(),
            "aud": audience,
            "exp": int(time.time()) + lifetime,
            "jti": secrets.token_urlsafe(16),
        }
        body = _b64(json.dumps(payload, separators=(",", ":")).encode())
        return f"{body}.{_b64(hmac.new(self.secret, body.encode(), hashlib.sha256).digest())}"

    def claim(self, token: str, audience: str, single_use: bool = False) -> dict:
        body, signature = token.split(".", 1)
        expected = _b64(hmac.new(self.secret, body.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            raise ValueError("invalid signature")
        payload = json.loads(urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        self._prune_used()
        if payload["aud"] != audience or payload["exp"] <= time.time() or payload["jti"] in self.used:
            raise ValueError("expired, reused, or wrong audience")
        if single_use:
            self.used[payload["jti"]] = payload["exp"]
        return payload

    def _prune_used(self) -> None:
        timestamp = int(time.time())
        self.used = {jti: expiry for jti, expiry in self.used.items() if expiry > timestamp}


class WalletAuth:
    def __init__(self, issuer: TokenIssuer):
        self.issuer = issuer
        self.challenges: dict[str, tuple[str, float]] = {}
        self.max_challenges = 4096

    def challenge(self, wallet: str) -> str:
        if not is_address(wallet):
            raise ValueError("invalid wallet")
        timestamp = time.time()
        self.challenges = {key: value for key, value in self.challenges.items() if value[1] > timestamp}
        if wallet.lower() not in self.challenges and len(self.challenges) >= self.max_challenges:
            raise ValueError("authentication challenge capacity temporarily exhausted")
        nonce = secrets.token_urlsafe(24)
        message = f"UME supplier authentication\nWallet: {wallet.lower()}\nNonce: {nonce}"
        self.challenges[wallet.lower()] = (message, time.time() + 300)
        return message

    def verify(self, wallet: str, signature: str) -> str:
        entry = self.challenges.pop(wallet.lower(), None)
        if not entry or entry[1] <= time.time():
            raise ValueError("challenge missing or expired")
        recovered = Account.recover_message(encode_defunct(text=entry[0]), signature=signature)
        if recovered.lower() != wallet.lower():
            raise ValueError("signature does not match wallet")
        return self.issuer.issue(wallet, "session", 3600)


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
        return await self.expires_at(wallet, feed_id) > time.time()

    async def contract_status(self) -> dict:
        if not self.contract or not is_address(self.contract):
            return {"configured": False, "reason": "SUPPLIER_FEED_ACCESS_ADDRESS is not configured"}

        async def read(signature: str) -> str:
            data = "0x" + keccak(text=signature)[:4].hex()
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
                body = response.json()
            if body.get("error"):
                raise RuntimeError(body["error"]["message"])
            return body["result"]

        treasury, fee, token = await read("treasury()"), await read("protocolFeeBps()"), await read("paymentToken()")
        return {
            "configured": True,
            "feed_access": self.contract.lower(),
            "payment_token": "0x" + token[-40:],
            "protocol_fee_wallet": "0x" + treasury[-40:],
            "protocol_fee_bps": int(fee, 16),
        }
