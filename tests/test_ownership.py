from eth_account import Account
from eth_account.messages import encode_defunct

from supplier import app as supplier_app


def test_ownership_binds_exact_public_origin(monkeypatch):
    account = Account.create()
    monkeypatch.setattr(supplier_app.cfg, "seller_private_key", account.key.hex())
    monkeypatch.setattr(supplier_app.cfg, "owner_wallet", account.address)
    monkeypatch.setattr(supplier_app.cfg, "public_url", "https://exact-name.trycloudflare.com")
    result = supplier_app.ownership()
    assert result["supplier_url"] == "https://exact-name.trycloudflare.com"
    assert Account.recover_message(encode_defunct(text=result["message"]), signature=result["signature"]).lower() == account.address.lower()


def test_browser_wallet_can_pair_exact_origin(monkeypatch):
    account = Account.create()
    origin = "https://paired.trycloudflare.com"
    monkeypatch.setattr(supplier_app.cfg, "public_url", origin)
    monkeypatch.setattr(supplier_app, "ownership_proof", None)
    message = "\n".join(["UME supplier origin ownership", "Version: 1", f"Supplier: {origin}", f"Owner: {account.address.lower()}"])
    signature = Account.sign_message(encode_defunct(text=message), account.key).signature.hex()
    result = supplier_app.pair_ownership(supplier_app.OwnershipIn(owner_wallet=account.address, supplier_url=origin, message=message, signature=signature))
    assert supplier_app.ownership() == result
