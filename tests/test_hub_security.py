import pytest
from eth_account import Account
from eth_account.messages import encode_defunct

from supplier.hub import FeedHub
from supplier.security import TokenIssuer, WalletAuth


def test_bounded_queue_drops_oldest():
    hub = FeedHub(2)
    queue = hub.subscribe("feed")
    hub.publish("feed", {"n": 1})
    hub.publish("feed", {"n": 2})
    hub.publish("feed", {"n": 3})
    assert queue.qsize() == 2
    assert queue.get_nowait() == {"n": 2}


def test_ticket_is_audience_bound_and_single_use():
    issuer = TokenIssuer("x" * 32)
    token = issuer.issue("0xabc", "stream:a")
    assert issuer.claim(token, "stream:a", single_use=True)["sub"] == "0xabc"
    with pytest.raises(ValueError):
        issuer.claim(token, "stream:a", single_use=True)
    with pytest.raises(ValueError):
        issuer.claim(issuer.issue("0xabc", "stream:a"), "stream:b")


def test_subscribers_share_hub():
    hub = FeedHub(2)
    first, second = hub.subscribe("feed"), hub.subscribe("feed")
    hub.publish("feed", {"n": 1})
    assert first.get_nowait() == second.get_nowait() == {"n": 1}


def test_wallet_session_binds_chain_contract_feed_expiry_and_nonce():
    account = Account.create()
    issuer = TokenIssuer("x" * 32)
    auth = WalletAuth(issuer)
    message = auth.challenge(account.address, "feed-a", 84532, "0x" + "1" * 40, 12345)
    signature = Account.sign_message(encode_defunct(text=message), account.key).signature.hex()
    token = auth.verify(account.address, message, signature)
    claims = issuer.claim(token, "session")
    assert claims["feed_id"] == "feed-a"
    assert claims["chain_id"] == 84532
    assert claims["contract"] == "0x" + "1" * 40
    assert claims["entitlement_expiry"] == 12345
    with pytest.raises(ValueError):
        auth.verify(account.address, message, signature)
