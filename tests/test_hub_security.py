import pytest

from supplier.hub import FeedHub
from supplier.security import TokenIssuer


def test_bounded_queue_drops_oldest():
    hub = FeedHub(2)
    queue = hub.subscribe("feed")
    hub.publish("feed", {"n": 1})
    hub.publish("feed", {"n": 2})
    hub.publish("feed", {"n": 3})
    assert queue.qsize() == 2
    assert queue.get_nowait() == {"n": 2}
    assert hub.dropped["feed"] == 1


def test_rate_window_and_exchange_collectors_are_bounded_and_opt_in():
    hub = FeedHub(2, rate_window_max_events=60)
    for n in range(1_000):
        hub.publish("feed", {"n": n})
    assert len(hub.event_times["feed"]) == 60


@pytest.mark.asyncio
async def test_exchange_collectors_disabled_by_default():
    hub = FeedHub(2)
    await hub.start()
    assert hub.tasks == {}


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
