import asyncio
import json
import logging
import random
import threading
from collections import defaultdict, deque
from datetime import UTC, datetime

import websockets

from .catalog import FEEDS, Feed
from .normalize import normalize

log = logging.getLogger("supplier.hub")


class FeedHub:
    def __init__(self, queue_size: int, rate_window_max_events: int = 4096, enable_exchange_feeds: bool = False):
        self.queue_size = queue_size
        self.rate_window_max_events = rate_window_max_events
        self.enable_exchange_feeds = enable_exchange_feeds
        self.rate_lock = threading.Lock()
        self.subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self.tasks: dict[str, asyncio.Task] = {}
        self.last_event: dict[str, datetime] = {}
        self.latest: dict[str, dict] = {}
        self.connected: dict[str, bool] = defaultdict(bool)
        self.reconnects: dict[str, int] = defaultdict(int)
        self.dropped: dict[str, int] = defaultdict(int)
        self.event_times: dict[str, deque] = defaultdict(lambda: deque(maxlen=self.rate_window_max_events))

    async def start(self) -> None:
        if not self.enable_exchange_feeds:
            self.tasks = {}
            return
        self.tasks = {
            key: asyncio.create_task(self._run(feed), name=f"upstream:{key}")
            for key, feed in FEEDS.items()
            if feed.upstream_url
        }

    async def stop(self) -> None:
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)

    def subscribe(self, feed_id: str) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=self.queue_size)
        self.subscribers[feed_id].add(queue)
        return queue

    def unsubscribe(self, feed_id: str, queue: asyncio.Queue) -> None:
        self.subscribers[feed_id].discard(queue)

    def publish(self, feed_id: str, event: dict) -> None:
        self.last_event[feed_id] = datetime.now(UTC)
        self.latest[feed_id] = event
        now = datetime.now(UTC).timestamp()
        with self.rate_lock:
            times = self.event_times[feed_id]
            times.append(now)
            cutoff = now - 60
            while times and times[0] < cutoff:
                times.popleft()
        for queue in tuple(self.subscribers[feed_id]):
            if queue.full():
                while not queue.empty():
                    queue.get_nowait()
                self.dropped[feed_id] += 1
                queue.put_nowait({"_ume_control": "DELIVERY_GAP", "feed_id": feed_id})
                if queue.maxsize > 1:
                    queue.put_nowait(event)
            else:
                queue.put_nowait(event)

    def health(self, feed_id: str) -> dict:
        last = self.last_event.get(feed_id)
        cutoff = datetime.now(UTC).timestamp() - 60
        with self.rate_lock:
            times = self.event_times[feed_id]
            while times and times[0] < cutoff:
                times.popleft()
            rate = len(times) / 60
        return {
            "connected": self.connected[feed_id],
            "subscribers": len(self.subscribers[feed_id]),
            "last_event_at": last.isoformat() if last else None,
            "reconnects": self.reconnects[feed_id],
            "dropped_events": self.dropped[feed_id],
            "events_per_second_60s": rate,
        }

    async def _run(self, feed: Feed) -> None:
        delay = 1.0
        while True:
            try:
                async with websockets.connect(feed.upstream_url, ping_interval=20, ping_timeout=20, max_queue=64) as ws:
                    if feed.subscribe:
                        await ws.send(json.dumps(feed.subscribe))
                    self.connected[feed.id] = True
                    delay = 1.0
                    async for raw in ws:
                        for event in normalize(feed, json.loads(raw)):
                            self.publish(feed.id, event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected[feed.id] = False
                self.reconnects[feed.id] += 1
                log.warning(
                    "upstream_disconnected", extra={"feed_id": feed.id, "error": str(exc), "retry_seconds": delay}
                )
                await asyncio.sleep(delay + random.random())
                delay = min(delay * 2, 30)
