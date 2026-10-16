import asyncio
import json
import logging
import random
from collections import defaultdict
from datetime import UTC, datetime

import websockets

from .catalog import FEEDS, Feed
from .normalize import normalize

log = logging.getLogger("supplier.hub")


class FeedHub:
    def __init__(self, queue_size: int):
        self.queue_size = queue_size
        self.subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self.tasks: dict[str, asyncio.Task] = {}
        self.last_event: dict[str, datetime] = {}
        self.connected: dict[str, bool] = defaultdict(bool)
        self.reconnects: dict[str, int] = defaultdict(int)

    async def start(self) -> None:
        self.tasks = {key: asyncio.create_task(self._run(feed), name=f"upstream:{key}") for key, feed in FEEDS.items()}

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
        for queue in tuple(self.subscribers[feed_id]):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)

    def health(self, feed_id: str) -> dict:
        last = self.last_event.get(feed_id)
        return {
            "connected": self.connected[feed_id],
            "subscribers": len(self.subscribers[feed_id]),
            "last_event_at": last.isoformat() if last else None,
            "reconnects": self.reconnects[feed_id],
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
