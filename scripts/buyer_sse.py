#!/usr/bin/env python3
"""Consume an entitled SSE feed and optionally send measured buyer acknowledgements."""

import argparse
import asyncio
from datetime import UTC, datetime

import httpx


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--ticket", required=True)
    parser.add_argument("--session")
    parser.add_argument("--feed-id", default="ethereum-wallet-trading-v1")
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()
    endpoint = f"{args.url.rstrip('/')}/feeds/{args.feed_id}/sse?ticket={args.ticket}"
    received = 0
    async with httpx.AsyncClient(timeout=None) as client, client.stream("GET", endpoint) as response:
        response.raise_for_status()
        event_id = None
        async for line in response.aiter_lines():
            if line.startswith("id: "):
                event_id = line[4:]
            if line.startswith("data: ") and event_id:
                receipt_at = datetime.now(UTC).isoformat()
                print(event_id, receipt_at, line[6:])
                if args.session:
                    ack = await client.post(
                        f"{args.url.rstrip('/')}/feeds/{args.feed_id}/delivery-ack",
                        headers={"Authorization": f"Bearer {args.session}"},
                        json={"event_id": event_id, "buyer_receipt_at": receipt_at},
                    )
                    print("ack", ack.status_code, ack.text)
                received += 1
                event_id = None
                if received >= args.count:
                    return


if __name__ == "__main__":
    asyncio.run(main())
