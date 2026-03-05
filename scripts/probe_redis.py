#!/usr/bin/env python3
"""Probe Redis to print sample tick data from each discovered stream."""

import asyncio
import json
import os

from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()


async def probe_streams():
    """Discover streams and print one sample entry from each."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_password = os.getenv("REDIS_PASSWORD", "")
    pattern = os.getenv("REDIS_STREAM_PATTERN", "tick_stream:*")
    
    print(f"Connecting to Redis: {redis_url}")
    redis = Redis.from_url(
        redis_url,
        decode_responses=True,
        password=redis_password or None,
    )
    
    try:
        await redis.ping()
        print("Connected!\n")
        
        # Discover streams
        cursor = 0
        streams = []
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=1000)
            streams.extend(keys)
            if cursor == 0:
                break
        
        print(f"Found {len(streams)} streams matching '{pattern}':\n")
        
        for stream in sorted(streams)[:20]:  # Limit to 20 for readability
            print(f"=== {stream} ===")
            
            # Get latest entry
            entries = await redis.xrevrange(stream, max="+", min="-", count=1)
            if not entries:
                print("  (no entries)\n")
                continue
            
            redis_id, data = entries[0]
            print(f"  Redis ID: {redis_id}")
            
            # Parse nested JSON
            if "data" in data:
                try:
                    outer = json.loads(data["data"])
                    if "tick" in outer:
                        tick = json.loads(outer["tick"])
                        print(f"  Parsed tick keys: {list(tick.keys())}")
                        
                        # Print depth if available
                        depth = tick.get("depth", {})
                        if depth:
                            buy = depth.get("buy", [])
                            sell = depth.get("sell", [])
                            if buy:
                                print(f"  Top bid: {buy[0]}")
                            if sell:
                                print(f"  Top ask: {sell[0]}")
                        
                        # Print other common fields
                        for field in ["exchange_timestamp", "last_price", "volume", "oi"]:
                            if field in tick:
                                print(f"  {field}: {tick[field]}")
                    else:
                        print(f"  Outer keys: {list(outer.keys())}")
                except json.JSONDecodeError as e:
                    print(f"  JSON parse error: {e}")
            else:
                print(f"  Raw fields: {list(data.keys())}")
            
            print()
        
        if len(streams) > 20:
            print(f"... and {len(streams) - 20} more streams")
            
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(probe_streams())
