#!/usr/bin/env python3
"""Check if any BSE/BFO tokens exist in Redis streams."""
import asyncio
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from redis.asyncio import Redis

async def main():
    repo_root = Path(__file__).parent.parent
    load_dotenv(repo_root / ".env")
    
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_password = os.getenv("REDIS_PASSWORD", "")
    pattern = os.getenv("REDIS_STREAM_PATTERN", "tick_stream:*")
    
    # Load BSE/BFO tokens
    tokens_file = repo_root / "allowed_tokens.txt"
    print(f"Loading tokens from {tokens_file}...")
    with open(tokens_file) as f:
        allowed_tokens = set(line.strip() for line in f if line.strip())
    
    print(f"Loaded {len(allowed_tokens)} BSE/BFO tokens")
    print(f"Sample tokens: {list(sorted(allowed_tokens, key=int)[:10])}")
    print(f"\nConnecting to Redis: {redis_url}")
    
    redis = Redis.from_url(
        redis_url,
        decode_responses=True,
        password=redis_password or None,
    )
    
    try:
        await redis.ping()
        print("✓ Connected\n")
        
        # Scan all streams
        cursor = 0
        total_streams = 0
        bse_bfo_streams = []
        checked_streams = 0
        
        print("Scanning all streams for BSE/BFO tokens...")
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=500)
            total_streams += len(keys)
            
            for stream in keys:
                checked_streams += 1
                if checked_streams % 1000 == 0:
                    print(f"  Checked {checked_streams}/{total_streams} streams...")
                
                # Get latest entry
                entries = await redis.xrevrange(stream, max="+", min="-", count=1)
                if not entries:
                    continue
                
                redis_id, data = entries[0]
                
                # Parse tick data
                try:
                    if 'data' in data:
                        outer = json.loads(data['data'])
                        if 'tick' in outer:
                            tick = json.loads(outer['tick'])
                            exchange_token = str(tick.get('exchange_token', ''))
                            
                            if exchange_token in allowed_tokens:
                                symbol = tick.get('symbol', 'N/A')
                                bse_bfo_streams.append({
                                    'stream': stream,
                                    'token': exchange_token,
                                    'symbol': symbol,
                                    'latest_id': redis_id,
                                })
                                print(f"\n✓ FOUND BSE/BFO: {stream}")
                                print(f"  Token: {exchange_token}, Symbol: {symbol}")
                except Exception as e:
                    continue
            
            if cursor == 0:
                break
        
        print(f"\n{'='*60}")
        print(f"Scan complete:")
        print(f"  Total streams scanned: {total_streams}")
        print(f"  BSE/BFO streams found: {len(bse_bfo_streams)}")
        
        if bse_bfo_streams:
            print(f"\nBSE/BFO streams:")
            for item in bse_bfo_streams[:20]:
                print(f"  {item['stream']} -> {item['symbol']} (token: {item['token']})")
            if len(bse_bfo_streams) > 20:
                print(f"  ... and {len(bse_bfo_streams) - 20} more")
        else:
            print("\n✗ No BSE/BFO tokens found in any Redis stream")
            print("\nPossible reasons:")
            print("  1. BSE/BFO data is not being published to Redis")
            print("  2. BSE/BFO uses different stream keys (not matching pattern)")
            print("  3. Tokens in CSV don't match actual Redis data")
            
    finally:
        await redis.aclose()

if __name__ == "__main__":
    asyncio.run(main())
