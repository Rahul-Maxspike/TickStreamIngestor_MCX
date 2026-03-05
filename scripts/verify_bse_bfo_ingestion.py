#!/usr/bin/env python3
"""Verify BSE/BFO data is being ingested from Redis to ClickHouse."""
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
from redis.asyncio import Redis
from aiohttp import ClientSession
from aiochclient import ChClient
import json

async def main():
    load_dotenv()
    
    # Load BSE/BFO token whitelist
    repo_root = Path(__file__).parent.parent
    with open(repo_root / "allowed_tokens.txt") as f:
        bse_bfo_tokens = {line.strip() for line in f if line.strip()}
    
    print(f"Loaded {len(bse_bfo_tokens)} BSE/BFO tokens from whitelist\n")
    
    # Connect to Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_password = os.getenv("REDIS_PASSWORD", "")
    redis = Redis.from_url(redis_url, decode_responses=True, password=redis_password or None)
    
    try:
        await redis.ping()
        print("✓ Connected to Redis\n")
        
        # Sample streams to check for BSE/BFO tokens
        print("Checking Redis streams for BSE/BFO tokens...")
        cursor = 0
        found_bse_bfo = []
        checked = 0
        
        while checked < 100:  # Check first 100 streams
            cursor, keys = await redis.scan(cursor=cursor, match="tick_stream:*", count=100)
            for stream in keys:
                checked += 1
                entries = await redis.xrevrange(stream, count=1)
                if not entries:
                    continue
                
                redis_id, data = entries[0]
                if "data" not in data:
                    continue
                
                try:
                    tick_data = json.loads(data["data"])
                    tick = json.loads(tick_data.get("tick", "{}"))
                    exchange_token = str(tick.get("exchange_token", ""))
                    
                    if exchange_token in bse_bfo_tokens:
                        found_bse_bfo.append({
                            "stream": stream,
                            "token": exchange_token,
                            "symbol": tick.get("symbol", ""),
                            "redis_id": redis_id
                        })
                        print(f"  ✓ Found BSE/BFO: {stream} (token={exchange_token}, symbol={tick.get('symbol')})")
                        
                        if len(found_bse_bfo) >= 5:  # Found enough examples
                            break
                except:
                    continue
            
            if cursor == 0 or len(found_bse_bfo) >= 5:
                break
        
        if not found_bse_bfo:
            print("  ✗ No BSE/BFO tokens found in Redis streams")
            print("  → BSE/BFO data may not be published to Redis yet")
            return
        
        print(f"\n✓ Found {len(found_bse_bfo)} BSE/BFO streams in Redis\n")
        
    finally:
        await redis.aclose()
    
    # Connect to ClickHouse
    ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    ch_db = os.getenv("CLICKHOUSE_DATABASE", "tickstream")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    
    async with ClientSession() as session:
        client = ChClient(
            session,
            url=f"http://{ch_host}:{ch_port}",
            user=ch_user,
            password=ch_password,
        )
        
        try:
            await client.execute("SELECT 1")
            print("✓ Connected to ClickHouse\n")
        except Exception as e:
            print(f"✗ ClickHouse connection failed: {e}")
            return
        
        # Check if BSE/BFO data exists in ClickHouse
        print("Checking ClickHouse for BSE/BFO data...")
        
        # Sample tokens from Redis findings
        sample_tokens = [item["token"] for item in found_bse_bfo[:5]]
        token_list = ",".join(f"'{t}'" for t in sample_tokens)
        
        query = f"""
        SELECT 
            exchange_token,
            symbol,
            count() as tick_count,
            min(exchange_time) as first_tick,
            max(exchange_time) as last_tick
        FROM {ch_db}.commodities_master_mcx_bfo_bse
        WHERE exchange_token IN ({token_list})
          AND toDate(exchange_time) = today()
        GROUP BY exchange_token, symbol
        ORDER BY tick_count DESC
        """
        
        rows = await client.fetch(query)
        
        if rows:
            print(f"✓ Found {len(rows)} BSE/BFO instruments in ClickHouse today:\n")
            for row in rows:
                print(f"  Token: {row[0]}")
                print(f"  Symbol: {row[1]}")
                print(f"  Ticks: {row[2]:,}")
                print(f"  Time range: {row[3]} → {row[4]}")
                print()
        else:
            print("✗ No BSE/BFO data found in ClickHouse")
            print("  → Check if ingestor is running")
            print("  → Check logs for filtering/ingestion errors")
            print(f"  → Searched for tokens: {sample_tokens}")
        
        # Overall statistics
        print("\nOverall BSE/BFO statistics today:")
        stats_query = f"""
        SELECT 
            count() as total_ticks,
            uniq(exchange_token) as unique_instruments,
            min(exchange_time) as earliest,
            max(exchange_time) as latest
        FROM {ch_db}.commodities_master_mcx_bfo_bse
        WHERE toInt32(exchange_token) <= 1200000
        AND toDate(exchange_time) = today()
        """
        
        stats = await client.fetchrow(stats_query)
        if stats and stats[0] > 0:
            print(f"  Total BSE/BFO ticks: {stats[0]:,}")
            print(f"  Unique instruments: {stats[1]:,}")
            print(f"  Time range: {stats[2]} → {stats[3]}")
        else:
            print("  No BSE/BFO data found")

if __name__ == "__main__":
    asyncio.run(main())
