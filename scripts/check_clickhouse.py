#!/usr/bin/env python3
"""Check ClickHouse connectivity and schema status."""

import asyncio
import os
import sys

from aiohttp import ClientSession
from aiochclient import ChClient
from dotenv import load_dotenv

load_dotenv()


async def check_clickhouse():
    """Test ClickHouse connection and print schema info."""
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database = os.getenv("CLICKHOUSE_DATABASE", "tickstream")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    
    print(f"Connecting to ClickHouse: http://{host}:{port}")
    
    async with ClientSession() as session:
        client = ChClient(
            session,
            url=f"http://{host}:{port}",
            user=user,
            password=password,
        )
        
        # Test connection
        try:
            version = await client.fetchval("SELECT version()")
            print(f"Connected! ClickHouse version: {version}\n")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)
        
        # Check if database exists
        databases = await client.fetch("SHOW DATABASES")
        db_names = [row[0] for row in databases]
        print(f"Databases: {db_names}")
        
        if database in db_names:
            print(f"\nDatabase '{database}' exists")
            
            # Check tables
            tables = await client.fetch(f"SHOW TABLES FROM {database}")
            table_names = [row[0] for row in tables]
            print(f"Tables in {database}: {table_names}")
            
            if "commodities_master_mcx_bfo_bse" in table_names:
                # Get row count
                count = await client.fetchval(f"SELECT count() FROM {database}.commodities_master_mcx_bfo_bse")
                print(f"\ncommodities_master_mcx_bfo_bse row count: {count:,}")
                
                # Get recent data
                if count > 0:
                    latest = await client.fetch(f"""
                        SELECT 
                            exchange_instrument_id,
                            instrument_segment,
                            exchange_time_s,
                            ingest_time,
                            bid1_price,
                            ask1_price,
                            redis_stream
                        FROM {database}.commodities_master_mcx_bfo_bse
                        ORDER BY exchange_time DESC
                        LIMIT 5
                    """)
                    print("\nLatest 5 ticks:")
                    for row in latest:
                        try:
                            print(f"  {tuple(row.values())}")
                        except Exception:
                            print(f"  {row}")
        else:
            print(f"\nDatabase '{database}' does not exist yet")
            print("It will be created when the ingestor starts")


if __name__ == "__main__":
    asyncio.run(check_clickhouse())
