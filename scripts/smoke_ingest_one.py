#!/usr/bin/env python3
"""Smoke test: read one Redis tick entry, parse it, insert into ClickHouse.

Useful to validate:
- Redis connectivity + payload shape
- parser.parse_tick_entry compatibility
- ClickHouse schema + insert path

This script inserts exactly 1 row.
"""

import asyncio
import os
import sys
from pathlib import Path

from aiohttp import ClientSession
from aiochclient import ChClient
from dotenv import load_dotenv
from redis.asyncio import Redis

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from src.parser import parse_tick_entry
from src.schema import INSERT_COLUMNS, ensure_schema


async def main() -> None:
    load_dotenv(dotenv_path=repo_root / ".env")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_password = os.getenv("REDIS_PASSWORD", "")
    pattern = os.getenv("REDIS_STREAM_PATTERN", "tick_stream:*")

    ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    ch_db = os.getenv("CLICKHOUSE_DATABASE", "tickstream")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    redis = Redis.from_url(
        redis_url,
        decode_responses=True,
        password=redis_password or None,
    )
    try:
        await redis.ping()

        # Discover one stream
        cursor, keys = await redis.scan(cursor=0, match=pattern, count=1000)
        if not keys:
            raise RuntimeError(f"No Redis streams found for pattern '{pattern}'")

        stream = sorted(keys)[0]
        entries = await redis.xrevrange(stream, max="+", min="-", count=1)
        if not entries:
            raise RuntimeError(f"Stream '{stream}' had no entries")

        redis_id, data = entries[0]
        row = parse_tick_entry(stream, redis_id, data)
        if not row:
            raise RuntimeError("parse_tick_entry returned None")

        async with ClientSession() as session:
            client = ChClient(
                session,
                url=f"http://{ch_host}:{ch_port}",
                user=ch_user,
                password=ch_password,
                database=ch_db,
            )
            await ensure_schema(client, ch_db)
            cols_sql = ", ".join(INSERT_COLUMNS)
            values = tuple(row.get(col) for col in INSERT_COLUMNS)
            await client.execute(f"INSERT INTO {ch_db}.commodities_master_mcx_bfo_bse ({cols_sql}) VALUES", values)
            count = await client.fetchval(f"SELECT count() FROM {ch_db}.commodities_master_mcx_bfo_bse")

        print(f"Inserted 1 row from {stream} id={redis_id}")
        print(f"commodities_master_mcx_bfo_bse row count now: {count}")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
