"""ClickHouse schema management - create database and table if not exists."""

from __future__ import annotations

INSERT_COLUMNS: tuple[str, ...] = (
  # Time
  "exchange_time",

  # Instrument identity
  "exchange_instrument_id",
  "instrument_segment",
  "redis_stream",
  "redis_id",

  # Common tick identity/metadata
  "tradable",
  "mode",
  "instrument_token",
  "exchange_token",
  "symbol",
  "last_trade_time",

  # Depth levels (flattened)
  "bid1_price",
  "bid1_qty",
  "bid1_orders",
  "bid2_price",
  "bid2_qty",
  "bid2_orders",
  "bid3_price",
  "bid3_qty",
  "bid3_orders",
  "bid4_price",
  "bid4_qty",
  "bid4_orders",
  "bid5_price",
  "bid5_qty",
  "bid5_orders",

  "ask1_price",
  "ask1_qty",
  "ask1_orders",
  "ask2_price",
  "ask2_qty",
  "ask2_orders",
  "ask3_price",
  "ask3_qty",
  "ask3_orders",
  "ask4_price",
  "ask4_qty",
  "ask4_orders",
  "ask5_price",
  "ask5_qty",
  "ask5_orders",

  # Optional common tick fields
  "last_price",
  "last_qty",
  "average_traded_price",
  "volume_traded",
  "total_buy_quantity",
  "total_sell_quantity",
  "change",
  "oi",
  "oi_day_high",
  "oi_day_low",

  # OHLC snapshot
  "ohlc_open",
  "ohlc_high",
  "ohlc_low",
  "ohlc_close",
)

CREATE_DATABASE_SQL = """
CREATE DATABASE IF NOT EXISTS {database}
"""

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {database}.commodities_master_mcx_bfo_bse
(
  -- Time
  exchange_time   DateTime64(3, 'Asia/Kolkata') CODEC(Delta, ZSTD(1)),
  -- Seconds-only display helpers (avoid `.000` in SELECT output)
  exchange_time_s DateTime('Asia/Kolkata') ALIAS toDateTime(exchange_time, 'Asia/Kolkata'),
  event_date      Date DEFAULT toDate(exchange_time),
  ingest_time     DateTime64(3, 'Asia/Kolkata') DEFAULT now64(3, 'Asia/Kolkata') CODEC(Delta, ZSTD(1)),

  -- Instrument identity
  exchange_instrument_id UInt32 CODEC(T64, ZSTD(1)),
  instrument_segment LowCardinality(String) CODEC(ZSTD(1)),
  redis_stream    LowCardinality(String) CODEC(ZSTD(1)),
  redis_id        String CODEC(ZSTD(1)),

  -- Common tick identity/metadata (observed via scripts/probe_redis.py)
  tradable UInt8 CODEC(T64, ZSTD(1)),
  mode LowCardinality(String) CODEC(ZSTD(1)),
  instrument_token UInt32 CODEC(T64, ZSTD(1)),
  exchange_token String CODEC(ZSTD(1)),
  symbol LowCardinality(String) CODEC(ZSTD(1)),

  last_trade_time Nullable(DateTime64(3, 'Asia/Kolkata')) CODEC(Delta, ZSTD(1)),
  last_trade_time_s Nullable(DateTime('Asia/Kolkata')) ALIAS toDateTime(last_trade_time, 'Asia/Kolkata'),

  -- Depth levels (flattened; L2)
  bid1_price Decimal(18,4) CODEC(ZSTD(1)),
  bid1_qty   UInt32 CODEC(T64, ZSTD(1)),
  bid1_orders UInt16 CODEC(T64, ZSTD(1)),
  bid2_price Decimal(18,4) CODEC(ZSTD(1)),
  bid2_qty   UInt32 CODEC(T64, ZSTD(1)),
  bid2_orders UInt16 CODEC(T64, ZSTD(1)),
  bid3_price Decimal(18,4) CODEC(ZSTD(1)),
  bid3_qty   UInt32 CODEC(T64, ZSTD(1)),
  bid3_orders UInt16 CODEC(T64, ZSTD(1)),
  bid4_price Decimal(18,4) CODEC(ZSTD(1)),
  bid4_qty   UInt32 CODEC(T64, ZSTD(1)),
  bid4_orders UInt16 CODEC(T64, ZSTD(1)),
  bid5_price Decimal(18,4) CODEC(ZSTD(1)),
  bid5_qty   UInt32 CODEC(T64, ZSTD(1)),
  bid5_orders UInt16 CODEC(T64, ZSTD(1)),

  ask1_price Decimal(18,4) CODEC(ZSTD(1)),
  ask1_qty   UInt32 CODEC(T64, ZSTD(1)),
  ask1_orders UInt16 CODEC(T64, ZSTD(1)),
  ask2_price Decimal(18,4) CODEC(ZSTD(1)),
  ask2_qty   UInt32 CODEC(T64, ZSTD(1)),
  ask2_orders UInt16 CODEC(T64, ZSTD(1)),
  ask3_price Decimal(18,4) CODEC(ZSTD(1)),
  ask3_qty   UInt32 CODEC(T64, ZSTD(1)),
  ask3_orders UInt16 CODEC(T64, ZSTD(1)),
  ask4_price Decimal(18,4) CODEC(ZSTD(1)),
  ask4_qty   UInt32 CODEC(T64, ZSTD(1)),
  ask4_orders UInt16 CODEC(T64, ZSTD(1)),
  ask5_price Decimal(18,4) CODEC(ZSTD(1)),
  ask5_qty   UInt32 CODEC(T64, ZSTD(1)),
  ask5_orders UInt16 CODEC(T64, ZSTD(1)),

  -- Optional common tick fields (populate when available)
  last_price  Decimal(18,4) CODEC(ZSTD(1)),
  last_qty    UInt32 CODEC(T64, ZSTD(1)),
  average_traded_price Decimal(18,4) CODEC(ZSTD(1)),
  volume_traded UInt64 CODEC(T64, ZSTD(1)),
  total_buy_quantity UInt64 CODEC(T64, ZSTD(1)),
  total_sell_quantity UInt64 CODEC(T64, ZSTD(1)),
  change Decimal(18,4) CODEC(ZSTD(1)),
  oi          UInt64 CODEC(T64, ZSTD(1)),
  oi_day_high UInt64 CODEC(T64, ZSTD(1)),
  oi_day_low  UInt64 CODEC(T64, ZSTD(1)),

  -- OHLC snapshot (when available)
  ohlc_open  Decimal(18,4) CODEC(ZSTD(1)),
  ohlc_high  Decimal(18,4) CODEC(ZSTD(1)),
  ohlc_low   Decimal(18,4) CODEC(ZSTD(1)),
  ohlc_close Decimal(18,4) CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(event_date)
ORDER BY (exchange_instrument_id, exchange_time, redis_stream, redis_id)
"""


DROP_RAW_JSON_SQL = """
ALTER TABLE {database}.commodities_master_mcx_bfo_bse DROP COLUMN IF EXISTS raw_json
"""


DROP_REDUNDANT_COLUMNS_SQL = """
ALTER TABLE {database}.commodities_master_mcx_bfo_bse
  DROP COLUMN IF EXISTS bid_price,
  DROP COLUMN IF EXISTS bid_qty,
  DROP COLUMN IF EXISTS ask_price,
  DROP COLUMN IF EXISTS ask_qty,
  DROP COLUMN IF EXISTS depth_buy,
  DROP COLUMN IF EXISTS depth_sell
"""


MODIFY_INGEST_TIME_TZ_SQL = """
ALTER TABLE {database}.commodities_master_mcx_bfo_bse
  MODIFY COLUMN ingest_time DateTime64(3, 'Asia/Kolkata') DEFAULT now64(3, 'Asia/Kolkata') CODEC(Delta, ZSTD(1))
"""


ADD_SECONDS_ALIAS_COLUMNS_SQL = """
ALTER TABLE {database}.commodities_master_mcx_bfo_bse
  ADD COLUMN IF NOT EXISTS exchange_time_s DateTime('Asia/Kolkata') ALIAS toDateTime(exchange_time, 'Asia/Kolkata'),
  ADD COLUMN IF NOT EXISTS last_trade_time_s Nullable(DateTime('Asia/Kolkata')) ALIAS toDateTime(last_trade_time, 'Asia/Kolkata')
"""


async def ensure_schema(client, database: str) -> None:
    """Create database and table if they don't exist."""
    await client.execute(CREATE_DATABASE_SQL.format(database=database))
    await client.execute(CREATE_TABLE_SQL.format(database=database))
    # Keep schema aligned across upgrades; raw_json is no longer stored.
    await client.execute(DROP_RAW_JSON_SQL.format(database=database))
    # Drop redundant columns (kept in earlier revisions).
    await client.execute(DROP_REDUNDANT_COLUMNS_SQL.format(database=database))
    # Ensure ingest_time is stored/displayed in IST.
    await client.execute(MODIFY_INGEST_TIME_TZ_SQL.format(database=database))
    # Add seconds-only alias columns for cleaner display.
    await client.execute(ADD_SECONDS_ALIAS_COLUMNS_SQL.format(database=database))
