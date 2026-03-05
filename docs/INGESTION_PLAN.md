# Redis Streams → ClickHouse (Single Table) Ingestion Plan

## Goals
- Ingest **all** Redis tick streams matching `tick_stream:*` into **one** ClickHouse table.
- Support:
  - **Real-time** access: “latest tick per instrument”.
  - **Historical** access: range queries for backtesting/analytics.
- No materialized views; **one physical table**.

## Observed Redis Tick Shape (from inspiration code)
- Stream keys are like:
  - `tick_stream:options:{exchange_instrument_id}`
  - `tick_stream:futures:{exchange_instrument_id}`
- In this environment we also observe:
  - `tick_stream:equity:{exchange_instrument_id}` (via `scripts/probe_redis.py`)
- Stream entries contain a field named `data` which is JSON.
- That JSON contains `tick` which is itself a JSON string.
- Parsed tick includes `depth.buy[]` and `depth.sell[]` with at least `price` (likely `quantity`, `orders`).

### Observed tick keys (via `scripts/probe_redis.py`)
Common keys seen for “full” ticks:
- `tradable`, `mode`
- `instrument_token`, `exchange_token`, `symbol`
- `exchange_timestamp` (observed as ISO string like `2025-12-11T12:35:30`)
- `last_price`, `last_traded_quantity`, `average_traded_price`
- `volume_traded`, `total_buy_quantity`, `total_sell_quantity`
- `change`
- `ohlc` (object with `open`, `high`, `low`, `close`)
- `last_trade_time`
- `oi`, `oi_day_high`, `oi_day_low`
- `depth` (object with `buy[]`/`sell[]` levels containing `price`, `quantity`, `orders`)

Some streams emit “lighter” ticks (e.g. missing `depth` and volume fields). The ingestion should tolerate this by:
- Populating typed columns when present, otherwise using defaults

Reference: `inspiration/redis_tick_stream.py`.

## Proposed Architecture
1. **Discovery**: periodically `SCAN` for `tick_stream:*` keys.
2. **Per-stream catchup**:
   - Determine last processed Redis ID (checkpoint).
  - Read missing entries via `XRANGE` in bounded batches.
  - If the process starts late, optionally backfill from today’s market open (IST) before going live.
   - Insert batches into ClickHouse.
3. **Live ingestion**:
   - Prefer `XREADGROUP` with consumer groups.
   - `XACK` only after successful ClickHouse insert.
4. **Idempotency**:
   - Store `redis_stream` and `redis_id` per row.
   - Use a MergeTree engine that tolerates retries/replays.

### Checkpointing
- Store checkpoints as `Hash` in Redis (simple + shared):
  - key: `tick_ingestor:checkpoint`
  - field: `{redis_stream}`
  - value: last committed `redis_id`

On restart:
- Backfill from `(last_id, +)` and then switch to live.

## ClickHouse Schema (Single Table)

### Database
```sql
CREATE DATABASE IF NOT EXISTS tickstream;
```

### Table
This schema stores:
- Columns optimized for “latest tick” queries (top-of-book fields)
- Flattened top-5 depth columns for fast dashboards
- No redundant columns: top-of-book comes from bid1/ask1, and depth arrays are not stored.

```sql
CREATE TABLE IF NOT EXISTS tickstream.commodities_master_mcx_bfo_bse
(
  -- Time
  exchange_time   DateTime64(3, 'Asia/Kolkata') CODEC(Delta, ZSTD(1)),
  exchange_time_s DateTime('Asia/Kolkata') ALIAS toDateTime(exchange_time, 'Asia/Kolkata'),
  event_date      Date DEFAULT toDate(exchange_time),
  ingest_time     DateTime64(3, 'Asia/Kolkata') DEFAULT now64(3, 'Asia/Kolkata') CODEC(Delta, ZSTD(1)),

  -- Instrument identity
  exchange_instrument_id UInt32 CODEC(T64, ZSTD(1)),
  instrument_segment LowCardinality(String) CODEC(ZSTD(1)),
  redis_stream    LowCardinality(String) CODEC(ZSTD(1)),
  redis_id        String CODEC(ZSTD(1)),

  -- Common tick identity/metadata
  tradable UInt8 CODEC(T64, ZSTD(1)),
  mode LowCardinality(String) CODEC(ZSTD(1)),
  instrument_token UInt32 CODEC(T64, ZSTD(1)),
  exchange_token String CODEC(ZSTD(1)),
  symbol LowCardinality(String) CODEC(ZSTD(1)),
  last_trade_time Nullable(DateTime64(3, 'Asia/Kolkata')) CODEC(Delta, ZSTD(1)),
  last_trade_time_s Nullable(DateTime('Asia/Kolkata')) ALIAS toDateTime(last_trade_time, 'Asia/Kolkata'),

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
  ohlc_close Decimal(18,4) CODEC(ZSTD(1)),

  -- Depth levels (flattened; L2)
  bid1_price Decimal(18,4), bid1_qty UInt32, bid1_orders UInt16,
  bid2_price Decimal(18,4), bid2_qty UInt32, bid2_orders UInt16,
  bid3_price Decimal(18,4), bid3_qty UInt32, bid3_orders UInt16,
  bid4_price Decimal(18,4), bid4_qty UInt32, bid4_orders UInt16,
  bid5_price Decimal(18,4), bid5_qty UInt32, bid5_orders UInt16,

  ask1_price Decimal(18,4), ask1_qty UInt32, ask1_orders UInt16,
  ask2_price Decimal(18,4), ask2_qty UInt32, ask2_orders UInt16,
  ask3_price Decimal(18,4), ask3_qty UInt32, ask3_orders UInt16,
  ask4_price Decimal(18,4), ask4_qty UInt32, ask4_orders UInt16,
  ask5_price Decimal(18,4), ask5_qty UInt32, ask5_orders UInt16,

)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(event_date)
ORDER BY (exchange_instrument_id, exchange_time, redis_stream, redis_id);
```

### Why this design
- **Fast writes**: single table, batch inserts.
- **Fast latest**: `ORDER BY (exchange_instrument_id, exchange_time, ...)` allows efficient top-per-instrument queries.
- **Backfill friendly**: storing `redis_id` and `redis_stream` supports audit and safe retries.
- **No extra tables**: real-time is derived from the same history table.

## Query Examples

### Latest tick per instrument (today)
```sql
SELECT *
FROM tickstream.commodities_master_mcx_bfo_bse
WHERE event_date = today()
  AND exchange_instrument_id IN (123, 456)
ORDER BY exchange_instrument_id, exchange_time DESC
LIMIT 1 BY exchange_instrument_id;
```

### Historical range
```sql
SELECT exchange_time, bid1_price, ask1_price, last_price, volume_traded
FROM tickstream.commodities_master_mcx_bfo_bse
WHERE exchange_instrument_id = 123
  AND exchange_time >= toDateTime64('2025-12-15 09:15:00', 3, 'Asia/Kolkata')
  AND exchange_time <  toDateTime64('2025-12-15 15:30:00', 3, 'Asia/Kolkata')
ORDER BY exchange_time;
```

Note: use `bid1_price`/`ask1_price` as top-of-book; `volume_traded` is the stored volume column.

### Historical pagination (avoid OFFSET)
Use keyset pagination for stable paging at high volumes.

Forward (older → newer):
```sql
SELECT *
FROM tickstream.commodities_master_mcx_bfo_bse
WHERE exchange_instrument_id = 123
  AND exchange_time >= toDateTime64('2025-12-15 09:15:00', 3, 'Asia/Kolkata')
  AND exchange_time <  toDateTime64('2025-12-15 15:30:00', 3, 'Asia/Kolkata')
  AND (exchange_time, redis_id) > (toDateTime64('2025-12-15 10:00:00', 3, 'Asia/Kolkata'), '1765436730000-0')
ORDER BY exchange_time, redis_id
LIMIT 10000;
```

Backward (newer → older):
```sql
SELECT *
FROM tickstream.commodities_master_mcx_bfo_bse
WHERE exchange_instrument_id = 123
  AND (exchange_time, redis_id) < (toDateTime64('2025-12-15 10:00:00', 3, 'Asia/Kolkata'), '1765436730000-0')
ORDER BY exchange_time DESC, redis_id DESC
LIMIT 10000;
```

## Performance Plan
- Batch inserts to ClickHouse (start with 2k–10k rows per insert).
- Flush on either:
  - `CH_INSERT_BATCH_SIZE` reached, or
  - `CH_FLUSH_INTERVAL_MS` elapsed.
- Backpressure:
  - Limit in-flight rows; pause `XREADGROUP` when ClickHouse is slow.
  - Bound per-stream catchup batch sizes.

## Reliability / Failure Modes
- Redis → ClickHouse insert fails:
  - Do **not** `XACK`.
  - Retry with exponential backoff.
- Process crash:
  - Pending messages remain in PEL (consumer group), can be reclaimed.
- Late start/backlog:
  - Catchup from checkpoints using `XRANGE` before switching to live.

## Cross-stream duplicates (equity/spot)
Some deployments may publish the same tick payload in multiple streams (e.g., `tick_stream:equity:*` and `tick_stream:spot:*`).
To avoid double-counting (candles/heatmaps), the ingestor can optionally skip exact-payload duplicates within a short TTL window.
This is implemented as an in-memory fingerprint of a stable subset of tick fields (e.g., token + time + top-of-book), within a short TTL.

## Environment Variables (planned)
- `REDIS_URL`
- `REDIS_PASSWORD`
- `REDIS_STREAM_PATTERN=tick_stream:*`
- `REDIS_CONSUMER_GROUP=tick_ingestor`
- `REDIS_CONSUMER_NAME`
- `REDIS_BLOCK_MS=5000`

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT=8123`
- `CLICKHOUSE_DATABASE=tickstream`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`

- `CH_INSERT_BATCH_SIZE=5000`
- `CH_FLUSH_INTERVAL_MS=1000`
- `LOG_LEVEL=INFO`

## Next Implementation Tasks
- Add `.env.example` (no secrets) + config loader.
- Write a Redis probe script to print one decoded entry per stream.
- Implement ClickHouse “create DB/table if not exists” on startup.
- Implement backfill + live ingestion loop.
