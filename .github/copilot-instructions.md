# GitHub Copilot Instructions (TickStreamIngestor)

## Big Picture
- Pipeline: Redis Streams (`tick_stream:*`) → parse nested JSON → ClickHouse single table `tickstream.commodities_master_mcx_bfo_bse`.
- Orchestration: `TickStreamIngestor` in [src/ingestor.py](src/ingestor.py) wires `RedisStreamReader` + `ClickHouseWriter`.

## Key Modules (start here)
- Entrypoint: [run_ingestor.py](run_ingestor.py) runs `asyncio.run(src.ingestor.main())`.
- Config: [src/config.py](src/config.py) loads `.env` via `python-dotenv` and exposes `AppConfig`.
- Redis: [src/redis_reader.py](src/redis_reader.py) handles discovery/backfill/live reads + checkpoints.
- Parsing: [src/parser.py](src/parser.py) expects stream entries with field `data` (JSON) containing `tick` (JSON string).
- ClickHouse: [src/clickhouse_writer.py](src/clickhouse_writer.py) buffers rows and flushes by count/time.
- Schema: [src/schema.py](src/schema.py) `ensure_schema()` creates DB/table if missing.

## Redis Ingestion Patterns
- Discovery uses `SCAN` in `RedisStreamReader.discover_streams()`.
- Checkpointing uses Redis hash key `tick_ingestor:checkpoint` (field = stream name, value = last `redis_id`).
- Backfill uses `XRANGE` with an exclusive start (`min=f"({last_id}"`) to avoid duplicates.
- Live mode uses consumer groups (`XREADGROUP`) with streams mapped to `">"` for new messages only; group creation uses `id="$"`.

## ClickHouse Write Patterns
- Uses HTTP (`aiochclient` + `aiohttp`) and creates schema on startup (`ClickHouseWriter.start()` calls `ensure_schema`).
- Batching is central: `ClickHouseWriter.add()` flushes on `CH_INSERT_BATCH_SIZE` or time; `TickStreamIngestor` also runs a periodic flush task.
- Reliability caveat to keep in mind when editing: `RedisStreamReader.read_live()` currently `XACK`s after the callback returns, but `ClickHouseWriter` may only have buffered data (not yet flushed). If you change semantics, coordinate ACK with a successful ClickHouse insert/flush.

## Correctness Invariants (when changing ingestion)
- Backfill must remain duplicate-safe: `XRANGE` uses an exclusive start (`min=f"({last_id}"`) and checkpoints are per-stream in `tick_ingestor:checkpoint`.
- Live ingestion uses consumer groups; do not acknowledge (`XACK`) entries until you’re confident the data is durably written (currently this is a known gap due to buffering).
- If you change ACK timing, keep checkpoint updates aligned with ACK/durable write so restarts don’t skip or duplicate unexpectedly.

## Developer Workflows
- Run ingestor: `python run_ingestor.py`
- Probe Redis payloads: `python scripts/probe_redis.py` (prints decoded top-of-book fields)
- Check ClickHouse connectivity + table status: `python scripts/check_clickhouse.py`
  - If you see `REQUIRED_PASSWORD`, set `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` in `.env`.

## Local Run (practical checklist)
- Configure via `.env` / environment variables only (names in section below).
- Sanity checks:
  - Redis reachable + streams exist: `python scripts/probe_redis.py`
  - ClickHouse reachable + credentials correct: `python scripts/check_clickhouse.py`
  - Then run ingestion: `python run_ingestor.py`
- ClickHouse auth gotcha: newer ClickHouse builds may require a password for the `default` user; this repo expects you to provide `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` (or change the server config).

## Environment Variables (document names only)
- Redis: `REDIS_URL`, `REDIS_PASSWORD`, `REDIS_STREAM_PATTERN`, `REDIS_CONSUMER_GROUP`, `REDIS_CONSUMER_NAME`, `REDIS_BLOCK_MS`
- ClickHouse: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- Ingestion: `CH_INSERT_BATCH_SIZE`, `CH_FLUSH_INTERVAL_MS`, `LOG_LEVEL`

## Repo Conventions
- Keep reusable logic in `src/`, operational scripts in `scripts/`, and design notes in `docs/`.
- Logging uses `logging` with `extra={...}`; include `redis_stream` and `redis_id` in entry-level logs.
- Never commit credentials/tokens/passwords/connection strings; config must come from `.env` / environment variables.
