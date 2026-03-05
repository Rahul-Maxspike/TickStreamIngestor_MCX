"""Configuration loader from environment variables."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class RedisConfig:
    url: str
    password: str
    stream_pattern: str
    exclude_segments: tuple[str, ...]
    consumer_group: str
    consumer_name: str
    block_ms: int


@dataclass
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class IngestionConfig:
    batch_size: int
    flush_interval_ms: int
    backfill_mode: str
    backfill_from_market_open: bool
    market_open_hhmm: str
    dedup_ttl_seconds: int


@dataclass
class AppConfig:
    redis: RedisConfig
    clickhouse: ClickHouseConfig
    ingestion: IngestionConfig
    log_level: str


def load_config() -> AppConfig:
    """Load configuration from environment variables."""
    exclude_segments_raw = os.getenv("REDIS_EXCLUDE_SEGMENTS", "").strip()
    exclude_segments = tuple(
        s.strip().lower()
        for s in exclude_segments_raw.split(",")
        if s.strip()
    )

    backfill_mode = os.getenv("BACKFILL_MODE", "").strip().lower()

    # Backward compat: existing env var.
    legacy_market_open = os.getenv("BACKFILL_FROM_MARKET_OPEN", "1") in {"1", "true", "True", "yes", "YES"}
    if not backfill_mode:
        backfill_mode = "today" if legacy_market_open else "all"

    if backfill_mode not in {"today", "all"}:
        raise ValueError("BACKFILL_MODE must be 'today' or 'all'")

    return AppConfig(
        redis=RedisConfig(
            url=os.getenv("REDIS_URL", "redis://localhost:6379"),
            password=os.getenv("REDIS_PASSWORD", ""),
            stream_pattern=os.getenv("REDIS_STREAM_PATTERN", "tick_stream:*"),
            exclude_segments=exclude_segments,
            consumer_group=os.getenv("REDIS_CONSUMER_GROUP", "tick_ingestor"),
            consumer_name=os.getenv("REDIS_CONSUMER_NAME", "worker-1"),
            block_ms=int(os.getenv("REDIS_BLOCK_MS", "5000")),
        ),
        clickhouse=ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DATABASE", "tickstream"),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        ),
        ingestion=IngestionConfig(
            batch_size=int(os.getenv("CH_INSERT_BATCH_SIZE", "5000")),
            flush_interval_ms=int(os.getenv("CH_FLUSH_INTERVAL_MS", "1000")),
            backfill_mode=backfill_mode,
            backfill_from_market_open=(backfill_mode == "today"),
            market_open_hhmm=os.getenv("MARKET_OPEN_HHMM", "09:15"),
            dedup_ttl_seconds=int(os.getenv("DEDUP_TTL_SECONDS", "0")),
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


@dataclass
class BseBfoConfig:
    """Configuration for BSE/BFO specific ClickHouse table."""
    clickhouse: ClickHouseConfig
    table_name: str


def load_bse_bfo_config(main_ch_config: ClickHouseConfig) -> BseBfoConfig:
    """Load BSE/BFO ClickHouse config, falling back to main config if not specified."""
    return BseBfoConfig(
        clickhouse=ClickHouseConfig(
            host=os.getenv("BSE_BFO_CLICKHOUSE_HOST") or main_ch_config.host,
            port=int(os.getenv("BSE_BFO_CLICKHOUSE_PORT") or main_ch_config.port),
            database=os.getenv("BSE_BFO_CLICKHOUSE_DATABASE") or main_ch_config.database,
            user=os.getenv("BSE_BFO_CLICKHOUSE_USER") or main_ch_config.user,
            password=os.getenv("BSE_BFO_CLICKHOUSE_PASSWORD") or main_ch_config.password,
        ),
        table_name=os.getenv("BSE_BFO_TABLE_NAME", "bse_bfo_ticks"),
    )

