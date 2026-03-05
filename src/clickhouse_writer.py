"""ClickHouse batch writer with count and time-based flushing.

Features:
- Batched inserts with count and time triggers
- TCP keepalive to prevent stale connections
- Retry logic with exponential backoff for network resilience
- Auto-reconnect on connection failure
"""

import asyncio
import logging
import random
import time
from typing import Any

from aiochclient import ChClient
from aiohttp import ClientSession, TCPConnector, ClientTimeout
from aiohttp.client_exceptions import ClientError

from .config import ClickHouseConfig, IngestionConfig
from .schema import INSERT_COLUMNS, ensure_schema

log = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY_S = 1.0
MAX_RETRY_DELAY_S = 30.0


class ClickHouseWriter:
    """Batched async writer for ClickHouse with time and count flushing."""

    def __init__(
        self,
        ch_config: ClickHouseConfig,
        ingestion_config: IngestionConfig,
    ):
        self.ch_config = ch_config
        self.batch_size = ingestion_config.batch_size
        self.flush_interval_s = ingestion_config.flush_interval_ms / 1000.0
        
        self._buffer: list[dict[str, Any]] = []
        self._last_flush_time = time.monotonic()
        self._client: ChClient | None = None
        self._session: ClientSession | None = None
        self._lock = asyncio.Lock()

    def _create_session(self) -> ClientSession:
        """Create an aiohttp session with proper timeout and keepalive settings."""
        connector = TCPConnector(
            keepalive_timeout=30,          # Keep TCP connections alive
            enable_cleanup_closed=True,    # Clean up dead connections
            force_close=False,             # Reuse connections when possible
        )
        timeout = ClientTimeout(
            total=300,      # 5 min max for entire request
            connect=10,     # 10 sec to establish connection
            sock_read=60,   # 60 sec to read response
        )
        return ClientSession(connector=connector, timeout=timeout)

    def _create_client(self, session: ClientSession) -> ChClient:
        """Create a ClickHouse client with the given session."""
        return ChClient(
            session,
            url=f"http://{self.ch_config.host}:{self.ch_config.port}",
            user=self.ch_config.user,
            password=self.ch_config.password,
            database=self.ch_config.database,
        )

    async def _reconnect(self) -> None:
        """Reconnect to ClickHouse by creating a fresh session and client."""
        log.info("Reconnecting to ClickHouse...")
        
        # Close old session if exists
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass  # Ignore errors during cleanup
        
        # Create new session and client
        self._session = self._create_session()
        self._client = self._create_client(self._session)
        
        log.info("Reconnected to ClickHouse", extra={
            "host": self.ch_config.host,
            "database": self.ch_config.database,
        })

    async def start(self) -> None:
        """Initialize ClickHouse connection and ensure schema exists."""
        self._session = self._create_session()
        self._client = self._create_client(self._session)
        await ensure_schema(self._client, self.ch_config.database)
        log.info("ClickHouse writer started", extra={
            "host": self.ch_config.host,
            "database": self.ch_config.database,
        })

    async def stop(self) -> None:
        """Flush remaining data and close connection."""
        await self._flush()
        if self._session:
            await self._session.close()
        log.info("ClickHouse writer stopped")

    async def add(self, row: dict[str, Any]) -> None:
        """Add a row to the buffer, flushing if needed."""
        async with self._lock:
            self._buffer.append(row)
            
            # Check if we should flush
            should_flush = (
                len(self._buffer) >= self.batch_size or
                (time.monotonic() - self._last_flush_time) >= self.flush_interval_s
            )
            
            if should_flush:
                await self._flush_unlocked()

    async def flush_if_needed(self) -> None:
        """Flush if time interval has elapsed (call periodically)."""
        async with self._lock:
            if (time.monotonic() - self._last_flush_time) >= self.flush_interval_s:
                await self._flush_unlocked()

    async def flush(self) -> None:
        """Force-flush any buffered rows now."""
        await self._flush()

    async def _flush(self) -> None:
        """Flush with lock."""
        async with self._lock:
            await self._flush_unlocked()

    async def _flush_unlocked(self) -> None:
        """Flush buffer to ClickHouse with retry logic (must hold lock)."""
        if not self._buffer:
            self._last_flush_time = time.monotonic()
            return
        
        rows_to_insert = self._buffer
        self._buffer = []
        self._last_flush_time = time.monotonic()
        
        last_error: Exception | None = None
        
        for attempt in range(MAX_RETRIES):
            try:
                if not self._client:
                    raise RuntimeError("ClickHouse client not initialized")

                cols_sql = ", ".join(INSERT_COLUMNS)
                rows_as_tuples = [tuple(row.get(col) for col in INSERT_COLUMNS) for row in rows_to_insert]
                await self._client.execute(
                    f"INSERT INTO {self.ch_config.database}.commodities_master_mcx_bfo_bse ({cols_sql}) VALUES",
                    *rows_as_tuples,
                )
                log.info("Flushed batch to ClickHouse", extra={
                    "row_count": len(rows_to_insert),
                })
                return  # Success!
                
            except (ClientError, ConnectionError, OSError) as e:
                # Network-related error - retry with backoff
                last_error = e
                
                if attempt < MAX_RETRIES - 1:
                    # Calculate delay with exponential backoff + jitter
                    delay = min(
                        MAX_RETRY_DELAY_S,
                        INITIAL_RETRY_DELAY_S * (2 ** attempt) + random.uniform(0, 1)
                    )
                    log.warning(
                        "ClickHouse flush failed, retrying...",
                        extra={
                            "error": str(e),
                            "attempt": attempt + 1,
                            "max_attempts": MAX_RETRIES,
                            "retry_delay_s": round(delay, 2),
                            "row_count": len(rows_to_insert),
                        }
                    )
                    
                    # Wait before retry
                    await asyncio.sleep(delay)
                    
                    # Reconnect before next attempt
                    try:
                        await self._reconnect()
                    except Exception as reconnect_error:
                        log.warning("Reconnect failed", extra={"error": str(reconnect_error)})
                else:
                    log.error(
                        "ClickHouse flush failed after all retries",
                        extra={
                            "error": str(e),
                            "attempts": MAX_RETRIES,
                            "row_count": len(rows_to_insert),
                        }
                    )
                    
            except Exception as e:
                # Non-network error (e.g., schema issue) - don't retry
                last_error = e
                log.error("Failed to flush to ClickHouse (non-retryable)", extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "row_count": len(rows_to_insert),
                })
                break
        
        # All retries exhausted - put rows back and raise
        self._buffer = rows_to_insert + self._buffer
        if last_error:
            raise last_error
