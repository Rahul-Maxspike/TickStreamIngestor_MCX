"""Main ingestion orchestrator - ties Redis reader and ClickHouse writer together."""

import asyncio
import hashlib
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Any

from .config import AppConfig
from .clickhouse_writer import ClickHouseWriter
from .parser import parse_tick_entry
from .redis_reader import RedisStreamReader

log = logging.getLogger(__name__)


class TickStreamIngestor:
    """Orchestrates backfill and live ingestion from Redis to ClickHouse."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.redis_reader = RedisStreamReader(config.redis)
        self.ch_writer = ClickHouseWriter(config.clickhouse, config.ingestion)
        self._shutdown_event = asyncio.Event()
        self._flush_task: asyncio.Task | None = None

        try:
            self._ist = ZoneInfo("Asia/Kolkata")
        except ZoneInfoNotFoundError:
            # Windows Python may not ship IANA tzdata; fall back to fixed IST offset.
            self._ist = timezone(timedelta(hours=5, minutes=30))
        self._dedup_ttl_s = max(0, int(config.ingestion.dedup_ttl_seconds))
        self._dedup_cache: dict[int, float] = {}
        
        # Load BSE/BFO token whitelist
        import os
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        bse_bfo_path = os.path.join(repo_root, "allowed_tokens.txt")
        self._allowed_tokens = self._load_allowed_tokens(bse_bfo_path)
        log.info(f"Loaded {len(self._allowed_tokens)} BSE/BFO tokens for filtering")

    async def start(self) -> None:
        """Initialize connections and start ingestion."""
        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._signal_handler)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass

        await self.redis_reader.start()
        await self.ch_writer.start()
        
        # Start periodic flush task
        self._flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._shutdown_event.set()
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        await self.ch_writer.stop()
        await self.redis_reader.stop()
        log.info("Ingestor stopped")

    def _signal_handler(self) -> None:
        """Handle shutdown signals."""
        log.info("Shutdown signal received")
        self._shutdown_event.set()

    async def _periodic_flush(self) -> None:
        """Periodically flush ClickHouse buffer."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.ingestion.flush_interval_ms / 1000.0)
                await self.ch_writer.flush_if_needed()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Error in periodic flush", extra={"error": str(e)})

    def _market_open_redis_id(self) -> str:
        """Return a Redis Stream ID (ms-0) for today's market open in IST."""
        hhmm = self.config.ingestion.market_open_hhmm
        hour_s, minute_s = hhmm.split(":", 1)
        hour = int(hour_s)
        minute = int(minute_s)

        now_ist = datetime.now(self._ist)
        open_ist = now_ist.replace(hour=hour, minute=minute, second=0, microsecond=0)
        # Convert to epoch ms (Redis IDs use epoch ms).
        open_ms = int(open_ist.timestamp() * 1000)
        return f"{open_ms}-0"

    def _dedup_key(self, row: dict[str, Any]) -> int:
        """Compute a stable fingerprint for cross-stream dedup.

        We dedupe within a short TTL using a stable subset of tick fields.
        This is intentionally conservative: it catches common “same tick published
        in multiple streams” cases without requiring the full raw payload.
        """
        exchange_token = str(row.get("exchange_token") or "")
        exchange_time = row.get("exchange_time")
        last_price = row.get("last_price")
        bid1_price = row.get("bid1_price")
        bid1_qty = row.get("bid1_qty")
        ask1_price = row.get("ask1_price")
        ask1_qty = row.get("ask1_qty")
        volume = row.get("volume_traded")
        oi = row.get("oi")

        payload = "|".join(
            [
                exchange_token,
                str(exchange_time or ""),
                str(last_price or ""),
                str(bid1_price or ""),
                str(bid1_qty or ""),
                str(ask1_price or ""),
                str(ask1_qty or ""),
                str(volume or ""),
                str(oi or ""),
            ]
        )
        h = hashlib.blake2b(payload.encode("utf-8"), digest_size=8)
        return int.from_bytes(h.digest(), "big", signed=False)

    def _dedup_should_skip(self, row: dict[str, Any]) -> bool:
        if self._dedup_ttl_s <= 0:
            return False

        now = time.monotonic()
        key = self._dedup_key(row)
        exp = self._dedup_cache.get(key)
        if exp is not None and exp > now:
            return True

        # Insert/update
        self._dedup_cache[key] = now + float(self._dedup_ttl_s)

        # Opportunistic cleanup to keep dict bounded
        if len(self._dedup_cache) > 200_000:
            cutoff = now
            self._dedup_cache = {k: v for k, v in self._dedup_cache.items() if v > cutoff}

        return False

    def _load_allowed_tokens(self, filepath: str) -> set[str]:
        """Load allowed exchange tokens from file."""
        try:
            with open(filepath, 'r') as f:
                tokens = {line.strip() for line in f if line.strip()}
            log.info(f"Loaded {len(tokens)} BSE/BFO tokens from {filepath}")
            return tokens
        except Exception as e:
            log.error(f"Failed to load allowed tokens from {filepath}: {e}")
            return set()

    def _is_bse_bfo(self, row: dict[str, Any]) -> bool:
        """Check if tick is from BSE/BFO by token whitelist."""
        if not self._allowed_tokens:
            return False
        exchange_token = str(row.get("exchange_token", ""))
        return exchange_token in self._allowed_tokens
    
    def _is_commodity(self, stream: str) -> bool:
        """Check if tick is from commodities by stream segment name."""
        parts = stream.split(":")
        if len(parts) >= 2:
            segment = parts[1].lower()
            return segment in ["commodities_futures", "commodities_options"]
        return False
    
    def _should_ingest(self, row: dict[str, Any], stream: str) -> bool:
        """
        Decide if we should ingest this tick.
        
        - MCX commodities: segment-based (commodities_futures/commodities_options)
        - BSE/BFO: token-based (exchange_token in allowed_tokens.txt)
        """
        # Commodity check: segment name
        if self._is_commodity(stream):
            return True
        # BSE/BFO check: token whitelist
        if self._is_bse_bfo(row):
            return True
        return False

    async def _process_entries(
        self,
        stream: str,
        entries: list[tuple[str, dict[str, Any]]],
    ) -> None:
        """Process a batch of Redis entries and write to ClickHouse."""
        for redis_id, data in entries:
            row = parse_tick_entry(stream, redis_id, data)
            if row:
                if self._dedup_should_skip(row):
                    continue
                
                # Only insert BSE/BFO or commodities ticks, skip all others
                if self._should_ingest(row, stream):
                    await self.ch_writer.add(row)
                else:
                    log.debug(f"Filtered out non-BSE/BFO tick: {row.get('symbol')} token={row.get('exchange_token')}")
            else:
                log.warning(f"Failed to parse entry stream={stream} id={redis_id}")

        # Important: ensure buffered rows are durably inserted before returning.
        # RedisStreamReader.read_live() XACKs after this callback returns.
        await self.ch_writer.flush()

    async def run(self) -> None:
        """Main ingestion loop: discover streams, backfill, then live read."""
        try:
            await self.start()
            
            # Discover streams
            streams = await self.redis_reader.discover_streams()
            if not streams:
                log.warning("No streams found matching pattern", extra={
                    "pattern": self.config.redis.stream_pattern,
                })
                return
            
            # Backfill all streams
            log.info(
                "Starting backfill phase",
                extra={
                    "stream_count": len(streams),
                    "backfill_mode": self.config.ingestion.backfill_mode,
                },
            )
            min_start_id = None
            if self.config.ingestion.backfill_from_market_open:
                try:
                    min_start_id = self._market_open_redis_id()
                    log.info(
                        "Backfill constrained by market open",
                        extra={
                            "market_open_hhmm": self.config.ingestion.market_open_hhmm,
                            "min_start_id": min_start_id,
                        },
                    )
                except Exception as e:
                    log.warning("Failed to compute market open start id", extra={"error": str(e)})

            for stream in streams:
                if self._shutdown_event.is_set():
                    break
                await self.redis_reader.backfill_stream(
                    stream,
                    self._process_entries,
                    min_start_id=min_start_id,
                )
            
            # Final flush after backfill
            await self.ch_writer.flush_if_needed()
            log.info("Backfill complete, switching to live mode")
            
            # Live ingestion
            if not self._shutdown_event.is_set():
                await self.redis_reader.read_live(streams, self._process_entries)

        except asyncio.CancelledError:
            # Expected during shutdown (e.g. Ctrl+C). Avoid noisy tracebacks.
            pass
        except Exception as e:
            log.exception("Fatal error in ingestor", extra={"error": str(e)})
            raise
        finally:
            # Ensure cleanup runs even if cancellation is in progress.
            try:
                await asyncio.shield(self.stop())
            except Exception as e:
                log.error("Error during shutdown", extra={"error": str(e)})


async def main() -> None:
    """Entry point for the ingestor."""
    from .config import load_config
    
    config = load_config()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    
    ingestor = TickStreamIngestor(config)
    await ingestor.run()



if __name__ == "__main__":
    asyncio.run(main())


