"""Redis stream reader with backfill and live ingestion support."""

import asyncio
import logging
import random
from typing import Callable, Coroutine, Any

from redis.asyncio import Redis

from .config import RedisConfig

log = logging.getLogger(__name__)

CHECKPOINT_KEY = "tick_ingestor:checkpoint"


class RedisStreamReader:
    """Reads from Redis streams with checkpoint-based backfill and consumer groups."""

    def __init__(self, config: RedisConfig):
        self.config = config
        self._redis: Redis | None = None
        self._running = False

    async def start(self) -> None:
        """Connect to Redis."""
        self._redis = Redis.from_url(
            self.config.url,
            decode_responses=True,
            password=self.config.password or None,
        )
        await self._redis.ping()
        log.info("Connected to Redis", extra={"url": self.config.url})

    async def stop(self) -> None:
        """Close Redis connection."""
        self._running = False
        if self._redis:
            await self._redis.close()
        log.info("Redis reader stopped")

    async def discover_streams(self) -> list[str]:
        """Discover all streams matching the pattern."""
        cursor = 0
        streams = []
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor,
                match=self.config.stream_pattern,
                count=1000,
            )
            streams.extend(keys)
            if cursor == 0:
                break

        # Optional: exclude certain segments (e.g. don't ingest tick_stream:spot:*).
        if self.config.exclude_segments:
            excluded = set(self.config.exclude_segments)
            filtered: list[str] = []
            for s in streams:
                parts = s.split(":")
                seg = parts[1].lower() if len(parts) >= 2 else ""
                if seg in excluded:
                    continue
                filtered.append(s)
            streams = filtered
        log.info("Discovered streams", extra={
            "pattern": self.config.stream_pattern,
            "count": len(streams),
        })
        return streams

    async def get_checkpoint(self, stream: str) -> str:
        """Get last processed Redis ID for a stream."""
        checkpoint = await self._redis.hget(CHECKPOINT_KEY, stream)
        return checkpoint or "0-0"

    async def set_checkpoint(self, stream: str, redis_id: str) -> None:
        """Store checkpoint for a stream."""
        await self._redis.hset(CHECKPOINT_KEY, stream, redis_id)

    async def ensure_consumer_group(self, stream: str) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            await self._redis.xgroup_create(
                stream,
                self.config.consumer_group,
                id="$",  # Start from latest for new groups
                mkstream=False,
            )
            log.info("Created consumer group", extra={
                "stream": stream,
                "group": self.config.consumer_group,
            })
        except Exception as e:
            if "BUSYGROUP" in str(e):
                pass  # Group already exists
            else:
                raise

    async def backfill_stream(
        self,
        stream: str,
        on_entries: Callable[[str, list[tuple[str, dict]]], Coroutine[Any, Any, None]],
        batch_size: int = 1000,
        min_start_id: str | None = None,
    ) -> None:
        """Backfill from checkpoint to current end of stream.
        
        Args:
            stream: Redis stream key
            on_entries: Callback receiving (stream, [(redis_id, data), ...])
            batch_size: Number of entries per XRANGE call
        """
        last_id = await self.get_checkpoint(stream)
        total_processed = 0

        # If we're starting from market open (or another min_start_id) and there is no checkpoint yet,
        # it's easy to end up backfilling nothing when Redis streams are stale (no data for today).
        # Emit a clear log in that case to avoid “it isn't working” confusion.
        if min_start_id and last_id == "0-0":
            try:
                latest = await self._redis.xrevrange(stream, max="+", min="-", count=1)
                if latest:
                    latest_id = latest[0][0]
                    latest_ms = int(latest_id.split("-")[0])
                    min_ms = int(min_start_id.split("-")[0])
                    if latest_ms < min_ms:
                        log.info(
                            "Stream has no entries since min_start_id; backfill will be empty",
                            extra={
                                "redis_stream": stream,
                                "min_start_id": min_start_id,
                                "latest_id": latest_id,
                            },
                        )
            except Exception:
                # Best-effort diagnostic only.
                pass
        
        log.info("Starting backfill", extra={
            "redis_stream": stream,
            "from_id": last_id,
            "min_start_id": min_start_id,
        })
        
        while True:
            # Choose a min ID for XRANGE.
            # - If a checkpoint exists: use exclusive start to avoid duplicates.
            # - If min_start_id is provided and checkpoint is older than that: start from min_start_id (inclusive).
            start_id: str
            if min_start_id and last_id != "0-0":
                try:
                    last_ms = int(last_id.split("-")[0])
                    min_ms = int(min_start_id.split("-")[0])
                except Exception:
                    last_ms = None
                    min_ms = None

                if last_ms is not None and min_ms is not None and last_ms < min_ms:
                    start_id = min_start_id  # inclusive
                else:
                    start_id = f"({last_id}"  # exclusive
            elif min_start_id and last_id == "0-0":
                start_id = min_start_id
            else:
                start_id = f"({last_id}" if last_id != "0-0" else "-"
            entries = await self._redis.xrange(
                stream,
                min=start_id,
                max="+",
                count=batch_size,
            )
            
            if not entries:
                break
            
            await on_entries(stream, entries)
            
            # Update checkpoint to last processed ID
            last_id = entries[-1][0]
            await self.set_checkpoint(stream, last_id)
            total_processed += len(entries)
            
            log.debug("Backfill progress", extra={
                "redis_stream": stream,
                "batch_size": len(entries),
                "last_id": last_id,
            })
        
        log.info("Backfill complete", extra={
            "redis_stream": stream,
            "total_entries": total_processed,
        })

    async def read_live(
        self,
        streams: list[str],
        on_entries: Callable[[str, list[tuple[str, dict]]], Coroutine[Any, Any, None]],
    ) -> None:
        """Read live entries using consumer groups.
        
        Args:
            streams: List of stream keys to read from
            on_entries: Callback receiving (stream, [(redis_id, data), ...])
        """
        self._running = True
        
        # Ensure consumer groups exist
        for stream in streams:
            await self.ensure_consumer_group(stream)
        
        log.info("Starting live read", extra={
            "streams": len(streams),
            "consumer_group": self.config.consumer_group,
            "consumer_name": self.config.consumer_name,
        })
        
        # Build stream dict for XREADGROUP: {stream: ">"} means new messages only
        stream_dict = {s: ">" for s in streams}
        retry_count = 0
        max_retries = 10
        
        while self._running:
            try:
                result = await self._redis.xreadgroup(
                    groupname=self.config.consumer_group,
                    consumername=self.config.consumer_name,
                    streams=stream_dict,
                    count=1000,
                    block=self.config.block_ms,
                )
                
                retry_count = 0  # Reset on success
                
                if not result:
                    continue
                
                for stream_key, entries in result:
                    if entries:
                        await on_entries(stream_key, entries)
                        
                        # ACK after successful processing
                        entry_ids = [e[0] for e in entries]
                        await self._redis.xack(
                            stream_key,
                            self.config.consumer_group,
                            *entry_ids,
                        )
                        
                        # Update checkpoint
                        await self.set_checkpoint(stream_key, entries[-1][0])
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    log.error("Max retries exceeded in live read", extra={"error": str(e)})
                    raise
                
                # Exponential backoff with jitter
                wait_time = min(30, (2 ** retry_count) + random.uniform(0, 1))
                log.warning("Error in live read, retrying", extra={
                    "error": str(e),
                    "retry_count": retry_count,
                    "wait_seconds": wait_time,
                })
                await asyncio.sleep(wait_time)
        
        log.info("Live read stopped")
