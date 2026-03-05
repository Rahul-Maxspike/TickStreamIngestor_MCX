"""Parse Redis tick stream entries into ClickHouse row format."""

import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from decimal import InvalidOperation

log = logging.getLogger(__name__)


try:
    _IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    # Windows may not ship IANA tzdata; fall back to fixed IST offset.
    _IST = timezone(timedelta(hours=5, minutes=30))


def _to_ist_naive(dt: datetime) -> datetime:
    """Normalize datetimes to IST, stored as naive values.

    ClickHouse columns are declared with Asia/Kolkata where applicable.
    Using naive IST values avoids ambiguity with client-side timezone conversions.
    """
    if dt.tzinfo is None:
        # Assume already in IST if timezone is missing (as observed in payloads).
        return dt.replace(tzinfo=None)
    return dt.astimezone(_IST).replace(tzinfo=None)


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _safe_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _loads_json_maybe(value: Any) -> Any:
    """Best-effort JSON loader.

    - bytes -> decode
    - str -> json.loads
    - dict/list -> returned as-is
    - anything else -> returned as-is
    """
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        value = _as_str(value)
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return value


def extract_segment(stream_key: str) -> str:
    """Extract instrument segment from stream key.
    
    e.g. tick_stream:options:12345 -> options
         tick_stream:futures:67890 -> futures
    """
    parts = stream_key.split(":")
    if len(parts) >= 2:
        return parts[1]
    return "unknown"


def _flatten_depth_levels(
    levels: list[dict] | None,
    side: str,
    max_levels: int = 5,
) -> dict[str, Any]:
    """Flatten depth levels into fixed columns.

    Produces keys like:
    - bid1_price, bid1_qty, bid1_orders ... bid5_orders
    - ask1_price, ask1_qty, ask1_orders ... ask5_orders
    """
    if side not in {"bid", "ask"}:
        raise ValueError("side must be 'bid' or 'ask'")

    data: dict[str, Any] = {}
    levels = levels or []

    for i in range(1, max_levels + 1):
        level = levels[i - 1] if len(levels) >= i else {}
        price = Decimal(str(level.get("price", 0) or 0))
        qty = int(level.get("quantity", level.get("qty", 0)) or 0)
        orders = int(level.get("orders", 0) or 0)

        data[f"{side}{i}_price"] = price
        data[f"{side}{i}_qty"] = qty
        data[f"{side}{i}_orders"] = orders

    return data


def _parse_iso_datetime(value: str) -> datetime | None:
    """Parse ISO8601-ish datetimes seen in ticks.

    Observed examples from Redis (via scripts/probe_redis.py):
    - "2025-12-11T12:35:30" (no timezone)
    - Potentially "...Z" (UTC) in some environments
    """
    try:
        # Handle common UTC suffix
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        return _truncate_to_seconds(_to_ist_naive(dt))
    except Exception:
        return None


def _parse_exchange_time(tick: dict[str, Any], redis_id: str) -> datetime:
    """Compute exchange_time.

    Preference order:
    1) tick["exchange_timestamp"] if parseable (string ISO or epoch-ms)
    2) Redis stream ID timestamp (ms since epoch)
    """
    exchange_ts = tick.get("exchange_timestamp")

    # Some feeds use epoch-ms; others use ISO strings.
    if isinstance(exchange_ts, (int, float)):
        dt_utc = datetime.fromtimestamp(float(exchange_ts) / 1000.0, tz=timezone.utc)
        return _truncate_to_seconds(_to_ist_naive(dt_utc))
    if isinstance(exchange_ts, str) and exchange_ts:
        parsed = _parse_iso_datetime(exchange_ts)
        if parsed is not None:
            return parsed

    ts_part = redis_id.split("-")[0]
    dt_utc = datetime.fromtimestamp(int(ts_part) / 1000.0, tz=timezone.utc)
    return _truncate_to_seconds(_to_ist_naive(dt_utc))


def parse_tick_entry(
    redis_stream: str,
    redis_id: str,
    raw_data: dict[str, Any],
) -> dict[str, Any] | None:
    """Parse a single Redis stream entry into a ClickHouse row dict.
    
    Args:
        redis_stream: The Redis stream key (e.g., tick_stream:options:12345)
        redis_id: The Redis entry ID (e.g., 1702631234567-0)
        raw_data: The raw entry data from Redis
        
    Returns:
        Dict with ClickHouse column values, or None if parsing fails.
    """
    try:
        # Extract data field and parse nested JSON.
        # The canonical format is: raw_data['data'] = JSON string with {"tick": "{...}"}
        # But we tolerate variants (tick already a dict, or data already the tick).
        data_val = raw_data.get("data") if isinstance(raw_data, dict) else None
        if data_val is None and isinstance(raw_data, dict):
            # In case the upstream uses bytes keys.
            data_val = raw_data.get(b"data")

        outer = _loads_json_maybe(data_val)
        if not isinstance(outer, dict):
            outer = {}

        tick_val = outer.get("tick")
        if tick_val is None:
            # Some deployments may store the tick directly in 'data'.
            tick_val = outer

        tick_obj = _loads_json_maybe(tick_val)
        if not isinstance(tick_obj, dict):
            return None
        tick: dict[str, Any] = tick_obj
        
        # Extract depth
        depth = tick.get("depth") or {}
        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])
        
        # Flatten depth into fixed L2 columns.
        bid_levels = _flatten_depth_levels(buy_depth, side="bid")
        ask_levels = _flatten_depth_levels(sell_depth, side="ask")
        
        # Extract exchange_instrument_id from stream key
        parts = _as_str(redis_stream).split(":")
        exchange_instrument_id = _safe_int(parts[-1], default=0) if parts else 0

        exchange_time = _parse_exchange_time(tick, redis_id)

        # Optional timestamps
        last_trade_time_val = tick.get("last_trade_time")
        last_trade_time = (
            _parse_iso_datetime(last_trade_time_val)
            if isinstance(last_trade_time_val, str) and last_trade_time_val
            else None
        )

        # Optional OHLC
        ohlc = tick.get("ohlc") or {}
        
        return {
            "exchange_time": exchange_time,
            "exchange_instrument_id": exchange_instrument_id,
            "instrument_segment": extract_segment(redis_stream),
            "redis_stream": redis_stream,
            "redis_id": redis_id,

            # Common tick identity/metadata
            "tradable": 1 if tick.get("tradable") else 0,
            "mode": str(tick.get("mode") or ""),
            "instrument_token": int(tick.get("instrument_token") or 0),
            "exchange_token": str(tick.get("exchange_token") or ""),
            "symbol": str(tick.get("symbol") or ""),
            "last_trade_time": last_trade_time,

            **bid_levels,
            **ask_levels,
            "last_price": _safe_decimal(tick.get("last_price", 0)),
            "last_qty": _safe_int(tick.get("last_quantity", tick.get("last_traded_quantity", 0)), default=0),
            "average_traded_price": _safe_decimal(tick.get("average_traded_price", 0)),
            "volume_traded": _safe_int(tick.get("volume", tick.get("volume_traded", 0)), default=0),
            "total_buy_quantity": _safe_int(tick.get("total_buy_quantity", 0), default=0),
            "total_sell_quantity": _safe_int(tick.get("total_sell_quantity", 0), default=0),
            "change": _safe_decimal(tick.get("change", 0)),
            "oi": _safe_int(tick.get("oi", tick.get("open_interest", 0)), default=0),
            "oi_day_high": _safe_int(tick.get("oi_day_high", 0), default=0),
            "oi_day_low": _safe_int(tick.get("oi_day_low", 0), default=0),

            "ohlc_open": _safe_decimal(ohlc.get("open", 0)),
            "ohlc_high": _safe_decimal(ohlc.get("high", 0)),
            "ohlc_low": _safe_decimal(ohlc.get("low", 0)),
            "ohlc_close": _safe_decimal(ohlc.get("close", 0)),
        }
    except Exception as e:
        # Include the exception in the message (extras are not shown by default log format).
        log.warning(
            f"Failed to parse tick entry: {type(e).__name__}: {e}",
            extra={
                "redis_stream": redis_stream,
                "redis_id": redis_id,
            },
        )
        return None
