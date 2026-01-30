from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def ensure_utc(dt: datetime) -> datetime:
    """
    Ensure datetime is timezone-aware UTC.
    If naive -> assume UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clip_non_negative_int(x: int) -> int:
    return x if x >= 0 else 0


def clip_non_negative_float(x: float) -> float:
    return x if x >= 0 else 0.0


def floor_to_bucket(dt: datetime, bucket_minutes: int) -> datetime:
    """
    Floor UTC datetime to bucket boundary, e.g. 5-min buckets.
    """
    dt = ensure_utc(dt)
    minute = (dt.minute // bucket_minutes) * bucket_minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


def safe_get(d: dict, key: str, default: Optional[Any] = None) -> Any:
    return d.get(key, default) if isinstance(d, dict) else default