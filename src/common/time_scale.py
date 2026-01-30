from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class TimeScaler:
    """
    Maps event-time to wall-time with a constant speedup factor.

    Example:
      factor=60 means 60 seconds of event time become 1 second of wall time.
    """
    factor: int
    anchor_event_time: datetime
    anchor_wall_time: datetime

    def __post_init__(self) -> None:
        if self.factor <= 0:
            raise ValueError("factor must be > 0")
        if self.anchor_event_time.tzinfo is None:
            raise ValueError("anchor_event_time must be timezone-aware")
        if self.anchor_wall_time.tzinfo is None:
            raise ValueError("anchor_wall_time must be timezone-aware")

    def to_wall_time(self, event_time: datetime) -> datetime:
        if event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")

        delta = event_time - self.anchor_event_time
        scaled = timedelta(seconds=delta.total_seconds() / self.factor)
        return self.anchor_wall_time + scaled

    def sleep_seconds_until(self, target_wall_time: datetime, now: datetime) -> float:
        """
        Utility to compute how long to sleep until target time.
        Returns 0 if target already passed.
        """
        if target_wall_time.tzinfo is None or now.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")

        seconds = (target_wall_time - now).total_seconds()
        return max(0.0, seconds)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)