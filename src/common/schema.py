from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_ALLOWED_DEPLOYMENT_TYPES = {"provisioned", "serverless", "unknown"}


def _to_utc(dt: datetime) -> datetime:
    # Ensure tz-aware UTC for consistent streaming/storage.
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class QueryMetricsEvent(BaseModel):
    """
    Canonical Kafka message schema for a single query event (cleaned + enriched).

    This is the "source of truth" stream written to DuckDB and optionally copied to Redshift.
    """

    model_config = ConfigDict(extra="forbid")

    # identity
    query_id: str = Field(min_length=1)
    deployment_type: str = Field(description="provisioned | serverless | unknown")
    instance_id: Optional[str] = None

    # timestamps
    arrival_timestamp: datetime

    # durations (ms)
    queue_duration_ms: int = Field(ge=0)
    compile_duration_ms: int = Field(ge=0)
    execution_duration_ms: int = Field(ge=0)

    # io / memory
    scanned_mb: float = Field(ge=0)
    spilled_mb: float = Field(ge=0)

    # derived timestamps
    execution_start_time: datetime
    execution_end_time: datetime

    # derived metrics (useful for analytics + UI)
    duration_seconds: float = Field(ge=0)
    spill_pressure: float = Field(ge=0, description="spilled_mb / max(scanned_mb, 1)")
    queued: bool = Field(description="True if queue_duration_ms > 0")

    @field_validator("deployment_type")
    @classmethod
    def _validate_deployment_type(cls, v: str) -> str:
        vv = (v or "").strip().lower()
        if vv not in _ALLOWED_DEPLOYMENT_TYPES:
            return "unknown"
        return vv

    @field_validator(
        "arrival_timestamp",
        "execution_start_time",
        "execution_end_time",
        mode="before",
    )
    @classmethod
    def _coerce_dt_to_utc(cls, v):
        # Let Pydantic parse strings; once it is datetime, make it UTC.
        if isinstance(v, datetime):
            return _to_utc(v)
        return v

    @model_validator(mode="after")
    def _validate_time_order(self) -> "QueryMetricsEvent":
        # Enforce coherent timestamps.
        if self.execution_end_time < self.execution_start_time:
            raise ValueError("execution_end_time must be >= execution_start_time")
        # arrival can be before start (e.g., queueing) but should not be after end in sane data
        if self.arrival_timestamp > self.execution_end_time:
            raise ValueError("arrival_timestamp must be <= execution_end_time")
        return self


class UiQueryMetricsEvent(BaseModel):
    """
    UI streaming payload.

    This is what the Streamlit app consumes for real-time tables/charts.
    It is intentionally denormalized and display-friendly.
    """

    model_config = ConfigDict(extra="forbid")

    query_id: str = Field(min_length=1)
    deployment_type: str
    instance_id: Optional[str] = None

    # Key times (UTC)
    arrival_timestamp: datetime
    execution_start_time: datetime
    execution_end_time: datetime

    # Key metrics (already cleaned)
    queue_duration_ms: int = Field(ge=0)
    compile_duration_ms: int = Field(ge=0)
    execution_duration_ms: int = Field(ge=0)
    scanned_mb: float = Field(ge=0)
    spilled_mb: float = Field(ge=0)

    # Derived / UI conveniences
    duration_seconds: float = Field(ge=0)
    spill_pressure: float = Field(ge=0)
    queued: bool

    @classmethod
    def from_canonical(cls, e: QueryMetricsEvent) -> "UiQueryMetricsEvent":
        return cls(
            query_id=e.query_id,
            deployment_type=e.deployment_type,
            instance_id=e.instance_id,
            arrival_timestamp=e.arrival_timestamp,
            execution_start_time=e.execution_start_time,
            execution_end_time=e.execution_end_time,
            queue_duration_ms=e.queue_duration_ms,
            compile_duration_ms=e.compile_duration_ms,
            execution_duration_ms=e.execution_duration_ms,
            scanned_mb=e.scanned_mb,
            spilled_mb=e.spilled_mb,
            duration_seconds=e.duration_seconds,
            spill_pressure=e.spill_pressure,
            queued=e.queued,
        )