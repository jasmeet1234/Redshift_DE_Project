from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from dateutil import parser as dt_parser

from src.common.schema import QueryMetricsEvent
from src.common.settings import ProcessingConfig


def _to_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        if x != x:  # NaN
            return None
        return x
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        # Some parquet readers yield numpy scalars; int() handles them.
        return int(v)
    except Exception:
        try:
            f = float(v)
            if f != f:
                return None
            return int(f)
        except Exception:
            return None


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        try:
            dt = dt_parser.parse(str(v))
        except Exception:
            return None

    # Normalize to UTC tz-aware
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_string(v: Optional[str], *, normalize: bool) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    if not s:
        return None
    return s.lower() if normalize else s


def _clip_nonnegative_int(v: int) -> int:
    return v if v >= 0 else 0


def _clip_nonnegative_float(v: float) -> float:
    return v if v >= 0 else 0.0


def _deployment_type(raw: Optional[str], allowed: set[str]) -> str:
    if not raw:
        return "unknown"
    v = raw.strip().lower()
    return v if v in allowed else "unknown"


def _pick_first(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row:
            return row.get(k)
    return None


def _coerce_ms(value: Any, default: int) -> int:
    iv = _to_int(value)
    return default if iv is None else iv


def _coerce_mb(value: Any, default: float) -> float:
    fv = _to_float(value)
    return default if fv is None else fv


def _coerce_ts(
    value: Any,
    *,
    invalid_action: str,
) -> Optional[datetime]:
    dt = _parse_dt(value)
    if dt is not None:
        return dt
    if invalid_action == "coerce_to_null":
        return None
    return None


def map_clean_enrich_row(row: Dict[str, Any], processing: ProcessingConfig) -> QueryMetricsEvent:
    """
    Map a raw parquet row into a cleaned + enriched QueryMetricsEvent.

    The Redset dataset has variations in naming between different exports, so we map
    with fallbacks for common aliases.
    """
    mv = processing.missing_values
    inc = processing.inconsistencies
    enr = processing.enrichment

    normalize = mv.normalize_strings

    # ---- identity fields ----
    query_id = _normalize_string(_to_str(_pick_first(row, "query_id", "queryid", "query")), normalize=normalize)
    if query_id is None:
        # Strong contract: must have an id
        raise ValueError("Missing query_id")

    deployment_raw = _normalize_string(_to_str(_pick_first(row, "deployment_type", "deployment", "cluster_type")), normalize=normalize)
    deployment_type = _deployment_type(deployment_raw, set(inc.allowed_deployment_types))

    instance_id = _normalize_string(_to_str(_pick_first(row, "instance_id", "cluster_identifier", "cluster_id")), normalize=normalize)

    # ---- timestamps ----
    arrival_ts = _coerce_ts(_pick_first(row, "arrival_timestamp", "arrival_time", "arrived_at"), invalid_action=mv.timestamp_invalid_action)
    if arrival_ts is None:
        # arrival is required for time series ordering
        raise ValueError("Missing or invalid arrival_timestamp")

    # Some datasets may provide explicit start/end; otherwise compute.
    start_ts = _coerce_ts(_pick_first(row, "execution_start_time", "start_time", "exec_start_time"), invalid_action="coerce_to_null")
    end_ts = _coerce_ts(_pick_first(row, "execution_end_time", "end_time", "exec_end_time"), invalid_action="coerce_to_null")

    # ---- durations (ms) ----
    numeric_fill_int = int(mv.numeric_fill_value) if mv.numeric_fill_value is not None else 0

    queue_ms = _coerce_ms(_pick_first(row, "queue_duration_ms", "queue_time_ms", "queue_ms"), numeric_fill_int)
    compile_ms = _coerce_ms(_pick_first(row, "compile_duration_ms", "compile_time_ms", "compile_ms"), numeric_fill_int)
    exec_ms = _coerce_ms(_pick_first(row, "execution_duration_ms", "execution_time_ms", "exec_ms"), numeric_fill_int)

    if inc.clip_negative_durations:
        queue_ms = _clip_nonnegative_int(queue_ms)
        compile_ms = _clip_nonnegative_int(compile_ms)
        exec_ms = _clip_nonnegative_int(exec_ms)

    # ---- io/memory ----
    numeric_fill_float = float(mv.numeric_fill_value) if mv.numeric_fill_value is not None else 0.0

    scanned_mb = _coerce_mb(
        _pick_first(row, "scanned_mb", "scan_mb", "scanned_megabytes", "mbytes_scanned"),
        numeric_fill_float,
    )
    spilled_mb = _coerce_mb(
        _pick_first(row, "spilled_mb", "spill_mb", "spilled_megabytes", "mbytes_spilled"),
        numeric_fill_float,
    )

    if inc.clip_negative_metrics:
        scanned_mb = _clip_nonnegative_float(scanned_mb)
        spilled_mb = _clip_nonnegative_float(spilled_mb)

    # ---- compute times if missing ----
    # If start/end not available, compute based on arrival + durations
    if start_ts is None:
        # arrival -> start after queue
        start_ts = arrival_ts + timedelta(milliseconds=queue_ms)

    if end_ts is None:
        # start -> end after compile+exec (compile often included before execution)
        end_ts = start_ts + timedelta(milliseconds=(compile_ms + exec_ms))

    # If inconsistent, fix if configured
    if inc.enforce_end_after_start and end_ts < start_ts:
        end_ts = start_ts

    # arrival should not exceed end; if it does, clamp arrival to start (best effort)
    if arrival_ts > end_ts:
        arrival_ts = min(arrival_ts, start_ts)

    # ---- derived fields ----
    duration_seconds = 0.0
    if enr.compute_duration_seconds:
        duration_seconds = max(0.0, (end_ts - start_ts).total_seconds())

    spill_pressure = 0.0
    if enr.compute_spill_pressure:
        denom = scanned_mb if scanned_mb > 0 else 1.0
        spill_pressure = max(0.0, spilled_mb / denom)

    queued = False
    if enr.compute_queue_flag:
        queued = queue_ms > 0

    # ---- build canonical event ----
    return QueryMetricsEvent(
        query_id=query_id,
        deployment_type=deployment_type,
        instance_id=instance_id,
        arrival_timestamp=arrival_ts,
        queue_duration_ms=queue_ms,
        compile_duration_ms=compile_ms,
        execution_duration_ms=exec_ms,
        scanned_mb=scanned_mb,
        spilled_mb=spilled_mb,
        execution_start_time=start_ts,
        execution_end_time=end_ts,
        duration_seconds=duration_seconds,
        spill_pressure=spill_pressure,
        queued=queued,
    )
