from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import duckdb


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, *, label: str) -> str:
    name = (name or "").strip()
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {label} '{name}'. Must match regex: {_IDENTIFIER_RE.pattern} "
            "(letters/numbers/underscore; cannot start with a number)."
        )
    return name


def _escape_sql_string(value: str) -> str:
    # DuckDB uses single quotes; escape embedded quotes by doubling them
    return value.replace("'", "''")


def _try_enable_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    """
    Best-effort: enable httpfs so DuckDB can read from http(s)/s3 when supported.
    If extension install/load fails (offline env), we keep going. Local files still work.
    """
    try:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
    except Exception:
        pass


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _build_query(
    parquet_url: str,
    *,
    select_clause: str,
    event_time_col: str,
    enforce_event_time_order: bool,
) -> str:
    parquet_sql = _escape_sql_string(str(parquet_url).strip())

    # Prefer stable ordering; NULLS LAST avoids NULL timestamps bubbling to the top.
    if enforce_event_time_order:
        return f"""
            SELECT {select_clause}
            FROM read_parquet('{parquet_sql}')
            ORDER BY {event_time_col} ASC NULLS LAST
        """

    # Unordered scan (still streaming). Useful if event time is missing or malformed.
    return f"""
        SELECT {select_clause}
        FROM read_parquet('{parquet_sql}')
    """


def read_parquet_as_dicts(
    parquet_url: str,
    *,
    event_time_col: str,
    batch_size: int = 1000,
    columns: Optional[List[str]] = None,
    enforce_event_time_order: bool = True,
) -> Iterator[Dict[str, Any]]:
    """
    Stream rows from a Parquet file/URL as dicts.

    - Uses DuckDB read_parquet()
    - Fetches rows in batches to avoid loading everything in memory
    - Validates identifiers to avoid broken SQL/injection
    - Escapes parquet_url for SQL safety
    - If `columns` is provided, ensures event_time_col is included if ordering is enabled
    - If ordering fails and enforce_event_time_order=False, falls back to unordered scan
    """
    if not parquet_url or not str(parquet_url).strip():
        raise ValueError("parquet_url is required")

    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    event_time_col = _validate_identifier(event_time_col, label="event_time_col")

    select_clause = "*"
    safe_cols: Optional[List[str]] = None
    if columns:
        cleaned = [(c or "").strip() for c in columns if (c or "").strip()]
        cleaned = _dedupe_preserve_order(cleaned)
        safe_cols = [_validate_identifier(c, label="column") for c in cleaned]

        # Ensure event_time_col is present for ORDER BY correctness
        if enforce_event_time_order and event_time_col not in safe_cols:
            safe_cols = [event_time_col] + safe_cols

        select_clause = ", ".join(safe_cols)

    con = duckdb.connect()
    try:
        _try_enable_httpfs(con)

        query = _build_query(
            parquet_url,
            select_clause=select_clause,
            event_time_col=event_time_col,
            enforce_event_time_order=enforce_event_time_order,
        )

        try:
            cur = con.execute(query)
        except Exception:
            # If strict ordering is off, attempt a fallback unordered scan.
            if enforce_event_time_order:
                raise
            fallback_query = _build_query(
                parquet_url,
                select_clause=select_clause,
                event_time_col=event_time_col,
                enforce_event_time_order=False,
            )
            cur = con.execute(fallback_query)

        colnames = [d[0] for d in cur.description]

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield dict(zip(colnames, row))
    finally:
        con.close()


@dataclass(frozen=True)
class ParquetReader:
    """
    Thin wrapper around read_parquet_as_dicts() so other modules can just iterate.
    """
    parquet_url: str
    event_time_col: str = "arrival_timestamp"
    batch_size: int = 1000
    columns: Optional[List[str]] = None
    enforce_event_time_order: bool = True

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return read_parquet_as_dicts(
            self.parquet_url,
            event_time_col=self.event_time_col,
            batch_size=self.batch_size,
            columns=self.columns,
            enforce_event_time_order=self.enforce_event_time_order,
        )