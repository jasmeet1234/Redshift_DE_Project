from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

import duckdb

from src.common.settings import Settings

logger = logging.getLogger(__name__)


class DuckDBClient:
    """
    DuckDB client with Windows-friendly concurrency settings.

    Key points:
    - WAL mode allows concurrent readers while a writer is active.
    - busy_timeout makes connections wait for locks briefly (helps Streamlit UI).
    - read_only connections for UI reduce lock contention.
    """

    def __init__(
        self,
        db_path: str,
        *,
        read_only: bool = False,
        busy_timeout_ms: int = 10_000,
        enable_wal: bool = True,
    ):
        self.db_path = db_path
        self.read_only = read_only
        self.busy_timeout_ms = busy_timeout_ms
        self.enable_wal = enable_wal

    @classmethod
    def from_settings(cls, settings: Settings) -> "DuckDBClient":
        # Default to read/write client here.
        # For Streamlit UI you should create DuckDBClient(..., read_only=True)
        return cls(db_path=settings.storage.duckdb.path)

    def connect(self) -> duckdb.DuckDBPyConnection:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        # read_only=True is important for UI processes
        con = duckdb.connect(self.db_path, read_only=self.read_only)

        # Wait for locks instead of failing immediately
        try:
            con.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_ms)};")
        except Exception as e:
            logger.debug("Could not set busy_timeout: %s", e)

        # WAL helps concurrent reads while a writer is active
        if self.enable_wal and not self.read_only:
            try:
                con.execute("PRAGMA journal_mode='wal';")
            except Exception as e:
                logger.debug("Could not enable WAL journal_mode: %s", e)

        return con

    # ----------------------------
    # Helpers
    # ----------------------------
    def execute(self, sql: str, params: Optional[Iterable[Any]] = None) -> None:
        """Execute SQL without returning results."""
        with self.connect() as con:
            if params is None:
                con.execute(sql)
            else:
                con.execute(sql, params)

    def fetchall(self, sql: str, params: Optional[Iterable[Any]] = None) -> List[tuple]:
        """Execute SQL and return all rows."""
        with self.connect() as con:
            if params is None:
                return con.execute(sql).fetchall()
            return con.execute(sql, params).fetchall()

    def fetchdf(self, sql: str, params: Optional[Sequence[Any]] = None):
        """Execute SQL and return a pandas DataFrame."""
        with self.connect() as con:
            if params is None:
                return con.execute(sql).df()
            return con.execute(sql, params).df()

    # Convenience: create a read-only view of the same DB (ideal for Streamlit)
    def as_read_only(self, *, busy_timeout_ms: Optional[int] = None) -> "DuckDBClient":
        return DuckDBClient(
            db_path=self.db_path,
            read_only=True,
            busy_timeout_ms=self.busy_timeout_ms if busy_timeout_ms is None else busy_timeout_ms,
            enable_wal=False,  # WAL is a writer-side concern
        )