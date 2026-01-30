from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb

logger = logging.getLogger("duckdb_store")


class DuckDBStore:
    """
    Thin storage layer for DuckDB.

    Responsibilities:
    - open/connect to DuckDB file
    - initialize schema
    - insert query event records
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.con = duckdb.connect(str(self.db_path))
        logger.info("Connected to DuckDB at %s", self.db_path)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def init_schema(self) -> None:
        """
        Create base tables if they do not exist.
        """
        logger.info("Initializing DuckDB schema")

        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS query_events (
                query_id BIGINT,
                user_id BIGINT,
                instance_id BIGINT,
                query_type VARCHAR,

                arrival_timestamp TIMESTAMP,

                execution_duration_ms BIGINT,
                queue_duration_ms BIGINT,
                compile_duration_ms DOUBLE,

                mbytes_scanned DOUBLE,
                mbytes_spilled DOUBLE,

                queue_score DOUBLE,
                scan_score DOUBLE,
                spill_score DOUBLE,
                compile_score DOUBLE
            )
            """
        )

    # ------------------------------------------------------------------
    # Inserts
    # ------------------------------------------------------------------
    def insert_events(self, events: Iterable[Dict]) -> int:
        """
        Insert a batch of event dicts into DuckDB.

        Returns number of rows inserted.
        """
        rows: List[tuple] = []

        for e in events:
            rows.append(
                (
                    e.get("query_id"),
                    e.get("user_id"),
                    e.get("instance_id"),
                    e.get("query_type"),
                    e.get("arrival_timestamp"),
                    e.get("execution_duration_ms"),
                    e.get("queue_duration_ms"),
                    e.get("compile_duration_ms"),
                    e.get("mbytes_scanned"),
                    e.get("mbytes_spilled"),
                    e.get("queue_score"),
                    e.get("scan_score"),
                    e.get("spill_score"),
                    e.get("compile_score"),
                )
            )

        if not rows:
            return 0

        self.con.executemany(
            """
            INSERT INTO query_events VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            rows,
        )

        logger.info("Inserted %s rows into DuckDB", len(rows))
        return len(rows)

    # ------------------------------------------------------------------
    # Queries (for sanity / debugging)
    # ------------------------------------------------------------------
    def count_events(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM query_events").fetchone()[0]

    def close(self) -> None:
        self.con.close()
        logger.info("DuckDB connection closed")