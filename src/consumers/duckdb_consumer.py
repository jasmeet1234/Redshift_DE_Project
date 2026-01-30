from __future__ import annotations

import json
import logging
import time
from typing import Dict, Iterable, List, Tuple

import duckdb
from kafka import KafkaConsumer

from src.common.schema import QueryMetricsEvent
from src.common.settings import Settings
from src.storage.duckdb_client import DuckDBClient

logger = logging.getLogger(__name__)


PROCESSED_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {processed_table} (
    query_id TEXT NOT NULL,
    deployment_type TEXT NOT NULL,
    instance_id TEXT,
    arrival_timestamp TIMESTAMPTZ NOT NULL,
    execution_start_time TIMESTAMPTZ NOT NULL,
    execution_end_time TIMESTAMPTZ NOT NULL,

    queue_duration_ms INTEGER NOT NULL,
    compile_duration_ms INTEGER NOT NULL,
    execution_duration_ms INTEGER NOT NULL,

    scanned_mb DOUBLE NOT NULL,
    spilled_mb DOUBLE NOT NULL,

    duration_seconds DOUBLE NOT NULL,
    spill_pressure DOUBLE NOT NULL,
    queued BOOLEAN NOT NULL
);
"""

ROLLUPS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {rollups_table} (
    window_start TIMESTAMPTZ NOT NULL,
    deployment_type TEXT NOT NULL,

    query_count BIGINT NOT NULL,
    avg_duration_seconds DOUBLE NOT NULL,
    avg_spill_pressure DOUBLE NOT NULL,
    queued_ratio DOUBLE NOT NULL
);
"""

INSERT_PROCESSED_SQL = """
INSERT INTO {processed_table} VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
);
"""

ROLLUP_AGG_SQL = """
INSERT INTO {rollups_table}
SELECT
    date_trunc('minute', arrival_timestamp) AS window_start,
    deployment_type,
    COUNT(*) AS query_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    AVG(spill_pressure) AS avg_spill_pressure,
    AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM {processed_table}
WHERE arrival_timestamp >= now() - INTERVAL '5 minutes'
GROUP BY 1, 2;
"""


def _make_consumer(settings: Settings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka.topics.processed_query_metrics,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_groups.get("duckdb_writer"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=1000,
    )


def _ensure_tables(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    tables = settings.storage.duckdb.tables
    con.execute(PROCESSED_SCHEMA_SQL.format(processed_table=tables["processed"]))
    con.execute(ROLLUPS_SCHEMA_SQL.format(rollups_table=tables["rollups"]))


def _insert_batch(
    con: duckdb.DuckDBPyConnection,
    events: Iterable[QueryMetricsEvent],
    settings: Settings,
) -> int:
    rows = [
        (
            e.query_id,
            e.deployment_type,
            e.instance_id,
            e.arrival_timestamp,
            e.execution_start_time,
            e.execution_end_time,
            e.queue_duration_ms,
            e.compile_duration_ms,
            e.execution_duration_ms,
            e.scanned_mb,
            e.spilled_mb,
            e.duration_seconds,
            e.spill_pressure,
            e.queued,
        )
        for e in events
    ]
    if not rows:
        return 0

    tables = settings.storage.duckdb.tables
    con.executemany(INSERT_PROCESSED_SQL.format(processed_table=tables["processed"]), rows)
    return len(rows)


def _collect_events(batch) -> Dict[str, QueryMetricsEvent]:
    """
    Build a de-duped dict of events from polled Kafka records.
    Uses (query_id, arrival_timestamp) composite as a stable key.
    """
    events: Dict[str, QueryMetricsEvent] = {}
    for _tp, records in batch.items():
        for r in records:
            try:
                evt = QueryMetricsEvent.model_validate(r.value)
                events[f"{evt.query_id}-{evt.arrival_timestamp.isoformat()}"] = evt
            except Exception as e:
                logger.debug("Dropping invalid event: %s", e)
    return events


def _is_duckdb_recoverable_error(e: Exception) -> bool:
    # DuckDB errors we can retry by reconnecting / waiting
    return isinstance(
        e,
        (
            duckdb.ConnectionException,
            duckdb.IOException,
            duckdb.CatalogException,
            duckdb.TransactionException,
        ),
    )


def run_duckdb_consumer(settings: Settings) -> None:
    logger.info("Starting DuckDB consumer")

    consumer = _make_consumer(settings)
    db_client = DuckDBClient.from_settings(settings)

    # Backoff on DB issues (Windows locks, transient IO, etc.)
    retry_sleep_s = 0.5
    max_retry_sleep_s = 8.0

    con: duckdb.DuckDBPyConnection | None = None

    def connect_or_reconnect() -> duckdb.DuckDBPyConnection:
        nonlocal con
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        con = db_client.connect()
        _ensure_tables(con, settings)
        return con

    con = connect_or_reconnect()

    while True:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            continue

        events = _collect_events(batch)
        if not events:
            continue

        # Insert with retry; commit Kafka offsets only after DB commit succeeded
        while True:
            try:
                assert con is not None

                # Explicit transaction (more reliable than nested context managers on Windows)
                con.execute("BEGIN;")
                inserted = _insert_batch(con, events.values(), settings)
                con.execute(
                    ROLLUP_AGG_SQL.format(
                        processed_table=settings.storage.duckdb.tables["processed"],
                        rollups_table=settings.storage.duckdb.tables["rollups"],
                    )
                )
                con.execute("COMMIT;")

                consumer.commit()
                logger.info("Persisted %d events to DuckDB", inserted)

                # Reset backoff after success
                retry_sleep_s = 0.5
                break

            except Exception as e:
                # Roll back if possible
                try:
                    if con is not None:
                        con.execute("ROLLBACK;")
                except Exception:
                    pass

                if not _is_duckdb_recoverable_error(e):
                    logger.exception("Fatal DuckDB error; exiting consumer: %s", e)
                    raise

                logger.warning("DuckDB error (will retry): %s", e)
                time.sleep(retry_sleep_s)
                retry_sleep_s = min(max_retry_sleep_s, retry_sleep_s * 2)

                # Reconnect and retry same batch (offsets NOT committed yet)
                con = connect_or_reconnect()