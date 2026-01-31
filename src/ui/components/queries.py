from __future__ import annotations

from typing import Optional

from src.storage.clickhouse_client import ClickHouseClient


def fetch_recent_processed(
    client: ClickHouseClient,
    *,
    processed_table: str,
    limit: int = 500,
    deployment_type: Optional[str] = None,
) -> "object":
    if deployment_type and deployment_type != "all":
        return client.query_df(
            f"""
            SELECT
                arrival_timestamp,
                query_id,
                dataset_type AS deployment_type,
                instance_id,
                duration_seconds,
                queue_duration_ms,
                compile_duration_ms,
                execution_duration_ms,
                scanned_mb,
                spilled_mb,
                spill_pressure,
                queued
            FROM {processed_table}
            WHERE dataset_type = %(deployment_type)s
            ORDER BY arrival_timestamp DESC
            LIMIT %(limit)s
            """,
            params={"deployment_type": deployment_type, "limit": limit},
        )

    return client.query_df(
        f"""
        SELECT
            arrival_timestamp,
            query_id,
            dataset_type AS deployment_type,
            instance_id,
            duration_seconds,
            queue_duration_ms,
            compile_duration_ms,
            execution_duration_ms,
            scanned_mb,
            spilled_mb,
            spill_pressure,
            queued
        FROM {processed_table}
        ORDER BY arrival_timestamp DESC
        LIMIT %(limit)s
        """,
        params={"limit": limit},
    )


def fetch_rollups_last_minutes(
    client: ClickHouseClient,
    *,
    rollups_table: str,
    minutes: int = 60,
    deployment_type: Optional[str] = None,
) -> "object":
    if deployment_type and deployment_type != "all":
        return client.query_df(
            f"""
            SELECT
                window_start,
                dataset_type AS deployment_type,
                query_count,
                avg_duration_seconds,
                avg_spill_pressure,
                queued_ratio
            FROM {rollups_table}
            WHERE window_start >= now() - INTERVAL %(minutes)s MINUTE
              AND dataset_type = %(deployment_type)s
            ORDER BY window_start ASC
            """,
            params={"deployment_type": deployment_type, "minutes": minutes},
        )

    return client.query_df(
        f"""
        SELECT
            window_start,
            dataset_type AS deployment_type,
            query_count,
            avg_duration_seconds,
            avg_spill_pressure,
            queued_ratio
        FROM {rollups_table}
        WHERE window_start >= now() - INTERVAL %(minutes)s MINUTE
        ORDER BY window_start ASC
        """,
        params={"minutes": minutes},
    )


def fetch_distinct_deployment_types(
    client: ClickHouseClient,
    *,
    processed_table: str,
) -> list[str]:
    df = client.query_df(
        f"SELECT DISTINCT dataset_type AS deployment_type FROM {processed_table} ORDER BY 1"
    )
    return df["deployment_type"].tolist()

SQL_HISTORICAL_DEPLOYMENT_REDSHIFT = """
SELECT
  date_trunc('hour', arrival_timestamp) AS window_start,
  deployment_type,
  COUNT(*) AS query_count,
  AVG(duration_seconds) AS avg_duration_seconds,
  AVG(spill_pressure) AS avg_spill_pressure,
  AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM query_metrics_processed
WHERE arrival_timestamp >= now() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY window_start ASC;
"""

SQL_HISTORICAL_DEPLOYMENT_CLICKHOUSE = """
SELECT
  toStartOfHour(arrival_timestamp) AS window_start,
  dataset_type AS deployment_type,
  COUNT(*) AS query_count,
  AVG(duration_seconds) AS avg_duration_seconds,
  AVG(spill_pressure) AS avg_spill_pressure,
  AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM query_metrics_processed
WHERE arrival_timestamp >= now() - INTERVAL 7 DAY
GROUP BY 1, 2
ORDER BY window_start ASC;
"""

SQL_TOP_QUERIES = """
SELECT
  query_id,
  dataset_type AS deployment_type,
  COUNT(*) AS occurrences,
  AVG(duration_seconds) AS avg_duration_seconds,
  AVG(spill_pressure) AS avg_spill_pressure,
  MAX(arrival_timestamp) AS last_seen
FROM query_metrics_processed
GROUP BY 1, 2
ORDER BY occurrences DESC, last_seen DESC
LIMIT 50;"""
