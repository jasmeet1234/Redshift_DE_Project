from __future__ import annotations

import os

import clickhouse_connect


def _get_client():
    host = os.environ.get("CH_HOST", "localhost")
    port = int(os.environ.get("CH_PORT", "8123"))
    user = os.environ.get("CH_USER", "default")
    password = os.environ.get("CH_PASSWORD", "")
    database = os.environ.get("CH_DB", "default")
    secure = os.environ.get("CH_SECURE", "false").lower() == "true"

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
        secure=secure,
        verify=False if secure else True,
    )


def main() -> None:
    client = _get_client()

    client.command(
        """
        CREATE TABLE IF NOT EXISTS raw_events (
          run_id String,
          arrival_timestamp DateTime64(6, 'UTC'),
          deployment_type LowCardinality(String),
          queue_duration_ms UInt32,
          execution_duration_ms UInt32,
          mbytes_scanned Float64,
          mbytes_spilled Float64,
          instance_id Nullable(UInt64),
          inserted_at DateTime('UTC') DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY run_id
        ORDER BY (run_id, arrival_timestamp, deployment_type)
        """
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS system_metrics_5min (
          run_id String,
          bucket_start DateTime('UTC'),
          deployment_type LowCardinality(String),
          running_count UInt64,
          queued_count UInt64,
          queue_pressure Float64,
          spill_pressure Float64,
          pressure_level LowCardinality(String),
          throughput_mb_s Float64,
          inserted_at DateTime('UTC') DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(inserted_at)
        PARTITION BY run_id
        ORDER BY (run_id, bucket_start, deployment_type)
        """
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS instance_topk_5min (
          run_id String,
          bucket_start DateTime('UTC'),
          deployment_type LowCardinality(String),
          instance_id UInt64,
          running_count UInt64,
          scanned_mb Float64,
          spilled_mb Float64,
          inserted_at DateTime('UTC') DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(inserted_at)
        PARTITION BY run_id
        ORDER BY (run_id, bucket_start, deployment_type, instance_id)
        """
    )

    client.command("DROP VIEW IF EXISTS mv_system_metrics_5min")
    client.command("DROP VIEW IF EXISTS mv_instance_metrics_5min")

    client.command(
        """
        CREATE MATERIALIZED VIEW mv_system_metrics_5min
        TO system_metrics_5min
        AS
        WITH base AS (
          SELECT
            run_id,
            toStartOfInterval(toDateTime(arrival_timestamp), INTERVAL 5 MINUTE) AS bucket_start,
            deployment_type,
            count() AS running_count,
            sum(queue_duration_ms > 0) AS queued_count,
            sum(mbytes_scanned) AS scanned_mb,
            sum(mbytes_spilled) AS spilled_mb,
            sum(execution_duration_ms) AS exec_ms
          FROM raw_events
          GROUP BY run_id, bucket_start, deployment_type
        )
        SELECT
          run_id,
          bucket_start,
          deployment_type,
          running_count,
          queued_count,
          if((queued_count + running_count) > 0,
             queued_count / (queued_count + running_count),
             0.0) AS queue_pressure,
          if((scanned_mb + spilled_mb) > 0,
             spilled_mb / (scanned_mb + spilled_mb),
             0.0) AS spill_pressure,
          multiIf(
            (spill_pressure > 0.6) OR (queue_pressure > 0.6), 'HIGH',
            (spill_pressure > 0.3) OR (queue_pressure > 0.3), 'MEDIUM',
            'LOW'
          ) AS pressure_level,
          if(exec_ms > 0, scanned_mb / (exec_ms / 1000.0), 0.0) AS throughput_mb_s,
          now() AS inserted_at
        FROM base
        """
    )

    client.command(
        """
        CREATE MATERIALIZED VIEW mv_instance_metrics_5min
        TO instance_topk_5min
        AS
        SELECT
          run_id,
          toStartOfInterval(toDateTime(arrival_timestamp), INTERVAL 5 MINUTE) AS bucket_start,
          deployment_type,
          toUInt64(instance_id) AS instance_id,
          count() AS running_count,
          sum(mbytes_scanned) AS scanned_mb,
          sum(mbytes_spilled) AS spilled_mb,
          now() AS inserted_at
        FROM raw_events
        WHERE instance_id IS NOT NULL
        GROUP BY run_id, bucket_start, deployment_type, instance_id
        """
    )

    client.command(
        """
        CREATE OR REPLACE VIEW instance_topk_5min_dedup AS
        SELECT
          run_id,
          bucket_start,
          deployment_type,
          instance_id,
          argMax(running_count, inserted_at) AS running_count
        FROM instance_topk_5min
        GROUP BY run_id, bucket_start, deployment_type, instance_id
        """
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS instance_capacity_p95 (
          run_id String,
          deployment_type LowCardinality(String),
          instance_id UInt64,
          cap_p95 Float64,
          computed_at DateTime('UTC') DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(computed_at)
        ORDER BY (run_id, deployment_type, instance_id)
        """
    )

    client.command(
        """
        CREATE OR REPLACE VIEW instance_util_pred_5min AS
        WITH base AS (
          SELECT
            d.run_id,
            d.bucket_start,
            d.deployment_type,
            d.instance_id,
            d.running_count,
            (d.running_count / greatest(c.cap_p95, 1)) AS util
          FROM instance_topk_5min_dedup d
          LEFT JOIN instance_capacity_p95 c
            ON d.run_id = c.run_id
           AND d.deployment_type = c.deployment_type
           AND d.instance_id = c.instance_id
        )
        SELECT
          *,
          median(util) OVER (
            PARTITION BY run_id, deployment_type, instance_id
            ORDER BY bucket_start
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
          ) AS util_pred,
          (util - util_pred) AS util_residual
        FROM base
        """
    )

    client.command(
        """
        CREATE OR REPLACE VIEW instance_top_active AS
        SELECT
          run_id,
          deployment_type,
          instance_id,
          argMax(util, bucket_start) AS util_latest
        FROM instance_util_pred_5min
        GROUP BY run_id, deployment_type, instance_id
        """
    )

    print("✅ ClickHouse tables + materialized views are ready.")


if __name__ == "__main__":
    main()
