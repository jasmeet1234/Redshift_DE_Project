-- =========================================================
-- 1) Populate dim_time_5m for the required range
--    (This uses a numbers table pattern; create a temp sequence via cross-joins)
-- =========================================================

-- Determine bounds (aligned to 5 minutes)
DROP TABLE IF EXISTS analytics._bounds;
CREATE TEMP TABLE analytics._bounds AS
SELECT
  date_trunc('minute', min(arrival_timestamp)) AS min_ts,
  date_trunc('minute', max(execution_end_time)) AS max_ts
FROM analytics.query_metrics_raw;

-- Build a temp numbers set up to ~1,000,000 steps (adjust if needed)
DROP TABLE IF EXISTS analytics._nums;
CREATE TEMP TABLE analytics._nums (n INT);

INSERT INTO analytics._nums
SELECT
  (a.n * 1000) + b.n AS n
FROM (
  SELECT row_number() over() - 1 AS n
  FROM (SELECT 1 FROM information_schema.columns LIMIT 1000) t
) a
CROSS JOIN (
  SELECT row_number() over() - 1 AS n
  FROM (SELECT 1 FROM information_schema.columns LIMIT 1000) t
) b;

-- Insert missing buckets
INSERT INTO analytics.dim_time_5m (bucket_start, bucket_end)
SELECT
  dateadd(minute, 5 * n, (SELECT min_ts FROM analytics._bounds)) AS bucket_start,
  dateadd(minute, 5 * n + 5, (SELECT min_ts FROM analytics._bounds)) AS bucket_end
FROM analytics._nums
WHERE dateadd(minute, 5 * n, (SELECT min_ts FROM analytics._bounds))
      <= (SELECT max_ts FROM analytics._bounds)
  AND NOT EXISTS (
      SELECT 1
      FROM analytics.dim_time_5m d
      WHERE d.bucket_start = dateadd(minute, 5 * n, (SELECT min_ts FROM analytics._bounds))
  );

-- =========================================================
-- 2) Deployment-level overlap-aware rollups
-- =========================================================

-- Overlap join: query interval intersects bucket interval
DROP TABLE IF EXISTS analytics._overlap_deploy;
CREATE TEMP TABLE analytics._overlap_deploy AS
SELECT
  d.bucket_start,
  r.deployment_type,

  greatest(r.execution_start_time, d.bucket_start) AS overlap_start,
  least(r.execution_end_time, d.bucket_end)        AS overlap_end,

  r.execution_duration_ms,
  r.scanned_mb,
  r.spilled_mb
FROM analytics.query_metrics_raw r
JOIN analytics.dim_time_5m d
  ON r.execution_start_time < d.bucket_end
 AND r.execution_end_time   > d.bucket_start;

-- Convert overlap duration to ms
DROP TABLE IF EXISTS analytics._contrib_deploy;
CREATE TEMP TABLE analytics._contrib_deploy AS
SELECT
  bucket_start,
  deployment_type,

  datediff(milliseconds, overlap_start, overlap_end) AS overlap_ms,

  execution_duration_ms,
  scanned_mb,
  spilled_mb
FROM analytics._overlap_deploy;

-- Upsert-like behavior: delete+insert for affected buckets
DELETE FROM analytics.metrics_5m_deployment
WHERE bucket_start IN (SELECT DISTINCT bucket_start FROM analytics._contrib_deploy);

INSERT INTO analytics.metrics_5m_deployment
SELECT
  bucket_start,
  deployment_type,

  COUNT(*) AS running_count,

  0::BIGINT AS queued_count,

  SUM(
    CASE WHEN execution_duration_ms > 0
      THEN scanned_mb * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
      ELSE 0
    END
  ) AS scanned_mb,

  SUM(
    CASE WHEN execution_duration_ms > 0
      THEN spilled_mb * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
      ELSE 0
    END
  ) AS spilled_mb,

  CASE
    WHEN SUM(
      CASE WHEN execution_duration_ms > 0
        THEN (scanned_mb + spilled_mb) * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
        ELSE 0
      END
    ) = 0 THEN 0
    ELSE
      SUM(
        CASE WHEN execution_duration_ms > 0
          THEN spilled_mb * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
          ELSE 0
        END
      )
      /
      SUM(
        CASE WHEN execution_duration_ms > 0
          THEN (scanned_mb + spilled_mb) * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
          ELSE 0
        END
      )
  END AS spill_pressure

FROM analytics._contrib_deploy
GROUP BY bucket_start, deployment_type;

-- =========================================================
-- 3) Instance-level overlap-aware rollups
-- =========================================================

DROP TABLE IF EXISTS analytics._overlap_instance;
CREATE TEMP TABLE analytics._overlap_instance AS
SELECT
  d.bucket_start,
  r.deployment_type,
  r.instance_id,

  greatest(r.execution_start_time, d.bucket_start) AS overlap_start,
  least(r.execution_end_time, d.bucket_end)        AS overlap_end,

  r.execution_duration_ms,
  r.scanned_mb,
  r.spilled_mb
FROM analytics.query_metrics_raw r
JOIN analytics.dim_time_5m d
  ON r.execution_start_time < d.bucket_end
 AND r.execution_end_time   > d.bucket_start;

DROP TABLE IF EXISTS analytics._contrib_instance;
CREATE TEMP TABLE analytics._contrib_instance AS
SELECT
  bucket_start,
  deployment_type,
  instance_id,

  datediff(milliseconds, overlap_start, overlap_end) AS overlap_ms,

  execution_duration_ms,
  scanned_mb,
  spilled_mb
FROM analytics._overlap_instance;

DELETE FROM analytics.metrics_5m_instance
WHERE bucket_start IN (SELECT DISTINCT bucket_start FROM analytics._contrib_instance);

INSERT INTO analytics.metrics_5m_instance
SELECT
  bucket_start,
  deployment_type,
  instance_id,

  COUNT(*) AS running_count,

  SUM(
    CASE WHEN execution_duration_ms > 0
      THEN scanned_mb * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
      ELSE 0
    END
  ) AS scanned_mb,

  SUM(
    CASE WHEN execution_duration_ms > 0
      THEN spilled_mb * (overlap_ms::DOUBLE PRECISION / execution_duration_ms::DOUBLE PRECISION)
      ELSE 0
    END
  ) AS spilled_mb

FROM analytics._contrib_instance
GROUP BY bucket_start, deployment_type, instance_id;