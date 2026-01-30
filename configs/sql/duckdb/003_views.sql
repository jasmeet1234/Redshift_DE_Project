-- Convenience views for the UI and quick sanity checks.
-- These are derived from the *processed* events table.

CREATE OR REPLACE VIEW v_query_metrics_processed_latest AS
SELECT
  arrival_timestamp,
  query_id,
  deployment_type,
  instance_id,
  duration_seconds,
  queue_duration_ms,
  compile_duration_ms,
  execution_duration_ms,
  scanned_mb,
  spilled_mb,
  spill_pressure,
  queued
FROM query_metrics_processed
ORDER BY arrival_timestamp DESC
LIMIT 500;

CREATE OR REPLACE VIEW v_pipeline_status AS
SELECT
  (SELECT COUNT(*) FROM query_metrics_processed)                 AS processed_rows,
  (SELECT MIN(arrival_timestamp) FROM query_metrics_processed)   AS earliest_arrival_ts,
  (SELECT MAX(arrival_timestamp) FROM query_metrics_processed)   AS latest_arrival_ts;