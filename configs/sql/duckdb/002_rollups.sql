-- Rollup views computed from processed events.
-- These are always up-to-date and require no batch jobs.

CREATE OR REPLACE VIEW v_rollups_minute AS
SELECT
    date_trunc('minute', arrival_timestamp) AS window_start,
    deployment_type,
    COUNT(*) AS query_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    AVG(spill_pressure) AS avg_spill_pressure,
    AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM query_metrics_processed
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_rollups_5min AS
SELECT
    date_trunc('minute', arrival_timestamp) - INTERVAL (EXTRACT(MINUTE FROM arrival_timestamp)::INTEGER % 5) MINUTE AS window_start,
    deployment_type,
    COUNT(*) AS query_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    AVG(spill_pressure) AS avg_spill_pressure,
    AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM query_metrics_processed
GROUP BY 1, 2;