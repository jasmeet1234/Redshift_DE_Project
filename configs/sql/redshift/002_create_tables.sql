-- =========================================================
-- Raw query metrics (append-only)
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.query_metrics_raw (
    query_id              VARCHAR(128) ENCODE lzo,
    deployment_type       VARCHAR(32)  ENCODE lzo,   -- provisioned | serverless
    instance_id           VARCHAR(128) ENCODE lzo,

    arrival_timestamp     TIMESTAMP    ENCODE az64,

    queue_duration_ms     BIGINT       ENCODE az64,
    compile_duration_ms   BIGINT       ENCODE az64,
    execution_duration_ms BIGINT       ENCODE az64,

    scanned_mb            DOUBLE PRECISION ENCODE az64,
    spilled_mb            DOUBLE PRECISION ENCODE az64,

    execution_start_time  TIMESTAMP    ENCODE az64,
    execution_end_time    TIMESTAMP    ENCODE az64
)
DISTSTYLE AUTO
SORTKEY (execution_start_time, execution_end_time);

-- =========================================================
-- 5-minute time dimension table (bucket boundaries)
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.dim_time_5m (
    bucket_start TIMESTAMP ENCODE az64,
    bucket_end   TIMESTAMP ENCODE az64,
    PRIMARY KEY(bucket_start)
)
DISTSTYLE ALL
SORTKEY(bucket_start);

-- =========================================================
-- 5-minute deployment rollups
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.metrics_5m_deployment (
    bucket_start    TIMESTAMP ENCODE az64,
    deployment_type VARCHAR(32) ENCODE lzo,

    running_count   BIGINT ENCODE az64,
    queued_count    BIGINT ENCODE az64,

    scanned_mb      DOUBLE PRECISION ENCODE az64,
    spilled_mb      DOUBLE PRECISION ENCODE az64,
    spill_pressure  DOUBLE PRECISION ENCODE az64,

    PRIMARY KEY(bucket_start, deployment_type)
)
DISTSTYLE AUTO
SORTKEY(bucket_start, deployment_type);

-- =========================================================
-- 5-minute instance rollups
-- =========================================================
CREATE TABLE IF NOT EXISTS analytics.metrics_5m_instance (
    bucket_start    TIMESTAMP ENCODE az64,
    deployment_type VARCHAR(32)  ENCODE lzo,
    instance_id     VARCHAR(128) ENCODE lzo,

    running_count   BIGINT ENCODE az64,
    scanned_mb      DOUBLE PRECISION ENCODE az64,
    spilled_mb      DOUBLE PRECISION ENCODE az64,

    PRIMARY KEY(bucket_start, deployment_type, instance_id)
)
DISTSTYLE AUTO
SORTKEY(bucket_start, deployment_type, instance_id);