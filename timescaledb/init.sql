-- init.sql — TimescaleDB schema for the room-temperature telemetry pipeline
--
-- Mounted into the TimescaleDB container and executed once on first start.
-- All four objects below are referenced by the ADR 002 data-flow diagram:
--   readings_raw        ← RawSinkJob (Flink Job 1)
--   readings_aggregated ← AggregationJob (Flink Job 2)
--   alerts              ← AlertJob (Flink Job 3, deferred)
--   hourly_temp         ← continuous aggregate over readings_raw (Phase 6)

-- ---------------------------------------------------------------------------
-- 1. readings_raw
--    Every individual sensor reading lands here.
--
--    Why a hypertable?
--    TimescaleDB automatically partitions this table into "chunks" by time
--    (default: 7-day chunks). This keeps recent data in memory-hot chunks and
--    lets old chunks be compressed or dropped without touching the hot path.
--
--    Why the unique constraint on (sensor_id, time)?
--    Flink's JDBC sink uses INSERT ... ON CONFLICT (sensor_id, time) DO NOTHING.
--    This makes every write idempotent: if the job restarts and replays Kafka
--    offsets, duplicate rows are silently dropped instead of causing errors or
--    double-counting. True 2-phase commit with TimescaleDB is not used; this
--    idempotent upsert is the replay-safety mechanism (see ADR 001).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS readings_raw (
    -- 'time' is the event timestamp (from timestamp_ms in the MQTT payload),
    -- NOT the wall-clock time the row was inserted. TimescaleDB requires the
    -- partitioning column to be called 'time' by convention (though any name
    -- works — we follow the convention for clarity in queries and Grafana).
    time        TIMESTAMPTZ     NOT NULL,

    sensor_id   TEXT            NOT NULL,   -- e.g. "esp32-bedroom"
    room        TEXT            NOT NULL,   -- e.g. "bedroom"
    temp_c      DOUBLE PRECISION NOT NULL,  -- degrees Celsius
    humidity    DOUBLE PRECISION,           -- percent relative humidity (nullable; sensor may omit)
    pressure    DOUBLE PRECISION            -- hPa (nullable; sensor may omit)
);

-- Promote readings_raw to a TimescaleDB hypertable, partitioned by 'time'.
-- 'if_not_exists => true' makes this script re-runnable without errors.
SELECT create_hypertable('readings_raw', 'time', if_not_exists => true);

-- Unique constraint supporting idempotent upsert from Flink RawSinkJob.
-- The Flink JDBC sink runs:
--   INSERT INTO readings_raw (...) VALUES (...)
--   ON CONFLICT (sensor_id, time) DO NOTHING
-- Without this constraint, ON CONFLICT has no target and will error.
-- Wrapped in DO/EXCEPTION so re-running init.sql (e.g. against a restored
-- volume that already has the tables) does not fail with "already exists".
DO $$ BEGIN
    ALTER TABLE readings_raw
        ADD CONSTRAINT readings_raw_sensor_id_time_key
        UNIQUE (sensor_id, time);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ---------------------------------------------------------------------------
-- 2. readings_aggregated
--    Flink AggregationJob writes one row per (room, 5-minute window).
--
--    'bucket' is the window-start timestamp (aligned to 5-minute boundaries
--    in Flink's tumbling window). Grafana queries this table for the
--    "5-minute rolling average" panel.
--
--    Also a hypertable so large historical aggregation history is chunked and
--    compressible, consistent with readings_raw.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS readings_aggregated (
    bucket      TIMESTAMPTZ     NOT NULL,   -- window start (5-min boundary)
    room        TEXT            NOT NULL,
    max_temp    DOUBLE PRECISION NOT NULL,  -- max temp_c across all sensors in the room during the window
    avg_temp    DOUBLE PRECISION NOT NULL,  -- mean temp_c across readings in the window
    max_humidity    DOUBLE PRECISION,       -- max humidity in the window (nullable; mirrors readings_raw)
    avg_humidity    DOUBLE PRECISION        -- mean humidity in the window (nullable)
);

SELECT create_hypertable('readings_aggregated', 'bucket', if_not_exists => true);

-- One aggregate row per room per 5-minute window. Prevents duplicates on
-- Flink job restart (same pattern as readings_raw).
DO $$ BEGIN
    ALTER TABLE readings_aggregated
        ADD CONSTRAINT readings_aggregated_room_bucket_key
        UNIQUE (room, bucket);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ---------------------------------------------------------------------------
-- 3. alerts
--    AlertJob (Flink Job 3, deferred to Phase 5) writes one row per alert
--    event: when max_temp exceeds the configured threshold for N consecutive
--    5-minute windows.
--
--    Fields match the AlertJob state machine described in ADR 002:
--      room               — the room that triggered the alert
--      window_start       — start of the N-th over-threshold window
--      max_temp           — the max temperature that triggered the alert
--      consecutive_count  — how many consecutive over-threshold windows fired
--                           before this alert was emitted
--      time               — wall-clock time the alert was written (insert time)
--
--    Not a hypertable: alert volume is expected to be very low (rare events),
--    so hypertable chunking overhead is not worthwhile here.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alerts (
    id                  BIGSERIAL       PRIMARY KEY,
    room                TEXT            NOT NULL,
    window_start        TIMESTAMPTZ     NOT NULL,   -- start of the triggering window
    max_temp            DOUBLE PRECISION NOT NULL,
    consecutive_count   INTEGER         NOT NULL,   -- number of consecutive over-threshold windows
    time                TIMESTAMPTZ     NOT NULL DEFAULT NOW()  -- insert time (processing time)
);


-- ---------------------------------------------------------------------------
-- 4. hourly_temp  (continuous aggregate)
--    A TimescaleDB continuous aggregate is a materialized view that
--    incrementally refreshes itself as new data arrives in readings_raw.
--    Unlike a standard PostgreSQL materialized view, it only re-computes the
--    buckets that have changed — making it cheap to keep up-to-date.
--
--    time_bucket('1 hour', time) rounds each event timestamp down to the
--    nearest hour boundary (e.g. 14:37 → 14:00). This gives Grafana an
--    efficient source for the "hourly heatmap" panel without scanning all of
--    readings_raw at query time.
--
--    WITH (timescaledb.continuous) is what tells TimescaleDB to manage
--    incremental materialization. The view refreshes automatically via the
--    TimescaleDB background worker; no manual REFRESH is needed in steady state.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_temp
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 hour', time)     AS bucket,
        room,
        MIN(temp_c)                     AS min_temp,
        MAX(temp_c)                     AS max_temp,
        AVG(temp_c)                     AS avg_temp
    FROM readings_raw
    GROUP BY bucket, room
WITH NO DATA;  -- populated lazily as data arrives; avoids error on empty table at init time

-- Automatic refresh policy: keep the aggregate up-to-date within 1 hour of
-- real time, re-materializing any bucket that changed in the last 2 hours.
-- start_offset and end_offset use INTERVAL to express the look-back window
-- relative to now().
SELECT add_continuous_aggregate_policy(
    'hourly_temp',
    start_offset => INTERVAL '2 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => true
);
