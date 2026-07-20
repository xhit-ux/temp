-- OPC UA → YMatrix PoC: Database bootstrap script
-- Run this once before starting the Go backend.
--
--   psql -h <host> -U <user> -d <database> -f migrations/001_init.sql
--
-- The Go backend does NOT auto-create tables; all DDL is managed here.

-- ============================================================================
-- 1. Main data point table (V2.0 Section 12.3)
-- ============================================================================
CREATE TABLE IF NOT EXISTS opc_point_data (
    event_id             uuid        NOT NULL,
    device_id            text        NOT NULL,
    point_name           text        NOT NULL,
    data_type            text        NOT NULL,
    unit                 text,
    value_number         double precision,
    value_text           text,
    value_time           timestamptz,
    quality_code         bigint      NOT NULL,
    quality_name         text        NOT NULL,
    event_type           text        NOT NULL,
    is_abnormal          boolean     NOT NULL DEFAULT false,
    abnormal_reason      text,
    source_timestamp     timestamptz,
    server_timestamp     timestamptz,
    collector_timestamp  timestamptz NOT NULL,
    received_at          timestamptz NOT NULL DEFAULT now(),
    batch_id             uuid
);

-- Unique index on event_id — placed here rather than via ALTER TABLE so the
-- table-level DDL stays self-contained.  Must be created AFTER the table exists.
CREATE UNIQUE INDEX IF NOT EXISTS ux_opc_point_event
    ON opc_point_data (event_id);

-- Composite index for trend queries: device + point_name + time
CREATE INDEX IF NOT EXISTS idx_opc_point_device_time
    ON opc_point_data (device_id, point_name, source_timestamp);

-- Index for abnormal point queries
CREATE INDEX IF NOT EXISTS idx_opc_point_abnormal_time
    ON opc_point_data (is_abnormal, source_timestamp);

-- ============================================================================
-- 2. Alarm event table (V2.0 Section 12.4)
-- ============================================================================
CREATE TABLE IF NOT EXISTS opc_alarm_event (
    alarm_id          uuid        NOT NULL,
    event_id          uuid        NOT NULL,
    device_id         text        NOT NULL,
    point_name        text,
    severity          text        NOT NULL,
    alarm_type        text        NOT NULL,
    message           text        NOT NULL,
    status            text        NOT NULL DEFAULT 'active',
    occurred_at       timestamptz NOT NULL,
    recovered_at      timestamptz,
    acknowledged_at   timestamptz,
    acknowledged_by   text,
    created_at        timestamptz NOT NULL DEFAULT now()
);

-- Unique index on alarm_id for idempotent alarm deduplication
CREATE UNIQUE INDEX IF NOT EXISTS ux_opc_alarm_id
    ON opc_alarm_event (alarm_id);

-- ============================================================================
-- 3. Operations log table
-- ============================================================================
CREATE TABLE IF NOT EXISTS opc_operation_log (
    id          varchar(36) NOT NULL,
    timestamp   timestamptz NOT NULL DEFAULT now(),
    level       text        NOT NULL,
    module      text        NOT NULL,
    message     text        NOT NULL,
    extra       text
);

CREATE INDEX IF NOT EXISTS idx_ops_log_ts
    ON opc_operation_log (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ops_log_level
    ON opc_operation_log (level, timestamp DESC);

-- ============================================================================
-- 4. Distribution (YMatrix / Greenplum only)
-- ============================================================================
-- Uncomment the following ALTER TABLE statements after connecting to a
-- Greenplum/YMatrix instance.  Standard PostgreSQL does not support
-- DISTRIBUTED BY; the statements will be silently ignored there.

-- ALTER TABLE opc_point_data SET DISTRIBUTED BY (event_id);
-- ALTER TABLE opc_alarm_event SET DISTRIBUTED BY (alarm_id);

-- ============================================================================
-- 4. Optional: Dedup view (V2.0 Section 13)
-- ============================================================================
-- Created but NOT enabled as default query target by the Go backend.
-- Decision to use it should follow EXPLAIN ANALYZE evaluation (Section 14).

-- CREATE OR REPLACE VIEW opc_point_data_dedup AS
-- SELECT * FROM (
--     SELECT
--         p.*,
--         row_number() OVER (PARTITION BY event_id ORDER BY received_at DESC) AS rn
--     FROM opc_point_data p
-- ) d
-- WHERE rn = 1;