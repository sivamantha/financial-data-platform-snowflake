-- =============================================================================
-- RESOURCE MONITORS: Cost governance and budget alerting
-- Run as: ACCOUNTADMIN
--
-- Demonstrates cost-awareness — a key architect-level concern.
-- Monitors trigger notifications at thresholds and can suspend warehouses
-- to prevent runaway spend.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ── Account-level monitor ────────────────────────────────────
-- Safety net: caps total monthly spend across all warehouses
CREATE OR REPLACE RESOURCE MONITOR ACCOUNT_MONTHLY_MONITOR
    WITH
        CREDIT_QUOTA = 50
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
;

ALTER ACCOUNT SET RESOURCE_MONITOR = ACCOUNT_MONTHLY_MONITOR;

-- ── Ingestion warehouse monitor ──────────────────────────────
CREATE OR REPLACE RESOURCE MONITOR INGESTION_MONITOR
    WITH
        CREDIT_QUOTA = 10
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
;

ALTER WAREHOUSE INGESTION_WH SET RESOURCE_MONITOR = INGESTION_MONITOR;

-- ── Transform warehouse monitor ──────────────────────────────
CREATE OR REPLACE RESOURCE MONITOR TRANSFORM_MONITOR
    WITH
        CREDIT_QUOTA = 20
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
;

ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = TRANSFORM_MONITOR;

-- ── Analytics warehouse monitor ──────────────────────────────
CREATE OR REPLACE RESOURCE MONITOR ANALYTICS_MONITOR
    WITH
        CREDIT_QUOTA = 15
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
;

ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = ANALYTICS_MONITOR;

-- ── Verify monitors ──────────────────────────────────────────
SHOW RESOURCE MONITORS;