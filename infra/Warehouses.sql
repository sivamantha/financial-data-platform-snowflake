-- =============================================================================
-- WAREHOUSES: Workload-isolated compute with cost governance
-- Run as: ACCOUNTADMIN or SYSADMIN
-- =============================================================================

-- Ingestion warehouse: small, auto-suspends quickly
-- Used by: Snowpipe, Airflow COPY INTO operations
CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Ingestion workloads: Snowpipe, Airflow COPY INTO'
;

-- Transform warehouse: dbt runs, silver/gold model builds
-- Used by: dbt, Snowflake Tasks
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Transformation workloads: dbt models, tasks'
;

-- Analytics warehouse: dashboards, ad-hoc queries
-- Used by: Snowsight dashboards, analysts
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics workloads: dashboards, ad-hoc queries'
;