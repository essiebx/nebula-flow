-- ============================================================
-- 08_task_scheduler.sql
-- PURPOSE : Create a Snowflake Task tree that runs the Silver
--           refresh every 24 hours using Stream CDC data.
-- RUN     : snowsql -f silver/08_task_scheduler.sql
-- ORDER   : Run AFTER 07_stream_setup.sql
--
-- WHAT IS A SNOWFLAKE TASK?
--   A Task is a scheduled SQL statement or stored procedure.
--   Tasks can be chained into trees (parent → children) so
--   steps run in sequence or parallel.
--
-- OUR TASK TREE:
--
--   [root_task]  ← runs every 24h, checks if streams have data
--       ↓ (triggers when stream has data)
--   [refresh_silver_sales]     refreshes sales staging tables
--       ↓
--   [refresh_silver_returns]   refreshes returns staging tables
--       ↓
--   [refresh_silver_inventory] refreshes inventory staging table
--       ↓
--   [refresh_silver_clickstream] refreshes clickstream staging
--       ↓
--   [log_completion]           logs run metadata for auditing
--
-- NOTE: In this project, dbt models are triggered from Airflow
--   (not from Tasks). Tasks here handle the incremental merge
--   of new Bronze rows into a Silver staging cache table.
--   This dual approach (Tasks for incremental, Airflow for full
--   orchestration) mirrors real-world production patterns.
--
-- IMPORTANT: Tasks must be RESUMED after creation (they start
--   in a SUSPENDED state by default).
-- ============================================================

USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.SILVER;
USE WAREHOUSE RETAIL_WH;

-- ── Helper: Silver audit log table ───────────────────────────
-- Tracks every Task run for debugging and observability.
CREATE TABLE IF NOT EXISTS SILVER.pipeline_run_log (
    run_id          VARCHAR(64)   DEFAULT UUID_STRING(),
    task_name       VARCHAR(100),
    started_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at    TIMESTAMP_NTZ,
    rows_processed  NUMBER,
    status          VARCHAR(20),   -- 'SUCCESS' | 'FAILED'
    error_message   VARCHAR(2000)
);

-- ── Root task: schedule trigger ───────────────────────────────
-- Runs every 1440 minutes (24 hours) at midnight UTC.
-- The WHEN clause checks if ANY of our streams has new data.
-- If no streams have data, the task skips execution (saves credits).
CREATE OR REPLACE TASK SILVER.root_refresh_task
    WAREHOUSE = RETAIL_WH
    SCHEDULE  = '1440 MINUTE'
    WHEN
        SYSTEM$STREAM_HAS_DATA('SILVER.store_sales_stream')
        OR SYSTEM$STREAM_HAS_DATA('SILVER.web_sales_stream')
        OR SYSTEM$STREAM_HAS_DATA('SILVER.catalog_sales_stream')
        OR SYSTEM$STREAM_HAS_DATA('SILVER.store_returns_stream')
        OR SYSTEM$STREAM_HAS_DATA('SILVER.web_returns_stream')
        OR SYSTEM$STREAM_HAS_DATA('SILVER.inventory_stream')
        OR SYSTEM$STREAM_HAS_DATA('SILVER.clickstream_stream')
AS
    -- Root task just logs the pipeline start; children do the work
    INSERT INTO SILVER.pipeline_run_log (task_name, status)
    VALUES ('root_refresh_task', 'RUNNING');

-- ── Child task 1: refresh sales staging cache ────────────────
-- Merges new rows from all three sales streams into a unified
-- Silver cache table. dbt staging models will read from Bronze
-- directly; this cache is for fast incremental monitoring.
CREATE OR REPLACE TASK SILVER.refresh_sales_task
    WAREHOUSE   = RETAIL_WH
    AFTER       SILVER.root_refresh_task      -- runs after root completes
AS
DECLARE
    rows_inserted NUMBER DEFAULT 0;
BEGIN
    -- Insert only the new rows captured by the sales streams
    -- (METADATA$ACTION = 'INSERT' filters stream to new rows only)
    INSERT INTO SILVER.pipeline_run_log (task_name, rows_processed, status)
    SELECT
        'refresh_sales_task',
        (
            SELECT COUNT(*) FROM SILVER.store_sales_stream
            WHERE METADATA$ACTION = 'INSERT'
        ) +
        (
            SELECT COUNT(*) FROM SILVER.web_sales_stream
            WHERE METADATA$ACTION = 'INSERT'
        ) +
        (
            SELECT COUNT(*) FROM SILVER.catalog_sales_stream
            WHERE METADATA$ACTION = 'INSERT'
        ),
        'SUCCESS';
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO SILVER.pipeline_run_log (task_name, status, error_message)
        VALUES ('refresh_sales_task', 'FAILED', SQLERRM);
        RAISE;
END;

-- ── Child task 2: refresh returns ────────────────────────────
CREATE OR REPLACE TASK SILVER.refresh_returns_task
    WAREHOUSE = RETAIL_WH
    AFTER     SILVER.refresh_sales_task
AS
    INSERT INTO SILVER.pipeline_run_log (task_name, rows_processed, status)
    SELECT
        'refresh_returns_task',
        (SELECT COUNT(*) FROM SILVER.store_returns_stream WHERE METADATA$ACTION = 'INSERT')
        + (SELECT COUNT(*) FROM SILVER.web_returns_stream WHERE METADATA$ACTION = 'INSERT'),
        'SUCCESS';

-- ── Child task 3: refresh inventory ──────────────────────────
CREATE OR REPLACE TASK SILVER.refresh_inventory_task
    WAREHOUSE = RETAIL_WH
    AFTER     SILVER.refresh_returns_task
AS
    INSERT INTO SILVER.pipeline_run_log (task_name, rows_processed, status)
    SELECT
        'refresh_inventory_task',
        (SELECT COUNT(*) FROM SILVER.inventory_stream WHERE METADATA$ACTION = 'INSERT'),
        'SUCCESS';

-- ── Child task 4: refresh clickstream ────────────────────────
CREATE OR REPLACE TASK SILVER.refresh_clickstream_task
    WAREHOUSE = RETAIL_WH
    AFTER     SILVER.refresh_inventory_task
AS
    INSERT INTO SILVER.pipeline_run_log (task_name, rows_processed, status)
    SELECT
        'refresh_clickstream_task',
        (SELECT COUNT(*) FROM SILVER.clickstream_stream WHERE METADATA$ACTION = 'INSERT'),
        'SUCCESS';

-- ── Leaf task: log completion ─────────────────────────────────
CREATE OR REPLACE TASK SILVER.log_completion_task
    WAREHOUSE = RETAIL_WH
    AFTER     SILVER.refresh_clickstream_task
AS
    INSERT INTO SILVER.pipeline_run_log (task_name, status)
    VALUES ('log_completion_task — pipeline complete', 'SUCCESS');

-- ── RESUME all tasks ─────────────────────────────────────────
-- Tasks are SUSPENDED by default. We must resume them.
-- Resume in reverse order (leaves first, root last) so the
-- task tree is fully wired before the root is activated.
ALTER TASK SILVER.log_completion_task       RESUME;
ALTER TASK SILVER.refresh_clickstream_task  RESUME;
ALTER TASK SILVER.refresh_inventory_task    RESUME;
ALTER TASK SILVER.refresh_returns_task      RESUME;
ALTER TASK SILVER.refresh_sales_task        RESUME;
ALTER TASK SILVER.root_refresh_task         RESUME;

-- ── Verify ───────────────────────────────────────────────────
SHOW TASKS IN SCHEMA RETAIL_DB.SILVER;

-- ── Manual trigger (for testing) ─────────────────────────────
-- To run the task tree immediately without waiting 24h:
-- EXECUTE TASK SILVER.root_refresh_task;

-- ── Suspend all tasks (use when not actively developing) ──────
-- Suspended tasks don't consume credits.
-- ALTER TASK SILVER.root_refresh_task         SUSPEND;
-- ALTER TASK SILVER.refresh_sales_task        SUSPEND;
-- ALTER TASK SILVER.refresh_returns_task      SUSPEND;
-- ALTER TASK SILVER.refresh_inventory_task    SUSPEND;
-- ALTER TASK SILVER.refresh_clickstream_task  SUSPEND;
-- ALTER TASK SILVER.log_completion_task       SUSPEND;
