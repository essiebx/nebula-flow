-- scripts/teardown_snowflake.sql
-- ============================================================
-- PURPOSE : Drop all Snowflake resources to stop credit usage.
-- WARNING : This is irreversible. All data will be lost.
-- USAGE   : snowsql -f scripts/teardown_snowflake.sql
-- ============================================================

-- Suspend tasks first (tasks can prevent DROP)
ALTER TASK IF EXISTS RETAIL_DB.SILVER.root_refresh_task        SUSPEND;
ALTER TASK IF EXISTS RETAIL_DB.SILVER.refresh_sales_task       SUSPEND;
ALTER TASK IF EXISTS RETAIL_DB.SILVER.refresh_returns_task     SUSPEND;
ALTER TASK IF EXISTS RETAIL_DB.SILVER.refresh_inventory_task   SUSPEND;
ALTER TASK IF EXISTS RETAIL_DB.SILVER.refresh_clickstream_task SUSPEND;
ALTER TASK IF EXISTS RETAIL_DB.SILVER.log_completion_task      SUSPEND;

-- Drop the entire database (CASCADE drops all schemas, tables, views, etc.)
DROP DATABASE IF EXISTS RETAIL_DB CASCADE;

-- Drop the warehouse (stops all future compute charges)
DROP WAREHOUSE IF EXISTS RETAIL_WH;

-- Drop roles
DROP ROLE IF EXISTS ENGINEER_ROLE;
DROP ROLE IF EXISTS ANALYST_ROLE;

-- To also stop Docker: docker-compose down -v
