-- ============================================================
-- 07_stream_setup.sql
-- PURPOSE : Create Snowflake Streams on Bronze tables to enable
--           Change Data Capture (CDC) for incremental loads.
-- RUN     : snowsql -f silver/07_stream_setup.sql
-- ORDER   : Run AFTER bronze/ files are complete.
--
-- WHAT IS A SNOWFLAKE STREAM?
--   A Stream is a CDC object that tracks row-level changes
--   (INSERT, UPDATE, DELETE) on a source table since the last
--   time the stream was consumed.
--
--   Each row in a stream has extra metadata columns:
--     METADATA$ACTION     : 'INSERT' or 'DELETE'
--     METADATA$ISUPDATE   : TRUE if this is the new row of an UPDATE
--     METADATA$ROW_ID     : unique row identifier
--
-- WHY APPEND_ONLY = TRUE?
--   Our Bronze tables are append-only (we never UPDATE or DELETE).
--   Setting APPEND_ONLY = TRUE makes the stream more efficient —
--   it only tracks new rows, which is all we need for incremental
--   Silver refreshes.
--
-- HOW STREAMS ARE CONSUMED:
--   A Snowflake Task (see 08_task_scheduler.sql) runs a DML
--   statement that reads from the stream. Once that DML succeeds,
--   Snowflake advances the stream's offset automatically.
--   If the DML fails, the stream offset does NOT advance —
--   giving us automatic retry safety.
-- ============================================================

USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.SILVER;
USE WAREHOUSE RETAIL_WH;

-- ── Sales streams ─────────────────────────────────────────────
CREATE OR REPLACE STREAM SILVER.store_sales_stream
    ON TABLE RETAIL_DB.BRONZE.store_sales
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: captures new rows in BRONZE.store_sales since last Task run';

CREATE OR REPLACE STREAM SILVER.web_sales_stream
    ON TABLE RETAIL_DB.BRONZE.web_sales
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: captures new rows in BRONZE.web_sales since last Task run';

CREATE OR REPLACE STREAM SILVER.catalog_sales_stream
    ON TABLE RETAIL_DB.BRONZE.catalog_sales
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: captures new rows in BRONZE.catalog_sales since last Task run';

-- ── Returns streams ───────────────────────────────────────────
CREATE OR REPLACE STREAM SILVER.store_returns_stream
    ON TABLE RETAIL_DB.BRONZE.store_returns
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: new in-store returns since last Task run';

CREATE OR REPLACE STREAM SILVER.web_returns_stream
    ON TABLE RETAIL_DB.BRONZE.web_returns
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: new web returns since last Task run';

-- ── Inventory stream ──────────────────────────────────────────
CREATE OR REPLACE STREAM SILVER.inventory_stream
    ON TABLE RETAIL_DB.BRONZE.inventory
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: new inventory snapshots since last Task run';

-- ── Clickstream stream ────────────────────────────────────────
CREATE OR REPLACE STREAM SILVER.clickstream_stream
    ON TABLE RETAIL_DB.BRONZE.raw_clickstream
    APPEND_ONLY = TRUE
    COMMENT     = 'CDC stream: new Kafka clickstream events since last Task run';

-- ── Verify streams ────────────────────────────────────────────
-- SHOW STREAMS lists all streams and their current stale state.
-- A stream becomes "stale" if not consumed within 14 days
-- (Snowflake's default data retention window).
SHOW STREAMS IN SCHEMA RETAIL_DB.SILVER;

-- ── Check if a stream has unprocessed data ────────────────────
-- Use this to verify new data is flowing before triggering Tasks:
-- SELECT SYSTEM$STREAM_HAS_DATA('SILVER.store_sales_stream');
-- Returns TRUE if there are unconsumed rows, FALSE otherwise.
