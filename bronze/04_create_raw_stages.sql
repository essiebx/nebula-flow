-- ============================================================
-- 04_create_raw_stages.sql
-- PURPOSE : Create internal Snowflake stages where the Kafka
--           consumer will land clickstream JSON files before
--           Snowpipe auto-ingests them into Bronze tables.
-- RUN     : snowsql -f bronze/04_create_raw_stages.sql
-- ORDER   : Run AFTER 03_clone_tpcds.sql
--
-- WHAT IS A STAGE?
--   A stage is a named location (here: internal to Snowflake)
--   that acts as a landing zone for files before they are
--   loaded into a table. Think of it as an inbox.
--   In production, stages typically point to S3/GCS/Azure Blob.
--   We use internal stages here to avoid cloud storage costs.
-- ============================================================

USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.BRONZE;
USE WAREHOUSE RETAIL_WH;

-- ── File format: JSON ────────────────────────────────────────
-- Our Kafka consumer writes one JSON object per line (NDJSON).
-- STRIP_OUTER_ARRAY = FALSE because each message is a single
-- JSON object, not wrapped in an array.
-- NULL_IF handles the string "null" becoming a SQL NULL.
CREATE OR REPLACE FILE FORMAT BRONZE.json_ndjson_format
    TYPE             = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    NULL_IF          = ('null', 'NULL', '')
    COMMENT          = 'NDJSON format for Kafka consumer output — one JSON object per line';

-- ── Stage: clickstream events ─────────────────────────────────
-- The Kafka consumer (ingestion/streaming/kafka_consumer_snowpipe.py)
-- writes batched clickstream messages here.
-- Snowpipe watches this stage and auto-ingests new files.
CREATE OR REPLACE STAGE BRONZE.clickstream_stage
    FILE_FORMAT = BRONZE.json_ndjson_format
    COMMENT     = 'Landing zone for Kafka clickstream events before Snowpipe ingestion';

-- ── Verify ───────────────────────────────────────────────────
SHOW STAGES  IN SCHEMA RETAIL_DB.BRONZE;
SHOW FILE FORMATS IN SCHEMA RETAIL_DB.BRONZE;
