-- ============================================================
-- 06_snowpipe_setup.sql
-- PURPOSE : Create a Snowpipe that auto-ingests clickstream
--           JSON files from the internal stage into the
--           raw_clickstream Bronze table.
-- RUN     : snowsql -f bronze/06_snowpipe_setup.sql
-- ORDER   : Run AFTER 05_create_raw_tables.sql
--
-- HOW SNOWPIPE WORKS:
--   1. Kafka consumer calls Snowpipe REST API with a file list
--   2. Snowpipe queues the file for ingestion (async, ~1 min)
--   3. COPY INTO executes automatically — no warehouse needed
--      (Snowpipe uses Snowflake-managed serverless compute)
--
-- AUTO_INGEST = FALSE:
--   We're using the REST API trigger instead of S3 event
--   notifications. The Kafka consumer explicitly calls the
--   insertFiles endpoint after staging each batch of events.
--   In production with S3, you'd set AUTO_INGEST = TRUE and
--   configure an SQS event notification.
-- ============================================================

USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.BRONZE;

-- ── Clickstream pipe ─────────────────────────────────────────
-- The COPY INTO maps JSON fields to table columns.
-- $1 = the JSON document. $1:field_name extracts a value.
-- ::TYPE casts the extracted string to the target type.
CREATE OR REPLACE PIPE BRONZE.clickstream_pipe
    AUTO_INGEST = FALSE
    COMMENT     = 'Ingests clickstream JSON batches from @clickstream_stage into raw_clickstream'
AS
COPY INTO BRONZE.raw_clickstream (
    session_id,
    user_id,
    item_id,
    event_type,
    event_timestamp,
    device_type,
    referrer_source
)
FROM (
    SELECT
        $1:session_id::VARCHAR(64),
        $1:user_id::NUMBER(38, 0),
        $1:item_id::NUMBER(38, 0),
        $1:event_type::VARCHAR(20),
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:device_type::VARCHAR(20),
        $1:referrer_source::VARCHAR(30)
    FROM @BRONZE.clickstream_stage
)
FILE_FORMAT = BRONZE.json_ndjson_format;

-- ── Get the pipe's ingest URL ─────────────────────────────────
-- After creating the pipe, run this to get the REST endpoint
-- URL that the Kafka consumer needs in .env:
--   SNOWPIPE_CLICKSTREAM_URL
-- Copy the value from the "notification_channel" column.
SHOW PIPES IN SCHEMA RETAIL_DB.BRONZE;

-- ── Manual trigger (for testing without Kafka) ───────────────
-- If you've manually PUT a file to the stage, trigger ingestion:
-- ALTER PIPE BRONZE.clickstream_pipe REFRESH;

-- ── Check pipe status ────────────────────────────────────────
-- SYSTEM$PIPE_STATUS returns JSON with pendingFileCount,
-- lastIngestedTimestamp, executionState etc.
-- SELECT SYSTEM$PIPE_STATUS('BRONZE.clickstream_pipe');
