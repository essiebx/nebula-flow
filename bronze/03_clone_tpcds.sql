-- ============================================================
-- 03_clone_tpcds.sql
-- PURPOSE : Zero-copy clone 12 TPC-DS tables from Snowflake's
--           hosted sample data into our Bronze layer.
-- RUN     : snowsql -f bronze/03_clone_tpcds.sql
-- ORDER   : Run AFTER 02_rbac_grants.sql
--
-- WHY CLONE INSTEAD OF COPY INTO?
--   CLONE creates a metadata pointer to the existing data —
--   it consumes zero additional storage and completes instantly
--   regardless of table size. It's one of Snowflake's most
--   powerful features for DE pipelines. In production this
--   pattern is used for: dev/test environments, disaster
--   recovery, and zero-downtime migrations.
--
-- WHY ARE THESE TABLES IN BRONZE?
--   Bronze = raw zone. These are exact replicas of the source.
--   No column rype changed. No row removed.
--   If something goes wrong downstream, we can always re-derive
--   Silver and Gold from this immutable raw layer.
-- ============================================================
-- 03_clone_tpcds.sql (fixed: CTAS instead of CLONE for shared tables)
USE DATABASE RETAIL_DB;
USE SCHEMA RETAIL_DB.BRONZE;
USE WAREHOUSE RETAIL_WH;

CREATE OR REPLACE TABLE BRONZE.store_sales    AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES;
CREATE OR REPLACE TABLE BRONZE.web_sales      AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES;
CREATE OR REPLACE TABLE BRONZE.catalog_sales  AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CATALOG_SALES;
CREATE OR REPLACE TABLE BRONZE.store_returns  AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_RETURNS;
CREATE OR REPLACE TABLE BRONZE.web_returns    AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_RETURNS;
CREATE OR REPLACE TABLE BRONZE.inventory      AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.INVENTORY;
CREATE OR REPLACE TABLE BRONZE.item           AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.ITEM;
CREATE OR REPLACE TABLE BRONZE.customer       AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER;
CREATE OR REPLACE TABLE BRONZE.customer_address AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_ADDRESS;
CREATE OR REPLACE TABLE BRONZE.date_dim       AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM;
CREATE OR REPLACE TABLE BRONZE.promotion      AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.PROMOTION;
CREATE OR REPLACE TABLE BRONZE.store          AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE;

SHOW TABLES IN SCHEMA RETAIL_DB.BRONZE;
-- ── Verify all 12 tables were created ────────────────────────
SHOW TABLES IN SCHEMA RETAIL_DB.BRONZE;

-- Expected output: 12 tables
-- store_sales, web_sales, catalog_sales
-- store_returns, web_returns
-- inventory
-- item, customer, customer_address, date_dim, promotion, store
