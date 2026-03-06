-- ============================================================
-- 01_env_provisioning.sql
-- PURPOSE : Create all Snowflake resources needed for the
--           Retail Intelligence Engine.
-- RUN     : snowsql -f setup/01_env_provisioning.sql
-- ORDER   : Run this FIRST before any other SQL file.
-- ============================================================

-- ── Step 1: Create the main database ─────────────────────────
-- All three layers (Bronze, Silver, Gold) live inside RETAIL_DB.
-- Using a single database keeps cross-schema JOINs simple and
-- avoids cross-database permission complexity.
CREATE DATABASE IF NOT EXISTS RETAIL_DB
    COMMENT = 'Retail Intelligence Engine — Medallion Architecture';

USE DATABASE RETAIL_DB;

-- ── Step 2: Create the three Medallion schemas ───────────────
-- BRONZE : Raw zone. Immutable clones + Snowpipe ingestion.
-- SILVER : Cleaned zone. dbt staging + intermediate models.
-- GOLD   : Serving zone. dbt mart tables + Materialized Views.

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.BRONZE
    COMMENT = 'Raw zone: zero-copy TPC-DS clones and raw synthetic tables. No transforms ever.';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.SILVER
    COMMENT = 'Cleaned zone: dbt-managed staging and intermediate models.';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.GOLD
    COMMENT = 'Serving zone: dbt mart tables and Materialized Views for BI tools.';

-- ── Step 3: Create the Virtual Warehouse ─────────────────────
-- X-SMALL is sufficient for this project's data volume.
-- AUTO_SUSPEND = 60  → warehouse pauses after 60s of inactivity
--                       (saves credits when not running queries)
-- AUTO_RESUME  = TRUE → warehouse restarts automatically when
--                       a query is submitted (no manual action)
-- INITIALLY_SUSPENDED → starts paused; only runs when needed.
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Retail Intelligence Engine compute warehouse';

USE WAREHOUSE RETAIL_WH;

-- ── Step 4: Verify setup ─────────────────────────────────────
-- Quick sanity check — should return RETAIL_DB with 3 schemas.
SHOW SCHEMAS IN DATABASE RETAIL_DB;
SHOW WAREHOUSES LIKE 'RETAIL_WH';

-- ── What to run next ─────────────────────────────────────────
-- 1. setup/02_rbac_grants.sql   → create roles and permissions
-- 2. bronze/03_clone_tpcds.sql  → load raw data
