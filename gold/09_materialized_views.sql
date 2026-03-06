-- gold/09_materialized_views.sql
-- ============================================================
-- PURPOSE : Create Materialized Views (MVs) in the GOLD schema
--           on top of each dbt mart table.
--
-- WHY MATERIALIZED VIEWS ON TOP OF dbt TABLES?
--   dbt mart models are already TABLE materialized — so they're fast.
--   MVs add one more layer:
--     1. MVs are automatically refreshed by Snowflake when the
--        underlying table changes (no manual trigger needed)
--     2. They appear as a stable object in the GOLD schema that
--        BI tools like Tableau and Grafana connect to
--     3. If the underlying mart changes structure, the MV catches
--        it at refresh time — an early warning system
--
-- NAMING CONVENTION:
--   mv_ prefix signals to analysts "this is a Materialized View"
--   not a raw table, so they know it auto-refreshes.
--
-- RUN: snowsql -f gold/09_materialized_views.sql
-- ============================================================

USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.GOLD;
USE WAREHOUSE RETAIL_WH;

-- ── MV 1: Daily Revenue ────────────────────────────────────────
CREATE OR REPLACE MATERIALIZED VIEW GOLD.mv_daily_revenue
    COMMENT = 'Auto-refreshing MV on mart_daily_revenue. BI tools connect here.'
AS
SELECT * FROM RETAIL_DB.GOLD.mart_daily_revenue;

-- ── MV 2: Returns Analysis ─────────────────────────────────────
CREATE OR REPLACE MATERIALIZED VIEW GOLD.mv_returns_analysis
    COMMENT = 'Auto-refreshing MV on mart_returns_analysis.'
AS
SELECT * FROM RETAIL_DB.GOLD.mart_returns_analysis;

-- ── MV 3: Inventory Health ─────────────────────────────────────
CREATE OR REPLACE MATERIALIZED VIEW GOLD.mv_inventory_health
    COMMENT = 'Auto-refreshing MV on mart_inventory_health. HIGH risk items.'
AS
SELECT * FROM RETAIL_DB.GOLD.mart_inventory_health;

-- ── MV 4: Customer LTV ─────────────────────────────────────────
CREATE OR REPLACE MATERIALIZED VIEW GOLD.mv_customer_ltv
    COMMENT = 'Auto-refreshing MV on mart_customer_ltv.'
AS
SELECT * FROM RETAIL_DB.GOLD.mart_customer_ltv;

-- ── MV 5: Promotion Effectiveness ─────────────────────────────
CREATE OR REPLACE MATERIALIZED VIEW GOLD.mv_promotion_effectiveness
    COMMENT = 'Auto-refreshing MV on mart_promotion_effectiveness.'
AS
SELECT * FROM RETAIL_DB.GOLD.mart_promotion_effectiveness;

-- ── Verify ────────────────────────────────────────────────────
SHOW MATERIALIZED VIEWS IN SCHEMA RETAIL_DB.GOLD;
