-- models/marts/mart_inventory_health.sql
-- ============================================================
-- PURPOSE : Latest inventory snapshot per item/warehouse with
--           stockout risk flagging based on days of supply.
--
-- BUSINESS QUESTION:
--   "Which items are at risk of stocking out in the next 7 days?"
--
-- STOCKOUT RISK LOGIC:
--   HIGH   : days_of_supply < 7   → urgent reorder needed
--   MEDIUM : days_of_supply < 14  → plan reorder soon
--   LOW    : days_of_supply >= 14 → sufficient stock
--
-- IMPORTANT: We only want the LATEST snapshot per item/warehouse.
--   int_inventory_daily has one row per item/warehouse/date.
--   We use ROW_NUMBER() to pick only the most recent date.
--   This is a common window function interview question!
-- ============================================================

{{
    config(
        materialized = 'table',
        cluster_by   = ['stockout_risk_flag', 'category'],
        tags         = ['inventory', 'gold']
    )
}}

WITH inventory_history AS (

    SELECT * FROM {{ ref('int_inventory_daily') }}

),

-- ── Step 1: Get the latest snapshot per item + warehouse ──────
-- ROW_NUMBER() assigns rank 1 to the most recent date.
-- We then filter to rank = 1 to keep only the latest.
-- WHY NOT MAX(inv_date)? Because MAX() + GROUP BY would require
-- re-joining to get the other columns. ROW_NUMBER() does it in
-- one pass — more efficient and cleaner to read.
latest_snapshot AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, warehouse_id   -- per item per warehouse
            ORDER BY inv_date DESC               -- most recent date = rank 1
        )                   AS row_num
    FROM inventory_history

),

most_recent AS (

    SELECT * FROM latest_snapshot
    WHERE row_num = 1           -- keep only the most recent row

),

-- ── Step 2: Add stockout risk classification ──────────────────
with_risk_flag AS (

    SELECT
        item_id,
        item_description,
        category,
        price_tier,
        warehouse_id,
        inv_date                AS snapshot_date,
        quantity_on_hand,
        avg_daily_sales_30d,
        days_of_supply,

        -- ── Stockout risk flag ─────────────────────────────────
        -- Variables defined in dbt_project.yml so thresholds
        -- can be adjusted without changing this model.
        CASE
            WHEN days_of_supply IS NULL OR days_of_supply < {{ var('stockout_high_threshold_days') }}
                THEN 'HIGH'
            WHEN days_of_supply < {{ var('stockout_medium_threshold_days') }}
                THEN 'MEDIUM'
            ELSE 'LOW'
        END                     AS stockout_risk_flag,

        -- Days until estimated stockout (capped at 999 for display)
        LEAST(ROUND(days_of_supply, 1), 999)    AS days_until_stockout

    FROM most_recent

)

SELECT * FROM with_risk_flag
ORDER BY days_of_supply ASC NULLS FIRST   -- most critical items first
