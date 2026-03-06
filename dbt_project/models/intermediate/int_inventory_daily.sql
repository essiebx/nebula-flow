-- models/intermediate/int_inventory_daily.sql
-- ============================================================
-- PURPOSE : Enrich inventory snapshots with item context and
--           compute a rolling 30-day average daily sales rate.
--           This is the foundation for mart_inventory_health.
--
-- KEY COMPUTED COLUMN — avg_daily_sales:
--   We use a window function to compute the average units sold
--   per day over the trailing 30 days for each item/warehouse.
--   This is then used to estimate how many days of stock remain.
--
-- WINDOW FUNCTION EXPLAINED:
--   AVG(units_sold) OVER (
--     PARTITION BY item_id, warehouse_id   -- per item per warehouse
--     ORDER BY inv_date                    -- in chronological order
--     ROWS BETWEEN 29 PRECEDING            -- look back 30 rows (days)
--         AND CURRENT ROW
--   )
--   This gives us a rolling average rather than a global average,
--   which is more accurate for seasonal products.
-- ============================================================

WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
),

-- Daily sales aggregated per item (across all channels)
-- Used as the denominator for days_of_supply
daily_sales_rate AS (

    SELECT
        item_id,
        sale_date,
        SUM(quantity_sold)      AS units_sold_that_day
    FROM {{ ref('int_unified_sales') }}
    GROUP BY item_id, sale_date

),

-- Join inventory to item metadata and date dimension
inventory_enriched AS (

    SELECT
        inv.inventory_sk,
        inv.item_id,
        inv.warehouse_id,
        inv.quantity_on_hand,
        d.calendar_date         AS inv_date,
        d.calendar_year,
        d.month_of_year,

        -- Item metadata for grouping in the mart
        i.category,
        i.price_tier,
        i.item_description,

        -- Daily sales rate for this item on this date
        -- NULL if no sales occurred that day (normal for slow-moving items)
        COALESCE(dsr.units_sold_that_day, 0) AS units_sold_that_day

    FROM inventory inv

    INNER JOIN {{ ref('stg_date_dim') }} d
        ON inv.date_sk = d.date_sk

    INNER JOIN {{ ref('stg_item') }} i
        ON inv.item_id = i.item_id

    LEFT JOIN daily_sales_rate dsr
        ON  inv.item_id    = dsr.item_id
        AND d.calendar_date = dsr.sale_date

),

-- Apply rolling 30-day average window function
with_rolling_avg AS (

    SELECT
        *,

        -- ── Rolling 30-day average daily sales ───────────────
        -- ROWS BETWEEN 29 PRECEDING AND CURRENT ROW = 30 rows total
        -- We use ROWS (physical rows) not RANGE (value-based)
        -- because our data has one row per day — they're equivalent
        -- here but ROWS is safer if there are date gaps.
        AVG(units_sold_that_day) OVER (
            PARTITION BY item_id, warehouse_id
            ORDER BY inv_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                       AS avg_daily_sales_30d,

        -- ── Days of supply ────────────────────────────────────
        -- How many days until this item runs out at the current sales rate.
        -- safe_divide macro prevents division by zero for items
        -- with no recent sales history.
        {{ safe_divide('quantity_on_hand',
           'AVG(units_sold_that_day) OVER (PARTITION BY item_id, warehouse_id ORDER BY inv_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)') }}
                                AS days_of_supply

    FROM inventory_enriched

)

SELECT * FROM with_rolling_avg
