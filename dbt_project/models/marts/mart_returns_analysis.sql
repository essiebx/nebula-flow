-- models/marts/mart_returns_analysis.sql
-- ============================================================
-- PURPOSE : Return rates by product, category, and price tier,
--           with a flag showing whether a promotion was active.
--
-- BUSINESS QUESTION:
--   "Which products have the highest return rates — and does
--    running a promotion correlate with higher returns?"
--
-- KEY METRICS:
--   return_rate_pct  : units_returned / units_sold * 100
--   was_promoted     : TRUE if a promotion was active on sale
--
-- HOW TO READ THE promo_return_rate vs baseline_return_rate:
--   If promo_return_rate >> baseline_return_rate, it may suggest
--   that promotions attract low-intent buyers who later return.
--   This is a real retail analytics problem — it's a great
--   talking point in interviews.
-- ============================================================

{{
    config(
        materialized = 'table',
        cluster_by   = ['category', 'price_tier'],
        tags         = ['returns', 'gold']
    )
}}

WITH returns_data AS (

    SELECT
        item_id,
        category,
        price_tier,
        quantity_returned,
        return_amount,
        was_promoted
    FROM {{ ref('int_unified_returns') }}

),

sales_data AS (

    SELECT
        item_id,
        category,
        price_tier,
        item_description,
        quantity_sold,
        gross_amount,
        -- Was a promotion associated with this sale?
        CASE WHEN promotion_id IS NOT NULL THEN TRUE ELSE FALSE END AS was_promoted
    FROM {{ ref('int_unified_sales') }}

),

-- Aggregate sales by item and promo flag
sales_agg AS (

    SELECT
        item_id,
        category,
        price_tier,
        item_description,
        was_promoted,
        SUM(quantity_sold)      AS units_sold,
        SUM(gross_amount)       AS gross_revenue,
        COUNT(*)                AS transaction_count
    FROM sales_data
    GROUP BY item_id, category, price_tier, item_description, was_promoted

),

-- Aggregate returns by item and promo flag
returns_agg AS (

    SELECT
        item_id,
        category,
        price_tier,
        was_promoted,
        SUM(quantity_returned)  AS units_returned,
        SUM(return_amount)      AS gross_return_value
    FROM returns_data
    GROUP BY item_id, category, price_tier, was_promoted

),

-- Join sales and returns to compute return rate per item per promo flag
joined AS (

    SELECT
        s.item_id,
        s.item_description,
        s.category,
        s.price_tier,
        s.was_promoted,
        s.units_sold,
        s.gross_revenue,
        s.transaction_count,

        COALESCE(r.units_returned,   0) AS units_returned,
        COALESCE(r.gross_return_value, 0) AS gross_return_value,

        -- ── Return rate ────────────────────────────────────────
        -- NULLIF prevents divide-by-zero for items with no sales.
        -- Multiplied by 100 to express as a percentage.
        ROUND(
            {{ safe_divide('COALESCE(r.units_returned, 0) * 100.0', 's.units_sold') }},
            2
        )                       AS return_rate_pct

    FROM sales_agg s
    LEFT JOIN returns_agg r
        ON  s.item_id     = r.item_id
        AND s.was_promoted = r.was_promoted

)

SELECT * FROM joined
ORDER BY return_rate_pct DESC
