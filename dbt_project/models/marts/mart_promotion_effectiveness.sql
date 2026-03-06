-- models/marts/mart_promotion_effectiveness.sql
-- ============================================================
-- PURPOSE : Measure whether promotions actually lift revenue
--           compared to equivalent non-promotion periods.
--
-- BUSINESS QUESTION:
--   "Do our promotions increase net revenue — or do they just
--    discount margin on sales that would have happened anyway?"
--
-- HOW THE ANALYSIS WORKS:
--   For each promotion, we compare:
--     promo_period    : net revenue during the promotion window
--     baseline_period : net revenue for the same items in an
--                       equivalent non-promo period
--   Revenue lift = (promo - baseline) / baseline * 100
--   A positive lift means the promo generated incremental revenue.
--   A negative lift means the promo cannibalized baseline sales.
--
-- BASELINE PERIOD DEFINITION:
--   We use the 30 days BEFORE the promotion start date as baseline.
--   This is a common but simplified approach. In production you
--   would also control for seasonality (compare same period last year).
--   This is worth mentioning in an interview — shows you understand
--   the limitations of the model.
--
-- CTE STRUCTURE (read top to bottom):
--   1. promo_windows      → get start/end date for each promo
--   2. promo_sales        → net revenue DURING promo window
--   3. baseline_sales     → net revenue 30 days BEFORE promo
--   4. final              → join + compute lift metrics
-- ============================================================

{{
    config(
        materialized = 'table',
        cluster_by   = ['promotion_id'],
        tags         = ['promotion', 'gold']
    )
}}

WITH net_sales AS (
    SELECT * FROM {{ ref('int_net_sales') }}
),

promotions AS (
    SELECT * FROM {{ ref('stg_promotion') }}
),

date_dim AS (
    SELECT * FROM {{ ref('stg_date_dim') }}
),

-- ── CTE 1: Map promotion keys to actual calendar dates ────────
-- TPC-DS stores start/end dates as integer SKs (date_sk).
-- We join to date_dim TWICE (once for start, once for end)
-- to convert them to actual dates.
promo_windows AS (

    SELECT
        p.promotion_id,
        p.promotion_code,
        p.promotion_name,
        p.channel_type,
        p.is_discount_active,
        -- Join date_dim for start date
        d_start.calendar_date       AS promo_start_date,
        -- Join date_dim for end date (alias the same table twice)
        d_end.calendar_date         AS promo_end_date,
        -- How long is the promotion?
        DATEDIFF('day',
            d_start.calendar_date,
            d_end.calendar_date
        )                           AS promo_duration_days,
        -- Baseline period: 30 days before promo start
        DATEADD('day', -30, d_start.calendar_date) AS baseline_start_date,
        DATEADD('day', -1,  d_start.calendar_date) AS baseline_end_date

    FROM promotions p

    -- Self-join date_dim for start date (alias: d_start)
    LEFT JOIN date_dim d_start
        ON p.start_date_sk = d_start.date_sk

    -- Self-join date_dim for end date (alias: d_end)
    LEFT JOIN date_dim d_end
        ON p.end_date_sk = d_end.date_sk

    -- Only include promotions with valid date ranges
    WHERE p.start_date_sk IS NOT NULL
      AND p.end_date_sk   IS NOT NULL

),

-- ── CTE 2: Revenue DURING the promotion window ────────────────
-- We join sales to promo_windows using:
--   1. promotion_id matching (the sale had this promo applied)
--   2. sale_date within the promo window (defensive date filter)
promo_sales AS (

    SELECT
        ns.promotion_id,
        COUNT(*)                    AS promo_transaction_count,
        SUM(ns.gross_amount)        AS promo_gross_revenue,
        SUM(ns.net_amount)          AS promo_net_revenue,
        SUM(COALESCE(ns.return_amount, 0)) AS promo_return_amount,
        -- Return rate during promo
        {{ safe_divide(
            'SUM(CASE WHEN ns.was_returned THEN 1 ELSE 0 END)',
            'COUNT(*)'
        ) }}                        AS promo_return_rate,
        COUNT(DISTINCT ns.customer_id) AS promo_unique_customers

    FROM net_sales ns

    INNER JOIN promo_windows pw
        ON  ns.promotion_id  = pw.promotion_id
        AND ns.sale_date    BETWEEN pw.promo_start_date AND pw.promo_end_date

    WHERE ns.promotion_id IS NOT NULL
    GROUP BY ns.promotion_id

),

-- ── CTE 3: Revenue in the BASELINE period (30 days before promo) ─
-- The baseline captures the same ITEMS but without the promotion.
-- We match on item_id (not promotion_id) and use the baseline
-- date window instead of the promo window.
baseline_sales AS (

    SELECT
        pw.promotion_id,    -- label by promo so we can join later
        COUNT(*)                    AS baseline_transaction_count,
        SUM(ns.gross_amount)        AS baseline_gross_revenue,
        SUM(ns.net_amount)          AS baseline_net_revenue,
        {{ safe_divide(
            'SUM(CASE WHEN ns.was_returned THEN 1 ELSE 0 END)',
            'COUNT(*)'
        ) }}                        AS baseline_return_rate

    FROM net_sales ns

    -- Match items that WOULD be covered by this promo
    -- (items that later had the promo applied)
    INNER JOIN promo_windows pw
        ON ns.sale_date BETWEEN pw.baseline_start_date AND pw.baseline_end_date

    -- Explicitly exclude rows where a promo WAS active
    -- (don't let a different promo pollute the baseline)
    WHERE ns.promotion_id IS NULL

    GROUP BY pw.promotion_id

),

-- ── CTE 4: Combine and compute lift metrics ───────────────────
final AS (

    SELECT
        pw.promotion_id,
        pw.promotion_code,
        pw.promotion_name,
        pw.channel_type,
        pw.promo_start_date,
        pw.promo_end_date,
        pw.promo_duration_days,

        -- Promo period metrics
        COALESCE(ps.promo_transaction_count, 0)     AS promo_transaction_count,
        COALESCE(ps.promo_gross_revenue, 0)         AS promo_gross_revenue,
        COALESCE(ps.promo_net_revenue, 0)           AS promo_net_revenue,
        COALESCE(ps.promo_return_rate, 0)           AS promo_return_rate,
        COALESCE(ps.promo_unique_customers, 0)      AS promo_unique_customers,

        -- Baseline period metrics
        COALESCE(bs.baseline_transaction_count, 0)  AS baseline_transaction_count,
        COALESCE(bs.baseline_net_revenue, 0)        AS baseline_net_revenue,
        COALESCE(bs.baseline_return_rate, 0)        AS baseline_return_rate,

        -- ── Revenue lift ────────────────────────────────────────
        -- Lift % = (promo revenue - baseline revenue) / baseline * 100
        -- Positive = promo drove incremental revenue
        -- Negative = promo cannibalized baseline (price cut with no volume lift)
        -- NULL = no baseline data (new product or first promo)
        ROUND(
            {{ safe_divide(
                '(COALESCE(ps.promo_net_revenue, 0) - COALESCE(bs.baseline_net_revenue, 0)) * 100.0',
                'NULLIF(COALESCE(bs.baseline_net_revenue, 0), 0)'
            ) }},
            2
        )                                           AS revenue_lift_pct,

        -- ── Return rate delta ────────────────────────────────────
        -- Did the promo attract higher return behavior?
        ROUND(
            COALESCE(ps.promo_return_rate, 0)
            - COALESCE(bs.baseline_return_rate, 0),
            4
        )                                           AS return_rate_delta

    FROM promo_windows pw
    LEFT JOIN promo_sales   ps ON pw.promotion_id = ps.promotion_id
    LEFT JOIN baseline_sales bs ON pw.promotion_id = bs.promotion_id

)

SELECT * FROM final
ORDER BY revenue_lift_pct DESC NULLS LAST
