-- models/marts/mart_daily_revenue.sql
-- ============================================================
-- PURPOSE : Daily net revenue broken down by channel and region.
--           This is the primary executive KPI mart.
--
-- BUSINESS QUESTION:
--   "What is our net revenue across store, web, and catalog
--    channels — by day and region?"
--
-- KEY METRICS:
--   gross_revenue    : total amount paid before returns
--   return_amount    : total value of returns on that day
--   net_revenue      : gross_revenue - return_amount  ← headline KPI
--   transaction_count: number of individual line items
--   avg_order_value  : net_revenue / transaction_count
--
-- MATERIALIZATION:
--   TABLE (not view) so BI tools query pre-computed results.
--   Clustered on (sale_date, channel) so date-range + channel
--   filters skip micro-partitions efficiently.
-- ============================================================

{{
    config(
        materialized  = 'table',
        cluster_by    = ['sale_date', 'channel'],
        tags          = ['daily', 'revenue', 'gold']
    )
}}

WITH net_sales AS (

    -- Pull from the intermediate net_sales model which already
    -- has gross_amount and return_amount side-by-side
    SELECT
        sale_date,
        channel,
        state_code,
        region,
        calendar_year,
        month_of_year,
        fiscal_year,
        fiscal_quarter,
        is_holiday,
        is_weekend,
        gross_amount,
        return_amount,
        net_amount,
        quantity_sold,
        was_returned,
        customer_id,
        promotion_id
    FROM {{ ref('int_net_sales') }}

),

aggregated AS (

    SELECT
        -- ── Dimensions (GROUP BY keys) ─────────────────────────
        sale_date,
        channel,
        state_code,
        region,
        calendar_year,
        month_of_year,
        fiscal_year,
        fiscal_quarter,
        is_holiday,
        is_weekend,

        -- ── Revenue metrics ────────────────────────────────────
        SUM(gross_amount)                   AS gross_revenue,
        SUM(COALESCE(return_amount, 0))     AS total_return_amount,
        SUM(net_amount)                     AS net_revenue,

        -- ── Volume metrics ─────────────────────────────────────
        COUNT(*)                            AS transaction_count,
        SUM(quantity_sold)                  AS total_units_sold,
        COUNT(DISTINCT customer_id)         AS unique_customers,
        COUNT(DISTINCT CASE WHEN promotion_id IS NOT NULL
              THEN 1 END)                   AS promo_transaction_count,

        -- ── Derived metrics ────────────────────────────────────
        -- Average order value = net revenue per transaction
        -- safe_divide protects against 0 transactions edge case
        {{ safe_divide('SUM(net_amount)', 'COUNT(*)') }}
                                            AS avg_order_value,

        -- Return rate = % of transactions that had a return
        {{ safe_divide('SUM(CASE WHEN was_returned THEN 1 ELSE 0 END)', 'COUNT(*)') }}
                                            AS return_rate

    FROM net_sales

    GROUP BY
        sale_date,
        channel,
        state_code,
        region,
        calendar_year,
        month_of_year,
        fiscal_year,
        fiscal_quarter,
        is_holiday,
        is_weekend

)

SELECT * FROM aggregated
