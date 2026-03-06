-- models/marts/mart_customer_ltv.sql
-- ============================================================
-- PURPOSE : Customer lifetime value (LTV) with segmentation
--           and preferred channel identification.
--
-- BUSINESS QUESTION:
--   "Who are our highest-value customers and what channel
--    do they prefer to shop on?"
--
-- KEY METRICS:
--   total_lifetime_spend : all-time net revenue from customer
--   order_count          : total number of transactions
--   avg_order_value      : spend per transaction
--   first_order_date     : when customer first purchased
--   last_order_date      : most recent purchase
--   days_since_last_order: recency metric (RFM-style)
--   preferred_channel    : channel with highest spend for customer
--   ltv_segment          : platinum / gold / silver / bronze
--
-- PREFERRED CHANNEL LOGIC:
--   We can't use the SQL standard MODE() function in Snowflake.
--   Instead, we use a CTE pattern:
--     1. Aggregate spend per customer per channel
--     2. Use ROW_NUMBER() to rank channels by spend (highest = 1)
--     3. Filter to rank 1 to get the preferred channel
--   This is a very common interview question about finding the
--   "top N per group" — know this pattern cold.
-- ============================================================

{{
    config(
        materialized = 'table',
        cluster_by   = ['ltv_segment', 'state_code'],
        tags         = ['customer', 'ltv', 'gold']
    )
}}

WITH net_sales AS (

    -- Start from net sales so LTV reflects actual revenue after returns
    SELECT
        customer_id,
        channel,
        state_code,
        region,
        sale_date,
        net_amount,
        ticket_number
    FROM {{ ref('int_net_sales') }}
    WHERE customer_id IS NOT NULL       -- exclude anonymous transactions

),

-- ── Step 1: Aggregate overall LTV metrics per customer ────────
customer_totals AS (

    SELECT
        customer_id,
        state_code,
        region,

        -- Lifetime spend (net of returns)
        SUM(net_amount)                         AS total_lifetime_spend,

        -- Order count = distinct tickets (not line items)
        COUNT(DISTINCT ticket_number)           AS order_count,

        -- Transaction count = individual line items
        COUNT(*)                                AS transaction_count,

        -- First and last purchase dates
        MIN(sale_date)                          AS first_order_date,
        MAX(sale_date)                          AS last_order_date,

        -- Recency in days (how long since last purchase)
        DATEDIFF('day', MAX(sale_date), CURRENT_DATE) AS days_since_last_order

    FROM net_sales
    GROUP BY customer_id, state_code, region

),

-- ── Step 2: Spend per customer per channel (for preferred channel) ──
channel_spend AS (

    SELECT
        customer_id,
        channel,
        SUM(net_amount)     AS channel_spend
    FROM net_sales
    GROUP BY customer_id, channel

),

-- ── Step 3: Rank channels per customer by spend ───────────────
-- ROW_NUMBER() assigns rank 1 to the highest-spend channel.
-- PARTITION BY customer_id: rank channels within each customer.
-- ORDER BY channel_spend DESC: highest spend = rank 1.
channel_ranked AS (

    SELECT
        customer_id,
        channel,
        channel_spend,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY channel_spend DESC
        )                   AS channel_rank
    FROM channel_spend

),

-- ── Step 4: Keep only the top-ranked (preferred) channel ──────
preferred_channel AS (

    SELECT
        customer_id,
        channel             AS preferred_channel,
        channel_spend       AS preferred_channel_spend
    FROM channel_ranked
    WHERE channel_rank = 1

),

-- ── Step 5: Join customer demographics ───────────────────────
customer_profile AS (

    SELECT
        ct.customer_id,
        c.first_name,
        c.last_name,
        ct.state_code,
        ct.region,
        ct.total_lifetime_spend,
        ct.order_count,
        ct.transaction_count,
        ct.first_order_date,
        ct.last_order_date,
        ct.days_since_last_order,
        pc.preferred_channel,

        -- Average order value = total spend / orders (not line items)
        {{ safe_divide('ct.total_lifetime_spend', 'ct.order_count') }}
                            AS avg_order_value

    FROM customer_totals ct
    LEFT JOIN {{ ref('stg_customer') }} c
        ON ct.customer_id = c.customer_id
    LEFT JOIN preferred_channel pc
        ON ct.customer_id = pc.customer_id

),

-- ── Step 6: LTV segmentation ──────────────────────────────────
-- Segment customers into 4 tiers based on total lifetime spend.
-- Thresholds are defined in dbt_project.yml vars so business
-- stakeholders can adjust them without touching SQL.
final AS (

    SELECT
        customer_id,
        first_name,
        last_name,
        state_code,
        region,
        total_lifetime_spend,
        order_count,
        transaction_count,
        avg_order_value,
        first_order_date,
        last_order_date,
        days_since_last_order,
        preferred_channel,

        -- ── LTV segment ────────────────────────────────────────
        -- platinum : top spenders ($10k+)
        -- gold     : high value ($5k–$9.99k)
        -- silver   : mid value ($1k–$4.99k)
        -- bronze   : entry level (< $1k)
        CASE
            WHEN total_lifetime_spend >= {{ var('ltv_platinum_threshold') }} THEN 'platinum'
            WHEN total_lifetime_spend >= {{ var('ltv_gold_threshold') }}     THEN 'gold'
            WHEN total_lifetime_spend >= {{ var('ltv_silver_threshold') }}   THEN 'silver'
            ELSE 'bronze'
        END                 AS ltv_segment

    FROM customer_profile

)

SELECT * FROM final
ORDER BY total_lifetime_spend DESC
