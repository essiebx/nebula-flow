-- models/intermediate/int_unified_sales.sql
-- ============================================================
-- PURPOSE : Combine all 3 sales channels into a single unified
--           sales spine with consistent column names.
--
-- WHY THIS MATTERS:
--   TPC-DS separates store_sales, web_sales, and catalog_sales
--   into three different tables with different column prefixes.
--   All our mart models need a single "sales" view across channels.
--   This intermediate model creates that unified view ONCE so we
--   don't repeat the UNION ALL logic in every mart.
--
-- DESIGN DECISION — UNION ALL vs UNION:
--   We use UNION ALL (not UNION) intentionally.
--   UNION would deduplicate rows — but there are no duplicates
--   across channels (a store sale and a web sale can't be the
--   same row). UNION ALL is faster because it skips the dedup scan.
--
-- WHAT THIS MODEL DOES NOT DO:
--   - No aggregation (that's for marts)
--   - No returns subtraction (that's for int_net_sales)
-- ============================================================

WITH store_sales AS (

    SELECT
        sale_sk,
        date_sk,
        item_id,
        customer_id,
        store_id,
        promotion_id,
        ticket_number,
        quantity_sold,
        list_price,
        sales_price,
        gross_amount,
        gross_amount_inc_tax,
        net_profit,
        -- Tag the channel so we can GROUP BY channel in marts
        'store'     AS channel
    FROM {{ ref('stg_store_sales') }}

),

web_sales AS (

    SELECT
        sale_sk,
        date_sk,
        item_id,
        customer_id,
        NULL            AS store_id,        -- web sales have no physical store
        promotion_id,
        ticket_number,
        quantity_sold,
        list_price,
        sales_price,
        gross_amount,
        gross_amount_inc_tax,
        net_profit,
        'web'       AS channel
    FROM {{ ref('stg_web_sales') }}

),

catalog_sales AS (

    SELECT
        sale_sk,
        date_sk,
        item_id,
        customer_id,
        NULL            AS store_id,        -- catalog sales have no physical store
        promotion_id,
        ticket_number,
        quantity_sold,
        list_price,
        sales_price,
        gross_amount,
        gross_amount_inc_tax,
        net_profit,
        'catalog'   AS channel
    FROM {{ ref('stg_catalog_sales') }}

),

-- Combine all 3 channels into one table
unified AS (

    SELECT * FROM store_sales
    UNION ALL
    SELECT * FROM web_sales
    UNION ALL
    SELECT * FROM catalog_sales

),

-- Enrich with item, date, customer, and store context
-- We do the joins HERE so every mart that builds on this model
-- gets the enrichment for free without repeating join logic.
enriched AS (

    SELECT
        u.sale_sk,
        u.channel,
        u.ticket_number,
        u.quantity_sold,
        u.gross_amount,
        u.gross_amount_inc_tax,
        u.net_profit,
        u.promotion_id,

        -- Date context
        d.calendar_date         AS sale_date,
        d.calendar_year,
        d.month_of_year,
        d.fiscal_year,
        d.fiscal_quarter,
        d.is_holiday,
        d.is_weekend,

        -- Item context
        i.item_id,
        i.item_description,
        i.category,
        i.item_class,
        i.brand,
        i.current_price,
        i.price_tier,

        -- Customer context
        c.customer_id,
        c.first_name,
        c.last_name,

        -- Geographic context (from customer address for web/catalog,
        -- from store for store sales)
        COALESCE(s.state_code, ca.state_code)   AS state_code,
        COALESCE(s.region,     ca.region)       AS region

    FROM unified u

    -- date_dim: always required, inner join to drop unmapped date SKs
    INNER JOIN {{ ref('stg_date_dim') }}        d  ON u.date_sk      = d.date_sk

    -- item: always required
    INNER JOIN {{ ref('stg_item') }}            i  ON u.item_id      = i.item_id

    -- customer: optional (some transactions may have no customer SK)
    LEFT JOIN  {{ ref('stg_customer') }}        c  ON u.customer_id  = c.customer_id

    -- customer_address: for web/catalog geography
    LEFT JOIN  {{ ref('stg_customer_address') }} ca ON c.current_address_id = ca.address_id

    -- store: for store-channel geography (NULL for web/catalog)
    LEFT JOIN  {{ ref('stg_store') }}           s  ON u.store_id     = s.store_id

)

SELECT * FROM enriched
