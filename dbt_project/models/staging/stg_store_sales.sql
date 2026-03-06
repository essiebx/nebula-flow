-- models/staging/stg_store_sales.sql
-- ============================================================
-- PURPOSE : Clean and rename raw store_sales columns.
--
-- RULES FOR ALL STAGING MODELS:
--   1. One model per Bronze source table (1:1 mapping)
--   2. Rename ugly TPC-DS prefix columns (ss_) to clean names
--   3. Cast data types (all TPC-DS numeric FKs are NUMBER)
--   4. Filter rows where primary/foreign keys are NULL
--   5. NO joins, NO aggregations, NO business logic here
--      (that belongs in intermediate/ and marts/)
--
-- TPC-DS NAMING CONVENTION (why columns look odd):
--   TPC-DS prefixes every column with the table abbreviation.
--   ss_ = store_sales, ws_ = web_sales, cs_ = catalog_sales
--   This makes the raw schema hard to read — cleaning it here
--   means all downstream models use consistent, readable names.
-- ============================================================

WITH source AS (

    -- Read directly from the Bronze clone
    SELECT * FROM {{ source('bronze', 'store_sales') }}

),

renamed AS (

    SELECT
        -- ── Surrogate key ─────────────────────────────────────
        -- TPC-DS has no single unique column for store_sales.
        -- We create a surrogate key from the natural composite:
        -- (ticket_number, item_id) which uniquely identifies a
        -- line item within a transaction.
        {{ generate_surrogate_key(['ss_ticket_number', 'ss_item_sk']) }}
            AS sale_sk,

        -- ── Foreign keys (dimension lookups) ─────────────────
        ss_sold_date_sk         AS date_sk,         -- → date_dim
        ss_item_sk              AS item_id,          -- → item
        ss_customer_sk          AS customer_id,      -- → customer
        ss_store_sk             AS store_id,         -- → store
        ss_promo_sk             AS promotion_id,     -- → promotion (NULL if no promo)

        -- ── Transaction identifiers ───────────────────────────
        ss_ticket_number        AS ticket_number,    -- receipt number
        ss_quantity             AS quantity_sold,

        -- ── Financial metrics ─────────────────────────────────
        -- TPC-DS has many price columns; we keep the most useful:
        ss_list_price           AS list_price,       -- retail/shelf price
        ss_sales_price          AS sales_price,      -- actual price charged
        ss_net_paid             AS gross_amount,     -- amount customer paid
        ss_net_paid_inc_tax     AS gross_amount_inc_tax,
        ss_net_profit           AS net_profit        -- after cost of goods

    FROM source

    -- ── Data quality filter ───────────────────────────────────
    -- Drop rows with NULL on critical FK columns.
    -- A sale with no item_id is unanalyzable and should not
    -- propagate downstream. In production, these would be
    -- routed to a quarantine table for investigation.
    WHERE ss_item_sk IS NOT NULL
      AND ss_ticket_number IS NOT NULL
      AND ss_sold_date_sk IS NOT NULL

)

SELECT * FROM renamed
