-- models/staging/stg_web_sales.sql
-- ============================================================
-- PURPOSE : Clean and rename raw web_sales columns.
--           Mirrors stg_store_sales.sql pattern — ws_ prefix.
--
-- KEY DIFFERENCE from store_sales:
--   Web sales have ship-to address and bill-to address FKs
--   (ws_ship_addr_sk, ws_bill_addr_sk) that store sales don't.
--   We keep ship_addr_sk as it's useful for geographic analysis.
-- ============================================================

WITH source AS (

    SELECT * FROM {{ source('bronze', 'web_sales') }}

),

renamed AS (

    SELECT
        -- ── Surrogate key ─────────────────────────────────────
        {{ generate_surrogate_key(['ws_order_number', 'ws_item_sk']) }}
            AS sale_sk,

        -- ── Foreign keys ──────────────────────────────────────
        ws_sold_date_sk         AS date_sk,
        ws_item_sk              AS item_id,
        ws_bill_customer_sk     AS customer_id,      -- billing customer
        ws_ship_addr_sk         AS ship_address_id,  -- ship-to address (web-specific)
        ws_promo_sk             AS promotion_id,

        -- ── Transaction identifiers ───────────────────────────
        ws_order_number         AS ticket_number,    -- maps to order_number for web
        ws_quantity             AS quantity_sold,

        -- ── Financial metrics ─────────────────────────────────
        ws_list_price           AS list_price,
        ws_sales_price          AS sales_price,
        ws_net_paid             AS gross_amount,
        ws_net_paid_inc_tax     AS gross_amount_inc_tax,
        ws_net_profit           AS net_profit

    FROM source

    WHERE ws_item_sk IS NOT NULL
      AND ws_order_number IS NOT NULL
      AND ws_sold_date_sk IS NOT NULL

)

SELECT * FROM renamed
