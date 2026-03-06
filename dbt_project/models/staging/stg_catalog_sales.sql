-- models/staging/stg_catalog_sales.sql
-- ============================================================
-- PURPOSE : Clean and rename raw catalog_sales (cs_ prefix).
--           Catalog = B2B / mail-order channel in TPC-DS.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'catalog_sales') }}
),

renamed AS (
    SELECT
        {{ generate_surrogate_key(['cs_order_number', 'cs_item_sk']) }}
            AS sale_sk,

        cs_sold_date_sk         AS date_sk,
        cs_item_sk              AS item_id,
        cs_bill_customer_sk     AS customer_id,
        cs_promo_sk             AS promotion_id,
        cs_order_number         AS ticket_number,
        cs_quantity             AS quantity_sold,
        cs_list_price           AS list_price,
        cs_sales_price          AS sales_price,
        cs_net_paid             AS gross_amount,
        cs_net_paid_inc_tax     AS gross_amount_inc_tax,
        cs_net_profit           AS net_profit

    FROM source
    WHERE cs_item_sk IS NOT NULL
      AND cs_order_number IS NOT NULL
      AND cs_sold_date_sk IS NOT NULL
)

SELECT * FROM renamed
