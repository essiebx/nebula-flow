-- models/staging/stg_inventory.sql
-- ============================================================
-- PURPOSE : Clean inventory table — stock levels per item
--           per warehouse per date.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'inventory') }}
),

renamed AS (
    SELECT
        {{ generate_surrogate_key(['inv_item_sk', 'inv_warehouse_sk', 'inv_date_sk']) }}
            AS inventory_sk,

        inv_date_sk             AS date_sk,
        inv_item_sk             AS item_id,
        inv_warehouse_sk        AS warehouse_id,
        inv_quantity_on_hand    AS quantity_on_hand

    FROM source
    WHERE inv_item_sk IS NOT NULL
      AND inv_warehouse_sk IS NOT NULL
)

SELECT * FROM renamed
