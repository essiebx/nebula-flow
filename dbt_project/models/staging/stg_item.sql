-- models/staging/stg_item.sql
-- ============================================================
-- PURPOSE : Product catalog with price_tier bucketing.
--           price_tier is created here (in the dimension model)
--           so ALL fact models inherit it via join — no logic
--           duplication across mart models.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'item') }}
),

renamed AS (
    SELECT
        i_item_sk               AS item_id,
        i_item_id               AS item_code,        -- business-facing alphanumeric ID
        i_item_desc             AS item_description,
        i_current_price         AS current_price,
        i_category              AS category,         -- e.g. 'Electronics', 'Clothing'
        i_class                 AS item_class,       -- sub-category
        i_brand                 AS brand,
        i_units                 AS unit_of_measure,
        i_size                  AS item_size,
        i_color                 AS color,

        -- ── Price tier (uses the price_tier macro) ─────────────
        -- budget   : price < $25
        -- mid      : $25 – $74.99
        -- premium  : $75 – $199.99
        -- luxury   : $200+
        -- This classification is used in returns analysis and
        -- promotion effectiveness to segment by product value.
        {{ price_tier('i_current_price') }} AS price_tier

    FROM source
    WHERE i_item_sk IS NOT NULL
)

SELECT * FROM renamed
