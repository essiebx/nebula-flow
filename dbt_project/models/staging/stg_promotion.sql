-- models/staging/stg_promotion.sql
-- ============================================================
-- PURPOSE : Consolidate TPC-DS multi-flag promotion channels
--           into a single channel_type column.
--
-- WHY THIS IS NEEDED:
--   TPC-DS stores promotion channels as separate boolean columns:
--     p_channel_email, p_channel_tv, p_channel_radio,
--     p_channel_direct_mail, p_channel_event, p_channel_demo
--   This is hard to GROUP BY in a mart. We convert to a single
--   channel_type string, taking the first active channel found.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'promotion') }}
),
renamed AS (
    SELECT
        p_promo_sk              AS promotion_id,
        p_promo_id              AS promotion_code,
        p_promo_name            AS promotion_name,
        p_start_date_sk         AS start_date_sk,
        p_end_date_sk           AS end_date_sk,
        p_cost                  AS promotion_cost,
        p_response_target       AS response_target,
        p_discount_active       AS is_discount_active,

        -- Consolidate channel flags to single column
        -- Priority order: email > tv > direct_mail > event > demo > other
        CASE
            WHEN p_channel_email       = 'Y' THEN 'email'
            WHEN p_channel_tv          = 'Y' THEN 'tv'
            WHEN p_channel_radio       = 'Y' THEN 'radio'
            WHEN p_channel_direct_mail = 'Y' THEN 'direct_mail'
            WHEN p_channel_event       = 'Y' THEN 'event'
            WHEN p_channel_demo        = 'Y' THEN 'demo'
            ELSE 'other'
        END                     AS channel_type

    FROM source
    WHERE p_promo_sk IS NOT NULL
)
SELECT * FROM renamed
