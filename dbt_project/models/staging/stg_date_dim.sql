-- models/staging/stg_date_dim.sql
-- ============================================================
-- PURPOSE : Expose the most useful date_dim columns cleanly.
--           In particular, expose is_holiday and is_weekend as
--           BOOLEAN so downstream models can filter with TRUE/FALSE
--           instead of 'Y'/'N' string comparisons.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'date_dim') }}
),
renamed AS (
    SELECT
        d_date_sk               AS date_sk,
        d_date                  AS calendar_date,
        d_day_name              AS day_name,
        d_month_seq             AS month_seq,
        d_year                  AS calendar_year,
        d_moy                   AS month_of_year,
        d_dom                   AS day_of_month,
        d_dow                   AS day_of_week,         -- 0=Sun … 6=Sat

        -- TPC-DS stores flags as 'Y'/'N' strings — cast to BOOLEAN
        -- for cleaner downstream filtering (WHERE is_holiday = TRUE)
        (d_holiday = 'Y')       AS is_holiday,
        (d_weekend = 'Y')       AS is_weekend,

        -- Fiscal calendar columns (useful for retail analysis)
        d_fiscal_year_sk        AS fiscal_year_sk,
        d_fy_year               AS fiscal_year,
        d_fy_quarter_seq        AS fiscal_quarter_seq,
        d_qoy                   AS fiscal_quarter

    FROM source
    WHERE d_date_sk IS NOT NULL
)
SELECT * FROM renamed
