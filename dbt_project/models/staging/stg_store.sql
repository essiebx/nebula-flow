-- models/staging/stg_store.sql

WITH source AS (
    SELECT * FROM {{ source('bronze', 'store') }}
),
renamed AS (
    SELECT
        s_store_sk              AS store_id,
        s_store_id              AS store_code,
        s_store_name            AS store_name,
        s_number_employees      AS employee_count,
        s_city                  AS city,
        UPPER(TRIM(s_state))    AS state_code,
        s_country               AS country,
        s_market_id             AS market_id,
        s_market_desc           AS market_description,
        s_geography_class       AS geography_class,
        -- Derive region from state using same mapping as stg_customer_address
        CASE UPPER(TRIM(s_state))
            WHEN 'CA' THEN 'West'   WHEN 'OR' THEN 'West'   WHEN 'WA' THEN 'West'
            WHEN 'TX' THEN 'South'  WHEN 'FL' THEN 'South'  WHEN 'GA' THEN 'South'
            WHEN 'NY' THEN 'Northeast' WHEN 'PA' THEN 'Northeast' WHEN 'MA' THEN 'Northeast'
            WHEN 'IL' THEN 'Midwest'   WHEN 'OH' THEN 'Midwest'
            ELSE 'Other'
        END                     AS region
    FROM source
    WHERE s_store_sk IS NOT NULL
)
SELECT * FROM renamed
