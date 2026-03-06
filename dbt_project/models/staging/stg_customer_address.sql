-- models/staging/stg_customer_address.sql

WITH source AS (
    SELECT * FROM {{ source('bronze', 'customer_address') }}
),
renamed AS (
    SELECT
        ca_address_sk           AS address_id,
        ca_address_id           AS address_code,
        ca_city                 AS city,
        -- Standardize state to uppercase 2-char code
        UPPER(TRIM(ca_state))   AS state_code,
        ca_zip                  AS zip_code,
        ca_country              AS country,
        -- Geographic grouping for regional analysis
        CASE UPPER(TRIM(ca_state))
            WHEN 'CA' THEN 'West'
            WHEN 'OR' THEN 'West'   WHEN 'WA' THEN 'West'
            WHEN 'TX' THEN 'South'  WHEN 'FL' THEN 'South'
            WHEN 'GA' THEN 'South'  WHEN 'NC' THEN 'South'
            WHEN 'NY' THEN 'Northeast' WHEN 'PA' THEN 'Northeast'
            WHEN 'MA' THEN 'Northeast' WHEN 'NJ' THEN 'Northeast'
            WHEN 'IL' THEN 'Midwest'   WHEN 'OH' THEN 'Midwest'
            WHEN 'MI' THEN 'Midwest'
            ELSE 'Other'
        END                     AS region
    FROM source
    WHERE ca_address_sk IS NOT NULL
)
SELECT * FROM renamed
