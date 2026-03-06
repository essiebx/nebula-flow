-- models/staging/stg_customer.sql

WITH source AS (
    SELECT * FROM {{ source('bronze', 'customer') }}
),
renamed AS (
    SELECT
        c_customer_sk           AS customer_id,
        c_customer_id           AS customer_code,
        c_first_name            AS first_name,
        c_last_name             AS last_name,
        c_email_address         AS email,
        c_current_addr_sk       AS current_address_id,

        -- PII protection: store decade instead of exact birth year
        -- This is a common interview talking point — shows awareness
        -- of data governance even in a portfolio project.
        FLOOR(c_birth_year / 10) * 10 AS birth_decade,

        c_preferred_cust_flag   AS is_preferred_customer,
        c_login                 AS login_name

    FROM source
    WHERE c_customer_sk IS NOT NULL
)
SELECT * FROM renamed
