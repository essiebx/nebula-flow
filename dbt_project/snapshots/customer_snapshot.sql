-- snapshots/customer_snapshot.sql
-- ============================================================
-- PURPOSE : Track changes to customer address over time using
--           SCD (Slowly Changing Dimension) Type 2.
--
-- WHAT IS SCD TYPE 2?
--   When a customer moves to a new address, instead of overwriting
--   the old record, we:
--     1. Keep the old record with a dbt_valid_to date
--     2. Insert a new record with the new address
--   This preserves history — a sale from 2020 can still be
--   attributed to the customer's 2020 address.
--
-- WHY THIS IS AN INTERVIEW TOPIC:
--   SCD Type 2 comes up in almost every senior DE interview.
--   "How would you handle a customer who changes their address?"
--   Answer: SCD Type 2 snapshot in dbt — exactly this file.
--
-- HOW TO QUERY IT:
--   Current records only: WHERE dbt_valid_to IS NULL
--   Historical records:   WHERE dbt_valid_from <= '2020-01-01'
--                           AND (dbt_valid_to > '2020-01-01' OR dbt_valid_to IS NULL)
-- ============================================================

{% snapshot customer_snapshot %}

{{
    config(
        target_schema   = 'silver',
        unique_key      = 'customer_id',
        strategy        = 'check',
        check_cols      = ['state_code', 'zip_code', 'city'],
        invalidate_hard_deletes = true
    )
}}

-- Source: the cleaned customer + address staging models
-- We join here to snapshot the combined customer record
SELECT
    c.customer_id,
    c.customer_code,
    c.first_name,
    c.last_name,
    c.email,
    c.is_preferred_customer,
    a.address_code,
    a.city,
    a.state_code,
    a.zip_code,
    a.country,
    a.region

FROM {{ ref('stg_customer') }} c
LEFT JOIN {{ ref('stg_customer_address') }} a
    ON c.current_address_id = a.address_id

{% endsnapshot %}
