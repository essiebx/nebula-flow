-- models/staging/stg_clickstream.sql
-- ============================================================
-- PURPOSE : Clean the raw Kafka clickstream events from Bronze.
--           This is the only staging model that reads from a
--           synthetic source (not TPC-DS).
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_clickstream') }}
),
cleaned AS (
    SELECT
        -- Surrogate key: session + timestamp uniquely identifies an event
        {{ generate_surrogate_key(['session_id', 'event_timestamp']) }}
            AS event_sk,

        session_id,
        user_id                 AS customer_id,      -- maps to TPC-DS customer
        item_id,                                     -- maps to TPC-DS item
        event_type,
        event_timestamp,
        device_type,
        referrer_source,
        _ingested_at

    FROM source

    -- Filter out any rows with invalid event types
    -- (protects downstream clickstream funnel models)
    WHERE event_type IN ('page_view', 'add_to_cart', 'purchase')
      AND event_timestamp IS NOT NULL
      AND session_id IS NOT NULL
)
SELECT * FROM cleaned
