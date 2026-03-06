-- models/staging/stg_store_returns.sql
-- ============================================================
-- PURPOSE : Clean raw store_returns and add return_channel tag.
--
-- HOW RETURNS JOIN TO SALES:
--   Returns reference the original sale via (sr_item_sk, sr_ticket_number).
--   This composite join key is how we compute net revenue:
--     net_amount = gross_amount - return_amount
--   (See int_net_sales.sql for the full join logic)
--
-- RETURN REASON CODES:
--   TPC-DS has a return_reason table (not included in our scope)
--   but the reason_sk FK is preserved here for potential future use.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'store_returns') }}
),

renamed AS (
    SELECT
        -- ── Surrogate key ─────────────────────────────────────
        {{ generate_surrogate_key(['sr_ticket_number', 'sr_item_sk']) }}
            AS return_sk,

        -- ── Join keys back to original sale ───────────────────
        -- These two columns are how we match a return to its sale
        sr_item_sk              AS item_id,
        sr_ticket_number        AS ticket_number,    -- same ticket as the original sale

        -- ── Other FKs ─────────────────────────────────────────
        sr_returned_date_sk     AS return_date_sk,
        sr_customer_sk          AS customer_id,
        sr_return_reason_sk     AS return_reason_id,

        -- ── Financial metrics ─────────────────────────────────
        sr_return_quantity      AS quantity_returned,
        sr_return_amt           AS return_amount,    -- amount refunded to customer
        sr_net_loss             AS net_loss,         -- merchant's net loss on the return

        -- ── Channel tag ───────────────────────────────────────
        -- We tag every return with its channel so that when we
        -- UNION ALL store + web returns in int_unified_returns,
        -- we can still distinguish which channel the return came from.
        'store'                 AS return_channel

    FROM source
    WHERE sr_item_sk IS NOT NULL
      AND sr_ticket_number IS NOT NULL
)

SELECT * FROM renamed
