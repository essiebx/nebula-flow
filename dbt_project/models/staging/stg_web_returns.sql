-- models/staging/stg_web_returns.sql
-- ============================================================
-- PURPOSE : Clean raw web_returns — mirrors stg_store_returns
--           but for the web channel (wr_ prefix).
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'web_returns') }}
),

renamed AS (
    SELECT
        {{ generate_surrogate_key(['wr_order_number', 'wr_item_sk']) }}
            AS return_sk,

        wr_item_sk              AS item_id,
        wr_order_number         AS ticket_number,
        wr_returned_date_sk     AS return_date_sk,
        wr_returning_customer_sk AS customer_id,
        wr_reason_sk            AS return_reason_id,
        wr_return_quantity      AS quantity_returned,
        wr_return_amt           AS return_amount,
        wr_net_loss             AS net_loss,
        'web'                   AS return_channel

    FROM source
    WHERE wr_item_sk IS NOT NULL
      AND wr_order_number IS NOT NULL
)

SELECT * FROM renamed
