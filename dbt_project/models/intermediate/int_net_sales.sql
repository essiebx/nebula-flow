-- models/intermediate/int_net_sales.sql
-- ============================================================
-- PURPOSE : Compute net revenue by subtracting returns from sales.
--
-- NET REVENUE FORMULA:
--   net_amount = gross_amount - COALESCE(return_amount, 0)
--
-- WHY LEFT JOIN (not INNER JOIN):
--   Most sales have NO return. An INNER JOIN would drop all
--   sale rows that were never returned — which is the majority
--   of the data. We use LEFT JOIN to keep all sales, and
--   COALESCE to treat "no return" as 0.
--
-- JOIN KEY:
--   We join on (ticket_number, item_id, channel).
--   ticket_number + item_id identifies a specific line item.
--   We also match on return_channel = channel to prevent
--   cross-channel false matches (unlikely but defensive).
-- ============================================================

WITH sales AS (
    SELECT * FROM {{ ref('int_unified_sales') }}
),

returns AS (
    SELECT
        ticket_number,
        item_id,
        return_channel,
        return_amount,
        quantity_returned
    FROM {{ ref('int_unified_returns') }}
),

net_sales AS (

    SELECT
        -- Carry all sales columns forward
        s.sale_sk,
        s.channel,
        s.sale_date,
        s.calendar_year,
        s.month_of_year,
        s.fiscal_year,
        s.fiscal_quarter,
        s.is_holiday,
        s.is_weekend,
        s.item_id,
        s.item_description,
        s.category,
        s.item_class,
        s.brand,
        s.price_tier,
        s.current_price,
        s.customer_id,
        s.first_name,
        s.last_name,
        s.state_code,
        s.region,
        s.ticket_number,
        s.promotion_id,
        s.quantity_sold,
        s.gross_amount,

        -- Return columns (NULL if no return occurred)
        r.return_amount,
        r.quantity_returned,

        -- ── NET REVENUE ───────────────────────────────────────
        -- This is the key metric all revenue marts build from.
        -- COALESCE(r.return_amount, 0) turns NULL → 0 so the
        -- subtraction works even when there is no return row.
        s.gross_amount - COALESCE(r.return_amount, 0) AS net_amount,

        -- Was this sale eventually returned?
        CASE WHEN r.return_amount IS NOT NULL THEN TRUE ELSE FALSE END
            AS was_returned

    FROM sales s

    LEFT JOIN returns r
        ON  s.ticket_number  = r.ticket_number
        AND s.item_id        = r.item_id

)

SELECT * FROM net_sales
