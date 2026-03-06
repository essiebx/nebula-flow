-- models/intermediate/int_unified_returns.sql
-- ============================================================
-- PURPOSE : Combine store_returns + web_returns into a single
--           returns spine with enriched item and date context.
--
-- NOTE: Catalog returns are not in our TPC-DS scope.
--   In practice, catalog returns exist but we omit them here
--   for simplicity. The pattern is identical to store/web.
-- ============================================================

WITH returns_union AS (

    SELECT
        return_sk,
        item_id,
        ticket_number,
        return_date_sk,
        customer_id,
        return_reason_id,
        quantity_returned,
        return_amount,
        net_loss,
        return_channel
    FROM {{ ref('stg_store_returns') }}

    UNION ALL

    SELECT
        return_sk,
        item_id,
        ticket_number,
        return_date_sk,
        customer_id,
        return_reason_id,
        quantity_returned,
        return_amount,
        net_loss,
        return_channel
    FROM {{ ref('stg_web_returns') }}

),

enriched AS (

    SELECT
        r.return_sk,
        r.return_channel,
        r.ticket_number,
        r.item_id,
        r.customer_id,
        r.quantity_returned,
        r.return_amount,
        r.net_loss,

        -- Date context for the return event
        d.calendar_date         AS return_date,
        d.calendar_year         AS return_year,
        d.month_of_year         AS return_month,

        -- Item context
        i.category,
        i.price_tier,

        -- Was a promotion active for this item?
        -- We check by joining to int_unified_sales using the ticket_number.
        -- If a matching sale row has a non-null promotion_id, flag it.
        -- This is used in mart_returns_analysis for promo correlation.
        CASE
            WHEN s.promotion_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END                     AS was_promoted

    FROM returns_union r

    LEFT JOIN {{ ref('stg_date_dim') }} d
        ON r.return_date_sk = d.date_sk

    LEFT JOIN {{ ref('stg_item') }} i
        ON r.item_id = i.item_id

    -- Join to unified sales to detect promotion linkage
    LEFT JOIN {{ ref('int_unified_sales') }} s
        ON  r.ticket_number = s.ticket_number
        AND r.item_id       = s.item_id

)

SELECT * FROM enriched
