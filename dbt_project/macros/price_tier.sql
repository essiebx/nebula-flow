-- macros/price_tier.sql
-- ============================================================
-- PURPOSE : Reusable macro to bucket item prices into tiers.
--           Called in stg_item.sql to keep the logic in one place.
--           If thresholds change, update here — not in every model.
--
-- USAGE:
--   {{ price_tier('i_current_price') }}
--
-- EXAMPLE OUTPUT:
--   CASE
--     WHEN i_current_price < 25   THEN 'budget'
--     WHEN i_current_price < 75   THEN 'mid'
--     WHEN i_current_price < 200  THEN 'premium'
--     ELSE                             'luxury'
--   END
-- ============================================================

{% macro price_tier(price_column) %}
    CASE
        WHEN {{ price_column }} <  25  THEN 'budget'
        WHEN {{ price_column }} <  75  THEN 'mid'
        WHEN {{ price_column }} < 200  THEN 'premium'
        ELSE                                'luxury'
    END
{% endmacro %}
