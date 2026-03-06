-- macros/safe_divide.sql
-- ============================================================
-- PURPOSE : NULL-safe division to prevent divide-by-zero errors.
--           Returns NULL instead of raising an error when the
--           denominator is zero.
--
-- USAGE:
--   {{ safe_divide('units_returned', 'units_sold') }}
--
-- EXAMPLE OUTPUT:
--   units_returned / NULLIF(units_sold, 0)
-- ============================================================

{% macro safe_divide(numerator, denominator) %}
    {{ numerator }} / NULLIF({{ denominator }}, 0)
{% endmacro %}


-- macros/channel_label.sql
-- ============================================================
-- PURPOSE : Maps a source table name to a clean channel label.
--           Used in int_unified_sales.sql and int_unified_returns.sql
--           so the channel column is consistent across all marts.
--
-- USAGE (as a literal string in a UNION ALL):
--   'store'   for store_sales rows
--   'web'     for web_sales rows
--   'catalog' for catalog_sales rows
--
-- This macro is a documentation aid — the actual values are
-- hardcoded strings in the intermediate models. See the note
-- in int_unified_sales.sql for why we chose strings over a macro.
-- ============================================================

{% macro channel_label(source_name) %}
    {% if source_name == 'store' %}    'store'
    {% elif source_name == 'web' %}    'web'
    {% elif source_name == 'catalog' %}'catalog'
    {% else %} {{ exceptions.raise_compiler_error("Unknown channel: " ~ source_name) }}
    {% endif %}
{% endmacro %}


-- macros/generate_surrogate_key.sql
-- ============================================================
-- PURPOSE : Thin wrapper around dbt_utils.generate_surrogate_key.
--           Creates a consistent MD5 hash key from multiple columns.
--           Using a wrapper means we can swap the underlying
--           implementation without updating every model.
--
-- USAGE:
--   {{ generate_surrogate_key(['sale_date', 'channel', 'item_id']) }}
-- ============================================================

{% macro generate_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
