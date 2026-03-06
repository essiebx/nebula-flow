-- tests/assert_all_channels_present.sql
-- Fail if any of the 3 expected channels is missing from mart_daily_revenue.
SELECT 'store'   AS expected_channel WHERE 'store'   NOT IN (SELECT DISTINCT channel FROM {{ ref('mart_daily_revenue') }})
UNION ALL
SELECT 'web'                           WHERE 'web'     NOT IN (SELECT DISTINCT channel FROM {{ ref('mart_daily_revenue') }})
UNION ALL
SELECT 'catalog'                       WHERE 'catalog' NOT IN (SELECT DISTINCT channel FROM {{ ref('mart_daily_revenue') }})
