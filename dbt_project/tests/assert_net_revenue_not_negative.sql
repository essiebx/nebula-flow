-- tests/assert_net_revenue_not_negative.sql
-- ============================================================
-- PURPOSE : Fail if any row in mart_daily_revenue has a
--           negative net_revenue value.
--
-- HOW dbt CUSTOM TESTS WORK:
--   The test query should return 0 rows when the assertion PASSES.
--   If the query returns any rows, dbt marks the test as FAILED.
--   So we SELECT the failing rows — dbt does the rest.
-- ============================================================

SELECT
    sale_date,
    channel,
    net_revenue
FROM {{ ref('mart_daily_revenue') }}
WHERE net_revenue < 0
