-- ============================================================
-- 02_rbac_grants.sql
-- PURPOSE : Create roles and grant permissions following the
--           principle of least privilege.
-- RUN     : snowsql -f setup/02_rbac_grants.sql
-- ORDER   : Run AFTER 01_env_provisioning.sql
--
-- ROLES CREATED:
--   ENGINEER_ROLE  → full access (read/write all schemas)
--   ANALYST_ROLE   → read-only access to GOLD schema only
-- ============================================================

USE DATABASE RETAIL_DB;
USE WAREHOUSE RETAIL_WH;

-- ── Step 1: Create roles ──────────────────────────────────────
-- ENGINEER_ROLE is for the pipeline — it needs to write to all
-- three schemas (Bronze, Silver, Gold).
-- ANALYST_ROLE is for BI tools — it only needs to SELECT from
-- Gold. Analysts should never touch raw data directly.
CREATE ROLE IF NOT EXISTS ENGINEER_ROLE
    COMMENT = 'Full pipeline access: read/write Bronze, Silver, Gold';

CREATE ROLE IF NOT EXISTS ANALYST_ROLE
    COMMENT = 'Read-only access to GOLD schema for BI tools';

-- ── Step 2: Grant warehouse access ───────────────────────────
-- Both roles need the warehouse to run queries.
GRANT USAGE ON WAREHOUSE RETAIL_WH TO ROLE ENGINEER_ROLE;
GRANT USAGE ON WAREHOUSE RETAIL_WH TO ROLE ANALYST_ROLE;

-- ── Step 3: Grant database access ────────────────────────────
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE ENGINEER_ROLE;
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE ANALYST_ROLE;

-- ── Step 4: Engineer role — full access to all schemas ───────
GRANT USAGE ON SCHEMA RETAIL_DB.BRONZE TO ROLE ENGINEER_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DB.SILVER TO ROLE ENGINEER_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DB.GOLD   TO ROLE ENGINEER_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA RETAIL_DB.BRONZE TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA RETAIL_DB.SILVER TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA RETAIL_DB.GOLD   TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL STAGES    IN SCHEMA RETAIL_DB.BRONZE TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL PIPES     IN SCHEMA RETAIL_DB.BRONZE TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL STREAMS   IN SCHEMA RETAIL_DB.SILVER TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL TASKS     IN SCHEMA RETAIL_DB.SILVER TO ROLE ENGINEER_ROLE;

-- Ensure future tables are also covered (important for dbt)
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RETAIL_DB.BRONZE TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RETAIL_DB.SILVER TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RETAIL_DB.GOLD   TO ROLE ENGINEER_ROLE;

-- ── Step 5: Analyst role — SELECT on GOLD only ───────────────
-- Analysts connect BI tools (Grafana, Tableau) to this role.
-- They can only read from the curated Gold layer.
GRANT USAGE  ON SCHEMA RETAIL_DB.GOLD TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES     IN SCHEMA RETAIL_DB.GOLD TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS      IN SCHEMA RETAIL_DB.GOLD TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES  IN SCHEMA RETAIL_DB.GOLD TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE VIEWS   IN SCHEMA RETAIL_DB.GOLD TO ROLE ANALYST_ROLE;

-- ── Step 6: Assign roles to your user ────────────────────────
-- Replace YOUR_SNOWFLAKE_USERNAME with your actual username.
-- In a team setting, grant roles to a service account instead.
-- GRANT ROLE ENGINEER_ROLE TO USER YOUR_SNOWFLAKE_USERNAME;
-- GRANT ROLE ANALYST_ROLE  TO USER YOUR_SNOWFLAKE_USERNAME;

-- ── Verify ───────────────────────────────────────────────────
SHOW GRANTS TO ROLE ENGINEER_ROLE;
SHOW GRANTS TO ROLE ANALYST_ROLE;
