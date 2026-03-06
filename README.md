# Snowflake Retail Intelligence Engine

> Production-pattern Data Engineering pipeline on the **TPC-DS** retail benchmark schema —
> the same dataset used by Amazon, Google, and Databricks to validate analytical systems at scale.

---

##  What This Demonstrates

| Skill | Implementation |
|---|---|
| Medallion Architecture | Bronze / Silver / Gold schemas in Snowflake |
| Real-time streaming | Apache Kafka → Snowpipe (clickstream events) |
| Multi-channel unification | store + web + catalog sales → single revenue view |
| Incremental CDC | Snowflake Streams + Tasks (24h schedule) |
| SQL transformations as code | dbt Core: 14 staging + 4 intermediate + 5 mart models |
| SCD Type 2 | dbt snapshot on customer address changes |
| Pipeline orchestration | Apache Airflow — 15-task master DAG |
| Data quality enforcement | Great Expectations — 3 checkpoint layers |
| CI/CD | dbt test + SQLFluff + flake8 + GE on every PR |
| Reproducible local env | Docker Compose — one command startup |

---

## Business Questions Answered (Gold Layer)

| Mart | Business Question |
|---|---|
| `mart_daily_revenue` | What is net revenue by day, channel (store/web/catalog), and region? |
| `mart_returns_analysis` | Which products have the highest return rates — and does promotion correlate? |
| `mart_inventory_health` | Which items are at risk of stocking out in the next 7 days? |
| `mart_customer_ltv` | Who are our highest-value customers and what channel do they prefer? |
| `mart_promotion_effectiveness` | Do promotions actually lift net revenue vs the baseline period? |

---

## Architecture

```
TPCDS_SF10TCL (Snowflake hosted)    Python Faker
     12 tables                       ↓
     CLONE (zero-copy)         Kafka Producer
          ↓                          ↓
    BRONZE (Raw)  ←──── Snowpipe ────┘
          ↓
    Snowflake Streams (CDC)
    Snowflake Tasks (24h)
    dbt staging models
          ↓
    SILVER (Cleaned + Tested)
          ↓
    dbt intermediate + mart models
    Materialized Views
          ↓
    GOLD (Serving Layer)
          ↓
  Grafana (local)    Tableau Public (cloud)
```

See `/docs/architecture.md` for the full Mermaid diagram.

---

##  Quickstart (~20 minutes)

```bash
# 1. Clone and configure
git clone https://github.com/<you>/snowflake-retail-intelligence
cd snowflake-retail-intelligence
cp .env.example .env           # fill in your Snowflake credentials

# 2. Validate your environment
python scripts/validate_env.py

# 3. Start local infrastructure (Kafka, Airflow, Grafana)
bash scripts/setup_local_env.sh

# 4. Provision Snowflake resources
snowsql -f setup/01_env_provisioning.sql
snowsql -f setup/02_rbac_grants.sql

# 5. Load Bronze layer
snowsql -f bronze/03_clone_tpcds.sql
snowsql -f bronze/04_create_raw_stages.sql
snowsql -f bronze/05_create_raw_tables.sql
snowsql -f bronze/06_snowpipe_setup.sql

# 6. Set up CDC
snowsql -f silver/07_stream_setup.sql
snowsql -f silver/08_task_scheduler.sql

# 7. Run dbt pipeline
cd dbt_project
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve   # optional: view lineage graph

# 8. Open dashboards
# Airflow:  http://localhost:8080  (user: airflow / pass: airflow)
# Grafana:  http://localhost:3000  (user: admin / pass: admin)
# Kafka UI: http://localhost:8090
```

---

## Interview Walkthrough

See `/docs/interview_story.md` — STAR-format narrative for every component.

---

## Repository Structure

See `/docs/project_structure.md` for the full annotated file tree.

---

## Teardown (Stop Snowflake Credits)

```bash
snowsql -f scripts/teardown_snowflake.sql
docker-compose down -v
```
