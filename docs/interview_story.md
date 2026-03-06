# interview_story.md
# ============================================================
# STAR-Format Interview Narratives — Retail Intelligence Engine
# Read this before every interview. Practice saying these aloud.
# ============================================================

---

## "Walk me through a data pipeline you built."

**Situation:**
I built an end-to-end retail analytics pipeline on Snowflake using the TPC-DS benchmark dataset — the same schema used by Amazon and Google to stress-test analytical systems. The challenge was transforming raw multi-channel retail data (store, web, and catalog sales) into reliable business insights.

**Task:**
Design a production-pattern pipeline with clean separation of concerns, automated data quality enforcement, and a Gold layer fast enough for BI tools to query in under a second.

**Action:**
I implemented a Medallion Architecture with three layers:
- **Bronze**: Zero-copy clones of TPC-DS tables — immutable, no transforms ever
- **Silver**: dbt models handling type casting, deduplication, and a UNION ALL unification of three sales channels
- **Gold**: dbt mart tables with Materialized Views, clustered on sale_date and channel

I added Kafka streaming for clickstream data (page_view, add_to_cart, purchase events), which flows through Snowpipe into Bronze automatically. Airflow orchestrates the full 15-task DAG daily.

**Result:**
Five analytical marts answering distinct business questions, with Great Expectations validating each layer. Every mart query runs in under a second from the Gold Materialized Views.

---

## "How do you handle data quality?"

**Situation:**
Raw retail data is messy — TPC-DS has NULL foreign keys, the sales tables use cryptic two-character column prefixes, and our synthetic clickstream can emit invalid event types.

**Task:**
Prevent bad data from reaching the Gold layer without stopping the pipeline unnecessarily.

**Action:**
Three layers of defence:
1. **Staging filters**: Every dbt staging model filters `WHERE primary_key IS NOT NULL`. Bad rows are silently dropped at the Bronze→Silver boundary (in production, I'd route these to a quarantine table).
2. **dbt schema tests**: `not_null`, `unique`, and `accepted_values` tests on every model. Custom SQL tests for business rules like `net_revenue >= 0`.
3. **Great Expectations checkpoints**: Three checkpoints (Bronze, Silver, Gold). A GE failure raises a Python exception in Airflow, which blocks downstream tasks and sends a Slack alert.

**Result:**
Data quality is enforced at three independent layers. An issue caught at Bronze is far cheaper to fix than one discovered in a Gold dashboard by an executive.

---

## "Walk me through your dbt project."

**Structure:**
- `staging/` — 14 models, one per Bronze source table. Rename, cast, filter. No joins.
- `intermediate/` — 4 models. Business logic joins (UNION ALL across channels, LEFT JOIN for returns). No aggregations.
- `marts/` — 5 models. Pre-aggregated tables for BI. Materialized as TABLE with clustering keys.

**Key design decisions:**
- `int_unified_sales` uses UNION ALL (not UNION) — there are no cross-channel duplicates, so deduplication would waste compute
- `int_net_sales` uses LEFT JOIN to int_unified_returns — most sales have no return, INNER JOIN would drop the majority of data
- `mart_customer_ltv` uses ROW_NUMBER() to find preferred channel — Snowflake doesn't have MODE(), so I rank channels by spend per customer and filter to rank=1
- `mart_promotion_effectiveness` has a 5-CTE structure with every step commented — it joins date_dim twice (for promo start and end) and computes a 30-day pre-promo baseline

**Testing:**
dbt schema tests on every column that matters. 4 custom SQL tests for cross-mart business rules. GitHub Actions runs `dbt test --select state:modified+` on every PR.

---

## "How does CDC work in your pipeline?"

**What CDC means here:**
Change Data Capture tracks row-level additions to Bronze tables since the last processing run.

**Implementation:**
Snowflake Streams — I created 7 streams (one per key Bronze table). A Stream is a metadata object that records the offset of the last row consumed. When a Snowflake Task reads from the stream, Snowflake advances the offset automatically. If the Task fails, the offset doesn't advance — giving automatic retry safety with no extra code.

**The Task tree:**
A root Task checks `SYSTEM$STREAM_HAS_DATA()` for all 7 streams. If any has new rows, the root fires and triggers child Tasks in sequence: sales → returns → inventory → clickstream → log completion.

**Why both Tasks AND Airflow?**
Tasks handle incremental CDC on Snowflake's native compute — no warehouse needed. Airflow handles the full orchestration (ingestion, dbt, GE, Slack). They run at different frequencies: Tasks at 24h for CDC, Airflow at 06:00 UTC for the full pipeline.

---

## "Why Kafka for clickstream?"

TPC-DS doesn't include clickstream data. In production, a retail website generates millions of click events per day — page views, add-to-carts, purchases. This is inherently high-velocity streaming data, not batch.

I used Kafka with a single topic (`retail.clickstream`) to simulate that realistically. A Python Faker producer generates events at ~500/run. A consumer batches them into NDJSON files, stages them to Snowflake's internal stage, and calls the Snowpipe REST API to trigger ingestion.

The key interview point: AUTO_INGEST=FALSE here because we're using local Docker without S3. In production, AUTO_INGEST=TRUE with an SQS event notification would fire Snowpipe automatically on every file drop to S3.

---

## "What would you do differently at larger scale?"

1. **Replace internal stages with S3** — enables AUTO_INGEST=TRUE Snowpipe and decouples storage from compute
2. **Add dbt incremental models** — instead of full TABLE refresh, use `{{ is_incremental() }}` to only process new rows
3. **Add Terraform** — all Snowflake resources (database, warehouse, roles) as code rather than SQL scripts
4. **Partition Great Expectations checkpoints** — run per-table rather than per-layer for faster failure isolation
5. **Add data lineage tooling** — OpenLineage or Atlan to track column-level lineage across Snowflake + dbt + Airflow
