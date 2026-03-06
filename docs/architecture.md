# architecture.md — Retail Intelligence Engine

## Full Pipeline Architecture

```mermaid
flowchart TD
    subgraph SOURCES["Data Sources"]
        TPCDS["SNOWFLAKE_SAMPLE_DATA\n.TPCDS_SF10TCL\n12 tables"]
        FAKER["Python Faker\nClickstream Generator"]
    end

    subgraph STREAMING["Streaming Layer"]
        KAFKA["Apache Kafka\nTopic: retail.clickstream\n~500 events/run"]
        CONSUMER["Kafka Consumer\nBatches → NDJSON\n→ Snowflake Stage"]
        SNOWPIPE["Snowpipe\nAUTO_INGEST=FALSE\nREST API trigger"]
    end

    subgraph BRONZE["🥉 BRONZE — Raw Zone\nRETAIL_DB.BRONZE"]
        B1["store_sales (clone)"]
        B2["web_sales (clone)"]
        B3["catalog_sales (clone)"]
        B4["store_returns (clone)"]
        B5["web_returns (clone)"]
        B6["inventory (clone)"]
        B7["item, customer, date_dim\npromotion, store, customer_address"]
        B8["raw_clickstream\n(Snowpipe target)"]
    end

    subgraph CDC["CDC Layer"]
        STREAMS["Snowflake Streams × 7\n(append-only CDC)"]
        TASKS["Snowflake Task Tree\n24h schedule\nroot → sales → returns\n→ inventory → clickstream"]
    end

    subgraph DBT["dbt Core — Silver + Gold"]
        subgraph STAGING["Staging (14 models — views)"]
            S1["stg_store/web/catalog_sales"]
            S2["stg_store/web_returns"]
            S3["stg_inventory, stg_item"]
            S4["stg_customer, stg_date_dim"]
            S5["stg_promotion, stg_store"]
            S6["stg_clickstream"]
        end
        subgraph INTERMEDIATE["Intermediate (4 models — views)"]
            I1["int_unified_sales\n(UNION ALL 3 channels)"]
            I2["int_unified_returns\n(UNION ALL 2 channels)"]
            I3["int_net_sales\n(sales LEFT JOIN returns)"]
            I4["int_inventory_daily\n(rolling 30d avg window fn)"]
        end
        subgraph MARTS["Marts (5 models — tables)"]
            M1["mart_daily_revenue"]
            M2["mart_returns_analysis"]
            M3["mart_inventory_health"]
            M4["mart_customer_ltv"]
            M5["mart_promotion_effectiveness"]
        end
        SNAPSHOT["customer_snapshot\nSCD Type 2"]
    end

    subgraph GOLD["🥇 GOLD — Serving Zone\nRETAIL_DB.GOLD"]
        MV1["mv_daily_revenue"]
        MV2["mv_returns_analysis"]
        MV3["mv_inventory_health"]
        MV4["mv_customer_ltv"]
        MV5["mv_promotion_effectiveness"]
    end

    subgraph ORCHESTRATION["Orchestration"]
        AIRFLOW["Apache Airflow\n15-task DAG\nDaily 06:00 UTC"]
        GE["Great Expectations\n3 checkpoints\nBronze / Silver / Gold"]
        SLACK["Slack Alerts\nSuccess + Failure"]
    end

    subgraph BI["Visualization"]
        GRAFANA["Grafana\nlocalhost:3000\n5 dashboards"]
        TABLEAU["Tableau Public\nPublished workbook"]
    end

    subgraph CICD["CI/CD — GitHub Actions"]
        CI1["dbt compile + test\n(slim CI, state:modified+)"]
        CI2["SQLFluff lint\n(Snowflake dialect)"]
        CI3["Great Expectations\n(Gold checkpoint on PR)"]
        CI4["flake8\n(Python lint)"]
    end

    TPCDS -->|"CLONE (zero-copy)"| BRONZE
    FAKER --> KAFKA --> CONSUMER --> SNOWPIPE --> B8

    BRONZE --> STREAMS --> TASKS
    BRONZE --> DBT

    STAGING --> INTERMEDIATE --> MARTS
    STAGING --> SNAPSHOT

    MARTS --> GOLD
    GOLD --> BI
    AIRFLOW --> DBT
    AIRFLOW --> GE
    AIRFLOW --> SLACK
```

## Layer Responsibilities

| Layer | Schema | Owner | Rules |
|---|---|---|---|
| Bronze | RETAIL_DB.BRONZE | Data Engineer | Immutable. No transforms. No deletes. Append-only. |
| Silver | RETAIL_DB.SILVER | dbt | Type-cast, renamed, null-filtered, CDC-aware |
| Gold | RETAIL_DB.GOLD | dbt + Snowflake | Pre-aggregated tables + Materialized Views. BI connects here ONLY. |
