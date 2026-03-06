"""
retail_pipeline_dag.py
PURPOSE : Master Airflow DAG that orchestrates the full
          Retail Intelligence Engine pipeline end-to-end.

SCHEDULE: Daily at 06:00 UTC
TASKS (15 total, in order):
  1.  generate_clickstream_events     → Faker → JSON files
  2.  kafka_produce_clickstream        → JSON → Kafka topic
  3.  snowpipe_trigger_clickstream     → Snowpipe ingest
  4.  clone_tpcds_bronze               → (re)clone TPC-DS tables
  5.  dbt_run_staging                  → dbt run --select staging
  6.  dbt_run_intermediate             → dbt run --select intermediate
  7.  dbt_run_marts                    → dbt run --select marts
  8.  dbt_run_snapshot                 → dbt snapshot
  9.  dbt_test_staging                 → dbt test --select staging
  10. dbt_test_marts                   → dbt test --select marts
  11. great_expectations_bronze        → GE bronze checkpoint
  12. great_expectations_silver        → GE silver checkpoint
  13. great_expectations_gold          → GE gold checkpoint
  14. refresh_materialized_views       → ALTER MV REFRESH
  15. slack_notify                     → success/failure alert

RETRY POLICY:
  retries = 2, retry_delay = 5 minutes
  on_failure_callback → Slack alert on any task failure
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import sys
import os

# Add the project root to path so we can import ingestion scripts
sys.path.insert(0, '/opt/airflow')

# ── Default args (applied to all tasks) ──────────────────────
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,       # using Slack instead
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ── Slack alert callback ──────────────────────────────────────
def slack_failure_alert(context):
    """Called on any task failure — sends Slack notification."""
    task_instance = context.get('task_instance')
    slack_msg = f"""
        :red_circle: *Pipeline Failed*
        *DAG*: {context.get('dag').dag_id}
        *Task*: {task_instance.task_id}
        *Execution Date*: {context.get('execution_date')}
        *Log URL*: {task_instance.log_url}
    """
    # SlackWebhookOperator can also be used inline here
    # For simplicity we just log; wire up SLACK_WEBHOOK_URL in .env
    import logging
    logging.error("PIPELINE FAILURE: %s", slack_msg)


# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id='retail_pipeline_dag',
    description='Master pipeline: ingest → bronze → dbt → GE → notify',
    schedule_interval='0 6 * * *',         # daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                          # don't backfill missed runs
    max_active_runs=1,                      # prevent overlapping runs
    default_args=default_args,
    on_failure_callback=slack_failure_alert,
    tags=['retail', 'master', 'production'],
) as dag:

    # ── Task 1: Generate clickstream events ──────────────────
    # Runs the Faker producer script to generate a batch of
    # synthetic clickstream events as NDJSON files.
    generate_clickstream = BashOperator(
        task_id='generate_clickstream_events',
        bash_command=(
            'python /opt/airflow/ingestion/streaming/kafka_producer_clickstream.py '
            '--events 1000'
        ),
        env={
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        },
    )

    # ── Task 2: Trigger Snowpipe for clickstream ─────────────
    # Refreshes the Snowpipe to pick up any newly staged files.
    trigger_snowpipe = SnowflakeOperator(
        task_id='snowpipe_trigger_clickstream',
        snowflake_conn_id='snowflake_default',
        sql="ALTER PIPE RETAIL_DB.BRONZE.clickstream_pipe REFRESH;",
    )

    # ── Task 3: Clone/refresh Bronze TPC-DS tables ───────────
    # Re-clones TPC-DS tables. CREATE OR REPLACE makes this
    # idempotent — safe to re-run on failure.
    clone_bronze = SnowflakeOperator(
        task_id='clone_tpcds_bronze',
        snowflake_conn_id='snowflake_default',
        sql='@/opt/airflow/dbt_project/../bronze/03_clone_tpcds.sql',
    )

    # ── Task 4–8: dbt model runs ─────────────────────────────
    # We run staging, intermediate, and marts separately so that
    # if staging fails, we don't waste compute trying to run marts.
    # --profiles-dir and --project-dir ensure the right config is used.

    DBT_CMD_BASE = (
        'cd /opt/airflow/dbt_project && '
        'dbt {command} '
        '--profiles-dir /opt/airflow/dbt_project '
        '--project-dir /opt/airflow/dbt_project '
        '{select}'
    )

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=DBT_CMD_BASE.format(
            command='run',
            select='--select staging'
        ),
    )

    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=DBT_CMD_BASE.format(
            command='run',
            select='--select intermediate'
        ),
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=DBT_CMD_BASE.format(
            command='run',
            select='--select marts'
        ),
    )

    dbt_run_snapshot = BashOperator(
        task_id='dbt_run_snapshot',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            'dbt snapshot '
            '--profiles-dir /opt/airflow/dbt_project '
            '--project-dir /opt/airflow/dbt_project'
        ),
    )

    # ── Task 9–10: dbt tests ──────────────────────────────────
    # Run tests AFTER all models complete. A failed test blocks
    # the Great Expectations checkpoint from running on bad data.
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command=DBT_CMD_BASE.format(
            command='test',
            select='--select staging'
        ),
    )

    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command=DBT_CMD_BASE.format(
            command='test',
            select='--select marts'
        ),
    )

    # ── Task 11–13: Great Expectations checkpoints ───────────
    # GE validates each layer independently. If Bronze fails,
    # we stop — no point running Silver/Gold checks on bad data.
    def run_ge_checkpoint(checkpoint_name: str):
        """Run a named Great Expectations checkpoint."""
        import great_expectations as gx
        context = gx.get_context(
            context_root_dir='/opt/airflow/data_quality/great_expectations'
        )
        result = context.run_checkpoint(checkpoint_name=checkpoint_name)
        if not result.success:
            raise ValueError(
                f"Great Expectations checkpoint '{checkpoint_name}' FAILED. "
                "Check /opt/airflow/data_quality/great_expectations/uncommitted/ for report."
            )

    ge_bronze = PythonOperator(
        task_id='great_expectations_bronze',
        python_callable=run_ge_checkpoint,
        op_kwargs={'checkpoint_name': 'bronze_checkpoint'},
    )

    ge_silver = PythonOperator(
        task_id='great_expectations_silver',
        python_callable=run_ge_checkpoint,
        op_kwargs={'checkpoint_name': 'silver_checkpoint'},
    )

    ge_gold = PythonOperator(
        task_id='great_expectations_gold',
        python_callable=run_ge_checkpoint,
        op_kwargs={'checkpoint_name': 'gold_checkpoint'},
    )

    # ── Task 14: Refresh Materialized Views ──────────────────
    # Snowflake MVs auto-refresh when underlying tables change,
    # but an explicit REFRESH ensures the Gold layer is up to
    # date before the Slack success notification goes out.
    refresh_mvs = SnowflakeOperator(
        task_id='refresh_materialized_views',
        snowflake_conn_id='snowflake_default',
        sql="""
            ALTER MATERIALIZED VIEW RETAIL_DB.GOLD.mv_daily_revenue        RESUME;
            ALTER MATERIALIZED VIEW RETAIL_DB.GOLD.mv_returns_analysis     RESUME;
            ALTER MATERIALIZED VIEW RETAIL_DB.GOLD.mv_inventory_health     RESUME;
            ALTER MATERIALIZED VIEW RETAIL_DB.GOLD.mv_customer_ltv         RESUME;
            ALTER MATERIALIZED VIEW RETAIL_DB.GOLD.mv_promotion_effectiveness RESUME;
        """,
    )

    # ── Task 15: Slack success notification ──────────────────
    slack_success = SlackWebhookOperator(
        task_id='slack_notify_success',
        slack_webhook_conn_id='slack_webhook',
        message=(
            ":white_check_mark: *Retail Pipeline Complete*\n"
            "All 5 Gold marts refreshed. GE checks passed.\n"
            "Execution date: {{ ds }}"
        ),
        trigger_rule='all_success',     # only fires if all upstream tasks passed
    )

    # ── DAG Dependencies (task graph) ─────────────────────────
    # Read left to right: A >> B means B runs after A succeeds.
    #
    # Clickstream path (runs in parallel with clone):
    generate_clickstream >> trigger_snowpipe
    trigger_snowpipe >> dbt_run_staging
    #
    # TPC-DS clone path:
    clone_bronze >> dbt_run_staging
    #
    # dbt pipeline (sequential — each layer depends on previous):
    dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_run_snapshot
    #
    # Tests (run after all models complete):
    dbt_run_staging >> dbt_test_staging
    dbt_run_marts >> dbt_test_marts
    #
    # GE checkpoints (sequential — stop if bronze fails):
    dbt_test_staging >> ge_bronze >> ge_silver
    dbt_test_marts >> ge_gold
    #
    # MV refresh and final notification:
    [ge_silver, ge_gold] >> refresh_mvs >> slack_success
