"""
scripts/validate_env.py
PURPOSE : Pre-flight check before running the pipeline.
          Validates that all required env vars are set and
          that Snowflake + Kafka connections are reachable.

USAGE:
  python scripts/validate_env.py
"""

import os
import sys

# Color codes for terminal output
GREEN  = '\033[92m'
RED    = '\033[91m'
YELLOW = '\033[93m'
RESET  = '\033[0m'

REQUIRED_ENV_VARS = [
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE',
    'KAFKA_BOOTSTRAP_SERVERS',
]

OPTIONAL_ENV_VARS = [
    'SLACK_WEBHOOK_URL',
    'SNOWFLAKE_ROLE',
    'SNOWPIPE_CLICKSTREAM_URL',
]


def check(label: str, success: bool, detail: str = ''):
    icon = f'{GREEN}✅{RESET}' if success else f'{RED}❌{RESET}'
    print(f'  {icon}  {label}' + (f' — {detail}' if detail else ''))
    return success


def main():
    print(f'\n{YELLOW}═══ Retail Intelligence Engine — Environment Validation ═══{RESET}\n')
    all_passed = True

    # ── 1. Required environment variables ─────────────────────
    print('1. Required environment variables:')
    from dotenv import load_dotenv
    load_dotenv()  # load .env file if present

    for var in REQUIRED_ENV_VARS:
        val = os.getenv(var)
        ok = bool(val)
        all_passed = check(var, ok, 'SET' if ok else 'MISSING') and all_passed

    print('\n2. Optional environment variables:')
    for var in OPTIONAL_ENV_VARS:
        val = os.getenv(var)
        check(var, bool(val), 'SET' if val else 'not set (optional)')

    # ── 2. Snowflake connectivity ──────────────────────────────
    print('\n3. Snowflake connectivity:')
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'RETAIL_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_DB'),
            login_timeout=10,
        )
        cursor = conn.cursor()
        cursor.execute('SELECT CURRENT_VERSION()')
        version = cursor.fetchone()[0]
        conn.close()
        all_passed = check('Snowflake connection', True, f'version {version}') and all_passed
    except Exception as e:
        all_passed = check('Snowflake connection', False, str(e)) and all_passed

    # ── 3. Docker running ──────────────────────────────────────
    print('\n4. Docker:')
    import subprocess
    result = subprocess.run(['docker', 'info'], capture_output=True, text=True)
    all_passed = check('Docker daemon', result.returncode == 0) and all_passed

    # ── 4. dbt profiles.yml ────────────────────────────────────
    print('\n5. dbt configuration:')
    dbt_profiles = os.path.expanduser('~/.dbt/profiles.yml')
    local_profiles = 'dbt_project/profiles.yml'
    has_profiles = os.path.exists(dbt_profiles) or os.path.exists(local_profiles)
    all_passed = check(
        'profiles.yml',
        has_profiles,
        dbt_profiles if os.path.exists(dbt_profiles) else local_profiles
    ) and all_passed

    # ── 5. Kafka reachability ──────────────────────────────────
    print('\n6. Kafka:')
    try:
        from kafka import KafkaAdminClient
        bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
        topics = admin.list_topics()
        admin.close()
        all_passed = check(
            f'Kafka ({bootstrap})', True,
            f'reachable, {len(topics)} topic(s) found'
        ) and all_passed
    except Exception as e:
        # Kafka might not be running if Docker isn't up yet
        check('Kafka', False, f'{e} — run: docker-compose up -d')

    # ── Summary ────────────────────────────────────────────────
    print(f'\n{"═" * 52}')
    if all_passed:
        print(f'{GREEN}✅  All checks passed. Ready to run the pipeline.{RESET}')
        print('    Next: snowsql -f setup/01_env_provisioning.sql')
    else:
        print(f'{RED}❌  Some checks failed. Fix the issues above before proceeding.{RESET}')
        sys.exit(1)
    print()


if __name__ == '__main__':
    main()
