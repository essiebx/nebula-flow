#!/bin/bash
# scripts/setup_local_env.sh
# ============================================================
# PURPOSE : One-command local environment setup.
#           Installs Python dependencies and starts Docker services.
# USAGE   : bash scripts/setup_local_env.sh
# ============================================================

set -e  # exit on any error

echo ""
echo "❄️  Retail Intelligence Engine — Local Setup"
echo "════════════════════════════════════════════"
echo ""

# ── Step 1: Check .env exists ─────────────────────────────────
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found. Copying from .env.example..."
    cp .env.example .env
    echo "✅ .env created. Fill in your Snowflake credentials before proceeding."
    echo "   Then re-run this script."
    exit 0
fi

# ── Step 2: Install Python dependencies ───────────────────────
echo "📦 Installing Python dependencies..."
pip install -r scripts/requirements.txt --quiet
echo "✅ Python dependencies installed."

# ── Step 3: Install dbt packages ──────────────────────────────
echo "📦 Installing dbt packages..."
cd dbt_project && dbt deps --quiet && cd ..
echo "✅ dbt packages installed."

# ── Step 4: Start Docker services ─────────────────────────────
echo "🐳 Starting Docker services (Kafka, Airflow, Grafana)..."
docker-compose up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to start (30 seconds)..."
sleep 30

# ── Step 5: Print service URLs ────────────────────────────────
echo ""
echo "════════════════════════════════════════════"
echo "✅ All services running!"
echo ""
echo "  🌊 Airflow:  http://localhost:8080"
echo "               user: airflow / pass: airflow"
echo ""
echo "  📊 Grafana:  http://localhost:3000"
echo "               user: admin / pass: admin"
echo ""
echo "  📬 Kafka UI: http://localhost:8090"
echo ""
echo "════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. python scripts/validate_env.py"
echo "  2. snowsql -f setup/01_env_provisioning.sql"
echo "  3. snowsql -f setup/02_rbac_grants.sql"
echo "  4. snowsql -f bronze/03_clone_tpcds.sql"
echo "  5. cd dbt_project && dbt run && dbt test"
echo ""
