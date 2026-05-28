#!/usr/bin/env bash
# =============================================================================
# health_check.sh — DAG and cluster health checks
#
# Verifies: Airflow API reachable, all DAGs healthy, Spark master reachable
# Exit codes: 0=all healthy  1=degraded  2=unreachable
# =============================================================================
set -euo pipefail

AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
SPARK_MASTER_UI="${SPARK_MASTER_UI:-http://localhost:8081}"
AIRFLOW_USER="${AIRFLOW_ADMIN_USER:-admin}"
AIRFLOW_PASS="${AIRFLOW_ADMIN_PASSWORD:-admin}"

HEALTHY=0
DEGRADED=0

ok()   { echo "  [OK]   $*"; }
warn() { echo "  [WARN] $*"; (( DEGRADED++ )) || true; }
fail() { echo "  [FAIL] $*"; (( HEALTHY=1 )) || true; }

echo ""
echo "=== Pipeline Health Check  $(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
echo ""

# ---------------------------------------------------------------------------
# 1. Airflow API
# ---------------------------------------------------------------------------
echo "[ Airflow ]"
if HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
    "$AIRFLOW_URL/api/v1/health" 2>/dev/null); then
    if [[ "$HTTP" == "200" ]]; then
        ok "Airflow API reachable (HTTP $HTTP)"
    else
        fail "Airflow API returned HTTP $HTTP"
    fi
else
    fail "Airflow API unreachable at $AIRFLOW_URL"
    HEALTHY=2
fi

# Check DAG states
if command -v python3 &>/dev/null; then
    python3 - <<PYEOF 2>/dev/null || warn "Could not retrieve DAG states"
import urllib.request, json, base64

creds = base64.b64encode(b"${AIRFLOW_USER}:${AIRFLOW_PASS}").decode()
req = urllib.request.Request(
    "${AIRFLOW_URL}/api/v1/dags?limit=50",
    headers={"Authorization": f"Basic {creds}"},
)
with urllib.request.urlopen(req, timeout=10) as resp:
    dags = json.load(resp).get("dags", [])

for dag in dags:
    dag_id   = dag["dag_id"]
    paused   = dag["is_paused"]
    active   = dag["is_active"]
    status   = "paused" if paused else ("active" if active else "inactive")
    marker   = "[OK]  " if active and not paused else "[WARN]"
    print(f"  {marker} {dag_id} — {status}")
PYEOF
fi

echo ""
echo "[ Spark ]"

# ---------------------------------------------------------------------------
# 2. Spark Master UI
# ---------------------------------------------------------------------------
if HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    "$SPARK_MASTER_UI" 2>/dev/null); then
    if [[ "$HTTP" == "200" ]]; then
        ok "Spark Master UI reachable (HTTP $HTTP)"
    else
        warn "Spark Master UI returned HTTP $HTTP"
    fi
else
    fail "Spark Master UI unreachable at $SPARK_MASTER_UI"
fi

echo ""
echo "[ Data Paths ]"

# ---------------------------------------------------------------------------
# 3. Data directory checks
# ---------------------------------------------------------------------------
for dir in \
    "${INGESTION_LANDING_PATH:-/data/landing}" \
    "${BRONZE_BASE_PATH:-/data/bronze}" \
    "${SILVER_BASE_PATH:-/data/silver}" \
    "${GOLD_BASE_PATH:-/data/gold}"; do

    if [[ -d "$dir" ]]; then
        ok "$dir exists"
    else
        warn "$dir missing — may not have been created yet"
    fi
done

echo ""
echo "=============================="
if (( HEALTHY == 2 )); then
    echo "Status: UNREACHABLE (exit 2)"
    exit 2
elif (( HEALTHY == 1 )) || (( DEGRADED > 0 )); then
    echo "Status: DEGRADED (exit 1)  degraded_checks=$DEGRADED"
    exit 1
else
    echo "Status: HEALTHY"
    exit 0
fi
