#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh — One-shot local environment setup
#
# Installs Python deps, builds sbt project, creates .env, initialises Airflow DB
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

log() { echo "[$(date -u '+%H:%M:%S')] $*"; }
err() { echo "[$(date -u '+%H:%M:%S')] ERROR: $*" >&2; }

cd "$PROJECT_ROOT"

# ---------------------------------------------------------------------------
# 1. Check prerequisites
# ---------------------------------------------------------------------------
log "Checking prerequisites…"
for cmd in java python3 pip docker docker-compose sbt; do
    if ! command -v "$cmd" &>/dev/null; then
        err "$cmd is not installed or not on PATH"
        exit 2
    fi
done

JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if (( JAVA_VER < 11 )); then
    err "Java 11+ required, found Java $JAVA_VER"
    exit 2
fi
log "Java $JAVA_VER detected ✓"

PYTHON_VER=$(python3 --version | awk '{print $2}')
log "Python $PYTHON_VER detected ✓"

# ---------------------------------------------------------------------------
# 2. Create .env if missing
# ---------------------------------------------------------------------------
if [[ ! -f .env ]]; then
    log "Creating .env from .env.example…"
    cp .env.example .env
    # Generate a Fernet key
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || echo "REPLACE_ME")
    sed -i "s|^AIRFLOW__CORE__FERNET_KEY=.*|AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY|" .env
    log ".env created — review and fill in any REPLACE_ME values"
else
    log ".env already exists — skipping"
fi

# ---------------------------------------------------------------------------
# 3. Python / Airflow dependencies
# ---------------------------------------------------------------------------
log "Installing Python dependencies…"
pip install --upgrade pip --quiet
pip install -r airflow/requirements.txt --quiet
log "Python dependencies installed ✓"

# ---------------------------------------------------------------------------
# 4. Build Spark fat-jar
# ---------------------------------------------------------------------------
log "Building Spark fat-jar (this may take a few minutes)…"
cd spark-jobs
sbt --batch assembly
cd ..
JAR=$(ls spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-*.jar 2>/dev/null | head -1)
if [[ -z "$JAR" ]]; then
    err "Fat-jar not found after sbt assembly"
    exit 2
fi
log "Fat-jar built: $JAR ✓"

# ---------------------------------------------------------------------------
# 5. Create local data directories
# ---------------------------------------------------------------------------
for dir in /data/landing /data/bronze /data/silver /data/gold /data/metrics /data/quality_audit; do
    mkdir -p "$dir"
done
log "Data directories created ✓"

# ---------------------------------------------------------------------------
# 6. Docker build + Airflow DB init
# ---------------------------------------------------------------------------
log "Building Docker images…"
docker compose -f docker/docker-compose.yml build --quiet

log "Initialising Airflow DB…"
docker compose -f docker/docker-compose.yml run --rm airflow-init
log "Airflow DB initialised ✓"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log ""
log "Bootstrap complete!"
log "  Start the stack : make docker-up"
log "  Run a pipeline  : make transform ENV=dev"
log "  Run quality checks: make quality"
