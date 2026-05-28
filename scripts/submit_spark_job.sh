#!/usr/bin/env bash
# =============================================================================
# submit_spark_job.sh — Parameterised spark-submit wrapper
#
# Usage:
#   ./submit_spark_job.sh \
#     --job-class com.pipeline.bronze.RawIngestJob \
#     --env dev \
#     --execution-date 2024-03-15 \
#     --input-path /data/landing \
#     --output-path /data/bronze \
#     [--extra-conf key=value ...]
#
# Exit codes: 0=success  1=job failed  2=infra/config error
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${LOG_DIR:-/var/log/pipeline}"
mkdir -p "$LOG_DIR"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG_DIR/spark_submit.log"; }
err() { log "ERROR: $*"; }

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
JOB_CLASS=""
ENV="${PIPELINE_ENV:-dev}"
EXECUTION_DATE=$(date +%Y-%m-%d)
INPUT_PATH=""
OUTPUT_PATH=""
EXTRA_CONF_ARGS=()
EXTRA_JOB_ARGS=()

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --job-class)       JOB_CLASS="$2";          shift 2 ;;
        --env)             ENV="$2";                 shift 2 ;;
        --execution-date)  EXECUTION_DATE="$2";      shift 2 ;;
        --input-path)      INPUT_PATH="$2";          shift 2 ;;
        --output-path)     OUTPUT_PATH="$2";         shift 2 ;;
        --extra-conf)      EXTRA_CONF_ARGS+=("--conf" "$2"); shift 2 ;;
        --*)               EXTRA_JOB_ARGS+=("$1" "$2"); shift 2 ;;
        *)                 err "Unknown argument: $1"; exit 2 ;;
    esac
done

[[ -z "$JOB_CLASS" ]]   && { err "--job-class required"; exit 2; }
[[ -z "$INPUT_PATH" ]]  && { err "--input-path required"; exit 2; }
[[ -z "$OUTPUT_PATH" ]] && { err "--output-path required"; exit 2; }

# ---------------------------------------------------------------------------
# Resolve fat-jar
# ---------------------------------------------------------------------------
JAR=$(ls "$PROJECT_ROOT/spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-"*.jar 2>/dev/null | head -1)
if [[ -z "$JAR" ]]; then
    err "Fat-jar not found — run: make build"
    exit 2
fi

# ---------------------------------------------------------------------------
# Resolve Spark master
# ---------------------------------------------------------------------------
case "$ENV" in
    dev)              MASTER="${SPARK_MASTER_URL:-local[*]}" ;;
    staging | prod)   MASTER="${SPARK_MASTER_URL:?SPARK_MASTER_URL must be set for env=$ENV}" ;;
    *)                MASTER="local[*]" ;;
esac

SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-200}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"

# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------
log "Submitting $JOB_CLASS"
log "  master=$MASTER  env=$ENV  date=$EXECUTION_DATE"
log "  input=$INPUT_PATH  output=$OUTPUT_PATH"
log "  jar=$JAR"

set -x
spark-submit \
    --master "$MASTER" \
    --class "$JOB_CLASS" \
    --deploy-mode client \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --files "$PROJECT_ROOT/config/$ENV/application.conf" \
    "${EXTRA_CONF_ARGS[@]}" \
    "$JAR" \
    --env             "$ENV" \
    --execution-date  "$EXECUTION_DATE" \
    --input-path      "$INPUT_PATH" \
    --output-path     "$OUTPUT_PATH" \
    "${EXTRA_JOB_ARGS[@]}"
set +x

log "$JOB_CLASS completed successfully"
exit 0
