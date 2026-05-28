#!/usr/bin/env bash
# =============================================================================
# ingest_db_export.sh — Export a table slice from a JDBC source via sqoop / sqlcmd
#
# Uses a thin Python helper (jdbc_export.py) so the actual JDBC driver remains
# on the JVM classpath.  Falls back to a mysqldump-style native export when
# the JDBC_TOOL env var is set to "mysqldump".
#
# Usage:
#   ./ingest_db_export.sh --execution-date YYYY-MM-DD --landing-path /data/landing
#
# Exit codes: 0=success  1=data error  2=infra error
# =============================================================================
set -euo pipefail

SCRIPT_NAME=$(basename "$0")
LOG_DIR="${LOG_DIR:-/var/log/pipeline}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${SCRIPT_NAME%.sh}.log"

log() {
    local level="$1"; shift
    local ts; ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    echo "[$ts] [$level] [$SCRIPT_NAME] $*" | tee -a "$LOG_FILE"
}
info()  { log INFO  "$@"; }
warn()  { log WARN  "$@"; }
error() { log ERROR "$@"; }

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
EXECUTION_DATE=""
LANDING_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --execution-date) EXECUTION_DATE="$2"; shift 2 ;;
        --landing-path)   LANDING_PATH="$2";   shift 2 ;;
        *) error "Unknown argument: $1"; exit 2 ;;
    esac
done

[[ -z "$EXECUTION_DATE" ]] && { error "--execution-date required"; exit 2; }
[[ -z "$LANDING_PATH"   ]] && { error "--landing-path required";   exit 2; }

DB_EXPORT_JDBC_URL="${DB_EXPORT_JDBC_URL:?DB_EXPORT_JDBC_URL env var is required}"
DB_EXPORT_USER="${DB_EXPORT_USER:?DB_EXPORT_USER env var is required}"
DB_EXPORT_PASSWORD="${DB_EXPORT_PASSWORD:?DB_EXPORT_PASSWORD env var is required}"
DB_EXPORT_TABLE="${DB_EXPORT_TABLE:-events}"
DB_EXPORT_DATE_COL="${DB_EXPORT_DATE_COL:-created_at}"
JDBC_TOOL="${JDBC_TOOL:-spark}"   # spark | sqoop | mysqldump

# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------
retry() {
    local max_attempts="$1"; shift
    local base_delay="$1";   shift
    local attempt=1
    until "$@"; do
        (( attempt >= max_attempts )) && { error "Failed after $attempt attempts"; return 2; }
        local delay=$(( base_delay * (2 ** (attempt - 1)) ))
        warn "Attempt $attempt failed. Retrying in ${delay}s…"
        sleep "$delay"
        (( attempt++ ))
    done
}

# ---------------------------------------------------------------------------
# Export using a minimal Python Spark JDBC snippet (requires Java)
# ---------------------------------------------------------------------------
export_via_spark() {
    local dest_dir="$1"
    info "Exporting via Spark JDBC: table=$DB_EXPORT_TABLE  date=$EXECUTION_DATE"
    python3 - <<PYEOF
import os, sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("jdbc-export").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("jdbc").options(
    url="${DB_EXPORT_JDBC_URL}",
    dbtable="(SELECT * FROM ${DB_EXPORT_TABLE} WHERE DATE(${DB_EXPORT_DATE_COL}) = '${EXECUTION_DATE}') t",
    user="${DB_EXPORT_USER}",
    password="${DB_EXPORT_PASSWORD}",
    fetchsize="10000",
).load()

row_count = df.count()
if row_count == 0:
    print("WARNING: zero rows exported", file=sys.stderr)

df.coalesce(4).write.mode("overwrite").parquet("${dest_dir}/db_export.parquet")
print(f"Exported {row_count} rows to ${dest_dir}/db_export.parquet")
spark.stop()
PYEOF
}

# ---------------------------------------------------------------------------
# Export using mysqldump (simpler, for small tables)
# ---------------------------------------------------------------------------
export_via_mysqldump() {
    local dest_dir="$1"
    local host port db
    # Parse jdbc:mysql://host:port/database
    host=$(echo "$DB_EXPORT_JDBC_URL" | sed -E 's|.*://([^:/]+).*|\1|')
    port=$(echo "$DB_EXPORT_JDBC_URL" | sed -E 's|.*:([0-9]+)/.*|\1|')
    db=$(echo "$DB_EXPORT_JDBC_URL"   | sed -E 's|.*/([^?]+).*|\1|')

    info "Exporting via mysqldump: $host:$port/$db.$DB_EXPORT_TABLE"
    MYSQL_PWD="$DB_EXPORT_PASSWORD" mysqldump \
        --host="$host" --port="$port" \
        --user="$DB_EXPORT_USER" \
        --single-transaction \
        --where="DATE($DB_EXPORT_DATE_COL) = '$EXECUTION_DATE'" \
        "$db" "$DB_EXPORT_TABLE" \
        > "$dest_dir/db_export.sql"
    info "Dump written to $dest_dir/db_export.sql"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
date_nodash=$(echo "$EXECUTION_DATE" | tr -d '-')
dest_dir="$LANDING_PATH/$date_nodash/db"
mkdir -p "$dest_dir"

info "Starting DB export: tool=$JDBC_TOOL  table=$DB_EXPORT_TABLE  date=$EXECUTION_DATE"

case "$JDBC_TOOL" in
    spark)    retry 3 30 export_via_spark    "$dest_dir" ;;
    mysqldump) retry 3 30 export_via_mysqldump "$dest_dir" ;;
    *) error "Unknown JDBC_TOOL: $JDBC_TOOL"; exit 2 ;;
esac

info "DB export completed successfully → $dest_dir"
exit 0
