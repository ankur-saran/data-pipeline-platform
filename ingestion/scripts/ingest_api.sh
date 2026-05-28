#!/usr/bin/env bash
# =============================================================================
# ingest_api.sh — Pull data from a paginated REST API with retry logic
#
# Usage:
#   ./ingest_api.sh --execution-date YYYY-MM-DD --landing-path /data/landing
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

API_BASE_URL="${API_BASE_URL:?API_BASE_URL env var is required}"
API_KEY="${API_KEY:?API_KEY env var is required}"
PAGE_SIZE="${API_PAGE_SIZE:-500}"
MAX_PAGES="${API_MAX_PAGES:-100}"

# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------
retry() {
    local max_attempts="$1"; shift
    local base_delay="$1";   shift
    local attempt=1
    until "$@"; do
        (( attempt >= max_attempts )) && { error "Failed after $attempt attempts: $*"; return 2; }
        local delay=$(( base_delay * (2 ** (attempt - 1)) ))
        warn "Attempt $attempt failed. Retrying in ${delay}s…"
        sleep "$delay"
        (( attempt++ ))
    done
}

# ---------------------------------------------------------------------------
# Fetch a single page and stream to a NDJSON file
# ---------------------------------------------------------------------------
fetch_page() {
    local page="$1"
    local dest_file="$2"

    local http_code
    http_code=$(curl --silent --show-error --fail-with-body \
        --max-time 60 \
        --retry 0 \
        -H "Authorization: Bearer $API_KEY" \
        -H "Accept: application/json" \
        -o "$dest_file" \
        -w "%{http_code}" \
        "${API_BASE_URL}/events?date=${EXECUTION_DATE}&page=${page}&page_size=${PAGE_SIZE}")

    if [[ "$http_code" -eq 200 ]]; then
        return 0
    elif [[ "$http_code" -eq 429 ]]; then
        warn "Rate limited (429) — backing off 60s"
        sleep 60
        return 1
    else
        error "Unexpected HTTP $http_code on page $page"
        return 2
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
date_nodash=$(echo "$EXECUTION_DATE" | tr -d '-')
dest_dir="$LANDING_PATH/$date_nodash/api"
mkdir -p "$dest_dir"

info "Starting API ingestion for $EXECUTION_DATE → $dest_dir"

page=1
total_records=0

while (( page <= MAX_PAGES )); do
    dest_file="$dest_dir/events_page_$(printf '%04d' "$page").json"
    info "Fetching page $page → $dest_file"

    retry 5 15 fetch_page "$page" "$dest_file"

    # Count records in this page; exit pagination when page returns empty
    record_count=$(python3 -c "
import json, sys
with open('$dest_file') as f:
    data = json.load(f)
items = data.get('items', data if isinstance(data, list) else [])
print(len(items))
" 2>/dev/null || echo 0)

    info "Page $page: $record_count records"
    (( total_records += record_count )) || true

    if (( record_count == 0 )); then
        info "Empty page — pagination complete at page $page"
        rm -f "$dest_file"     # remove empty page file
        break
    fi

    (( page++ ))
done

info "API ingestion complete: $total_records total records across $((page - 1)) pages"

if (( total_records == 0 )); then
    warn "Zero records returned from API for $EXECUTION_DATE"
    exit 1
fi

exit 0
