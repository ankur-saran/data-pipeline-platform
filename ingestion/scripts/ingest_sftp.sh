#!/usr/bin/env bash
# =============================================================================
# ingest_sftp.sh — Pull files from SFTP with exponential-backoff retry
#
# Usage:
#   ./ingest_sftp.sh --execution-date YYYY-MM-DD --landing-path /data/landing
#
# Exit codes: 0=success  1=data error  2=infra error
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
SCRIPT_NAME=$(basename "$0")
LOG_DIR="${LOG_DIR:-/var/log/pipeline}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${SCRIPT_NAME%.sh}.log"

log() {
    local level="$1"; shift
    local ts; ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    local msg="[$ts] [$level] [$SCRIPT_NAME] $*"
    echo "$msg" | tee -a "$LOG_FILE"
}

info()  { log INFO  "$@"; }
warn()  { log WARN  "$@"; }
error() { log ERROR "$@"; }

# ---------------------------------------------------------------------------
# Argument parsing
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

[[ -z "$EXECUTION_DATE" ]] && { error "--execution-date is required"; exit 2; }
[[ -z "$LANDING_PATH"   ]] && { error "--landing-path is required";   exit 2; }

# SFTP credentials from environment (never hardcoded)
SFTP_HOST="${SFTP_HOST:?SFTP_HOST env var is required}"
SFTP_PORT="${SFTP_PORT:-22}"
SFTP_USER="${SFTP_USER:?SFTP_USER env var is required}"
SFTP_PASSWORD="${SFTP_PASSWORD:?SFTP_PASSWORD env var is required}"
SFTP_REMOTE_DIR="${SFTP_REMOTE_DIR:-/exports}"

# ---------------------------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------------------------
retry() {
    local max_attempts="$1"; shift
    local base_delay="$1";   shift
    local attempt=1
    until "$@"; do
        if (( attempt >= max_attempts )); then
            error "Command failed after $attempt attempts: $*"
            return 2
        fi
        local delay=$(( base_delay * (2 ** (attempt - 1)) ))
        warn "Attempt $attempt failed. Retrying in ${delay}s…"
        sleep "$delay"
        (( attempt++ ))
    done
    info "Command succeeded on attempt $attempt"
}

# ---------------------------------------------------------------------------
# SFTP pull function
# ---------------------------------------------------------------------------
pull_sftp() {
    local date_nodash; date_nodash=$(echo "$EXECUTION_DATE" | tr -d '-')
    local dest_dir="$LANDING_PATH/$date_nodash/sftp"
    mkdir -p "$dest_dir"

    info "Connecting to SFTP $SFTP_HOST:$SFTP_PORT as $SFTP_USER"
    info "Remote dir: $SFTP_REMOTE_DIR  →  Local: $dest_dir"

    # sshpass required for password-based auth; prefer SSH keys in prod
    if ! command -v sshpass &>/dev/null; then
        warn "sshpass not found — attempting key-based SFTP"
        sftp -P "$SFTP_PORT" \
            -o StrictHostKeyChecking=no \
            -o BatchMode=yes \
            "${SFTP_USER}@${SFTP_HOST}:${SFTP_REMOTE_DIR}/*_${EXECUTION_DATE}*" \
            "$dest_dir/"
    else
        SSHPASS="$SFTP_PASSWORD" sshpass -e sftp \
            -P "$SFTP_PORT" \
            -o StrictHostKeyChecking=no \
            "${SFTP_USER}@${SFTP_HOST}:${SFTP_REMOTE_DIR}/*_${EXECUTION_DATE}*" \
            "$dest_dir/"
    fi

    local file_count; file_count=$(find "$dest_dir" -type f | wc -l)
    info "Downloaded $file_count file(s) to $dest_dir"

    if (( file_count == 0 )); then
        warn "No files found for date $EXECUTION_DATE — this may be expected on weekends"
        exit 0
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
info "Starting SFTP ingestion for execution_date=$EXECUTION_DATE"

retry 5 10 pull_sftp

info "SFTP ingestion completed successfully"
exit 0
