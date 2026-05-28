"""
ingestion_dag.py — Shell-based raw data ingestion

Pulls data from SFTP, REST API, and DB export sources into the landing zone.
Idempotent: uses execution_date as a partition key so re-runs are safe.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args — applied to every task unless overridden
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [os.getenv("ALERT_EMAIL", "data-eng@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _validate_env(**context: dict) -> None:
    """Confirm required Airflow Variables and env-vars are present."""
    required_vars = ["PIPELINE_ENV", "INGESTION_LANDING_PATH"]
    missing = [v for v in required_vars if not Variable.get(v, default_var=None)]
    if missing:
        raise ValueError(f"Missing Airflow Variables: {missing}")
    log.info(
        "Environment validated — execution_date=%s",
        context["data_interval_start"],
    )


with DAG(
    dag_id="ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingest raw files from SFTP, REST API, and DB export into landing zone",
    schedule_interval="0 2 * * *",          # 02:00 UTC daily
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "bronze"],
    doc_md=__doc__,
) as dag:

    # Resolved at runtime from Airflow Variables (set via UI or CLI)
    pipeline_env      = "{{ var.value.PIPELINE_ENV }}"
    landing_path      = "{{ var.value.INGESTION_LANDING_PATH }}"
    execution_date_ds = "{{ ds }}"           # YYYY-MM-DD partition key

    # ------------------------------------------------------------------
    # Task 1: Pre-flight environment check
    # ------------------------------------------------------------------
    validate_env = PythonOperator(
        task_id="validate_env",
        python_callable=_validate_env,
    )

    # ------------------------------------------------------------------
    # Task 2a: SFTP ingestion
    # ------------------------------------------------------------------
    ingest_sftp = BashOperator(
        task_id="ingest_sftp",
        bash_command=(
            "bash {{ conf.get('core', 'dags_folder') }}/../../../ingestion/scripts/ingest_sftp.sh "
            "--execution-date {{ ds }} "
            "--landing-path {{ var.value.INGESTION_LANDING_PATH }} "
        ),
        env={
            "SFTP_HOST":       "{{ var.value.get('SFTP_HOST', '') }}",
            "SFTP_PORT":       "{{ var.value.get('SFTP_PORT', '22') }}",
            "SFTP_USER":       "{{ var.value.get('SFTP_USER', '') }}",
            "SFTP_PASSWORD":   "{{ var.value.get('SFTP_PASSWORD', '') }}",
            "SFTP_REMOTE_DIR": "{{ var.value.get('SFTP_REMOTE_DIR', '/exports') }}",
        },
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 2b: REST API ingestion
    # ------------------------------------------------------------------
    ingest_api = BashOperator(
        task_id="ingest_api",
        bash_command=(
            "bash {{ conf.get('core', 'dags_folder') }}/../../../ingestion/scripts/ingest_api.sh "
            "--execution-date {{ ds }} "
            "--landing-path {{ var.value.INGESTION_LANDING_PATH }} "
        ),
        env={
            "API_BASE_URL": "{{ var.value.get('API_BASE_URL', '') }}",
            "API_KEY":      "{{ var.value.get('API_KEY', '') }}",
        },
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 2c: DB export
    # ------------------------------------------------------------------
    ingest_db = BashOperator(
        task_id="ingest_db_export",
        bash_command=(
            "bash {{ conf.get('core', 'dags_folder') }}/../../../ingestion/scripts/ingest_db_export.sh "
            "--execution-date {{ ds }} "
            "--landing-path {{ var.value.INGESTION_LANDING_PATH }} "
        ),
        env={
            "DB_EXPORT_JDBC_URL": "{{ var.value.get('DB_EXPORT_JDBC_URL', '') }}",
            "DB_EXPORT_USER":     "{{ var.value.get('DB_EXPORT_USER', '') }}",
            "DB_EXPORT_PASSWORD": "{{ var.value.get('DB_EXPORT_PASSWORD', '') }}",
        },
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Task 3: Landing-zone row-count sanity check
    # ------------------------------------------------------------------
    landing_check = BashOperator(
        task_id="landing_check",
        bash_command=(
            "FILE_COUNT=$(find {{ var.value.INGESTION_LANDING_PATH }}/{{ ds_nodash }} "
            "-type f 2>/dev/null | wc -l); "
            'echo "Landing files for {{ ds }}: $FILE_COUNT"; '
            "[ $FILE_COUNT -gt 0 ] || { echo 'ERROR: No files landed'; exit 1; }"
        ),
    )

    # ------------------------------------------------------------------
    # DAG wiring
    # ------------------------------------------------------------------
    validate_env >> [ingest_sftp, ingest_api, ingest_db] >> landing_check
