"""
data_quality_dag.py — Post-transform Great Expectations quality checks

Runs GE checkpoints against Silver and Gold layers, then executes custom
Python checks (null rates, schema drift, row counts).
All results are appended to a persistent quality_audit table.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [os.getenv("ALERT_EMAIL", "data-eng@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_ge_checkpoint(layer: str, execution_date: str, base_path: str) -> None:
    """Invoke a Great Expectations checkpoint for the given layer and date."""
    import subprocess, sys
    result = subprocess.run(
        [
            sys.executable,
            "data_quality/validators/run_great_expectations.py",
            "--layer",          layer,
            "--execution-date", execution_date,
            "--base-path",      base_path,
        ],
        capture_output=True, text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"GE checkpoint failed for layer={layer} date={execution_date}")


def _run_custom_checks(layer: str, execution_date: str, base_path: str) -> None:
    """Run Python custom quality checks for the given layer."""
    import subprocess, sys
    result = subprocess.run(
        [
            sys.executable,
            "data_quality/validators/custom_checks.py",
            "--layer",          layer,
            "--execution-date", execution_date,
            "--base-path",      base_path,
        ],
        capture_output=True, text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"Custom checks failed for layer={layer} date={execution_date}")


with DAG(
    dag_id="data_quality_pipeline",
    default_args=DEFAULT_ARGS,
    description="Post-transform data quality checks (GE + custom) for Silver and Gold",
    schedule_interval="0 6 * * *",         # 06:00 UTC, after transforms at 04:00
    catchup=True,
    max_active_runs=1,
    tags=["quality", "great-expectations"],
    doc_md=__doc__,
) as dag:

    exec_date   = "{{ ds }}"
    silver_path = "{{ var.value.SILVER_BASE_PATH }}"
    gold_path   = "{{ var.value.GOLD_BASE_PATH }}"

    # ------------------------------------------------------------------
    # Silver quality
    # ------------------------------------------------------------------
    silver_ge = PythonOperator(
        task_id="silver_ge_checkpoint",
        python_callable=_run_ge_checkpoint,
        op_kwargs={"layer": "silver", "execution_date": exec_date, "base_path": silver_path},
    )

    silver_custom = PythonOperator(
        task_id="silver_custom_checks",
        python_callable=_run_custom_checks,
        op_kwargs={"layer": "silver", "execution_date": exec_date, "base_path": silver_path},
    )

    # ------------------------------------------------------------------
    # Gold quality
    # ------------------------------------------------------------------
    gold_ge = PythonOperator(
        task_id="gold_ge_checkpoint",
        python_callable=_run_ge_checkpoint,
        op_kwargs={"layer": "gold", "execution_date": exec_date, "base_path": gold_path},
    )

    gold_custom = PythonOperator(
        task_id="gold_custom_checks",
        python_callable=_run_custom_checks,
        op_kwargs={"layer": "gold", "execution_date": exec_date, "base_path": gold_path},
    )

    # ------------------------------------------------------------------
    # DAG wiring: silver checks run in parallel, then gold checks
    # ------------------------------------------------------------------
    [silver_ge, silver_custom] >> gold_ge >> gold_custom
