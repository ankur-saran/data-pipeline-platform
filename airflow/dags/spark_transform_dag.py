"""
spark_transform_dag.py — Bronze → Silver → Gold Spark transformation pipeline

Triggers parameterised Spark jobs for each medallion layer in sequence.
Uses data_interval_start so backfills replay the correct partition.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": True,               # Silver must not run if yesterday's Silver failed
    "start_date": days_ago(1),
    "email": [os.getenv("ALERT_EMAIL", "data-eng@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

JAR = "/opt/spark-jobs/data-pipeline-platform-assembly-1.0.0.jar"

# Spark configuration applied to every job; overridden per-task as needed
BASE_SPARK_CONF = {
    "spark.sql.shuffle.partitions": "{{ var.value.get('SPARK_SHUFFLE_PARTITIONS', '200') }}",
    "spark.executor.memory": "{{ var.value.get('SPARK_EXECUTOR_MEMORY', '2g') }}",
    "spark.driver.memory": "{{ var.value.get('SPARK_DRIVER_MEMORY', '1g') }}",
    "spark.executor.cores": "{{ var.value.get('SPARK_EXECUTOR_CORES', '2') }}",
    "spark.sql.adaptive.enabled": "true",
}


def _check_upstream_landing(**context: dict) -> None:
    """Verify that the ingestion DAG produced files before running transforms."""
    import subprocess
    landing = Variable.get("INGESTION_LANDING_PATH")
    ds_nodash = context["ds_nodash"]
    result = subprocess.run(
        ["bash", "-c", f"find {landing}/{ds_nodash} -type f 2>/dev/null | wc -l"],
        capture_output=True, text=True, check=True,
    )
    count = int(result.stdout.strip())
    if count == 0:
        raise RuntimeError(
            f"No landing files found for {ds_nodash} — ingestion DAG may have failed"
        )
    log.info("Upstream check passed: %d files found in landing zone", count)


with DAG(
    dag_id="spark_transform_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver → Gold Spark medallion transforms",
    schedule_interval="0 4 * * *",         # 04:00 UTC, after ingestion at 02:00
    catchup=True,
    max_active_runs=1,
    tags=["spark", "transform", "medallion"],
    doc_md=__doc__,
) as dag:

    env          = "{{ var.value.PIPELINE_ENV }}"
    exec_date    = "{{ ds }}"
    bronze_path  = "{{ var.value.BRONZE_BASE_PATH }}"
    silver_path  = "{{ var.value.SILVER_BASE_PATH }}"
    gold_path    = "{{ var.value.GOLD_BASE_PATH }}"
    landing_path = "{{ var.value.INGESTION_LANDING_PATH }}"

    # ------------------------------------------------------------------
    # Task 1: Upstream check
    # ------------------------------------------------------------------
    check_landing = PythonOperator(
        task_id="check_landing_files",
        python_callable=_check_upstream_landing,
    )

    # ------------------------------------------------------------------
    # Task 2: Bronze — raw ingest
    # ------------------------------------------------------------------
    bronze_job = SparkSubmitOperator(
        task_id="spark_bronze_ingest",
        application=JAR,
        java_class="com.pipeline.bronze.RawIngestJob",
        application_args=[
            "--env",            env,
            "--execution-date", exec_date,
            "--input-path",     landing_path,
            "--output-path",    bronze_path,
            "--source-system",  "pipeline",
            "--format",         "csv",
        ],
        conf={
            **BASE_SPARK_CONF,
            "spark.app.name": "RawIngestJob-{{ ds_nodash }}",
        },
        conn_id="spark_default",
        verbose=True,
    )

    # ------------------------------------------------------------------
    # Task 3: Silver — cleanse and transform
    # ------------------------------------------------------------------
    silver_job = SparkSubmitOperator(
        task_id="spark_silver_transform",
        application=JAR,
        java_class="com.pipeline.silver.CleanseTransformJob",
        application_args=[
            "--env",            env,
            "--execution-date", exec_date,
            "--input-path",     bronze_path,
            "--output-path",    silver_path,
        ],
        conf={
            **BASE_SPARK_CONF,
            "spark.sql.shuffle.partitions": "{{ var.value.get('SPARK_SHUFFLE_PARTITIONS', '400') }}",
            "spark.app.name": "CleanseTransformJob-{{ ds_nodash }}",
        },
        conn_id="spark_default",
        verbose=True,
    )

    # ------------------------------------------------------------------
    # Task 4: Gold — aggregate and enrich
    # ------------------------------------------------------------------
    gold_job = SparkSubmitOperator(
        task_id="spark_gold_aggregate",
        application=JAR,
        java_class="com.pipeline.gold.AggregateEnrichJob",
        application_args=[
            "--env",            env,
            "--execution-date", exec_date,
            "--input-path",     silver_path,
            "--output-path",    gold_path,
            "--metrics-path",   gold_path + "/metrics",
        ],
        conf={
            **BASE_SPARK_CONF,
            "spark.app.name": "AggregateEnrichJob-{{ ds_nodash }}",
        },
        conn_id="spark_default",
        verbose=True,
    )

    # ------------------------------------------------------------------
    # DAG wiring: landing check → bronze → silver → gold
    # ------------------------------------------------------------------
    check_landing >> bronze_job >> silver_job >> gold_job
