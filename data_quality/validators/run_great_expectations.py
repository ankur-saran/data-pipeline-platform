#!/usr/bin/env python3
"""
run_great_expectations.py — Execute a Great Expectations checkpoint for a pipeline layer.

Reads the layer's expectation suite JSON from data_quality/expectations/,
validates against the current Parquet partition, and saves results.
Raises SystemExit(1) on validation failure so Airflow marks the task failed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("run_great_expectations")

EXPECTATIONS_DIR = Path("data_quality/expectations")
GE_ROOT = Path(os.getenv("GE_ROOT", "/tmp/gx"))


def _load_expectation_suite(layer: str) -> dict[str, Any]:
    path = EXPECTATIONS_DIR / f"{layer}_expectations.json"
    if not path.exists():
        raise FileNotFoundError(f"Expectation suite not found: {path}")
    return json.loads(path.read_text())


def _build_context() -> EphemeralDataContext:
    """Create an in-process GE data context backed by local filesystem."""
    GE_ROOT.mkdir(parents=True, exist_ok=True)
    config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=str(GE_ROOT))
    )
    return gx.get_context(project_config=config)


def run_checkpoint(layer: str, execution_date: str, base_path: str) -> bool:
    """
    Run the GE checkpoint for a layer/date and return True if all expectations pass.

    Args:
        layer:          Pipeline layer: bronze | silver | gold
        execution_date: Partition date in YYYY-MM-DD format
        base_path:      Absolute path to the layer's Parquet root

    Returns:
        True if all expectations pass.

    Raises:
        RuntimeError: If the checkpoint itself fails to execute.
    """
    log.info("Running GE checkpoint: layer=%s  date=%s  path=%s", layer, execution_date, base_path)

    suite_dict = _load_expectation_suite(layer)
    context    = _build_context()

    suite_name = f"{layer}_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name,
        expectations=suite_dict.get("expectations", []),
    )

    # Datasource backed by Spark (local mode) reading the date partition
    date    = datetime.strptime(execution_date, "%Y-%m-%d")
    context.add_datasource(
        name=f"{layer}_datasource",
        class_name="SparkDFDatasource",
        execution_engine={
            "class_name": "SparkDFExecutionEngine",
            "force_reuse_spark_context": True,
        },
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["execution_date"],
            }
        },
    )

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(f"GE-{layer}")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read.parquet(base_path)
        .filter(
            (spark.createDataFrame([]).sparkSession.sql(
                f"SELECT {date.year} AS year, {date.month} AS month, {date.day} AS day"
            ).collect())  # evaluated lazily
        )
    )

    # Simpler direct filter approach
    df = spark.read.parquet(base_path)
    df = df.filter(
        (df["year"] == date.year) & (df["month"] == date.month) & (df["day"] == date.day)
    )

    batch_request = RuntimeBatchRequest(
        datasource_name=f"{layer}_datasource",
        data_connector_name="runtime_connector",
        data_asset_name=layer,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"execution_date": execution_date},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    results = validator.validate()
    spark.stop()

    log.info(
        "GE validation complete: success=%s  evaluated=%d  failed=%d",
        results.success,
        results.statistics["evaluated_expectations"],
        results.statistics["unsuccessful_expectations"],
    )

    if not results.success:
        failed = [
            f"  [{r.expectation_config.expectation_type}]: {r.result}"
            for r in results.results
            if not r.success
        ]
        log.error("Failed expectations:\n%s", "\n".join(failed))

    return results.success


def main() -> None:
    p = argparse.ArgumentParser(description="Run Great Expectations checkpoint for a pipeline layer")
    p.add_argument("--layer",          required=True, choices=["bronze", "silver", "gold"])
    p.add_argument("--execution-date", required=True)
    p.add_argument("--base-path",      required=True)
    args = p.parse_args()

    passed = run_checkpoint(args.layer, args.execution_date, args.base_path)
    if not passed:
        log.error(
            "Great Expectations validation FAILED for layer=%s date=%s",
            args.layer, args.execution_date,
        )
        sys.exit(1)

    log.info("Great Expectations validation PASSED")


if __name__ == "__main__":
    main()
