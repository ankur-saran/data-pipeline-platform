#!/usr/bin/env python3
"""
custom_checks.py — Custom data quality checks for pipeline layers

Validates:
  - Row count within expected range
  - Null percentage below threshold per critical column
  - Schema drift: detects new/removed columns vs a reference snapshot
  - Partition completeness: the expected date partition exists and is non-empty

All failures raise PipelineDataQualityError and are persisted to the audit log.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("custom_checks")

AUDIT_BASE = os.getenv("QUALITY_AUDIT_PATH", "/data/quality_audit")


class PipelineDataQualityError(Exception):
    """Raised when one or more quality checks fail."""


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def check_row_count(
    df: Any,
    min_rows: int,
    max_rows: int = sys.maxsize,
    label: str = "dataset",
) -> dict[str, Any]:
    """Return a quality result dict; does NOT raise — caller decides."""
    count = df.count()
    passed = min_rows <= count <= max_rows
    return {
        "check_name": f"row_count.{label}",
        "passed": passed,
        "actual": str(count),
        "expected": f"[{min_rows}, {max_rows if max_rows != sys.maxsize else '∞'}]",
        "message": (
            f"Row count {count} within range" if passed
            else f"Row count {count} outside expected range [{min_rows}, {max_rows}]"
        ),
    }


def check_null_rate(
    df: Any,
    columns: list[str],
    max_null_rate: float = 0.05,
) -> list[dict[str, Any]]:
    """Return one result dict per column."""
    from pyspark.sql import functions as F

    total = df.count()
    if total == 0:
        return []

    results = []
    for col in columns:
        if col not in df.columns:
            results.append({
                "check_name": f"null_rate.{col}",
                "passed": False,
                "actual": "column_missing",
                "expected": f"<= {max_null_rate}",
                "message": f"Column '{col}' not found in DataFrame",
            })
            continue

        null_count = df.filter(F.col(col).isNull()).count()
        rate = null_count / total
        passed = rate <= max_null_rate
        results.append({
            "check_name": f"null_rate.{col}",
            "passed": passed,
            "actual": f"{rate:.4f}",
            "expected": f"<= {max_null_rate}",
            "message": (
                f"'{col}' null rate OK ({rate * 100:.2f}%)" if passed
                else f"'{col}' null rate {rate * 100:.2f}% exceeds threshold {max_null_rate * 100:.2f}%"
            ),
        })
    return results


def check_schema_drift(
    df: Any,
    reference_path: str,
    layer: str,
) -> list[dict[str, Any]]:
    """Compare current schema to a stored reference JSON; detect drift."""
    ref_file = Path(reference_path) / f"{layer}_schema_ref.json"
    current_cols = set(df.columns)

    if not ref_file.exists():
        log.info("No schema reference found at %s — writing baseline", ref_file)
        ref_file.write_text(json.dumps(sorted(current_cols)))
        return [{
            "check_name": f"schema_drift.{layer}",
            "passed": True,
            "actual": str(sorted(current_cols)),
            "expected": "baseline written",
            "message": "Schema reference created; no drift check this run",
        }]

    reference_cols = set(json.loads(ref_file.read_text()))
    added   = current_cols - reference_cols
    removed = reference_cols - current_cols
    passed  = not added and not removed

    return [{
        "check_name": f"schema_drift.{layer}",
        "passed": passed,
        "actual": str(sorted(current_cols)),
        "expected": str(sorted(reference_cols)),
        "message": (
            "Schema unchanged" if passed
            else f"Schema drift detected — added={sorted(added)} removed={sorted(removed)}"
        ),
    }]


def check_partition_exists(base_path: str, execution_date: str) -> dict[str, Any]:
    """Verify the expected date partition directory exists and is non-empty."""
    from pathlib import Path

    date = datetime.strptime(execution_date, "%Y-%m-%d")
    partition = Path(base_path) / f"year={date.year}" / f"month={date.month}" / f"day={date.day}"

    exists    = partition.exists()
    non_empty = exists and any(partition.iterdir())
    passed    = exists and non_empty

    return {
        "check_name": f"partition_exists.{execution_date}",
        "passed": passed,
        "actual": str(partition) if exists else "missing",
        "expected": str(partition),
        "message": (
            f"Partition {partition} exists and is non-empty" if passed
            else f"Partition {partition} missing or empty"
        ),
    }


# ---------------------------------------------------------------------------
# Audit persistence
# ---------------------------------------------------------------------------

def persist_audit_results(
    results: list[dict[str, Any]],
    layer: str,
    execution_date: str,
    pipeline_run_id: str,
) -> None:
    """Append quality results as NDJSON to the audit log directory."""
    audit_dir = Path(AUDIT_BASE) / layer
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_file = audit_dir / f"{execution_date}.ndjson"

    with audit_file.open("a") as fh:
        for r in results:
            record = {
                **r,
                "layer": layer,
                "execution_date": execution_date,
                "pipeline_run_id": pipeline_run_id,
                "checked_at": datetime.utcnow().isoformat() + "Z",
            }
            fh.write(json.dumps(record) + "\n")

    log.info("Persisted %d quality results to %s", len(results), audit_file)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run custom pipeline data quality checks")
    p.add_argument("--layer",          required=True, choices=["bronze", "silver", "gold"])
    p.add_argument("--execution-date", required=True)
    p.add_argument("--base-path",      required=True)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    import uuid
    pipeline_run_id = str(uuid.uuid4())

    log.info(
        "Starting custom checks: layer=%s  date=%s  path=%s",
        args.layer, args.execution_date, args.base_path,
    )

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(f"CustomChecks-{args.layer}")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Read the partition for this execution date
    date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    df = (
        spark.read.parquet(args.base_path)
        .filter(
            (spark.sparkContext._jvm.org.apache.spark.sql.functions.col("year")  == date.year)  # type: ignore
        )
    )

    # Layer-specific config
    configs: dict[str, dict] = {
        "bronze": {"min_rows": 0,   "critical_cols": ["pipeline_run_id", "source_system"], "max_null": 0.0},
        "silver": {"min_rows": 1,   "critical_cols": ["id", "source_system"],              "max_null": 0.02},
        "gold":   {"min_rows": 1,   "critical_cols": ["source_system", "execution_date"],  "max_null": 0.0},
    }
    cfg = configs[args.layer]

    all_results: list[dict] = []
    all_results.append(check_row_count(df, min_rows=cfg["min_rows"], label=args.layer))
    all_results.extend(check_null_rate(df, cfg["critical_cols"], max_null_rate=cfg["max_null"]))
    all_results.extend(check_schema_drift(df, args.base_path, args.layer))
    all_results.append(check_partition_exists(args.base_path, args.execution_date))

    persist_audit_results(all_results, args.layer, args.execution_date, pipeline_run_id)

    failures = [r for r in all_results if not r["passed"]]
    if failures:
        for f in failures:
            log.error("FAIL [%s]: %s", f["check_name"], f["message"])
        spark.stop()
        raise PipelineDataQualityError(
            f"{len(failures)} quality check(s) failed for layer={args.layer} date={args.execution_date}"
        )

    log.info(
        "All %d checks passed for layer=%s date=%s",
        len(all_results), args.layer, args.execution_date,
    )
    spark.stop()


if __name__ == "__main__":
    main()
