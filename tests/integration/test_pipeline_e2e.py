"""
test_pipeline_e2e.py — End-to-end pipeline integration test

Validates the full Bronze → Silver → Gold flow using synthetic data
without a real Airflow scheduler.  Runs Spark locally (local[*]).

Requires:
  - PySpark installed  (pip install pyspark)
  - JAR built         (make build)
  - Data dirs writable
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path
from typing import Generator

import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.parent
JAR = next(
    PROJECT_ROOT.glob("spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-*.jar"),
    None,
)
EXECUTION_DATE = "2024-03-15"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tmp_data_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory tree for all data layers."""
    base = Path(tempfile.mkdtemp(prefix="pipeline_e2e_"))
    for sub in ["landing", "bronze", "silver", "gold", "quality_audit"]:
        (base / sub).mkdir()
    yield base
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(scope="module")
def sample_csv(tmp_data_dir: Path) -> Path:
    """Write a minimal CSV file into the landing zone."""
    date_nodash = EXECUTION_DATE.replace("-", "")
    landing_dir = tmp_data_dir / "landing" / date_nodash / "sftp"
    landing_dir.mkdir(parents=True, exist_ok=True)
    csv_path = landing_dir / "events_2024-03-15.csv"
    csv_path.write_text(
        "id,amount,event_date,created_at,is_active,source_system\n"
        "1,100.0,2024-03-15,2024-03-15T08:00:00,true,sftp\n"
        "2,200.0,2024-03-15,2024-03-15T09:00:00,false,sftp\n"
        "3,50.0,2024-03-15,2024-03-15T10:00:00,true,api\n"
        "3,50.0,2024-03-15,2024-03-15T10:00:00,true,api\n"  # duplicate
        "4,-10.0,2024-03-15,2024-03-15T11:00:00,true,api\n"  # negative → filtered in silver
    )
    return csv_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spark_submit(
    java_class: str,
    args: list[str],
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run a Spark job in-process via subprocess and return the result."""
    if JAR is None:
        pytest.skip("Fat-jar not found — run 'make build' first")

    cmd = [
        "spark-submit",
        "--master", "local[2]",
        "--class", java_class,
        "--conf", "spark.sql.shuffle.partitions=4",
        "--conf", "spark.sql.adaptive.enabled=false",
        "--conf", "spark.ui.enabled=false",
        str(JAR),
        *args,
    ]
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(cmd, capture_output=True, text=True, env=merged_env)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBronzeIngest:
    def test_bronze_job_succeeds(self, tmp_data_dir: Path, sample_csv: Path) -> None:
        date_nodash = EXECUTION_DATE.replace("-", "")
        input_path  = str(tmp_data_dir / "landing" / date_nodash / "sftp")
        output_path = str(tmp_data_dir / "bronze")

        result = _spark_submit(
            "com.pipeline.bronze.RawIngestJob",
            [
                "--env", "dev",
                "--execution-date", EXECUTION_DATE,
                "--input-path", input_path,
                "--output-path", output_path,
                "--source-system", "sftp",
                "--format", "csv",
            ],
        )
        if result.returncode != 0:
            pytest.fail(f"Bronze job failed:\n{result.stderr}")

    def test_bronze_partition_created(self, tmp_data_dir: Path) -> None:
        partition = (
            tmp_data_dir / "bronze"
            / "year=2024" / "month=3" / "day=15"
        )
        assert partition.exists(), f"Expected partition {partition} not found"
        parquet_files = list(partition.glob("*.parquet"))
        assert len(parquet_files) > 0, "No Parquet files in Bronze partition"


class TestSilverTransform:
    def test_silver_job_succeeds(self, tmp_data_dir: Path, sample_csv: Path) -> None:
        result = _spark_submit(
            "com.pipeline.silver.CleanseTransformJob",
            [
                "--env", "dev",
                "--execution-date", EXECUTION_DATE,
                "--input-path", str(tmp_data_dir / "bronze"),
                "--output-path", str(tmp_data_dir / "silver"),
            ],
        )
        if result.returncode != 0:
            pytest.fail(f"Silver job failed:\n{result.stderr}")

    def test_silver_removes_duplicates(self, tmp_data_dir: Path) -> None:
        """Row id=3 source_system=api appeared twice — silver should have 1 copy."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            pytest.skip("pyspark not installed")

        spark = SparkSession.builder.master("local[2]").appName("e2e-check").getOrCreate()
        try:
            df = spark.read.parquet(str(tmp_data_dir / "silver"))
            api_id3 = df.filter((df["source_system"] == "api") & (df["id"] == 3))
            assert api_id3.count() == 1, "Duplicate row for id=3 was not removed by Silver"
        finally:
            spark.stop()

    def test_silver_removes_negative_amounts(self, tmp_data_dir: Path) -> None:
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            pytest.skip("pyspark not installed")

        spark = SparkSession.builder.master("local[2]").appName("e2e-check").getOrCreate()
        try:
            df = spark.read.parquet(str(tmp_data_dir / "silver"))
            negatives = df.filter(df["amount"] < 0)
            assert negatives.count() == 0, "Negative-amount rows leaked into Silver"
        finally:
            spark.stop()


class TestGoldAggregate:
    def test_gold_job_succeeds(self, tmp_data_dir: Path, sample_csv: Path) -> None:
        result = _spark_submit(
            "com.pipeline.gold.AggregateEnrichJob",
            [
                "--env", "dev",
                "--execution-date", EXECUTION_DATE,
                "--input-path", str(tmp_data_dir / "silver"),
                "--output-path", str(tmp_data_dir / "gold"),
                "--metrics-path", str(tmp_data_dir / "gold" / "metrics"),
            ],
        )
        if result.returncode != 0:
            pytest.fail(f"Gold job failed:\n{result.stderr}")

    def test_gold_daily_summary_has_data(self, tmp_data_dir: Path) -> None:
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            pytest.skip("pyspark not installed")

        spark = SparkSession.builder.master("local[2]").appName("e2e-check").getOrCreate()
        try:
            daily = spark.read.parquet(str(tmp_data_dir / "gold" / "daily_summary"))
            assert daily.count() > 0, "Gold daily_summary is empty"
            assert "total_records" in daily.columns
            assert "total_amount" in daily.columns
        finally:
            spark.stop()
