# Pipeline Runbook — Step-by-Step Operating Guide

This document is the single authoritative reference for installing, configuring, building, running, testing, monitoring, and troubleshooting the **data-pipeline-platform** in every environment.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Layout Quick-Reference](#2-repository-layout-quick-reference)
3. [First-Time Setup](#3-first-time-setup)
   - 3.1 [Clone and enter the project](#31-clone-and-enter-the-project)
   - 3.2 [Configure the environment file](#32-configure-the-environment-file)
   - 3.3 [Run bootstrap](#33-run-bootstrap)
4. [Building the Spark Fat-Jar](#4-building-the-spark-fat-jar)
5. [Starting and Stopping the Local Stack](#5-starting-and-stopping-the-local-stack)
   - 5.1 [Start all services](#51-start-all-services)
   - 5.2 [Verify services are healthy](#52-verify-services-are-healthy)
   - 5.3 [Stop all services](#53-stop-all-services)
6. [Configuring Airflow Variables](#6-configuring-airflow-variables)
7. [Running the Pipeline — Three Modes](#7-running-the-pipeline--three-modes)
   - 7.1 [Mode A — Makefile (quickest)](#71-mode-a--makefile-quickest)
   - 7.2 [Mode B — Direct spark-submit (fine-grained control)](#72-mode-b--direct-spark-submit-fine-grained-control)
   - 7.3 [Mode C — Airflow DAGs (production workflow)](#73-mode-c--airflow-dags-production-workflow)
8. [Layer-by-Layer Details](#8-layer-by-layer-details)
   - 8.1 [Ingestion (Shell → Landing Zone)](#81-ingestion-shell--landing-zone)
   - 8.2 [Bronze — Raw Ingest Spark Job](#82-bronze--raw-ingest-spark-job)
   - 8.3 [Silver — Cleanse & Transform Spark Job](#83-silver--cleanse--transform-spark-job)
   - 8.4 [Gold — Aggregate & Enrich Spark Job](#84-gold--aggregate--enrich-spark-job)
9. [Data Quality Checks](#9-data-quality-checks)
   - 9.1 [Great Expectations checkpoints](#91-great-expectations-checkpoints)
   - 9.2 [Custom Python checks](#92-custom-python-checks)
   - 9.3 [Reading the audit log](#93-reading-the-audit-log)
10. [Running Tests](#10-running-tests)
    - 10.1 [Scala unit tests](#101-scala-unit-tests)
    - 10.2 [Python integration tests](#102-python-integration-tests)
    - 10.3 [Linting](#103-linting)
11. [Backfilling Historical Dates](#11-backfilling-historical-dates)
12. [Health Checks and Monitoring](#12-health-checks-and-monitoring)
13. [Environment Promotion (dev → staging → prod)](#13-environment-promotion-dev--staging--prod)
14. [Troubleshooting](#14-troubleshooting)
15. [Makefile Target Reference](#15-makefile-target-reference)
16. [Environment Variable Reference](#16-environment-variable-reference)

---

## 1. Prerequisites

Install all tools before running any commands. The bootstrap script will **not** install them for you — it checks for them and exits if any are missing.

| Tool | Minimum Version | Install reference |
|------|----------------|-------------------|
| **Java (JDK)** | 11 (17 recommended) | https://adoptium.net |
| **Python** | 3.10 | https://python.org |
| **sbt** | 1.9 | https://www.scala-sbt.org/download.html |
| **Apache Spark** | 3.4 | https://spark.apache.org/downloads.html |
| **Docker Engine** | 24 | https://docs.docker.com/engine/install |
| **Docker Compose** | v2 (plugin) | Bundled with Docker Desktop |
| **pip** | latest | `python -m pip install --upgrade pip` |
| **shellcheck** | any | `apt install shellcheck` / `brew install shellcheck` |

### Verify versions

```bash
java   -version          # expect: openjdk version "17..." or "11..."
python3 --version        # expect: Python 3.10.x or higher
sbt    --version         # expect: sbt 1.9.x
spark-submit --version   # expect: version 3.4.x
docker --version         # expect: Docker version 24.x or higher
docker compose version   # expect: Docker Compose version v2.x
```

### Disk and memory minimums (local dev)

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 8 GB | 16 GB |
| Free disk | 10 GB | 20 GB |
| CPU cores | 4 | 8 |

The docker-compose stack starts: Postgres, Redis, Airflow webserver, scheduler, one Celery worker, Spark master, and 2 Spark workers — 7 containers in total.

---

## 2. Repository Layout Quick-Reference

```
data-pipeline-platform/
├── airflow/              Airflow DAGs, plugins, config, requirements
├── config/               HOCON configs — dev / staging / prod
├── data_quality/         Great Expectations suites + custom validators
├── docker/               docker-compose, Dockerfiles, log4j2.properties
├── ingestion/            Shell ingestion scripts + sources.yaml
├── scripts/              bootstrap, submit_spark_job, health_check
├── spark-jobs/           Scala/Spark source + sbt build
│   └── src/main/scala/com/pipeline/
│       ├── bronze/       RawIngestJob
│       ├── silver/       CleanseTransformJob
│       ├── gold/         AggregateEnrichJob
│       └── common/       SparkSessionFactory, ConfigLoader, DataQualityUtils
├── tests/integration/    End-to-end pytest suite
├── .env.example          Template for all environment variables
└── Makefile              One-stop command interface
```

---

## 3. First-Time Setup

### 3.1 Clone and enter the project

```bash
git clone <your-repo-url> data-pipeline-platform
cd data-pipeline-platform
```

### 3.2 Configure the environment file

The entire platform is driven by a `.env` file that is **never** committed to source control.

```bash
cp .env.example .env
```

Open `.env` in your editor. The mandatory fields for local development are:

```bash
# ── Mandatory for local dev ──────────────────────────────────────────
PIPELINE_ENV=dev

# Generate with: python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__FERNET_KEY=<generated-key>

# These default values work as-is with docker-compose — change only for external infra
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW_UID=50000

# Data paths — these are bind-mounted inside every container
BRONZE_BASE_PATH=/data/bronze
SILVER_BASE_PATH=/data/silver
GOLD_BASE_PATH=/data/gold
INGESTION_LANDING_PATH=/data/landing

# Spark — defaults work for the compose cluster
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_SHUFFLE_PARTITIONS=200
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g
```

> **Tip — generate the Fernet key:**
> ```bash
> python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
> ```
> Paste the output into `.env` next to `AIRFLOW__CORE__FERNET_KEY=`.

Fill in source-system credentials only if you intend to run live ingestion. The ingestion scripts are designed to exit gracefully with a warning when they have nothing to pull.

### 3.3 Run bootstrap

Bootstrap is a one-shot script that checks prerequisites, installs Python deps, builds the Spark fat-jar, creates the local `/data/*` directories, builds Docker images, and initialises the Airflow database.

```bash
bash scripts/bootstrap.sh
# or equivalently:
make bootstrap
```

Expected output (abbreviated):

```
[08:00:01] Checking prerequisites…
[08:00:02] Java 17 detected ✓
[08:00:02] Python 3.10.12 detected ✓
[08:00:02] Creating .env from .env.example…
[08:00:03] Installing Python dependencies…
[08:00:45] Python dependencies installed ✓
[08:00:45] Building Spark fat-jar (this may take a few minutes)…
[08:03:12] Fat-jar built: spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-1.0.0.jar ✓
[08:03:12] Data directories created ✓
[08:03:13] Building Docker images…
[08:06:00] Initialising Airflow DB…
[08:06:40] Airflow DB initialised ✓

Bootstrap complete!
  Start the stack : make docker-up
  Run a pipeline  : make transform ENV=dev
  Run quality checks: make quality
```

> **If bootstrap fails**, check the specific error. Common fixes are covered in [§14 Troubleshooting](#14-troubleshooting).

---

## 4. Building the Spark Fat-Jar

The `sbt assembly` plugin compiles all Scala source files and packages them — together with runtime dependencies — into a single fat-jar at:

```
spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-1.0.0.jar
```

This jar is mounted read-only into every container at `/opt/spark-jobs/`.

### Build commands

```bash
# Full clean + compile + assembly (what bootstrap and make build both call)
make build

# Equivalent direct sbt invocation
cd spark-jobs && sbt clean assembly

# Incremental recompile only (faster during active development)
cd spark-jobs && sbt assembly
```

### Verify the jar was produced

```bash
ls -lh spark-jobs/target/scala-2.12/*.jar
# Expected: data-pipeline-platform-assembly-1.0.0.jar  (~100–200 MB)
```

> **Important:** Rebuild the jar after **any** change to Scala source files. Makefile targets `spark-bronze`, `spark-silver`, `spark-gold`, and `transform` call `make build` automatically, so you do not need to remember this during development.

---

## 5. Starting and Stopping the Local Stack

### 5.1 Start all services

```bash
make docker-up
```

This runs `docker compose -f docker/docker-compose.yml up -d` and starts:

| Container | Exposes | Purpose |
|-----------|---------|---------|
| `postgres` | (internal) 5432 | Airflow metadata DB |
| `redis` | (internal) 6379 | Celery task broker |
| `airflow-init` | — | One-shot: DB migrations + admin user creation |
| `airflow-webserver` | `8080→8080` | Airflow web UI and REST API |
| `airflow-scheduler` | — | DAG parsing and task scheduling |
| `airflow-worker` | — | Celery task executor |
| `spark-master` | `7077→7077`, `8081→8080` | Spark cluster master + UI |
| `spark-worker` ×2 | — | Spark executors (2 GB / 2 cores each) |

The startup sequence is health-check gated: Postgres and Redis must pass their checks before Airflow services start; the scheduler must be healthy before the worker starts.

**Allow 60–90 seconds** on first boot for images to pull and `airflow-init` to complete.

### 5.2 Verify services are healthy

```bash
# Check all containers are Up (not Restarting or Exited)
docker compose -f docker/docker-compose.yml ps

# Run the pipeline health-check script
bash scripts/health_check.sh
```

Expected health check output:

```
=== Pipeline Health Check  2024-03-15T08:00:00Z ===

[ Airflow ]
  [OK]   Airflow API reachable (HTTP 200)
  [WARN] ingestion_pipeline — paused       ← normal; DAGs start paused
  [WARN] spark_transform_pipeline — paused
  [WARN] data_quality_pipeline — paused

[ Spark ]
  [OK]   Spark Master UI reachable (HTTP 200)

[ Data Paths ]
  [OK]   /data/landing exists
  [OK]   /data/bronze exists
  [OK]   /data/silver exists
  [OK]   /data/gold exists

==============================
Status: DEGRADED (exit 1)  degraded_checks=3
```

> DAGs start **paused** by design (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"`). The DEGRADED status here is expected — it reflects paused DAGs, not a fault. Activate the DAGs in step [§7.3](#73-mode-c--airflow-dags-production-workflow).

### Open the web UIs

| UI | URL | Credentials |
|----|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master | http://localhost:8081 | — |

### 5.3 Stop all services

```bash
# Stop containers and remove volumes (full teardown)
make docker-down

# Stop containers but preserve volumes (data is retained on restart)
docker compose -f docker/docker-compose.yml stop
```

---

## 6. Configuring Airflow Variables

The DAGs read all runtime paths and settings from **Airflow Variables** (not hardcoded values). These must be set before triggering any DAG.

### Option A — Airflow UI (easiest)

1. Navigate to http://localhost:8080
2. Go to **Admin → Variables**
3. Click **+** and add each variable below

### Option B — Airflow CLI (scriptable)

```bash
# Execute inside the airflow-scheduler container
docker exec -it <airflow-scheduler-container-id> bash

# Or via make (runs the command inside the container)
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set PIPELINE_ENV dev

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set INGESTION_LANDING_PATH /data/landing

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set BRONZE_BASE_PATH /data/bronze

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set SILVER_BASE_PATH /data/silver

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set GOLD_BASE_PATH /data/gold

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set SPARK_SHUFFLE_PARTITIONS 200

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set SPARK_EXECUTOR_MEMORY 2g

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set SPARK_DRIVER_MEMORY 1g

docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow variables set SPARK_EXECUTOR_CORES 2
```

### Required variables summary

| Variable | Dev value | Description |
|----------|-----------|-------------|
| `PIPELINE_ENV` | `dev` | Active environment for ConfigLoader |
| `INGESTION_LANDING_PATH` | `/data/landing` | Shell ingestion drop zone |
| `BRONZE_BASE_PATH` | `/data/bronze` | Bronze Parquet root |
| `SILVER_BASE_PATH` | `/data/silver` | Silver Parquet root |
| `GOLD_BASE_PATH` | `/data/gold` | Gold Parquet root |
| `SPARK_SHUFFLE_PARTITIONS` | `200` | spark.sql.shuffle.partitions |
| `SPARK_EXECUTOR_MEMORY` | `2g` | Executor heap |
| `SPARK_DRIVER_MEMORY` | `1g` | Driver heap |
| `SPARK_EXECUTOR_CORES` | `2` | Cores per executor |

Source-system variables (set only if running live ingestion):

| Variable | Description |
|----------|-------------|
| `SFTP_HOST` | SFTP server hostname |
| `SFTP_USER` | SFTP username |
| `SFTP_PASSWORD` | SFTP password |
| `SFTP_REMOTE_DIR` | Remote directory to pull from |
| `API_BASE_URL` | REST API base URL |
| `API_KEY` | API authentication token |
| `DB_EXPORT_JDBC_URL` | JDBC connection string |
| `DB_EXPORT_USER` | DB username |
| `DB_EXPORT_PASSWORD` | DB password |

---

## 7. Running the Pipeline — Three Modes

### 7.1 Mode A — Makefile (quickest)

Best for: developer workstations, quick one-off runs, CI.

```bash
# Run full pipeline for today's date
make transform

# Run for a specific date
make transform EXECUTION_DATE=2024-03-15

# Run for a non-default environment
make transform ENV=staging EXECUTION_DATE=2024-03-15

# Run only one layer
make spark-bronze  EXECUTION_DATE=2024-03-15
make spark-silver  EXECUTION_DATE=2024-03-15
make spark-gold    EXECUTION_DATE=2024-03-15

# Run ingestion scripts, then transform, then quality
make ingest
make transform EXECUTION_DATE=2024-03-15
make quality   EXECUTION_DATE=2024-03-15
```

> The `transform` target depends on `build`, so it automatically recompiles the fat-jar if any Scala source has changed.

### 7.2 Mode B — Direct spark-submit (fine-grained control)

Best for: debugging a specific job, passing non-standard conf flags, running against a remote cluster.

The `scripts/submit_spark_job.sh` wrapper handles jar resolution, master URL selection, and log file management.

```bash
# Bronze
bash scripts/submit_spark_job.sh \
  --job-class     com.pipeline.bronze.RawIngestJob \
  --env           dev \
  --execution-date 2024-03-15 \
  --input-path    /data/landing \
  --output-path   /data/bronze \
  --source-system sftp \
  --format        csv

# Silver
bash scripts/submit_spark_job.sh \
  --job-class     com.pipeline.silver.CleanseTransformJob \
  --env           dev \
  --execution-date 2024-03-15 \
  --input-path    /data/bronze \
  --output-path   /data/silver

# Gold
bash scripts/submit_spark_job.sh \
  --job-class     com.pipeline.gold.AggregateEnrichJob \
  --env           dev \
  --execution-date 2024-03-15 \
  --input-path    /data/silver \
  --output-path   /data/gold \
  --metrics-path  /data/gold/metrics
```

#### Passing extra Spark conf flags

```bash
bash scripts/submit_spark_job.sh \
  --job-class    com.pipeline.silver.CleanseTransformJob \
  --env          prod \
  --execution-date 2024-03-15 \
  --input-path   s3a://my-bucket/bronze \
  --output-path  s3a://my-bucket/silver \
  --extra-conf   spark.executor.instances=10 \
  --extra-conf   spark.sql.shuffle.partitions=800
```

#### Submit from inside the Spark master container

```bash
docker exec -it <spark-master-container-id> bash

spark-submit \
  --master spark://spark-master:7077 \
  --class  com.pipeline.bronze.RawIngestJob \
  --conf   spark.sql.shuffle.partitions=200 \
  /opt/spark-jobs/data-pipeline-platform-assembly-1.0.0.jar \
  --env dev \
  --execution-date 2024-03-15 \
  --input-path  /data/landing \
  --output-path /data/bronze
```

### 7.3 Mode C — Airflow DAGs (production workflow)

Best for: scheduled daily runs, backfills, full observability and retry management.

#### Step 1 — Unpause the DAGs

In the Airflow UI (http://localhost:8080):

1. Go to the **DAGs** list
2. Toggle the pause button (blue slider) on each of the three DAGs:
   - `ingestion_pipeline`
   - `spark_transform_pipeline`
   - `data_quality_pipeline`

Or via CLI:

```bash
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  bash -c "
    airflow dags unpause ingestion_pipeline &&
    airflow dags unpause spark_transform_pipeline &&
    airflow dags unpause data_quality_pipeline
  "
```

#### Step 2 — Trigger a manual run

**Via UI:**
1. Click the DAG name → **Trigger DAG** (play button, top right)
2. In the dialog, set `execution_date` to the partition you want to process (e.g., `2024-03-15`)
3. Click **Trigger**

**Via CLI:**

```bash
# Trigger ingestion for a specific date
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger ingestion_pipeline \
    --exec-date 2024-03-15T00:00:00+00:00

# Trigger transform pipeline
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger spark_transform_pipeline \
    --exec-date 2024-03-15T00:00:00+00:00

# Trigger quality checks
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow dags trigger data_quality_pipeline \
    --exec-date 2024-03-15T00:00:00+00:00
```

#### Step 3 — Monitor task progress

In the Airflow UI:
- **Grid view:** colour-coded status for every task run across every date
- **Graph view:** live status of the current run's tasks with the >> dependency chain visible
- **Logs:** click any task box → **Log** tab for real-time stdout/stderr

In the terminal:

```bash
# Stream all Airflow container logs
make docker-logs

# Stream only scheduler logs
docker compose -f docker/docker-compose.yml logs -f airflow-scheduler
```

#### DAG schedule summary

| DAG | Cron | UTC time | Runs after |
|-----|------|----------|------------|
| `ingestion_pipeline` | `0 2 * * *` | 02:00 | — |
| `spark_transform_pipeline` | `0 4 * * *` | 04:00 | Ingestion |
| `data_quality_pipeline` | `0 6 * * *` | 06:00 | Transform |

The 2-hour gaps between DAGs are deliberate — they give each stage time to complete before the next one starts. In production, replace these with explicit `ExternalTaskSensor` sensors for tighter coupling.

---

## 8. Layer-by-Layer Details

### 8.1 Ingestion (Shell → Landing Zone)

The three ingestion scripts write raw files to a date-partitioned directory:

```
/data/landing/
  └── <YYYYMMDD>/
        ├── sftp/         SFTP pulled files
        ├── api/          REST API JSON pages
        └── db/           JDBC export Parquet or SQL dump
```

#### Run manually

```bash
# SFTP pull
export SFTP_HOST=sftp.example.com SFTP_USER=ingest SFTP_PASSWORD=secret
bash ingestion/scripts/ingest_sftp.sh \
  --execution-date 2024-03-15 \
  --landing-path   /data/landing

# REST API pull
export API_BASE_URL=https://api.example.com API_KEY=mytoken
bash ingestion/scripts/ingest_api.sh \
  --execution-date 2024-03-15 \
  --landing-path   /data/landing

# DB export (Spark JDBC by default, or set JDBC_TOOL=mysqldump)
export DB_EXPORT_JDBC_URL=jdbc:mysql://host:3306/mydb \
       DB_EXPORT_USER=reader DB_EXPORT_PASSWORD=secret
bash ingestion/scripts/ingest_db_export.sh \
  --execution-date 2024-03-15 \
  --landing-path   /data/landing
```

#### Retry behaviour

All three scripts implement exponential backoff with up to 5 attempts:

| Attempt | Delay before retry |
|---------|--------------------|
| 1 → 2 | 10 s (SFTP/API) / 30 s (DB) |
| 2 → 3 | 20 s / 60 s |
| 3 → 4 | 40 s / 120 s |
| 4 → 5 | 80 s / 240 s |

#### Ingestion logs

All scripts write timestamped logs to `/var/log/pipeline/<script-name>.log` in addition to stdout:

```bash
tail -f /var/log/pipeline/ingest_sftp.log
```

#### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Data error (zero files, zero records) |
| 2 | Infrastructure error (unreachable host, missing env var) |

### 8.2 Bronze — Raw Ingest Spark Job

**Class:** `com.pipeline.bronze.RawIngestJob`

Reads raw files from the landing zone, attaches four metadata columns, and writes Parquet partitioned by `(year, month, day)`.

**Metadata columns added:**

| Column | Value |
|--------|-------|
| `pipeline_run_id` | Fresh UUID per job invocation |
| `ingested_at` | ISO-8601 timestamp at time of ingestion |
| `source_system` | Passed via `--source-system` argument |
| `year` / `month` / `day` | Derived from `--execution-date` |

**Arguments:**

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--env` | No | `dev` | Config environment |
| `--execution-date` | No | today | Partition date (YYYY-MM-DD) |
| `--input-path` | **Yes** | — | Landing zone path |
| `--output-path` | **Yes** | — | Bronze output path |
| `--source-system` | No | `unknown` | Source label |
| `--format` | No | `csv` | `csv` \| `json` \| `parquet` |

**Output path structure:**

```
/data/bronze/
  └── year=2024/month=3/day=15/
        └── part-00000-<uuid>.snappy.parquet
```

**Run via make:**

```bash
make spark-bronze EXECUTION_DATE=2024-03-15
```

**Run directly:**

```bash
bash scripts/submit_spark_job.sh \
  --job-class     com.pipeline.bronze.RawIngestJob \
  --env           dev \
  --execution-date 2024-03-15 \
  --input-path    /data/landing \
  --output-path   /data/bronze \
  --format        csv
```

**Validate Bronze output:**

```bash
# Count files written
find /data/bronze/year=2024/month=3/day=15 -name "*.parquet" | wc -l

# Inspect schema (requires PySpark or spark-shell)
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[2]').getOrCreate()
df = spark.read.parquet('/data/bronze')
df.printSchema()
df.filter('year=2024 AND month=3 AND day=15').show(5, truncate=False)
spark.stop()
"
```

### 8.3 Silver — Cleanse & Transform Spark Job

**Class:** `com.pipeline.silver.CleanseTransformJob`

Reads the Bronze partition for the given execution date and applies, in sequence:

1. **Type casting** — strings cast to `Long`, `Double`, `Date`, `Timestamp`, `Boolean`
2. **Deduplication** — `dropDuplicates(["id", "event_date", "source_system"])`
3. **Business rules** — drops rows where `id IS NULL` or `amount < 0`
4. **Dimension join** (optional) — left-joins with a dimension table if `--dim-path` is supplied

**Arguments:**

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--env` | No | `dev` | Config environment |
| `--execution-date` | No | today | Partition date |
| `--input-path` | **Yes** | — | Bronze input root |
| `--output-path` | **Yes** | — | Silver output root |
| `--dim-path` | No | _(empty)_ | Dimension tables root (skipped if absent) |

**Run via make:**

```bash
make spark-silver EXECUTION_DATE=2024-03-15
```

**Run directly:**

```bash
bash scripts/submit_spark_job.sh \
  --job-class     com.pipeline.silver.CleanseTransformJob \
  --env           dev \
  --execution-date 2024-03-15 \
  --input-path    /data/bronze \
  --output-path   /data/silver
```

**Quality gate inside the job** (fail-fast before writing):
- Null rate on `id` and `source_system` must be ≤ `pipeline.quality.max_null_rate_silver` from `application.conf`
- Row count must be ≥ 1

### 8.4 Gold — Aggregate & Enrich Spark Job

**Class:** `com.pipeline.gold.AggregateEnrichJob`

Reads the Silver partition and produces two output tables plus an optional JSON-Lines metrics log.

**Outputs:**

| Path | Content |
|------|---------|
| `<output-path>/daily_summary/` | Grouped by `(source_system, year, month, day)` — record counts, sum/avg/min/max of amount |
| `<output-path>/kpi_summary/` | Grouped by `source_system` — unique entity count, total value, active rate |
| `<metrics-path>/` | NDJSON metrics log, one record per KPI row, with `pipeline_run_id` |

**Arguments:**

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--env` | No | `dev` | Config environment |
| `--execution-date` | No | today | Partition date |
| `--input-path` | **Yes** | — | Silver input root |
| `--output-path` | **Yes** | — | Gold output root |
| `--metrics-path` | No | _(empty)_ | Metrics log path |

**Run via make:**

```bash
make spark-gold EXECUTION_DATE=2024-03-15
```

**Run directly:**

```bash
bash scripts/submit_spark_job.sh \
  --job-class     com.pipeline.gold.AggregateEnrichJob \
  --env           dev \
  --execution-date 2024-03-15 \
  --input-path    /data/silver \
  --output-path   /data/gold \
  --metrics-path  /data/gold/metrics
```

**Validate Gold output:**

```bash
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[2]').getOrCreate()
daily = spark.read.parquet('/data/gold/daily_summary')
print('=== Daily Summary ===')
daily.show(truncate=False)
kpis = spark.read.parquet('/data/gold/kpi_summary')
print('=== KPI Summary ===')
kpis.show(truncate=False)
spark.stop()
"
```

---

## 9. Data Quality Checks

Quality checks run automatically inside each Spark job (as fail-fast guards) and again as a dedicated Airflow DAG after the transforms complete.

### 9.1 Great Expectations checkpoints

GE checkpoints validate the Parquet output against the expectation suites in `data_quality/expectations/`.

**Run via make:**

```bash
make quality EXECUTION_DATE=2024-03-15
```

**Run directly (one layer at a time):**

```bash
python3 data_quality/validators/run_great_expectations.py \
  --layer         silver \
  --execution-date 2024-03-15 \
  --base-path     /data/silver

python3 data_quality/validators/run_great_expectations.py \
  --layer         gold \
  --execution-date 2024-03-15 \
  --base-path     /data/gold
```

**Exit codes:**

| Code | Meaning |
|------|---------|
| 0 | All expectations passed |
| 1 | One or more expectations failed |

**Expectation suites:**

| Suite file | Layer | Key checks |
|------------|-------|-----------|
| `bronze_expectations.json` | Bronze | Metadata columns present, UUIDs non-null, timestamps well-formed |
| `silver_expectations.json` | Silver | Row count ≥ 1, no null IDs, amounts ≥ 0, composite unique key |
| `gold_expectations.json` | Gold | Aggregate columns present, no negative totals, source_system non-null |

### 9.2 Custom Python checks

Custom checks go beyond GE to include partition-level and schema-drift detection.

```bash
python3 data_quality/validators/custom_checks.py \
  --layer         silver \
  --execution-date 2024-03-15 \
  --base-path     /data/silver
```

**Checks performed per layer:**

| Check | Bronze | Silver | Gold |
|-------|--------|--------|------|
| Row count | ≥ 0 | ≥ 1 | ≥ 1 |
| Null rate | 0% on metadata cols | ≤ 2% on `id`, `source_system` | 0% on aggregation keys |
| Schema drift | Baseline on first run | Compared vs saved reference | Compared vs saved reference |
| Partition exists | ✓ | ✓ | ✓ |

On first run per layer, `custom_checks.py` writes a schema reference JSON file to `<base-path>/<layer>_schema_ref.json`. Subsequent runs diff against this baseline to detect drift.

**What "schema drift" means:**

- **Added columns** — new columns appeared in the data (warn: may be expected)
- **Removed columns** — existing columns disappeared (error: likely a breaking upstream change)

### 9.3 Reading the audit log

All check results (from both GE and custom checks) are appended to NDJSON files:

```
/data/quality_audit/
  └── <layer>/
        └── <YYYY-MM-DD>.ndjson
```

Each record:

```json
{
  "check_name": "null_rate.id",
  "passed": true,
  "actual": "0.0000",
  "expected": "<= 0.02",
  "message": "'id' null rate OK (0.00%)",
  "layer": "silver",
  "execution_date": "2024-03-15",
  "pipeline_run_id": "a1b2c3d4-...",
  "checked_at": "2024-03-15T06:01:23Z"
}
```

**Query the audit log:**

```bash
# All failures for a given date
grep '"passed": false' /data/quality_audit/silver/2024-03-15.ndjson | python3 -m json.tool

# Count checks by layer
for f in /data/quality_audit/**/*.ndjson; do
  echo "$f: $(wc -l < "$f") records"
done
```

---

## 10. Running Tests

### 10.1 Scala unit tests

Spark tests run in `local[*]` mode with `parallelExecution := false` to avoid SparkContext conflicts.

```bash
# All Scala tests
cd spark-jobs && sbt test

# One specific test class
cd spark-jobs && sbt "testOnly com.pipeline.bronze.RawIngestJobSpec"

# One specific test method
cd spark-jobs && sbt "testOnly com.pipeline.silver.CleanseTransformJobSpec -- -t 'deduplicate should remove rows'"

# Continuous test-on-save (press Enter to re-run)
cd spark-jobs && sbt "~testQuick"
```

**Test classes and what they cover:**

| Class | Key assertions |
|-------|---------------|
| `RawIngestJobSpec` | Metadata columns added, CSV read, Parquet roundtrip, unsupported format throws |
| `CleanseTransformJobSpec` | Type casting, deduplication, null-id removal, negative-amount removal |
| `AggregateEnrichJobSpec` | Daily sums correct, active_rate calculation, unique entity count |

### 10.2 Python integration tests

The e2e tests write a synthetic CSV, run all three Spark jobs via `spark-submit` subprocess, then read back the results using PySpark.

**Prerequisite:** the fat-jar must exist (`make build`).

```bash
# All integration tests
pytest tests/integration/ -v

# One test class
pytest tests/integration/test_pipeline_e2e.py::TestSilverTransform -v

# One test method
pytest tests/integration/test_pipeline_e2e.py::TestSilverTransform::test_silver_removes_duplicates -v

# With captured output shown (useful for debugging)
pytest tests/integration/ -v -s
```

**What the e2e test validates:**

| Class | Validates |
|-------|-----------|
| `TestBronzeIngest` | Job exits 0, date partition directory created, ≥ 1 Parquet file present |
| `TestSilverTransform` | Job exits 0, duplicate row for `id=3` removed, row with `amount=-10` removed |
| `TestGoldAggregate` | Job exits 0, `daily_summary` non-empty, `total_records` and `total_amount` columns present |

### 10.3 Linting

```bash
# Python (PEP8) and Shell (shellcheck) in one command
make lint

# Python only
flake8 airflow/dags airflow/plugins data_quality tests

# Shell only
shellcheck ingestion/scripts/*.sh scripts/*.sh
```

---

## 11. Backfilling Historical Dates

All three DAGs have `catchup=True`, meaning Airflow will automatically schedule and run all missed intervals between `start_date` and today when the DAG is first unpaused.

### Controlled backfill via CLI

```bash
# Backfill ingestion for a date range
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow dags backfill ingestion_pipeline \
    --start-date 2024-03-01 \
    --end-date   2024-03-14 \
    --reset-dagruns

# Backfill transforms for the same range
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
  airflow dags backfill spark_transform_pipeline \
    --start-date 2024-03-01 \
    --end-date   2024-03-14
```

### Via make (single date)

```bash
for d in 2024-03-01 2024-03-02 2024-03-03; do
  make transform EXECUTION_DATE="$d"
done
```

### Idempotency guarantee

Every write in the pipeline uses `SaveMode.Overwrite` with `partitionBy(year, month, day)`. Re-running the same date **replaces** only that partition — other partitions are untouched. Re-runs are therefore always safe.

---

## 12. Health Checks and Monitoring

### Pipeline health check script

```bash
bash scripts/health_check.sh
```

The script checks:
- Airflow REST API responds with HTTP 200
- All registered DAGs are listed (reports paused vs active)
- Spark Master UI responds with HTTP 200
- All four data directories exist

**Override default URLs:**

```bash
AIRFLOW_URL=http://myairflow:8080 \
SPARK_MASTER_UI=http://myspark:8081 \
AIRFLOW_ADMIN_USER=myuser \
AIRFLOW_ADMIN_PASSWORD=mypass \
bash scripts/health_check.sh
```

### Spark job logs

All `spark-submit` calls tee logs to `/var/log/pipeline/spark_submit.log`. View with:

```bash
tail -100 /var/log/pipeline/spark_submit.log
```

### Ingestion logs

```bash
tail -f /var/log/pipeline/ingest_sftp.log
tail -f /var/log/pipeline/ingest_api.log
tail -f /var/log/pipeline/ingest_db_export.log
```

### Container logs

```bash
# All containers
make docker-logs

# Specific service
docker compose -f docker/docker-compose.yml logs -f airflow-scheduler
docker compose -f docker/docker-compose.yml logs -f spark-master
docker compose -f docker/docker-compose.yml logs -f airflow-worker
```

### Spark UI (live job progress)

Open http://localhost:8081 to see:
- Active and completed applications
- Executor allocation
- Stage-level task breakdowns
- SQL query plans (click "SQL" tab while a job is running)

---

## 13. Environment Promotion (dev → staging → prod)

### Configuration differences by environment

| Setting | dev | staging | prod |
|---------|-----|---------|------|
| Spark master | `local[*]` | `${SPARK_MASTER_URL}` | `${SPARK_MASTER_URL}` |
| Shuffle partitions | 4 | 100 | 200 |
| Max null rate (Silver) | 5% | 2% | 1% |
| Min row count (Silver) | 1 | 100 | 1,000 |
| Dynamic allocation | false | true | true |

### Running in staging or prod

1. **Set `PIPELINE_ENV`** in the environment or `.env`:

   ```bash
   export PIPELINE_ENV=staging
   ```

2. **Set all required env vars** (paths, Spark master URL, credentials) — no defaults exist for non-dev environments.

3. **Deploy the fat-jar** to the cluster's shared storage:

   ```bash
   # HDFS
   hdfs dfs -put spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-1.0.0.jar \
     hdfs:///pipeline/jars/

   # S3
   aws s3 cp spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-1.0.0.jar \
     s3://my-pipeline-bucket/jars/
   ```

4. **Update `JAR` in `spark_transform_dag.py`** to point to the cluster path:

   ```python
   JAR = "hdfs:///pipeline/jars/data-pipeline-platform-assembly-1.0.0.jar"
   ```

5. **Set `data_interval_start` paths to cloud storage** in Airflow Variables:

   ```
   BRONZE_BASE_PATH  →  s3a://my-bucket/bronze
   SILVER_BASE_PATH  →  s3a://my-bucket/silver
   GOLD_BASE_PATH    →  s3a://my-bucket/gold
   ```

6. **Switch the secret backend** (Vault / AWS SSM):

   ```bash
   export SECRET_BACKEND=ssm
   export AWS_REGION=us-east-1
   export AWS_SSM_PREFIX=/pipeline/prod
   ```

---

## 14. Troubleshooting

### `bootstrap.sh` fails: "X is not installed or not on PATH"

Install the missing tool (see [§1 Prerequisites](#1-prerequisites)) and re-run:

```bash
bash scripts/bootstrap.sh
```

### `sbt assembly` fails with "out of memory"

Increase the JVM heap used by sbt:

```bash
export SBT_OPTS="-Xmx4g -Xms1g -XX:+UseG1GC"
cd spark-jobs && sbt assembly
```

### Docker containers are restarting

```bash
# Identify which container is failing
docker compose -f docker/docker-compose.yml ps

# Read its logs
docker compose -f docker/docker-compose.yml logs <service-name>
```

**Most common causes:**

| Symptom | Cause | Fix |
|---------|-------|-----|
| `airflow-webserver` exits with "FERNET_KEY not set" | `AIRFLOW__CORE__FERNET_KEY` is blank in `.env` | Generate and fill in the Fernet key (see §3.2) |
| `postgres` exits immediately | Port 5432 already in use on host | `sudo lsof -i :5432` then kill the conflicting process |
| `spark-worker` keeps restarting | Not enough RAM for 2 workers × 2 GB | Reduce `SPARK_WORKER_MEMORY` to `1g` in docker-compose.yml |

### Airflow DAG shows "No module named ..." in task logs

The Python package is installed on the host but not inside the Airflow container. Add it to `airflow/requirements.txt` and rebuild:

```bash
docker compose -f docker/docker-compose.yml build airflow-webserver airflow-scheduler airflow-worker
make docker-down && make docker-up
```

### Spark job fails with "FileNotFoundException" on input path

The landing zone or Bronze path doesn't exist or is empty for the given date. Verify:

```bash
# Check landing zone
find /data/landing -type f | head -20

# Check Bronze partition
find /data/bronze/year=2024/month=3/day=15 -type f 2>/dev/null || echo "Partition missing"
```

Then re-run ingestion or the preceding layer before retrying the failed job.

### Spark job fails with "java.lang.OutOfMemoryError: Java heap space"

The executor doesn't have enough memory for the data volume. Increase executor memory:

```bash
bash scripts/submit_spark_job.sh \
  ... \
  --extra-conf spark.executor.memory=4g \
  --extra-conf spark.driver.memory=2g
```

Or reduce the partition size by increasing `SPARK_SHUFFLE_PARTITIONS`:

```bash
export SPARK_SHUFFLE_PARTITIONS=400
make spark-silver EXECUTION_DATE=2024-03-15
```

### Quality check fails: "Partition missing"

The Gold or Silver write did not complete successfully. Check the Spark job logs:

```bash
tail -200 /var/log/pipeline/spark_submit.log | grep -E "ERROR|WARN|Exception"
```

Re-run the failing layer, then re-run quality checks.

### `airflow dags backfill` is very slow

By default, backfill runs DAG runs serially. Parallelise with:

```bash
airflow dags backfill spark_transform_pipeline \
  --start-date 2024-01-01 \
  --end-date   2024-03-15 \
  --max-active-runs 4
```

### GE validation fails with "No module named 'great_expectations'"

```bash
pip install great-expectations==0.18.9
```

If running inside a container, add it to `airflow/requirements.txt` and rebuild.

---

## 15. Makefile Target Reference

Run `make` (no arguments) to see all targets with descriptions.

| Target | What it does |
|--------|-------------|
| `make build` | `sbt clean assembly` — produces the fat-jar |
| `make test` | Scala unit tests + Python integration tests |
| `make lint` | `flake8` on Python, `shellcheck` on shell scripts |
| `make clean` | Removes `target/`, `__pycache__/`, `.pyc` files |
| `make docker-up` | Starts all 7 containers in detached mode |
| `make docker-down` | Stops containers and removes volumes |
| `make docker-logs` | Tails logs from all containers |
| `make bootstrap` | One-shot environment setup |
| `make ingest` | Runs SFTP and API shell ingestion scripts |
| `make spark-bronze` | Rebuilds jar + submits `RawIngestJob` |
| `make spark-silver` | Rebuilds jar + submits `CleanseTransformJob` |
| `make spark-gold` | Rebuilds jar + submits `AggregateEnrichJob` |
| `make transform` | All three Spark layers in sequence |
| `make quality` | GE checkpoints for Silver and Gold |

**Makefile variables (override on the command line):**

| Variable | Default | Example |
|----------|---------|---------|
| `ENV` | `dev` | `make transform ENV=prod` |
| `EXECUTION_DATE` | today | `make transform EXECUTION_DATE=2024-03-15` |
| `SPARK_MASTER` | `local[*]` | `make spark-bronze SPARK_MASTER=spark://host:7077` |

---

## 16. Environment Variable Reference

All variables are defined in `.env.example`. Copy to `.env` and fill in values.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PIPELINE_ENV` | Yes | `dev` | Active environment passed to `ConfigLoader` |
| `AIRFLOW__CORE__FERNET_KEY` | Yes | — | Airflow encryption key for connections |
| `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | Yes | postgres URL | Airflow metadata DB connection |
| `AIRFLOW__CELERY__RESULT_BACKEND` | Yes | postgres URL | Celery result backend |
| `AIRFLOW__CELERY__BROKER_URL` | Yes | redis URL | Celery broker |
| `AIRFLOW_UID` | Yes | `50000` | Linux UID for Airflow processes |
| `SPARK_MASTER_URL` | staging/prod | `local[*]` | Spark master endpoint |
| `SPARK_SHUFFLE_PARTITIONS` | No | `200` | `spark.sql.shuffle.partitions` |
| `SPARK_EXECUTOR_MEMORY` | No | `2g` | Executor JVM heap |
| `SPARK_DRIVER_MEMORY` | No | `1g` | Driver JVM heap |
| `SPARK_EXECUTOR_CORES` | No | `2` | Cores per executor |
| `INGESTION_LANDING_PATH` | Yes | `/data/landing` | Shell ingestion drop zone |
| `BRONZE_BASE_PATH` | Yes | `/data/bronze` | Bronze Parquet root |
| `SILVER_BASE_PATH` | Yes | `/data/silver` | Silver Parquet root |
| `GOLD_BASE_PATH` | Yes | `/data/gold` | Gold Parquet root |
| `QUALITY_AUDIT_PATH` | No | `/data/quality_audit` | Quality check audit log root |
| `SFTP_HOST` | SFTP only | — | SFTP server hostname |
| `SFTP_PORT` | No | `22` | SFTP port |
| `SFTP_USER` | SFTP only | — | SFTP username |
| `SFTP_PASSWORD` | SFTP only | — | SFTP password |
| `SFTP_REMOTE_DIR` | No | `/exports` | Remote directory to pull from |
| `API_BASE_URL` | API only | — | REST API base URL |
| `API_KEY` | API only | — | API bearer token |
| `API_PAGE_SIZE` | No | `500` | Records per API page |
| `API_MAX_PAGES` | No | `100` | Max pages to fetch |
| `DB_EXPORT_JDBC_URL` | DB only | — | JDBC connection string |
| `DB_EXPORT_USER` | DB only | — | DB username |
| `DB_EXPORT_PASSWORD` | DB only | — | DB password |
| `DB_EXPORT_TABLE` | No | `events` | Table to export |
| `DB_EXPORT_DATE_COL` | No | `created_at` | Date filter column |
| `JDBC_TOOL` | No | `spark` | `spark` \| `mysqldump` |
| `ALERT_EMAIL` | No | `data-eng@company.com` | Failure notification address |
| `SECRET_BACKEND` | No | `env` | `env` \| `vault` \| `ssm` |
| `VAULT_ADDR` | vault only | — | HashiCorp Vault address |
| `VAULT_TOKEN` | vault only | — | Vault authentication token |
| `AWS_REGION` | ssm only | `us-east-1` | AWS region for SSM |
| `AWS_SSM_PREFIX` | ssm only | `/pipeline/prod` | SSM parameter path prefix |

---

*End of Runbook*
