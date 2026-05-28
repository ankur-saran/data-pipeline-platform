# data-pipeline-platform

Production-ready data processing platform implementing the **medallion (Bronze → Silver → Gold)** architecture, orchestrated by Apache Airflow and processed by Apache Spark.

---

## Architecture Overview

```
[Source Systems]
   SFTP / REST API / JDBC DB
         │
         ▼  (Shell Ingestion)
   ┌─────────────┐
   │  Landing    │  Raw files, date-partitioned
   └──────┬──────┘
          │  BashOperator (ingestion_dag)
          ▼
   ┌─────────────┐
   │   Bronze    │  Schema-on-read Parquet, metadata columns attached
   └──────┬──────┘
          │  SparkSubmitOperator (spark_transform_dag)
          ▼
   ┌─────────────┐
   │   Silver    │  Typed, deduplicated, business rules applied
   └──────┬──────┘
          │  SparkSubmitOperator
          ▼
   ┌─────────────┐
   │    Gold     │  Aggregates, KPIs, ML-ready feature tables
   └──────┬──────┘
          │  GE + custom checks (data_quality_dag)
          ▼
   ┌─────────────┐
   │   Serving   │  BI tools, ML pipelines, APIs
   └─────────────┘
```

### DAG Schedule

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `ingestion_pipeline` | `0 2 * * *` | Pull raw data from all sources |
| `spark_transform_pipeline` | `0 4 * * *` | Bronze → Silver → Gold |
| `data_quality_pipeline` | `0 6 * * *` | GE checkpoints + custom checks |

---

## Quick Start

### Prerequisites

| Tool | Minimum Version |
|------|----------------|
| Java | 11 |
| Python | 3.10 |
| sbt | 1.9 |
| Docker | 24 |
| docker compose | v2 |

### 1. Bootstrap local environment

```bash
# Clone and enter the project
cd data-pipeline-platform

# One-shot setup: installs Python deps, builds fat-jar, creates .env
bash scripts/bootstrap.sh
```

Review `.env` and fill in any source-system credentials.

### 2. Start the local stack

```bash
make docker-up
```

- Airflow UI → http://localhost:8080 (admin / admin)
- Spark UI   → http://localhost:8081

### 3. Run each pipeline layer

```bash
# Ingest raw data
make ingest

# Full transform: Bronze → Silver → Gold
make transform ENV=dev EXECUTION_DATE=2024-03-15

# Quality checks
make quality EXECUTION_DATE=2024-03-15
```

Or trigger individually:

```bash
make spark-bronze ENV=dev EXECUTION_DATE=2024-03-15
make spark-silver ENV=dev EXECUTION_DATE=2024-03-15
make spark-gold   ENV=dev EXECUTION_DATE=2024-03-15
```

### 4. Run tests

```bash
# All tests (Scala + Python integration)
make test

# Scala unit tests only
cd spark-jobs && sbt test

# Python integration tests only (requires PySpark installed)
pytest tests/integration/ -v
```

---

## Project Structure

```
data-pipeline-platform/
├── airflow/               Airflow DAGs, plugins, config
│   ├── dags/
│   │   ├── ingestion_dag.py        Shell-based raw ingest
│   │   ├── spark_transform_dag.py  Bronze→Silver→Gold Spark jobs
│   │   └── data_quality_dag.py     GE + custom quality checks
│   └── plugins/hooks/custom_spark_hook.py
│
├── spark-jobs/            Scala/Spark processing code
│   ├── build.sbt
│   └── src/main/scala/com/pipeline/
│       ├── bronze/RawIngestJob.scala
│       ├── silver/CleanseTransformJob.scala
│       ├── gold/AggregateEnrichJob.scala
│       └── common/   SparkSessionFactory, ConfigLoader, DataQualityUtils
│
├── ingestion/scripts/     POSIX Bash ingest scripts
├── data_quality/          GE expectation suites + custom checks
├── config/                HOCON per-environment configs
├── docker/                Dockerfiles + docker-compose
├── scripts/               bootstrap, submit_spark_job, health_check
└── tests/integration/     End-to-end DAG run validation
```

---

## How to Add a New Data Source

1. **Define the source** in [`ingestion/config/sources.yaml`](ingestion/config/sources.yaml)

2. **Add an ingestion script** in `ingestion/scripts/` following the pattern of `ingest_sftp.sh`:
   - `set -euo pipefail`
   - Retry with exponential backoff
   - Log with timestamps to stdout + rotating log file
   - Exit codes: 0 / 1 (data) / 2 (infra)

3. **Add a BashOperator task** in [`airflow/dags/ingestion_dag.py`](airflow/dags/ingestion_dag.py) wired into the `landing_check` dependency.

4. **Add a source_system label** in your ingestion script so the Bronze → Silver join works.

5. **Extend GE expectations** in `data_quality/expectations/silver_expectations.json` if the new source introduces new columns.

6. **Add unit tests** in `spark-jobs/src/test/` covering any source-specific transform logic.

---

## Configuration

Configs are layered HOCON files:

```
spark-jobs/src/main/resources/application.conf  ← base defaults
config/<env>/application.conf                    ← env overrides
```

`ConfigLoader.load("prod")` merges both, with env-specific values winning.

**No credentials are ever stored in config files.** All secret keys use `${?ENV_VAR}` substitution syntax.

### Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PIPELINE_ENV` | Yes | `dev` | Active environment |
| `SPARK_MASTER_URL` | staging/prod | `local[*]` | Spark master endpoint |
| `SPARK_SHUFFLE_PARTITIONS` | No | `200` | spark.sql.shuffle.partitions |
| `SPARK_EXECUTOR_MEMORY` | No | `2g` | Executor heap per node |
| `SPARK_DRIVER_MEMORY` | No | `1g` | Driver heap |
| `BRONZE_BASE_PATH` | Yes | `/data/bronze` | Bronze layer root path |
| `SILVER_BASE_PATH` | Yes | `/data/silver` | Silver layer root path |
| `GOLD_BASE_PATH` | Yes | `/data/gold` | Gold layer root path |
| `INGESTION_LANDING_PATH` | Yes | `/data/landing` | Landing zone root |
| `SFTP_HOST` | SFTP source | — | SFTP server hostname |
| `SFTP_USER` | SFTP source | — | SFTP username |
| `SFTP_PASSWORD` | SFTP source | — | SFTP password |
| `API_BASE_URL` | API source | — | REST API base URL |
| `API_KEY` | API source | — | API authentication key |
| `DB_EXPORT_JDBC_URL` | DB source | — | JDBC connection string |
| `DB_EXPORT_USER` | DB source | — | DB username |
| `DB_EXPORT_PASSWORD` | DB source | — | DB password |
| `ALERT_EMAIL` | No | `data-eng@company.com` | Failure notification address |
| `AIRFLOW__CORE__FERNET_KEY` | Yes (Airflow) | — | Airflow encryption key |
| `QUALITY_AUDIT_PATH` | No | `/data/quality_audit` | Quality check audit log |

---

## Makefile Reference

```
make build          Compile and assemble Spark fat-jar
make test           Run all tests (Scala + Python)
make lint           Flake8 (Python) + shellcheck (shell)
make clean          Remove build artefacts
make docker-up      Start Airflow + Spark local cluster
make docker-down    Stop and remove containers
make docker-logs    Tail all container logs
make bootstrap      One-shot local environment setup
make ingest         Run shell-based ingestion
make spark-bronze   Submit Bronze Spark job
make spark-silver   Submit Silver Spark job
make spark-gold     Submit Gold Spark job
make transform      Full Bronze → Silver → Gold pipeline
make quality        Run Great Expectations + custom checks
```

---

## Data Quality

- **Great Expectations** checkpoints run after Silver and Gold writes via `data_quality_dag`.
- **Custom checks** (`custom_checks.py`) validate row counts, null rates, schema drift, and partition completeness.
- All check results are persisted to `QUALITY_AUDIT_PATH/<layer>/<YYYY-MM-DD>.ndjson` for lineage tracing.
- Failures raise `PipelineDataQualityError` which causes the Airflow task to fail and triggers the configured alert email.

---

## Secrets Management

| Environment | Backend | Configuration |
|-------------|---------|---------------|
| `dev` | `.env` file | `SECRET_BACKEND=env` |
| `staging` | AWS SSM Parameter Store | `SECRET_BACKEND=ssm` |
| `prod` | HashiCorp Vault | `SECRET_BACKEND=vault` |

The application reads secrets exclusively from environment variables. Swap the backend by setting `SECRET_BACKEND` and providing the corresponding `VAULT_*` or `AWS_SSM_*` variables — no code changes required.
