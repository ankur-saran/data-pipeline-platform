# =============================================================================
# data-pipeline-platform — Makefile
# Usage: make <target>
# =============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

ENV            ?= dev
EXECUTION_DATE ?= $(shell date +%Y-%m-%d)
SPARK_MASTER   ?= local[*]
JAR_PATH       := spark-jobs/target/scala-2.12/data-pipeline-platform-assembly-1.0.0.jar

# Colours
CYAN  := \033[36m
RESET := \033[0m

.PHONY: help build test clean \
        docker-up docker-down docker-logs \
        ingest transform quality \
        spark-bronze spark-silver spark-gold \
        bootstrap lint

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "$(CYAN)%-20s$(RESET) %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
build: ## Compile and assemble the Spark fat-jar
	cd spark-jobs && sbt clean assembly

test: ## Run all tests (Scala + Python)
	cd spark-jobs && sbt test
	cd airflow && pip install -r requirements.txt -q && \
	  python -m pytest ../tests/ -v

lint: ## Lint Python (flake8) and Shell (shellcheck)
	flake8 airflow/dags airflow/plugins data_quality tests
	shellcheck ingestion/scripts/*.sh scripts/*.sh

clean: ## Remove build artefacts
	cd spark-jobs && sbt clean
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
docker-up: ## Start local Airflow + Spark cluster
	docker compose -f docker/docker-compose.yml up -d
	@echo "Airflow UI  → http://localhost:8080  (admin/admin)"
	@echo "Spark UI    → http://localhost:8081"

docker-down: ## Stop and remove containers
	docker compose -f docker/docker-compose.yml down -v

docker-logs: ## Tail all container logs
	docker compose -f docker/docker-compose.yml logs -f

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
bootstrap: ## One-shot local environment setup
	bash scripts/bootstrap.sh

# ---------------------------------------------------------------------------
# Pipeline layer targets
# ---------------------------------------------------------------------------
ingest: ## Run shell-based ingestion (SFTP + API)
	bash ingestion/scripts/ingest_sftp.sh
	bash ingestion/scripts/ingest_api.sh

spark-bronze: build ## Submit Bronze (raw ingest) Spark job
	bash scripts/submit_spark_job.sh \
	  --job-class com.pipeline.bronze.RawIngestJob \
	  --env $(ENV) \
	  --execution-date $(EXECUTION_DATE) \
	  --input-path $${INGESTION_LANDING_PATH} \
	  --output-path $${BRONZE_BASE_PATH}

spark-silver: build ## Submit Silver (cleanse/transform) Spark job
	bash scripts/submit_spark_job.sh \
	  --job-class com.pipeline.silver.CleanseTransformJob \
	  --env $(ENV) \
	  --execution-date $(EXECUTION_DATE) \
	  --input-path $${BRONZE_BASE_PATH} \
	  --output-path $${SILVER_BASE_PATH}

spark-gold: build ## Submit Gold (aggregate/enrich) Spark job
	bash scripts/submit_spark_job.sh \
	  --job-class com.pipeline.gold.AggregateEnrichJob \
	  --env $(ENV) \
	  --execution-date $(EXECUTION_DATE) \
	  --input-path $${SILVER_BASE_PATH} \
	  --output-path $${GOLD_BASE_PATH}

transform: spark-bronze spark-silver spark-gold ## Run full Bronze→Silver→Gold pipeline

quality: ## Run Great Expectations data quality checks
	python data_quality/validators/run_great_expectations.py \
	  --layer silver --execution-date $(EXECUTION_DATE)
	python data_quality/validators/run_great_expectations.py \
	  --layer gold   --execution-date $(EXECUTION_DATE)
