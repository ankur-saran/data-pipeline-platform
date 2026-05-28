"""
custom_spark_hook.py — Reusable SparkSubmit wrapper hook

Wraps Airflow's SparkSubmitHook to enforce pipeline-standard defaults:
  - Mandatory conf keys (shuffle partitions, app name tagging)
  - Fat-jar location resolution from Airflow Variable
  - Structured logging of submission parameters
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

log = logging.getLogger(__name__)

_DEFAULT_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}


class PipelineSparkHook(SparkSubmitHook):
    """Thin extension of SparkSubmitHook that merges pipeline-standard Spark conf.

    Args:
        application:        Path to the fat-jar (overrides parent's ``application``).
        java_class:         Fully-qualified main class.
        application_args:   CLI args forwarded to the Spark job's ``main()``.
        pipeline_env:       Pipeline environment: dev | staging | prod.
        shuffle_partitions: spark.sql.shuffle.partitions value.
        extra_conf:         Additional spark.* settings (merged last; highest priority).
        conn_id:            Airflow connection ID for the Spark cluster.
    """

    def __init__(
        self,
        application: str,
        java_class: str,
        application_args: list[str] | None = None,
        pipeline_env: str = "dev",
        shuffle_partitions: int = 200,
        extra_conf: dict[str, str] | None = None,
        conn_id: str = "spark_default",
        **kwargs: Any,
    ) -> None:
        merged_conf: dict[str, str] = {
            **_DEFAULT_CONF,
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.app.name": java_class.split(".")[-1],
            **(extra_conf or {}),
        }

        super().__init__(
            application=application,
            java_class=java_class,
            application_args=application_args or [],
            conf=merged_conf,
            conn_id=conn_id,
            **kwargs,
        )
        self._pipeline_env = pipeline_env

    def submit(self, application: str = "", **kwargs: Any) -> None:  # type: ignore[override]
        log.info(
            "PipelineSparkHook.submit — class=%s  env=%s  conf=%s",
            self._java_class,
            self._pipeline_env,
            self._conf,
        )
        super().submit(application=application or self._application, **kwargs)
