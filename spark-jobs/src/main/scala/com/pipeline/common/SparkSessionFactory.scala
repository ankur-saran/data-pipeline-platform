package com.pipeline.common

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager

/** Builds a SparkSession tuned for the target environment.
 *
 *  - local[*]  — unit / integration tests, developer workstations
 *  - yarn      — YARN-managed cluster (staging / prod on-prem)
 *  - k8s       — Kubernetes cluster (staging / prod cloud-native)
 */
object SparkSessionFactory {

  private val log = LogManager.getLogger(getClass)

  /** @param appName       Logical application name surfaced in the Spark UI.
   *  @param env           Runtime environment: dev | staging | prod.
   *  @param extraConfigs  Additional spark.* key→value pairs applied last.
   */
  def build(
    appName: String,
    env: String,
    extraConfigs: Map[String, String] = Map.empty
  ): SparkSession = {

    val master = env match {
      case "dev"            => "local[*]"
      case "staging" | "prod" => sys.env.getOrElse("SPARK_MASTER_URL", "yarn")
      case other            =>
        log.warn(s"Unknown env '$other', defaulting to local[*]")
        "local[*]"
    }

    log.info(s"Building SparkSession: app=$appName  env=$env  master=$master")

    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)
      // Hive support for metastore-backed tables
      .enableHiveSupport()
      // Baseline performance settings — overridden by extraConfigs
      .config("spark.sql.shuffle.partitions",
        sys.env.getOrElse("SPARK_SHUFFLE_PARTITIONS", "200"))
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Parquet optimisations
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.mergeSchema", "false")
      // Kryo serialisation for better performance
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512m")
      // Dynamic allocation for cluster modes
      .config("spark.dynamicAllocation.enabled",
        if (master == "local[*]") "false" else "true")
      .config("spark.dynamicAllocation.minExecutors", "1")
      .config("spark.dynamicAllocation.maxExecutors", "20")

    val builderWithExtras = extraConfigs.foldLeft(builder) {
      case (b, (k, v)) => b.config(k, v)
    }

    val spark = if (master == "local[*]") {
      // Disable Hive in local mode to avoid metastore dependency
      SparkSession.builder()
        .appName(appName)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    } else {
      builderWithExtras.getOrCreate()
    }

    spark.sparkContext.setLogLevel("WARN")
    log.info(s"SparkSession started — Spark ${spark.version}")
    spark
  }

  def stop(spark: SparkSession): Unit = {
    log.info("Stopping SparkSession")
    spark.stop()
  }
}
