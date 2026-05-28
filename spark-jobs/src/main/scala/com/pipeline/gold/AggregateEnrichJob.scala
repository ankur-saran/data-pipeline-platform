package com.pipeline.gold

import com.pipeline.common.{ConfigLoader, DataQualityUtils, SparkSessionFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager
import scopt.OParser

import java.time.{Instant, LocalDate}
import java.util.UUID

/** Gold (Aggregate & Enrich) job.
 *
 *  Reads Silver data, computes business-level aggregates and KPIs, writes
 *  summary tables to the Gold layer. Also emits a metrics log as JSON Lines.
 */
object AggregateEnrichJob {

  private val log = LogManager.getLogger(getClass)

  case class Args(
    env: String           = "dev",
    executionDate: String = LocalDate.now().toString,
    inputPath: String     = "",
    outputPath: String    = "",
    metricsPath: String   = ""
  )

  def main(argv: Array[String]): Unit = {
    val args          = parseArgs(argv)
    val pipelineRunId = UUID.randomUUID().toString

    log.info(s"Starting AggregateEnrichJob — env=${args.env}  runId=$pipelineRunId  date=${args.executionDate}")

    val config = ConfigLoader.load(args.env)
    implicit val spark: SparkSession = SparkSessionFactory.build("AggregateEnrichJob", args.env)

    try {
      val silver   = readSilver(spark, args.inputPath, args.executionDate)
      val daily    = computeDailyAggregates(silver, args.executionDate)
      val kpis     = computeKpis(silver, args.executionDate)

      val qcResults = Seq(
        DataQualityUtils.checkRowCount(daily, minRows = 1, label = "gold_daily"),
        DataQualityUtils.checkRowCount(kpis,  minRows = 1, label = "gold_kpis")
      )
      DataQualityUtils.assertAllPassed(qcResults)

      writeOutput(daily, s"${args.outputPath}/daily_summary",  args.executionDate)
      writeOutput(kpis,  s"${args.outputPath}/kpi_summary",    args.executionDate)

      if (args.metricsPath.nonEmpty)
        emitMetricsLog(kpis, args.metricsPath, pipelineRunId, args.executionDate)

      val auditPath = config.getString("pipeline.quality.audit_path")
      DataQualityUtils.persistAuditResults(qcResults, auditPath, pipelineRunId, "gold")

      log.info("AggregateEnrichJob completed successfully")
    } finally {
      SparkSessionFactory.stop(spark)
    }
  }

  def readSilver(spark: SparkSession, inputPath: String, executionDate: String): DataFrame = {
    val date = LocalDate.parse(executionDate)
    spark.read.parquet(inputPath)
      .filter(
        col("year")  === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day")   === date.getDayOfMonth
      )
  }

  def computeDailyAggregates(df: DataFrame, executionDate: String): DataFrame = {
    val date = LocalDate.parse(executionDate)
    df
      .groupBy("source_system", "year", "month", "day")
      .agg(
        count("*").alias("total_records"),
        countDistinct("id").alias("distinct_ids"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount"),
        min("amount").alias("min_amount")
      )
      .withColumn("computed_at", lit(Instant.now().toString))
  }

  def computeKpis(df: DataFrame, executionDate: String): DataFrame = {
    df
      .groupBy("source_system")
      .agg(
        count("*").alias("record_count"),
        countDistinct("id").alias("unique_entities"),
        sum("amount").alias("total_value"),
        avg("amount").alias("avg_value"),
        (sum(when(col("is_active") === true, 1).otherwise(0)).cast("double") /
          count("*").cast("double")).alias("active_rate")
      )
      .withColumn("execution_date", lit(executionDate))
      .withColumn("computed_at",    lit(Instant.now().toString))
  }

  def writeOutput(df: DataFrame, outputPath: String, executionDate: String): Unit = {
    log.info(s"Writing Gold output to $outputPath")
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .parquet(outputPath)
    log.info(s"Gold write complete: $outputPath")
  }

  /** Appends one JSON-Lines record per KPI row for downstream metric ingestion. */
  def emitMetricsLog(
    kpis: DataFrame,
    metricsPath: String,
    pipelineRunId: String,
    executionDate: String
  ): Unit = {
    kpis
      .withColumn("pipeline_run_id", lit(pipelineRunId))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .json(metricsPath)
    log.info(s"Metrics log written to $metricsPath")
  }

  private def parseArgs(argv: Array[String]): Args = {
    val builder = OParser.builder[Args]
    val parser = {
      import builder._
      OParser.sequence(
        programName("AggregateEnrichJob"),
        opt[String]("env").action((v, c) => c.copy(env = v)),
        opt[String]("execution-date").action((v, c) => c.copy(executionDate = v)),
        opt[String]("input-path").required().action((v, c) => c.copy(inputPath = v)),
        opt[String]("output-path").required().action((v, c) => c.copy(outputPath = v)),
        opt[String]("metrics-path").action((v, c) => c.copy(metricsPath = v))
      )
    }
    OParser.parse(parser, argv, Args()).getOrElse {
      System.exit(1)
      Args()
    }
  }
}
