package com.pipeline.bronze

import com.pipeline.common.{ConfigLoader, DataQualityUtils, SparkSessionFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager
import scopt.OParser

import java.time.{Instant, LocalDate}
import java.util.UUID

/** Bronze (Raw Ingest) job.
 *
 *  Reads raw files from the landing zone, attaches pipeline metadata columns,
 *  and writes them to the Bronze layer partitioned by (year, month, day).
 *  No business-logic transforms — schema-on-read, data immutable.
 */
object RawIngestJob {

  private val log = LogManager.getLogger(getClass)

  case class Args(
    env: String           = "dev",
    executionDate: String = LocalDate.now().toString,
    inputPath: String     = "",
    outputPath: String    = "",
    sourceSystem: String  = "unknown",
    format: String        = "csv"
  )

  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)
    val pipelineRunId = UUID.randomUUID().toString

    log.info(s"Starting RawIngestJob — env=${args.env}  runId=$pipelineRunId  date=${args.executionDate}")

    val config = ConfigLoader.load(args.env)
    implicit val spark: SparkSession = SparkSessionFactory.build("RawIngestJob", args.env)

    try {
      val raw = readRaw(spark, args.inputPath, args.format)
      val enriched = attachMetadata(raw, pipelineRunId, args.sourceSystem, args.executionDate)
      writeOutput(enriched, args.outputPath, args.executionDate)

      val qcResults = DataQualityUtils.checkNullRate(enriched,
        Seq("pipeline_run_id", "ingested_at", "source_system"), maxNullRate = 0.0)
      DataQualityUtils.assertAllPassed(qcResults)

      val auditPath = config.getString("pipeline.quality.audit_path")
      DataQualityUtils.persistAuditResults(qcResults, auditPath, pipelineRunId, "bronze")

      log.info("RawIngestJob completed successfully")
    } finally {
      SparkSessionFactory.stop(spark)
    }
  }

  def readRaw(spark: SparkSession, inputPath: String, format: String): DataFrame = {
    log.info(s"Reading $format files from $inputPath")
    format.toLowerCase match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("multiLine", "true")
          .option("escape", "\"")
          .csv(inputPath)
      case "json" =>
        spark.read.option("multiLine", "false").json(inputPath)
      case "parquet" =>
        spark.read.parquet(inputPath)
      case other =>
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }
  }

  def attachMetadata(
    df: DataFrame,
    pipelineRunId: String,
    sourceSystem: String,
    executionDate: String
  ): DataFrame = {
    val date    = LocalDate.parse(executionDate)
    df
      .withColumn("pipeline_run_id", lit(pipelineRunId))
      .withColumn("ingested_at",     lit(Instant.now().toString))
      .withColumn("source_system",   lit(sourceSystem))
      .withColumn("year",            lit(date.getYear))
      .withColumn("month",           lit(date.getMonthValue))
      .withColumn("day",             lit(date.getDayOfMonth))
  }

  def writeOutput(df: DataFrame, outputPath: String, executionDate: String): Unit = {
    log.info(s"Writing Bronze output to $outputPath")
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .parquet(outputPath)
    log.info(s"Bronze write complete: $outputPath")
  }

  private def parseArgs(argv: Array[String]): Args = {
    val builder = OParser.builder[Args]
    val parser = {
      import builder._
      OParser.sequence(
        programName("RawIngestJob"),
        opt[String]("env").action((v, c) => c.copy(env = v)),
        opt[String]("execution-date").action((v, c) => c.copy(executionDate = v)),
        opt[String]("input-path").required().action((v, c) => c.copy(inputPath = v)),
        opt[String]("output-path").required().action((v, c) => c.copy(outputPath = v)),
        opt[String]("source-system").action((v, c) => c.copy(sourceSystem = v)),
        opt[String]("format").action((v, c) => c.copy(format = v))
      )
    }
    OParser.parse(parser, argv, Args()).getOrElse {
      System.exit(1)
      Args()
    }
  }
}
