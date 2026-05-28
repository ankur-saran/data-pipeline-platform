package com.pipeline.silver

import com.pipeline.common.{ConfigLoader, DataQualityUtils, SparkSessionFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.logging.log4j.LogManager
import scopt.OParser

import java.time.{Instant, LocalDate}
import java.util.UUID

/** Silver (Cleanse & Transform) job.
 *
 *  Reads Bronze Parquet, applies:
 *    - Explicit type casting
 *    - Deduplication on business key
 *    - Null / dirty-data removal per thresholds
 *    - Enrichment join with dimension tables
 *
 *  Writes to Silver partitioned by (year, month, day).
 */
object CleanseTransformJob {

  private val log = LogManager.getLogger(getClass)

  case class Args(
    env: String           = "dev",
    executionDate: String = LocalDate.now().toString,
    inputPath: String     = "",
    outputPath: String    = "",
    dimPath: String       = ""
  )

  def main(argv: Array[String]): Unit = {
    val args          = parseArgs(argv)
    val pipelineRunId = UUID.randomUUID().toString

    log.info(s"Starting CleanseTransformJob — env=${args.env}  runId=$pipelineRunId  date=${args.executionDate}")

    val config = ConfigLoader.load(args.env)
    implicit val spark: SparkSession = SparkSessionFactory.build("CleanseTransformJob", args.env)

    try {
      val bronze   = readBronze(spark, args.inputPath, args.executionDate)
      val cast     = castTypes(bronze)
      val deduped  = deduplicate(cast)
      val filtered = applyBusinessRules(deduped)
      val enriched = if (args.dimPath.nonEmpty) enrichWithDimensions(filtered, args.dimPath, spark)
                     else filtered

      val maxNullRate = config.getDouble("pipeline.quality.max_null_rate_silver")
      val criticalCols = Seq("id", "event_date", "source_system")

      val qcResults = DataQualityUtils.checkNullRate(enriched, criticalCols, maxNullRate) ++
        Seq(DataQualityUtils.checkRowCount(enriched, minRows = 1, label = "silver"))
      DataQualityUtils.assertAllPassed(qcResults)

      writeOutput(enriched, args.outputPath, args.executionDate)

      val auditPath = config.getString("pipeline.quality.audit_path")
      DataQualityUtils.persistAuditResults(qcResults, auditPath, pipelineRunId, "silver")

      log.info("CleanseTransformJob completed successfully")
    } finally {
      SparkSessionFactory.stop(spark)
    }
  }

  def readBronze(spark: SparkSession, inputPath: String, executionDate: String): DataFrame = {
    val date = LocalDate.parse(executionDate)
    spark.read.parquet(inputPath)
      .filter(
        col("year")  === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day")   === date.getDayOfMonth
      )
  }

  def castTypes(df: DataFrame): DataFrame = {
    // Casting assumes a generic events schema; adjust for your domain.
    val schemaToCast = Map(
      "id"           -> LongType,
      "amount"       -> DoubleType,
      "event_date"   -> DateType,
      "created_at"   -> TimestampType,
      "is_active"    -> BooleanType
    )

    schemaToCast.foldLeft(df) { case (acc, (colName, targetType)) =>
      if (acc.columns.contains(colName))
        acc.withColumn(colName, col(colName).cast(targetType))
      else acc
    }
  }

  def deduplicate(df: DataFrame): DataFrame = {
    val businessKey = Seq("id", "event_date", "source_system")
    val existingKey = businessKey.filter(df.columns.contains)

    if (existingKey.isEmpty) {
      log.warn("No business-key columns found for deduplication — returning as-is")
      df
    } else {
      val before = df.count()
      val deduped = df.dropDuplicates(existingKey)
      val after   = deduped.count()
      log.info(s"Deduplication: $before rows → $after rows (${before - after} removed)")
      deduped
    }
  }

  def applyBusinessRules(df: DataFrame): DataFrame = {
    // Drop rows where critical identifier columns are null
    val criticalNotNull = Seq("id", "source_system").foldLeft(df) { (acc, c) =>
      if (acc.columns.contains(c)) acc.filter(col(c).isNotNull) else acc
    }

    // Drop rows with negative amounts (adjust per your domain rules)
    if (criticalNotNull.columns.contains("amount"))
      criticalNotNull.filter(col("amount") >= 0)
    else criticalNotNull
  }

  def enrichWithDimensions(df: DataFrame, dimPath: String, spark: SparkSession): DataFrame = {
    log.info(s"Joining with dimension tables from $dimPath")
    // Load dimension: expects columns [dim_id, dim_label, ...]
    val dim = spark.read.parquet(s"$dimPath/dim_source")
      .select(col("source_system").alias("dim_source_system"), col("source_label"))

    df.join(broadcast(dim), df("source_system") === dim("dim_source_system"), "left")
      .drop("dim_source_system")
  }

  def writeOutput(df: DataFrame, outputPath: String, executionDate: String): Unit = {
    log.info(s"Writing Silver output to $outputPath")
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .parquet(outputPath)
    log.info(s"Silver write complete: $outputPath")
  }

  private def parseArgs(argv: Array[String]): Args = {
    val builder = OParser.builder[Args]
    val parser = {
      import builder._
      OParser.sequence(
        programName("CleanseTransformJob"),
        opt[String]("env").action((v, c) => c.copy(env = v)),
        opt[String]("execution-date").action((v, c) => c.copy(executionDate = v)),
        opt[String]("input-path").required().action((v, c) => c.copy(inputPath = v)),
        opt[String]("output-path").required().action((v, c) => c.copy(outputPath = v)),
        opt[String]("dim-path").action((v, c) => c.copy(dimPath = v))
      )
    }
    OParser.parse(parser, argv, Args()).getOrElse {
      System.exit(1)
      Args()
    }
  }
}
