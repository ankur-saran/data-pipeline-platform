package com.pipeline.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.logging.log4j.LogManager

import java.time.Instant

/** Data quality helpers used inside Spark jobs.
 *
 *  All check results are returned as structured [[QualityResult]] objects so
 *  callers can decide whether to fail-fast or accumulate and log.
 */
object DataQualityUtils {

  private val log = LogManager.getLogger(getClass)

  /** Summary of a single quality check. */
  case class QualityResult(
    checkName: String,
    passed: Boolean,
    actual: String,
    expected: String,
    message: String,
    checkedAt: String = Instant.now().toString
  )

  /** Validates that every required column in `expectedSchema` is present in
   *  `df` and has a compatible data type.
   *
   *  @return QualityResult per mismatched / missing field.
   */
  def validateSchema(df: DataFrame, expectedSchema: StructType): Seq[QualityResult] = {
    val actualFieldMap = df.schema.fields.map(f => f.name.toLowerCase -> f.dataType).toMap

    expectedSchema.fields.map { expected =>
      val name = expected.name.toLowerCase
      actualFieldMap.get(name) match {
        case None =>
          QualityResult(
            checkName = s"schema.field_present.$name",
            passed    = false,
            actual    = "missing",
            expected  = expected.dataType.typeName,
            message   = s"Required column '$name' is absent from the DataFrame"
          )
        case Some(actualType) if actualType != expected.dataType =>
          QualityResult(
            checkName = s"schema.field_type.$name",
            passed    = false,
            actual    = actualType.typeName,
            expected  = expected.dataType.typeName,
            message   = s"Column '$name' has type '${actualType.typeName}', expected '${expected.dataType.typeName}'"
          )
        case _ =>
          QualityResult(
            checkName = s"schema.field_ok.$name",
            passed    = true,
            actual    = expected.dataType.typeName,
            expected  = expected.dataType.typeName,
            message   = s"Column '$name' OK"
          )
      }
    }.toSeq
  }

  /** Checks that the null rate of each column stays below `maxNullRate`.
   *
   *  @param maxNullRate  Maximum tolerated null fraction (0.0–1.0).
   */
  def checkNullRate(
    df: DataFrame,
    columns: Seq[String],
    maxNullRate: Double = 0.05
  ): Seq[QualityResult] = {
    val total = df.count()
    if (total == 0L) {
      log.warn("checkNullRate called on empty DataFrame — skipping")
      return Seq.empty
    }

    val nullCounts = df
      .select(columns.map(c => sum(col(c).isNull.cast("long")).alias(c)): _*)
      .head()

    columns.map { c =>
      val nullCount  = nullCounts.getAs[Long](c)
      val actualRate = nullCount.toDouble / total
      val passed     = actualRate <= maxNullRate
      if (!passed)
        log.warn(s"Null-rate check failed for '$c': ${actualRate * 100:.2f}% > ${maxNullRate * 100:.2f}%")
      QualityResult(
        checkName = s"null_rate.$c",
        passed    = passed,
        actual    = f"$actualRate%.4f",
        expected  = s"<= $maxNullRate",
        message   = if (passed) s"'$c' null rate OK"
                    else s"'$c' null rate ${actualRate * 100:.2f}% exceeds threshold ${maxNullRate * 100:.2f}%"
      )
    }
  }

  /** Verifies the row count is within [minRows, maxRows]. Pass Long.MaxValue
   *  to skip the upper bound, or 0 to skip the lower bound.
   */
  def checkRowCount(
    df: DataFrame,
    minRows: Long,
    maxRows: Long = Long.MaxValue,
    label: String = "dataset"
  ): QualityResult = {
    val count  = df.count()
    val passed = count >= minRows && count <= maxRows
    val range  = if (maxRows == Long.MaxValue) s">= $minRows" else s"[$minRows, $maxRows]"
    log.info(s"Row count check [$label]: count=$count  expected=$range  passed=$passed")
    QualityResult(
      checkName = s"row_count.$label",
      passed    = passed,
      actual    = count.toString,
      expected  = range,
      message   = if (passed) s"Row count $count within $range"
                  else s"Row count $count outside expected range $range"
    )
  }

  /** Persists check results to a Delta/Parquet audit table.
   *
   *  @param results        Check results to append.
   *  @param auditTablePath Absolute path (or table name) for the audit log.
   *  @param pipelineRunId  Unique identifier for this pipeline run.
   */
  def persistAuditResults(
    results: Seq[QualityResult],
    auditTablePath: String,
    pipelineRunId: String,
    layer: String
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val df = results.toDF()
      .withColumn("pipeline_run_id", lit(pipelineRunId))
      .withColumn("layer", lit(layer))
      .withColumn("written_at", lit(Instant.now().toString))

    df.write
      .mode("append")
      .partitionBy("layer")
      .parquet(auditTablePath)

    log.info(s"Persisted ${results.size} quality results to $auditTablePath")
  }

  /** Throws if any result in the collection is a failure. */
  def assertAllPassed(results: Seq[QualityResult]): Unit = {
    val failures = results.filterNot(_.passed)
    if (failures.nonEmpty) {
      val msg = failures.map(r => s"  [${r.checkName}] ${r.message}").mkString("\n")
      throw new PipelineDataQualityException(
        s"${failures.size} quality check(s) failed:\n$msg"
      )
    }
  }
}

class PipelineDataQualityException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
