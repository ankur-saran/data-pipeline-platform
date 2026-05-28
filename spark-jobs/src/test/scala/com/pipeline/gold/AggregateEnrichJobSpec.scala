package com.pipeline.gold

import com.pipeline.common.SparkSessionFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregateEnrichJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSessionFactory.build("AggregateEnrichJobSpec", "dev")

  override def afterAll(): Unit =
    SparkSessionFactory.stop(spark)

  val schema: StructType = StructType(Seq(
    StructField("id",            LongType),
    StructField("amount",        DoubleType),
    StructField("event_date",    DateType),
    StructField("source_system", StringType),
    StructField("is_active",     BooleanType),
    StructField("year",          IntegerType),
    StructField("month",         IntegerType),
    StructField("day",           IntegerType)
  ))

  private def date(s: String) = java.sql.Date.valueOf(s)

  private def silverRows = Seq(
    Row(1L, 100.0, date("2024-03-15"), "sftp", true,  2024, 3, 15),
    Row(2L, 200.0, date("2024-03-15"), "sftp", false, 2024, 3, 15),
    Row(3L,  50.0, date("2024-03-15"), "api",  true,  2024, 3, 15),
    Row(4L, 150.0, date("2024-03-15"), "api",  true,  2024, 3, 15)
  )

  // ---------------------------------------------------------------------------
  "computeDailyAggregates" should "group by source_system and compute correct sums" in {
    val df     = spark.createDataFrame(spark.sparkContext.parallelize(silverRows), schema)
    val result = AggregateEnrichJob.computeDailyAggregates(df, "2024-03-15")

    result.count() shouldBe 2

    val sftp = result.filter(result("source_system") === "sftp").head()
    sftp.getAs[Long]("total_records")   shouldBe 2
    sftp.getAs[Double]("total_amount")  shouldBe 300.0
    sftp.getAs[Double]("max_amount")    shouldBe 200.0
  }

  it should "include a computed_at timestamp column" in {
    val df     = spark.createDataFrame(spark.sparkContext.parallelize(silverRows), schema)
    val result = AggregateEnrichJob.computeDailyAggregates(df, "2024-03-15")
    result.columns should contain ("computed_at")
  }

  // ---------------------------------------------------------------------------
  "computeKpis" should "calculate active_rate correctly" in {
    val df     = spark.createDataFrame(spark.sparkContext.parallelize(silverRows), schema)
    val result = AggregateEnrichJob.computeKpis(df, "2024-03-15")

    val sftp = result.filter(result("source_system") === "sftp").head()
    // 1 of 2 sftp rows is active → 0.5
    sftp.getAs[Double]("active_rate") shouldBe 0.5 +- 0.001

    val api = result.filter(result("source_system") === "api").head()
    // Both api rows active → 1.0
    api.getAs[Double]("active_rate") shouldBe 1.0 +- 0.001
  }

  it should "count unique entities correctly" in {
    val df     = spark.createDataFrame(spark.sparkContext.parallelize(silverRows), schema)
    val result = AggregateEnrichJob.computeKpis(df, "2024-03-15")
    val total  = result.agg(org.apache.spark.sql.functions.sum("unique_entities")).head().getLong(0)
    total shouldBe 4L
  }
}
