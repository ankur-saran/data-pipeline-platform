package com.pipeline.silver

import com.pipeline.common.SparkSessionFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CleanseTransformJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  val schema: StructType = StructType(Seq(
    StructField("id",            StringType),
    StructField("amount",        StringType),
    StructField("event_date",    StringType),
    StructField("created_at",    StringType),
    StructField("is_active",     StringType),
    StructField("source_system", StringType),
    StructField("year",          IntegerType),
    StructField("month",         IntegerType),
    StructField("day",           IntegerType)
  ))

  override def beforeAll(): Unit =
    spark = SparkSessionFactory.build("CleanseTransformJobSpec", "dev")

  override def afterAll(): Unit =
    SparkSessionFactory.stop(spark)

  private def makeDF(rows: Seq[Row]) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  // ---------------------------------------------------------------------------
  "castTypes" should "cast string columns to correct types" in {
    val df = makeDF(Seq(
      Row("1", "19.99", "2024-03-15", "2024-03-15T10:00:00", "true", "sftp", 2024, 3, 15)
    ))
    val result = CleanseTransformJob.castTypes(df)
    result.schema("id").dataType         shouldBe LongType
    result.schema("amount").dataType     shouldBe DoubleType
    result.schema("is_active").dataType  shouldBe BooleanType
  }

  it should "not fail when optional columns are absent" in {
    val minSchema = StructType(Seq(
      StructField("source_system", StringType),
      StructField("year",  IntegerType),
      StructField("month", IntegerType),
      StructField("day",   IntegerType)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("api", 2024, 3, 1))), minSchema)
    noException should be thrownBy CleanseTransformJob.castTypes(df)
  }

  // ---------------------------------------------------------------------------
  "deduplicate" should "remove rows with the same (id, event_date, source_system)" in {
    val rows = Seq(
      Row("10", "5.0", "2024-03-15", null, null, "sftp", 2024, 3, 15),
      Row("10", "5.0", "2024-03-15", null, null, "sftp", 2024, 3, 15),  // duplicate
      Row("11", "7.0", "2024-03-15", null, null, "sftp", 2024, 3, 15)
    )
    val df     = makeDF(rows)
    val result = CleanseTransformJob.deduplicate(df)
    result.count() shouldBe 2
  }

  // ---------------------------------------------------------------------------
  "applyBusinessRules" should "drop rows with null id" in {
    val rows = Seq(
      Row("1",  "10.0", "2024-01-01", null, null, "api",  2024, 1, 1),
      Row(null, "5.0",  "2024-01-01", null, null, "api",  2024, 1, 1),
      Row("3",  "2.0",  "2024-01-01", null, null, "api",  2024, 1, 1)
    )
    val df     = makeDF(rows)
    val result = CleanseTransformJob.applyBusinessRules(df)
    result.count() shouldBe 2
  }

  it should "drop rows with negative amount" in {
    val rows = Seq(
      Row("1", "10.0",  "2024-01-01", null, null, "api", 2024, 1, 1),
      Row("2", "-5.0",  "2024-01-01", null, null, "api", 2024, 1, 1),  // negative → drop
      Row("3", "0.0",   "2024-01-01", null, null, "api", 2024, 1, 1)
    )
    val df     = makeDF(rows)
    val cast   = CleanseTransformJob.castTypes(df)
    val result = CleanseTransformJob.applyBusinessRules(cast)
    result.count() shouldBe 2
  }
}
