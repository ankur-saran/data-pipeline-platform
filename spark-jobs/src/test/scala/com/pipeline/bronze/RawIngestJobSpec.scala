package com.pipeline.bronze

import com.pipeline.common.SparkSessionFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Path}

class RawIngestJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var tmpDir: Path = _

  override def beforeAll(): Unit = {
    spark  = SparkSessionFactory.build("RawIngestJobSpec", "dev")
    tmpDir = Files.createTempDirectory("bronze-test")
  }

  override def afterAll(): Unit = {
    SparkSessionFactory.stop(spark)
    deleteDir(tmpDir.toFile)
  }

  // ---------------------------------------------------------------------------
  "attachMetadata" should "add pipeline_run_id, ingested_at, source_system columns" in {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("value", StringType)))
    val data   = Seq(Row(1, "a"), Row(2, "b"))
    val df     = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val result = RawIngestJob.attachMetadata(df, "run-001", "test-src", "2024-03-15")

    result.columns should contain allOf ("pipeline_run_id", "ingested_at", "source_system", "year", "month", "day")
    val row = result.collect().head
    row.getAs[String]("pipeline_run_id") shouldBe "run-001"
    row.getAs[String]("source_system")   shouldBe "test-src"
    row.getAs[Int]("year")               shouldBe 2024
    row.getAs[Int]("month")              shouldBe 3
    row.getAs[Int]("day")                shouldBe 15
  }

  it should "preserve all original columns" in {
    val schema = StructType(Seq(StructField("customer_id", LongType), StructField("amount", DoubleType)))
    val data   = Seq(Row(100L, 9.99), Row(200L, 19.50))
    val df     = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val result = RawIngestJob.attachMetadata(df, "run-002", "crm", "2024-01-01")

    result.columns should contain allOf ("customer_id", "amount")
    result.count() shouldBe 2
  }

  // ---------------------------------------------------------------------------
  "readRaw" should "read a CSV file and infer schema" in {
    val csvContent = "id,name,amount\n1,Alice,10.5\n2,Bob,20.0\n"
    val csvPath    = tmpDir.resolve("sample.csv")
    Files.writeString(csvPath, csvContent)

    val df = RawIngestJob.readRaw(spark, csvPath.getParent.toString, "csv")

    df.count() shouldBe 2
    df.columns should contain allOf ("id", "name", "amount")
  }

  it should "throw for unsupported formats" in {
    an [IllegalArgumentException] should be thrownBy {
      RawIngestJob.readRaw(spark, tmpDir.toString, "xlsx")
    }
  }

  // ---------------------------------------------------------------------------
  "writeOutput + readRaw" should "roundtrip as Parquet partitioned by year/month/day" in {
    val schema = StructType(Seq(
      StructField("id",    IntegerType),
      StructField("value", StringType),
      StructField("year",  IntegerType),
      StructField("month", IntegerType),
      StructField("day",   IntegerType)
    ))
    val data = Seq(Row(1, "x", 2024, 3, 15))
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val outPath = tmpDir.resolve("bronze-out").toString
    RawIngestJob.writeOutput(df, outPath, "2024-03-15")

    val reread = spark.read.parquet(outPath)
    reread.count() shouldBe 1
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteDir)
    f.delete()
  }
}
