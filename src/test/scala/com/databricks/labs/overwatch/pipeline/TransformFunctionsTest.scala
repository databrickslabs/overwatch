package com.databricks.labs.overwatch.pipeline

import java.sql.{Date, Timestamp}

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

class TransformFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {
  import spark.implicits._
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  describe("SilverTransformFunctions.fillFromLookupsByTS") {
    it("should fillFromLookupsByTS") {

    }
  }
  describe("SilverTransformFunctions.subtractTime") {
    it("should subtractTime") {

    }

  }
  describe("SilverTransformFunctions.removeNullCols") {
    it("should removeNullCols") {

    }

  }
  describe("SilverTransformFunctions.toTS") {
    it("should work for TimestampType") {
      val sourceTS = 123456789L
      val sourceDF = Seq((sourceTS)).toDF("long")
      val actualDF = sourceDF.withColumn("tsmilli", TransformFunctions.toTS(col("long")))
        .withColumn("tssec", TransformFunctions.toTS(col("long"), inputResolution = "sec"))
      assertResult("`long` BIGINT,`tsmilli` TIMESTAMP,`tssec` TIMESTAMP")(actualDF.schema.toDDL)
      assertResult(1)(actualDF.count())
      val first = actualDF.first()
      assertResult(sourceTS)(first.getLong(0))
      assertResult(Timestamp.valueOf("1970-01-02 11:17:36"))(first.getTimestamp(1))
      assertResult(Timestamp.valueOf("1973-11-29 22:33:09"))(first.getTimestamp(2))
    }

    it("should work for DateType") {
      val sourceTS = 123456789L
      val sourceDF = Seq((sourceTS)).toDF("long")
      val actualDF = sourceDF.withColumn("tsmilli",
        TransformFunctions.toTS(col("long"), outputResultType = DateType))
        .withColumn("tssec",
          TransformFunctions.toTS(col("long"), inputResolution = "sec", outputResultType = DateType))
      assertResult("`long` BIGINT,`tsmilli` DATE,`tssec` DATE")(actualDF.schema.toDDL)
      assertResult(1)(actualDF.count())
      val first = actualDF.first()
      assertResult(sourceTS)(first.getLong(0))
      assertResult(Date.valueOf("1970-01-02"))(first.getDate(1))
      assertResult(Date.valueOf("1973-11-29"))(first.getDate(2))
    }

    it("shouldn't work for LongType") {
      assertThrows[IllegalArgumentException](TransformFunctions.toTS(col("abc"), outputResultType = LongType))
    }
  }

  describe("SilverTransformFunctions.getJobsBase") {
    val schema = StructType(
      Seq(StructField("serviceName", StringType, true),
        StructField("requestParams",
          StructType(Seq(
            StructField("par1", StringType, true),
            StructField("par2", IntegerType, true)
          )),true)))

    it("should get job base") {
      val sourceData = sc.parallelize(Seq(Row("jobs", Row("t1", 1)),Row("non-jobs", Row("t2", 2))))
      val sourceDF = spark.createDataFrame(sourceData, schema)
      val df = TransformFunctions.getJobsBase(sourceDF)
      assertResult("`serviceName` STRING,`par1` STRING,`par2` INT")(df.schema.toDDL)
      assertResult(1)(df.count())
      val first = df.first()
      assertResult("jobs")(first.getAs[String]("serviceName"))
      assertResult("t1")(first.getAs[String]("par1"))
      assertResult(1)(first.getAs[Int]("par2"))
    }

    it("should not get job base") {
      val sourceData = sc.parallelize(Seq(Row("no-jobs2", Row("t1", 1)),Row("non-jobs", Row("t2", 2))))
      val sourceDF = spark.createDataFrame(sourceData, schema)
      val df = TransformFunctions.getJobsBase(sourceDF)
      assertResult("`serviceName` STRING,`par1` STRING,`par2` INT")(df.schema.toDDL)
      assertResult(0)(df.count())
    }

  }
}
