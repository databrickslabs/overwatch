package com.databricks.labs.overwatch.pipeline

import java.sql.{Date, Timestamp}
import java.time.Instant

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.pipeline.TransformFunctions.toTS
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit, struct}
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
      val schema = StructType(
        Seq(StructField("start", LongType, false),
          StructField("end", LongType, false),
          StructField("RunTime",
            StructType(Seq(
              StructField("startEpochMS", LongType, false),
              StructField("startTS", TimestampType, true),
              StructField("endEpochMS", LongType, false),
              StructField("endTS", TimestampType, true),
              StructField("runTimeMS", LongType, false),
              StructField("runTimeS", DoubleType, true),
              StructField("runTimeM", DoubleType, true),
              StructField("runTimeH", DoubleType, true)
            )),false)))

      val sourceStartTS = 123456789000L
      val sourceEndTS = 223256289000L
      val sourceDF = Seq((sourceStartTS, sourceEndTS)).toDF("start", "end")

      val expectedData = sc.parallelize(Seq(
        Row(sourceStartTS, sourceEndTS,
          Row(sourceStartTS, Timestamp.from(Instant.ofEpochMilli(sourceStartTS)),
            sourceEndTS, Timestamp.from(Instant.ofEpochMilli(sourceEndTS)),
            99799500000L, 99799500.0, 1663325.0, 27722.083333333333333))))
      val expectedDF = spark.createDataFrame(expectedData, schema)

      val actualDF = sourceDF.withColumn("RunTime",
        TransformFunctions.subtractTime($"start", $"end"))
      assertResult("`start` BIGINT,`end` BIGINT,`RunTime` STRUCT<`startEpochMS`: BIGINT, `startTS`: TIMESTAMP, `endEpochMS`: BIGINT, `endTS`: TIMESTAMP, `runTimeMS`: BIGINT, `runTimeS`: DOUBLE, `runTimeM`: DOUBLE, `runTimeH`: DOUBLE>")(actualDF.schema.toDDL)
      assertResult(1)(actualDF.count())
      assertApproximateDataFrameEquality(actualDF, expectedDF, 0.001)
    }

  }
  describe("SilverTransformFunctions.removeNullCols") {
    val schema = StructType(
      Seq(StructField("serviceName", StringType, true),
        StructField("abc", LongType, true),
        StructField("requestParams",
          StructType(Seq(
            StructField("par1", StringType, true),
            StructField("par2", IntegerType, true)
          )),true)))

    it("should removeNullCols") {
      val sourceData = sc.parallelize(Seq(Row("jobs", null, Row("t1", 1)),Row("non-jobs", null, Row("t2", 2))))
      val sourceDF = spark.createDataFrame(sourceData, schema)
      val (_, df) = TransformFunctions.removeNullCols(sourceDF)
      assertResult("`serviceName` STRING,`requestParams` STRUCT<`par1`: STRING, `par2`: INT>")(df.schema.toDDL)
      assertResult(2)(df.count())
      val first = df.first()
      assertResult("jobs")(first.getAs[String]("serviceName"))
      assert(first.getAs[Row]("requestParams") != null)
    }

    it("should not remove boolean column") {
      // TODO: implement after the code is fixed
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
