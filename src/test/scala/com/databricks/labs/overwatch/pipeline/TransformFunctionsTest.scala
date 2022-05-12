package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.utils.Helpers
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, concat_ws, explode, lit}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import java.time.Instant
import scala.reflect.io.Path

class TransformFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {
  import spark.implicits._
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  describe("TransformFunctions.fillFromLookupsByTS") {
    it("should fillFromLookupsByTS") {

    }
  }

  describe("TransformFunctions.subtractTime") {
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
  describe("TransformFunctions.removeNullCols") {
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

  describe("TransformFunctions.toTS") {
    it("should work for TimestampType") {
      val sourceTS = 123456789L
      val sourceDF = Seq((sourceTS)).toDF("long")
      val actualDF = sourceDF.withColumn("tsmilli", TransformFunctions.toTS(col("long")))
        .withColumn("tssec", TransformFunctions.toTS(col("long"), inputResolution = "sec"))
      assertResult("`long` BIGINT,`tsmilli` TIMESTAMP,`tssec` TIMESTAMP")(actualDF.schema.toDDL)
      assertResult(1)(actualDF.count())
      val first = actualDF.first()
      assertResult(sourceTS)(first.getLong(0))
      assertResult(Instant.parse("1970-01-02T10:17:36Z"))(first.getTimestamp(1).toInstant)
      assertResult(Instant.parse("1973-11-29T21:33:09Z"))(first.getTimestamp(2).toInstant)
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

//  TOMES -- removed this test as the getJobsBase function does not belong in TransformFunctions
//  describe("TransformFunctions.getJobsBase") {
//    val schema = StructType(
//      Seq(StructField("serviceName", StringType, true),
//        StructField("requestParams",
//          StructType(Seq(
//            StructField("par1", StringType, true),
//            StructField("par2", IntegerType, true)
//          )),true)))
//
//    it("should get job base") {
//      val sourceData = sc.parallelize(Seq(Row("jobs", Row("t1", 1)),Row("non-jobs", Row("t2", 2))))
//      val sourceDF = spark.createDataFrame(sourceData, schema)
//      val df = TransformFunctions.getJobsBase(sourceDF)
//      assertResult("`serviceName` STRING,`par1` STRING,`par2` INT")(df.schema.toDDL)
//      assertResult(1)(df.count())
//      val first = df.first()
//      assertResult("jobs")(first.getAs[String]("serviceName"))
//      assertResult("t1")(first.getAs[String]("par1"))
//      assertResult(1)(first.getAs[Int]("par2"))
//    }
//
//    it("should not get job base") {
//      val sourceData = sc.parallelize(Seq(Row("no-jobs2", Row("t1", 1)),Row("non-jobs", Row("t2", 2))))
//      val sourceDF = spark.createDataFrame(sourceData, schema)
//      val df = TransformFunctions.getJobsBase(sourceDF)
//      assertResult("`serviceName` STRING,`par1` STRING,`par2` INT")(df.schema.toDDL)
//      assertResult(0)(df.count())
//    }
//
//  }

  describe("TransformFunctions.moveColumnsToFront") {
    it("should moveColumnsToFront") {
      assertResult(Seq("field3", "field2", "field1")){
        val df = spark.createDataFrame(Seq((1,2,3))).toDF("field1", "field2", "field3")
        TransformFunctions.moveColumnsToFront(df, Array("field3", "field2")).schema.names.toSeq
      }
    }
  }

  describe("TransformFunctions.stringTsToUnixMillis") {
    it("should convert timestamp string to milliseconds") {
      val df = Seq(("2020-11-06T08:10:12.123Z")).toDF("col1")
      val expectedDF = Seq((1604650212123L)).toDF("col1")
      val actualDF = df.withColumn("col1", TransformFunctions.stringTsToUnixMillis($"col1"))
      assertSmallDataFrameEquality(actualDF, expectedDF, ignoreNullable = true)
    }

    it("should not convert timestamp string to milliseconds") {
      val df = Seq(("2020-11-06T08:10:12Z")).toDF("col1")
      val expectedDF = Seq((1L)).toDF("col1").select(lit(null).cast("long").as("col1"))
      val actualDF = df.withColumn("col1", TransformFunctions.stringTsToUnixMillis($"col1"))
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("should extract log paths") {
      val tempPath = System.getProperty("java.io.tmpdir")
      val GoodlogDir = "cluster-logs/recommendations/feature-store/production/0825-194151-adapt687/eventlog"
      val BadlogDir1 = "scribd-data-eng-logs-prod/databricks/kitchensink-etl/user_browser_uuids_partitioned_by_ds/2022-04-11T022100+0000/1/0412-041842-702htn9o/eventlog"
      val BadlogDir = "scribd-data-eng-logs-prod/databricks/kitchensink-etl/user_browser_uuids_partitioned_by_ds/2022-04-11T02:21:00+00:00/1/0412-041842-702htn9o/eventlog"
      val GoodlogSubDir = "/0825-194151-adapt687_10_230_54_88/2370849322415120956"
      val BadlogSubdir = "/0412-041842-702htn9o_10_230_49_221/6516587549925865915"
      val GoodfullPath = tempPath + GoodlogDir + GoodlogSubDir
      val BadfullPath = tempPath + BadlogDir + BadlogSubdir
      val Goodresult = new File(GoodfullPath).mkdirs()
      val Badresult = new File(BadfullPath).mkdirs()
      val GoodlogFile = GoodfullPath + "/eventlog-2021-08-29--08-00.gz"
      val BadLogFile = BadfullPath + "/eventlog"
      import java.io.PrintWriter
      new PrintWriter(GoodlogFile) { write("file contents"); close }
      println(GoodfullPath)
      println("file://" + GoodlogFile)
      new PrintWriter(BadLogFile) { write("file contents"); close }
      println(BadfullPath)
      println("file://" + BadLogFile)
      val hadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
      // "hdfs:/cluster-logs/recommendations/feature-store/production/0825-194151-adapt687/eventlog/0825-194151-adapt687_10_230_54_88/2370849322415120956/eventlog-2021-08-29--08-00.gz"
      val allEventLogPrefixes = spark.createDataFrame(Seq(("" + tempPath + BadlogDir,1))).toDF("logPathPrefix","order")
      allEventLogPrefixes
        .select("logPathPrefix")
        .as[String]
        .map(x => Helpers.parListFiles(x, hadoopConf)) // parallelized file lister since large / shared / long-running (months) clusters will have MANy paths
        .select(explode('value).alias("logPathPrefix"))
        .withColumn("logPathPrefix", concat_ws("/", 'logPathPrefix, lit("*"), lit("eventlo*")))
        .as[String]
        .map(p => Helpers.globPath(p, hadoopConf, Some(1L), Some(999999999999999999L)))
        .select(explode('value).alias("simpleFileStatus"))
        .selectExpr("simpleFileStatus.*")
        .withColumnRenamed("pathString", "filename")
        .show(false)
    }
  }
}
