package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import TransformFunctions._

import java.sql.{Date, Timestamp}
import java.time.Instant

import com.databricks.labs.overwatch.pipeline.TransformFunctions._

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
      val df = sourceDF.cullNull
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
        df.moveColumnsToFront(Array("field3", "field2")).schema.names.toSeq
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
  }

  describe("TransformFunctions.joinWithLag") {
    val dfSchema1 = StructType(
      Seq(StructField("organization_id", StringType, true),
        StructField("clusterId", StringType, true),
        StructField("SparkContextID",StringType, true),
        StructField("ExecutorID", IntegerType, true),
        StructField("Date", DateType, true),
        StructField("event_log_1",
          StructType(Seq(
            StructField("filename", StringType, true)))
        )))
    val dfData1= sc.parallelize(Seq(
      Row("25379","09-s999","388",2,Date.valueOf("2016-09-28"),Row("/hfg/16-00.gz")),
      Row("25379","09-s891","387",0,Date.valueOf("2016-09-29"), Row("/edv/5-00.gz")),
      Row("25379","09-s796","386",1,Date.valueOf("2016-09-30"),Row("/abc/15-00.gz")),
      Row("25379","09-s888","385",1,Date.valueOf("2016-09-30"),Row("/abc/75-00.gz"))
    ))

    val dfSchema2 = StructType(
      Seq(StructField("organization_id", StringType, true),
        StructField("clusterId", StringType, true),
        StructField("SparkContextID",StringType, true),
        StructField("ExecutorID", IntegerType, true),
        StructField("Date", DateType, true),
        StructField("event_log_2",
          StructType(Seq(
            StructField("filename", StringType, true)))
        )))
    val dfData2= sc.parallelize(Seq(
      Row("25379","09-s999","388",2,Date.valueOf("2016-09-29"),Row("/faq/55-00.gz")),
      Row("25379","09-s891","387",0,Date.valueOf("2016-09-29"), Row("/lmn/53-00.gz")),
      Row("25379","09-s796","386",1,Date.valueOf("2016-10-30"),Row("/ijk/52-00.gz")),
      Row("25379","11-s111","454",2,Date.valueOf("2016-11-01"),Row("/stc/46-00.gz"))))


    it("should join dataframes with lag - jointype: left, laggingside: left") {
      val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutorID")
      val lagDays = 30
      val lagColumn = "Date"
      val df1 = spark.createDataFrame(dfData1,dfSchema1)
      val df2 = spark.createDataFrame(dfData2,dfSchema2)

      val expectedDFSchema = StructType(
        Seq(StructField("organization_id", StringType, true),
          StructField("clusterId", StringType, true),
          StructField("SparkContextID",StringType, true),
          StructField("ExecutorID", IntegerType, true),
          StructField("Date", DateType, true),
          StructField("event_log_1",
            StructType(Seq(
              StructField("filename", StringType, true)))),
          StructField("event_log_2",
            StructType(Seq(
              StructField("filename", StringType, true)))
          )))
      var expectedDf= spark.createDataFrame(sc.parallelize(Seq(
        Row("25379","09-s796","386",1,Date.valueOf("2016-09-30"),Row("/abc/15-00.gz"),Row("/ijk/52-00.gz")),
        Row("25379","09-s888","385",1,Date.valueOf("2016-09-30"),Row("/abc/75-00.gz"),null),
        Row("25379","09-s891","387",0,Date.valueOf("2016-09-29"), Row("/edv/5-00.gz"),Row("/lmn/53-00.gz")),
        Row("25379","09-s999","388",2,Date.valueOf("2016-09-28"),Row("/hfg/16-00.gz"),Row("/faq/55-00.gz"))))
        ,expectedDFSchema)

      var actualDF = df1.joinWithLag(df2,joinKeys, lagColumn,laggingSide="left", lagDays = lagDays, joinType = "left")
      assertSmallDataFrameEquality(expectedDf, actualDF, ignoreNullable = true)
    }

    it("should join dataframes with lag - jointype:inner, laggingside:left") {
      val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutorID")
      val lagDays = 30
      val lagColumn = "Date"
      val df1 = spark.createDataFrame(dfData1,dfSchema1)
      val df2 = spark.createDataFrame(dfData2,dfSchema2)

      var expectedDFSchema = StructType(
        Seq(StructField("organization_id", StringType, true),
          StructField("clusterId", StringType, true),
          StructField("SparkContextID",StringType, true),
          StructField("ExecutorID", IntegerType, true),
          StructField("Date", DateType, true),
          StructField("event_log_1",
            StructType(Seq(
              StructField("filename", StringType, true)))),
          StructField("event_log_2",
            StructType(Seq(
              StructField("filename", StringType, true)))
          )))
      var expectedDf= spark.createDataFrame(sc.parallelize(Seq(
        Row("25379","09-s796","386",1,Date.valueOf("2016-09-30"),Row("/abc/15-00.gz"),Row("/ijk/52-00.gz")),
        Row("25379","09-s891","387",0,Date.valueOf("2016-09-29"), Row("/edv/5-00.gz"),Row("/lmn/53-00.gz")),
        Row("25379","09-s999","388",2,Date.valueOf("2016-09-28"),Row("/hfg/16-00.gz"),Row("/faq/55-00.gz")),
      )),expectedDFSchema)

      var actualDF = df1.joinWithLag(df2,joinKeys, lagColumn,laggingSide="left", lagDays = lagDays, joinType = "inner")
      assertSmallDataFrameEquality(expectedDf, actualDF, ignoreNullable = true)
    }

    it("should join dataframes with lag - jointype: left, laggingside: right") {
      val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutorID")
      val lagDays = 30
      val lagColumn = "Date"
      val df1 = spark.createDataFrame(dfData1,dfSchema1)
      val df2 = spark.createDataFrame(dfData2,dfSchema2)

      var expectedDFSchema = StructType(
        Seq(StructField("organization_id", StringType, true),
          StructField("clusterId", StringType, true),
          StructField("SparkContextID",StringType, true),
          StructField("ExecutorID", IntegerType, true),
          StructField("Date", DateType, true),
          StructField("event_log_2",
            StructType(Seq(
              StructField("filename", StringType, true)))),
          StructField("event_log_1",
            StructType(Seq(
              StructField("filename", StringType, true)))
          )))
      var expectedDf= spark.createDataFrame(sc.parallelize(Seq(
        Row("25379","09-s796","386",1,Date.valueOf("2016-10-30"),Row("/ijk/52-00.gz"),Row("/abc/15-00.gz")),
        Row("25379","09-s891","387",0,Date.valueOf("2016-09-29"), Row("/lmn/53-00.gz"),Row("/edv/5-00.gz")),
        Row("25379","09-s999","388",2,Date.valueOf("2016-09-29"),Row("/faq/55-00.gz"),Row("/hfg/16-00.gz")),
        Row("25379","11-s111","454",2,Date.valueOf("2016-11-01"),Row("/stc/46-00.gz"),null)
      ))
        ,expectedDFSchema)
      var actualDF = df2.joinWithLag(df1,joinKeys, lagColumn,laggingSide="right", lagDays = lagDays, joinType = "left")
      assertSmallDataFrameEquality(expectedDf, actualDF, ignoreNullable = true)
    }

    it("should join dataframes with lag - jointype: inner, laggingside: right") {
      val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutorID")
      val lagDays = 30
      val lagColumn = "Date"
      val df1 = spark.createDataFrame(dfData1,dfSchema1)
      val df2 = spark.createDataFrame(dfData2,dfSchema2)

      var expectedDFSchema = StructType(
        Seq(StructField("organization_id", StringType, true),
          StructField("clusterId", StringType, true),
          StructField("SparkContextID",StringType, true),
          StructField("ExecutorID", IntegerType, true),
          StructField("Date", DateType, true),
          StructField("event_log_2",
            StructType(Seq(
              StructField("filename", StringType, true)))),
          StructField("event_log_1",
            StructType(Seq(
              StructField("filename", StringType, true)))
          )))
      var expectedDf= spark.createDataFrame(sc.parallelize(Seq(
        Row("25379","09-s796","386",1,Date.valueOf("2016-10-30"),Row("/ijk/52-00.gz"),Row("/abc/15-00.gz")),
        Row("25379","09-s891","387",0,Date.valueOf("2016-09-29"), Row("/lmn/53-00.gz"),Row("/edv/5-00.gz")),
        Row("25379","09-s999","388",2,Date.valueOf("2016-09-29"),Row("/faq/55-00.gz"),Row("/hfg/16-00.gz"))
      ))
        ,expectedDFSchema)
      var actualDF = df2.joinWithLag(df1,joinKeys, lagColumn,laggingSide="right", lagDays = lagDays, joinType = "inner")
      assertSmallDataFrameEquality(expectedDf, actualDF, ignoreNullable = true)
    }

    it("should not work for missing laggingcolumn in either dataframes") {
      val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutorID")
      val lagDays = 30
      val lagColumn = "Date_Test"
      val df1 = spark.createDataFrame(dfData1,dfSchema1)
      val df2 = spark.createDataFrame(dfData2,dfSchema2)
      var actualResult= ""
      val expectedResult = "IllegalArgumentException"
      try{
        val actualDF = df1.joinWithLag(df2,joinKeys, lagColumn,laggingSide="left", lagDays = lagDays, joinType = "left")
      }
      catch{
        case e:IllegalArgumentException => actualResult ="IllegalArgumentException"
      }
      assertResult(expectedResult)(actualResult)
    }
  }

}
