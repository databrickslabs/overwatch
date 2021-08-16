package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

class PipelineFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {

  describe("Tests for add and subtract incremental ticks") {

    val rawDF = spark.createDataFrame(
      Seq((2, 2l, 2.0d, java.sql.Date.valueOf("2020-10-30"),
        java.sql.Timestamp.valueOf("2011-10-31 10:01:11.000")))
    ).toDF("int", "long", "double", "date", "timestamp")

    it("add tick to every column") {
      val generatedDf = rawDF
        .select(PipelineFunctions.addNTicks(col("int"), 1, IntegerType).as("int"),
          PipelineFunctions.addNTicks(col("long"), 1, LongType).as("long"),
          PipelineFunctions.addNTicks(col("double"), 1, DoubleType).as("double"),
          PipelineFunctions.addNTicks(col("date"), 1, DateType).as("date"),
          PipelineFunctions.addNTicks(col("timestamp"), 1).as("timestamp")
        )

      assertResult("`int` INT,`long` BIGINT,`double` DOUBLE,`date` DATE,`timestamp` TIMESTAMP") {
        generatedDf.schema.toDDL
      }

      val mustBeDf = spark.createDataFrame(
        Seq((3, 3l, 2.001d, java.sql.Date.valueOf("2020-10-31"),
          java.sql.Timestamp.valueOf("2011-10-31 10:01:11.001")))
      ).toDF("int", "long", "double", "date", "timestamp")

      assertApproximateDataFrameEquality(generatedDf, mustBeDf, 0.001)
    }

    it("subtract tick from every column") {
      val generatedDf = rawDF
        .select(PipelineFunctions.subtractNTicks(col("int"), 1, IntegerType).as("int"),
          PipelineFunctions.subtractNTicks(col("long"), 1, LongType).as("long"),
          PipelineFunctions.subtractNTicks(col("double"), 1, DoubleType).as("double"),
          PipelineFunctions.subtractNTicks(col("date"), 1, DateType).as("date"),
          PipelineFunctions.subtractNTicks(col("timestamp"), 1).as("timestamp")
        )

      assertResult("`int` INT,`long` BIGINT,`double` DOUBLE,`date` DATE,`timestamp` TIMESTAMP") {
        generatedDf.schema.toDDL
      }

      val mustBeDf = spark.createDataFrame(
        Seq((1, 1l, 1.999d, java.sql.Date.valueOf("2020-10-29"),
          java.sql.Timestamp.valueOf("2011-10-31 10:01:10.999")))
      ).toDF("int", "long", "double", "date", "timestamp")

      assertApproximateDataFrameEquality(generatedDf, mustBeDf, 0.001)
    }

    it("should fail on wrong column type") {
      val df = spark.range(1)
      assertThrows[UnsupportedOperationException] {
        df.withColumn("abc", PipelineFunctions.addNTicks(col("id"), 1, StringType))
      }
    }
  }

  describe("Tests for getSourceDFParts") {

    it("using streaming") {
      implicit val sqlCtx: SQLContext = spark.sqlContext
      import spark.implicits._
      val streamingDF = MemoryStream[String].toDF

      assert(PipelineFunctions.getSourceDFParts(streamingDF) === 200)
    }

    it("using a small dataframe") {
      val smallDF = spark.createDataFrame(Seq((15, 1))).toDF("int", "dummy")
      assert(PipelineFunctions.getSourceDFParts(smallDF) === 1)
    }
  }

  describe("Tests for applyFilters") {
    it("match record applyFilter") {
      val smallDF = spark.createDataFrame(Seq((15, 1))).toDF("int", "dummy")
      val filterSeq: Seq[Column] = Seq(col("int") === 15)
      assertResult(1) {
        PipelineFunctions.applyFilters(smallDF, filterSeq, false).first().getAs("dummy")
      }
    }
    it("not match record applyFilter") {
      val smallDF = spark.createDataFrame(Seq((15, 1))).toDF("int", "dummy")
      val filterSeq: Seq[Column] = Seq(col("int") === 14)
      assertResult(0) {
        PipelineFunctions.applyFilters(smallDF, filterSeq, false).count()
      }
    }
  }


  //describe("Tests for withIncrementalFilters") {

  /*it("should filter out not necessary data (single column)") {
    val filters = Seq(
      IncrementalFilter("int", lit(10), lit(20))
    )

    val sourceDF = spark.createDataFrame(Seq((100, 1), (10, 1), (15, 1))).toDF("int", "dummy")
    val actualDF = PipelineFunctions.withIncrementalFilters(sourceDF, Module(0, "Test Module"), filters)

    assertResult("`int` INT,`dummy` INT") {
      actualDF.schema.toDDL
    }

    val expectedDF = spark.createDataFrame(Seq((15, 1))).toDF("int", "dummy")
    assertSmallDataFrameEquality(actualDF, expectedDF)
  }*/

  //    it("should filter out not necessary data (two columns)") {
  //      val filters = Seq(
  //        IncrementalFilter("int", lit(10), lit(20)),
  //        IncrementalFilter("dummy", lit(10), lit(20))
  //      )
  //
  //      val sourceDF = spark.createDataFrame(Seq((100, 1), (10, 1), (15, 11), (15, 1))).toDF("int", "dummy")
  //      val actualDF = PipelineFunctions.withIncrementalFilters(sourceDF, Module(0, "Test Module"), filters)
  //      assertResult("`int` INT,`dummy` INT") {
  //        actualDF.schema.toDDL
  //      }
  //
  //      val expectedDF = spark.createDataFrame(Seq((15, 11))).toDF("int", "dummy")
  //      assertSmallDataFrameEquality(actualDF, expectedDF)
  //    }
  //  }

  describe("Tests for cleansePathURI") {
    it("should work for DBFS") {
      assertResult("dbfs:/12312/12332423")(
        PipelineFunctions.cleansePathURI("dbfs://12312//12332423")
      )
    }
    it("should work without schema") {
      assertResult("dbfs:/12132/122132")(
        PipelineFunctions.cleansePathURI("/12132/122132")
      )
    }
    it("should work for ABFSS") {
      assertResult("abfss://test2@aottlrs.dfs.core.windows.net/1235")(
        PipelineFunctions.cleansePathURI("abfss://test2@aottlrs.dfs.core.windows.net/1235")
      )
    }
    it("should work for ABFSS double slashes") {
      assertResult("abfss://test2@aottlrs.dfs.core.windows.net/1235")(
        PipelineFunctions.cleansePathURI("abfss://test2@aottlrs.dfs.core.windows.net//1235")
      )
    }
    it("should work for S3") {
      assertResult("s3a://commoncrawl/path")(
        PipelineFunctions.cleansePathURI("s3a://commoncrawl/path")
      )
    }
  }

  describe("Tests for setSparkOverrides") {

    it("should set necessary configuration params") {
      val overrides = Map("spark.cassandra.connection.host" -> "localhost",
        "spark.sql.globalTempDatabase" -> "my_global"
      )
      PipelineFunctions.setSparkOverrides(spark, overrides)
      assertResult("localhost") {
        spark.conf.get("spark.cassandra.connection.host")
      }
      assertResult(StaticSQLConf.GLOBAL_TEMP_DATABASE.defaultValueString) {
        spark.conf.get("spark.sql.globalTempDatabase")
      }
      assertThrows[java.util.NoSuchElementException](
        spark.conf.get("unknown_key")
      )

    }
  }

}