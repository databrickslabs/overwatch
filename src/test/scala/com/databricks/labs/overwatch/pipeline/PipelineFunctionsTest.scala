package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Frequency, IncrementalFilter}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

class PipelineFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {

  describe("Tests for addOneTick") {

    it("add tick to every column") {
      val frequency = Frequency.milliSecond
      val generatedDf = spark.createDataFrame(
        Seq((1, 2l, 1.0d, java.sql.Date.valueOf("2020-10-30"),
          java.sql.Timestamp.valueOf("2011-10-31 10:01:11.000")))
      ).toDF("int", "long", "double", "date", "timestamp")
        .select(PipelineFunctions.addOneTick(col("int"), frequency, IntegerType).as("int"),
          PipelineFunctions.addOneTick(col("long"), frequency, LongType).as("long"),
          PipelineFunctions.addOneTick(col("double"), frequency, DoubleType).as("double"),
          PipelineFunctions.addOneTick(col("date"), frequency, DateType).as("date"),
          PipelineFunctions.addOneTick(col("timestamp"), frequency).as("timestamp")
        )

      assertResult("`int` INT,`long` BIGINT,`double` DOUBLE,`date` DATE,`timestamp` TIMESTAMP") {
        generatedDf.schema.toDDL
      }

      val mustBeDf = spark.createDataFrame(
        Seq((2, 3l, 1.001d, java.sql.Date.valueOf("2020-10-31"),
          java.sql.Timestamp.valueOf("2011-10-31 10:01:11.001")))
      ).toDF("int", "long", "double", "date", "timestamp")

      assertApproximateDataFrameEquality(generatedDf, mustBeDf, 0.001)
    }

    it("should fail on wrong column type") {
      val df = spark.range(1)
      assertThrows[UnsupportedOperationException] {
        df.withColumn("abc", PipelineFunctions.addOneTick(col("id"), Frequency.milliSecond, StringType))
      }
    }
  }

//  describe("Tests for withIncrementalFilters") {
//
//    it("should filter out not necessary data (single column)") {
//      val filters = Seq(
//        IncrementalFilter("int", lit(10), lit(20))
//      )
//
//      val sourceDF = spark.createDataFrame(Seq((100, 1), (10, 1), (15, 1))).toDF("int", "dummy")
//      val actualDF = PipelineFunctions.withIncrementalFilters(sourceDF, Module(0, "Test Module"), filters)
//
//      assertResult("`int` INT,`dummy` INT") {
//        actualDF.schema.toDDL
//      }
//
//      val expectedDF = spark.createDataFrame(Seq((15, 1))).toDF("int", "dummy")
//      assertSmallDataFrameEquality(actualDF, expectedDF)
//    }
//
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