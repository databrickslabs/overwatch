package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

class PipelineFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {

  describe("Tests for PipelineFunctions") {

    it("add tick to every column") {
      val generatedDf = spark.createDataFrame(
        Seq((1, 2l, 1.0d, java.sql.Date.valueOf("2020-10-30"),
          java.sql.Timestamp.valueOf("2011-10-31 10:01:11.000")))
      ).toDF("int", "long", "double", "date", "timestamp")
        .select(PipelineFunctions.addOneTick(col("int"), IntegerType).as("int"),
          PipelineFunctions.addOneTick(col("long"), LongType).as("long"),
          PipelineFunctions.addOneTick(col("double"), DoubleType).as("double"),
          PipelineFunctions.addOneTick(col("date"), DateType).as("date"),
          PipelineFunctions.addOneTick(col("timestamp")).as("timestamp")
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
        df.withColumn("abc", PipelineFunctions.addOneTick(col("id"), StringType))
      }
    }

  }
}
