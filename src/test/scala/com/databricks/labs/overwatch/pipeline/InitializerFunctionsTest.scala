package com.databricks.labs.overwatch.pipeline
import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.utils.Config
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec

class InitializerFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {
  val conf = new Config
  conf.setOrganizationId("demo")
  val Initializer = new InitializerV2(conf, disableValidations = false, isSnap = false, initDB = true)

  describe("Tests for InitializerFunctions.loadLocalResource") {
    it("should throw exception for non-existing resource") {
      assertThrows[RuntimeException](Initializer.Init.loadLocalResource("/non-existent"))
    }
    it("should read text file") {
      val data = Initializer.Init.loadLocalResource("/file.txt")
      assertResult(false)(data.isEmpty)
      assertResult(2)(data.size)
      assertResult("test1")(data(1))
    }
  }

  describe("Tests for InitializerFunctions.loadLocalCSVResource") {
    it("should load CSV from local resource") {
      val df = Initializer.Init.loadLocalCSVResource(spark, "/file1.csv")
      assertResult("`field1` INT,`field2` STRING,`field3` BOOLEAN")(df.schema.toDDL)
      assertResult(3)(df.count())
      val mustBeDf = spark.createDataFrame(Seq((1,"text1", false), (2,"text2",true), (3,"text3",false)))
        .toDF("field1", "field2", "field3")
      assertSmallDataFrameEquality(df, mustBeDf, ignoreNullable = true)
    }

    it("should load empty dataframe if no header") {
      val df = Initializer.Init.loadLocalCSVResource(spark, "/noheader.csv")
      assertResult("`1` STRING,`text1` STRING,`false` STRING")(df.schema.toDDL)
      assertResult(0)(df.count())
    }
  }
}
