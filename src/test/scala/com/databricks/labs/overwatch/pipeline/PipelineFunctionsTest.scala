package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.utils.{BadConfigException, Config}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.scalatest.GivenWhenThen
import org.scalatest.funspec.AnyFunSpec

import scala.reflect.io.Directory


class PipelineFunctionsTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper with GivenWhenThen {
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  def delDir(name:String) = {
    val dir = Directory("/tmp/overwatch/tests/" + name)
    dir.deleteRecursively()
  }

  delDir("")

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

  /**
   * Below tests are added as a part of OV-43
   */

  describe("Tests for parseEHConnectionString") {
    it("should throw an exception - empty string") {
      Given("an event hub connection string")
      val ehConnString = ""

      When("the connection string is not in correct format")

      Then("the function throws an exception")
      assertThrows[BadConfigException] (PipelineFunctions.parseAndValidateEHConnectionString(ehConnString, true))
    }
    it("should throw an exception - incorrect format") {
      Given("an event hub connection string")
      val ehConnString = "Endpoint=sb:/<NamespaceName>.servicebus.windows.net/;SharedAccessKey=<KeyValue>"

      When("the connection string is not in correct format")

      Then("the function throws an exception")
      assertThrows[BadConfigException] (PipelineFunctions.parseAndValidateEHConnectionString(ehConnString, true))
    }
    it("should parse the connection string") {
      Given("an event hub connection string")
      val ehConnString = "Endpoint=sb://<NamespaceName>.servicebus.windows.net/;SharedAccessKey=<KeyValue>"

      When("the connection string is in correct format")

      Then("the function returns the connection string")
      assertResult(ehConnString) (PipelineFunctions.parseAndValidateEHConnectionString(ehConnString, true))
    }
    it("should parse the connection string without SAS") {
      Given("an event hub connection string")
      val ehConnString = "Endpoint=sb://<NamespaceName>.servicebus.windows.net/;"

      When("the connection string is in correct format")

      Then("the function returns the connection string")
      assertResult(ehConnString) (PipelineFunctions.parseAndValidateEHConnectionString(ehConnString, false))
    }
    it("should parse the connection string with special characters") {
      Given("an event hub connection string")
      val ehConnString = "Endpoint=sb://<NamespaceName>.servicebus.windows.net/;SharedAccessKey=$+<abcdefgh+1234+>"

      When("the connection string is in correct format")

      Then("the function returns the connection string")
      assertResult(ehConnString) (PipelineFunctions.parseAndValidateEHConnectionString(ehConnString, true))
    }

  }

  describe("Tests for cleansePathURI - Part 2") {
    it("should work for S3 - double slashes") {
      Given("URI path with double slashes")
      val uriPath = "s3a://commoncrawl//path"

      When("function is called")

      Then("returns the cleansed URI path")
      assertResult("s3a://commoncrawl/path")(
        PipelineFunctions.cleansePathURI(uriPath)
      )
    }
  }

  describe("Tests for epochMilliToTs") {
    it("should convert epoch time to timestamp and preserve milliseconds") {
      Given("a dataframe with epoch milliseconds time of long type")
      //Reference for epoch millis https://currentmillis.com/
      val df = spark.sql("select '1660109379388' as epoch_millisecond")

      When("function is called on this column")

      Then("returns a column of type timestamp and preserves milliseconds")

      val result = df
        .withColumn("converted_column", PipelineFunctions.epochMilliToTs("epoch_millisecond"))

      assertResult("TimestampType") (result.schema.filter( x => x.name == "converted_column")
        .head.dataType.toString
      )

      result.select(col("converted_column").cast("String")).show(false)

      assertResult("2022-08-10 05:29:39.388") (result
        .select(col("converted_column").cast("String"))
        .collect().head.get(0).toString
      )
    }
    it("should return null for unexpected timestamp format") {
      Given("a dataframe with timestamp column of unexpected format")
      val df = spark.sql("select '10/14/2016 09:28 PM' as ts")

      When("function is called on this column")

      Then("return a null value for the converted column")
      assertResult(null) (df
        .withColumn("converted_column", PipelineFunctions.epochMilliToTs("ts"))
        .select("converted_column")
        .collect().head.get(0)
      )
    }
  }

  describe("Tests for tsToEpochMilli") {
    it("should convert timestamp to epoch time and preserve milliseconds") {
      Given("a dataframe with timestamp ")
      val df = spark.sql("select '2021-11-12 02:12:23.8870' as ts")

      When("function is called on this column")

      Then("convert timestamp to epoch time and preserve milliseconds")
      df.withColumn("epoch_milliseconds", PipelineFunctions.tsToEpochMilli("ts").cast("Long").cast("String")).show(false)

      assertResult("DoubleType") (df
        .withColumn("epoch_milliseconds", PipelineFunctions.tsToEpochMilli("ts"))
        .schema.filter( x => x.name == "epoch_milliseconds").head.dataType.toString
      )
      assertResult("1636683143887") (df
        .withColumn("epoch_milliseconds", PipelineFunctions.tsToEpochMilli("ts").cast("Long").cast("String"))
        .select("epoch_milliseconds").collect().head.getAs[String](0)
      )
    }
    it("should return null for unexpected values") {
      Given("a dataframe with unexpected timestamp value")
      val df = spark.sql("select '1636663343887' as ts")

      When("function is called on this column")

      Then("return a null value for the converted column")
      assertResult(null) (df
        .withColumn("epoch_milliseconds", PipelineFunctions.tsToEpochMilli("ts"))
        .select("epoch_milliseconds")
        .collect().head.get(0)
      )

    }
  }

  describe("Tests for getDeltaHistory") {
    // delete the delta table
    val tbl ="table1"
    delDir(tbl)
    it("should get the history of delta table") {
      Given("a delta table")
      val df = spark.range(10).withColumn("c1", lit("c1"))
      df.write.format("delta").mode("overwrite").save(s"/tmp/overwatch/tests/${tbl}")

      val dummyConfig = new Config()
      dummyConfig.setDatabaseNameAndLoc("dummy", "dummy", "/tmp/overwatch/tests")

      val testTable: PipelineTable = PipelineTable(name = "table1", _keys = Array("id"), config = dummyConfig)

      When("function is called on the given delta table")
      val functionOutputDf: DataFrame = PipelineFunctions.getDeltaHistory(spark, testTable)

      Then("returns a dataframe with detal table history")
      val expectedDf: DataFrame = DeltaTable.forPath(testTable.tableLocation).history(9999)
        .select("version", "timestamp", "operation", "clusterId", "operationMetrics", "userMetadata")

      assertSmallDatasetEquality(functionOutputDf, expectedDf)
    }
    it("should throw an exception when path is not a delta table") {
      Given("a delta table")
      val df = spark.range(10).withColumn("c1", lit("c1"))
      df.write.format("delta").mode("overwrite").save(s"/tmp/overwatch/tests/${tbl}")

      val dummyConfig = new Config()
      dummyConfig.setDatabaseNameAndLoc("dummy", "dummy", "/tmp/overwatch/test")

      val testTable: PipelineTable = PipelineTable(name = "table1", _keys = Array("id"), config = dummyConfig)

      When("function is called on incorrect delta table path")

      Then("throws an exception")
      assertThrows[org.apache.spark.sql.AnalysisException] (PipelineFunctions.getDeltaHistory(spark, testTable))
    }
    // delete the delta table
    delDir(tbl)
  }


  describe("Tests for getLastOptimized") {
    // delete the delta table
    val tbl ="table2"
    delDir(tbl)
    it("should return 0 as long type when the table is never optimized") {
      Given("a delta table")
      // delete the delta table
      val df = spark.range(10).withColumn("c1", lit("c1"))
      df.write.format("delta").mode("overwrite").save(s"/tmp/overwatch/tests/${tbl}")

      val dummyConfig = new Config()
      dummyConfig.setDatabaseNameAndLoc("dummy", "dummy", "/tmp/overwatch/tests")

      val testTable: PipelineTable = PipelineTable(name = "table1", _keys = Array("id"), config = dummyConfig)

      When("function is called on the delta table that is never optimized")

      Then("return 0")
      assertResult(0L) (PipelineFunctions.getLastOptimized(spark, testTable))
    }
    it("should throw an exception when path is not a delta table") {
      Given("a delta table")
      val df = spark.range(10).withColumn("c1", lit("c1"))
      df.write.format("delta").mode("overwrite").save(s"/tmp/overwatch/tests/${tbl}")

      val dummyConfig = new Config()
      dummyConfig.setDatabaseNameAndLoc("dummy", "dummy", "/tmp/overwatch/test")

      val testTable: PipelineTable = PipelineTable(name = "table1", _keys = Array("id"), config = dummyConfig)

      When("function is called on the delta table that is never optimized")

      Then("return 0")
      assertThrows[org.apache.spark.sql.AnalysisException] (PipelineFunctions.getLastOptimized(spark, testTable))
    }
    // delete the delta table
    delDir(tbl)
  }
}
