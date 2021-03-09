package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec


class SchemaTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {

//  import spark.implicits._
//
//  describe("Schema") {
//    describe("when invalid module id is passed") {
//      it("should return the same df back but with log a warn message") {
//        val sourceDF = Seq(
//          ("jose"),
//          ("li"),
//          ("luisa")
//        ).toDF("name")
//        val returnDF = Schema.verifyDF(sourceDF, Module(100, "dummy"))
//        assertSmallDataFrameEquality(returnDF, sourceDF)
//      }
//    }
//
//    describe("test for module id 2006") {
//      val schema = StructType(Seq(
//        StructField("organization_id",StringType),
//        StructField("Event", StringType, nullable = true),
//        StructField("clusterId", StringType, nullable = true),
//        StructField("SparkContextID", StringType, nullable = true),
//        StructField("JobID", StringType, nullable = true),
//        StructField("JobResult", StringType, nullable = true),
//        StructField("CompletionTime", StringType, nullable = true),
//        StructField("StageIDs", StringType, nullable = true),
//        StructField("SubmissionTime", StringType, nullable = true),
//        StructField("Pipeline_SnapTS", StringType, nullable = true),
//        StructField("fileCreateEpochMS", LongType),
//        StructField("fileCreateTS",TimestampType),
//        StructField("fileCreateDate", DateType),
//        StructField("filenameGroup", StructType(Seq(
//          StructField("filename",StringType),
//          StructField("byCluster",StringType),
//          StructField("byDriverHost",StringType),
//          StructField("bySparkContext",StringType))), nullable = false),
//        StructField("actionName", StringType, nullable = true),
//        StructField("Properties", MapType(
//          StringType, StringType, valueContainsNull = true
//        )),
//        StructField("sourceIPAddress", StringType, nullable = true),
//        StructField("version", StringType, nullable = true)
//      ))
//      it("should return the same df back since the there no missing columns") {
//        val dummyDF = DataframeGenerator.arbitraryDataFrame(spark.sqlContext, schema).arbitrary.sample.get
//        val frame = Schema.verifyDF(dummyDF, Module(2006, "SparkJobs"))
//        assertSmallDataFrameEquality(dummyDF, frame)
//      }
//      it("should mark everything null since some of the cols are missing") {
//        val df = spark.createDataFrame(sc.emptyRDD[Row], schema)
//        val df1 = df.drop("JobID")
//        val frame = Schema.verifyDF(df1, Module(2006, "SparkJobs"))
//        assertSmallDataFrameEquality(df, frame)
//      }
//    }
//
//  }


}
