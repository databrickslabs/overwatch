package com.overwatch.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.Schema
import com.databricks.labs.overwatch.utils.Module
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.overwatch.labs.overwatch.SparkSessionTestWrapper
import org.scalatest.funspec.AnyFunSpec


class SchemaTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Schema") {
    describe("when invalid module id is passed") {
      it("should return the same df back but with log a warn message") {
        val sourceDF = Seq(
          ("jose"),
          ("li"),
          ("luisa")
        ).toDF("name")
        val returnDF = Schema.verifyDF(sourceDF, new Module(100, "dummy"))
        assertSmallDataFrameEquality(returnDF, sourceDF)
      }
    }

  }


}
