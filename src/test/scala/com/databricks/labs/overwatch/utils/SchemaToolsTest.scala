package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import org.scalatest.funspec.AnyFunSpec

class SchemaToolsTest extends AnyFunSpec with SparkSessionTestWrapper {
  describe("SchemaToolsTest") {
    it("should scrub schema") {
      assertResult(Seq("field1", "field2")){
        val df = spark.createDataFrame(Seq((1,2))).toDF("field1", "field2")
        SchemaTools.scrubSchema(df).schema.names.toSeq
      }
      assertResult(Seq("field1", "field2")){
        val df = spark.createDataFrame(Seq((1,2))).toDF("field-1", "f-i-e-l-d\\\\2")
        SchemaTools.scrubSchema(df).schema.names.toSeq
      }
      // TODO: add test that will do cleanup of the struct type field, etc.
    }

    it("should moveColumnsToFront") {
      assertResult(Seq("field3", "field1", "field2")){
        val df = spark.createDataFrame(Seq((1,2,3))).toDF("field1", "field2", "field3")
        SchemaTools.moveColumnsToFront(df, Array("field3")).schema.names.toSeq
      }
    }
  }
}
