package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import org.scalatest.funspec.AnyFunSpec

class SchemaToolsTest extends AnyFunSpec with SparkSessionTestWrapper {
  import spark.implicits._

  describe("SchemaToolsTest") {

    it("should scrub schema (simple)") {
      assertResult("`field1` INT,`field2` INT"){
        val df = spark.createDataFrame(Seq((1,2))).toDF("field1", "field2")
        SchemaTools.scrubSchema(df).schema.toDDL
      }
      assertResult("`field1` INT,`field2` INT"){
        val df = spark.createDataFrame(Seq((1,2))).toDF("field-1", "f-i-e-l-d\\\\2")
        SchemaTools.scrubSchema(df).schema.toDDL
      }
    }

    it("should scrub schema (array)") {
      val strings = Seq(
        "{\"i-1\": 1, \"b-2-2-2\": [{\"c_1-\\\\45\": 123, \"abc\": \"ttt\"}, {\"c_1-\\\\45\": 124, \"abc\": \"sss\"}]}",
        "{\"i-1\": 2, \"b-2-2-2\": [{\"c_1-\\\\45\": 234, \"abc\": \"bbb\"}, {\"c_1-\\\\45\": 434, \"abc\": \"aaa\"}]}"
      ).toDS
      val df = spark.read.json(strings)

      assertResult("`b-2-2-2` ARRAY<STRUCT<`abc`: STRING, `c_1-\\45`: BIGINT>>,`i-1` BIGINT") {
        df.schema.toDDL
      }
      assertResult("`b222` ARRAY<STRUCT<`abc`: STRING, `c_145`: BIGINT>>,`i1` BIGINT") {
        SchemaTools.scrubSchema(df).schema.toDDL
      }
    }

    it("should scrub schema (struct)") {
      val strings = Seq(
        "{\"i-1\": 1, \"b-2-2-2\": {\"c_1-\\\\45\": 123, \"abc\": \"ttt\"}}",
        "{\"i-1\": 2, \"b-2-2-2\": {\"c_1-\\\\45\": 456, \"abc\": \"sss\"}}"
      ).toDS
      val df = spark.read.json(strings)

      assertResult("`b-2-2-2` STRUCT<`abc`: STRING, `c_1-\\45`: BIGINT>,`i-1` BIGINT") {
        df.schema.toDDL
      }
      assertResult("`b222` STRUCT<`abc`: STRING, `c_145`: BIGINT>,`i1` BIGINT") {
        SchemaTools.scrubSchema(df).schema.toDDL
      }
    }

    it("should collect column names 1 ") {
      val strings = Seq(
        "{\"i1\": 1, \"b222\": [{\"c_145\": 123, \"abc\": \"ttt\"}, {\"c_145\": 124, \"abc\": \"sss\"}]}",
        "{\"i1\": 2, \"b222\": [{\"c_145\": 234, \"abc\": \"bbb\"}, {\"c_145\": 434, \"abc\": \"aaa\"}]}"
      ).toDS
      val df = spark.read.json(strings)
      assertResult(Array("b222", "i1"))(SchemaTools.getAllColumnNames(df.schema))
    }

    it("should collect column names 2") {
      val strings = Seq(
        "{\"i1\": 1, \"b222\": {\"c_145\": 123, \"abc\": \"ttt\"}}",
        "{\"i1\": 2, \"b222\": {\"c_145\": 234, \"abc\": \"bbb\"}}"
      ).toDS
      val df = spark.read.json(strings)
      assertResult(Array("b222.abc", "b222.c_145", "i1"))(SchemaTools.getAllColumnNames(df.schema))
    }

    it("should flatten column names 1 ") {
      val strings = Seq(
        "{\"i1\": 1, \"b222\": [{\"c_145\": 123, \"abc\": \"ttt\"}, {\"c_145\": 124, \"abc\": \"sss\"}]}",
        "{\"i1\": 2, \"b222\": [{\"c_145\": 234, \"abc\": \"bbb\"}, {\"c_145\": 434, \"abc\": \"aaa\"}]}"
      ).toDS
      val df = spark.read.json(strings)
      assertResult("`b222` ARRAY<STRUCT<`abc`: STRING, `c_145`: BIGINT>>,`i1` BIGINT") {
        df.select(SchemaTools.flattenSchema(df): _*).schema.toDDL
      }
    }

    it("should flatten column names 2") {
      val strings = Seq(
        "{\"i1\": 1, \"b222\": {\"c_145\": 123, \"abc\": \"ttt\"}}",
        "{\"i1\": 2, \"b222\": {\"c_145\": 234, \"abc\": \"bbb\"}}"
      ).toDS
      val df = spark.read.json(strings)
      assertResult("`b222_abc` STRING,`b222_c_145` BIGINT,`i1` BIGINT") {
        df.select(SchemaTools.flattenSchema(df): _*).schema.toDDL
      }
    }

  }
}
