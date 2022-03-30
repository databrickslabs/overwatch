package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import org.scalatest.funspec.AnyFunSpec

class SchemaToolsTest extends AnyFunSpec with SparkSessionTestWrapper {
  import spark.implicits._

  describe("SchemaToolsTest") {

    it("should scrub schema (simple)") {
      assertResult("`field1` INT,`field2` INT"){
        val df = spark.createDataFrame(Seq((1,2))).toDF("field1", "field2")
        SchemaScrubber.scrubSchema(df).schema.toDDL
      }
      assertResult("`field_1` INT,`f_i_e_l_d__2` INT"){
        val df = spark.createDataFrame(Seq((1,2))).toDF("field-1", "f-i-e-l-d\\\\2")
        SchemaScrubber.scrubSchema(df).schema.toDDL
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
      assertResult("`b_2_2_2` ARRAY<STRUCT<`abc`: STRING, `c_1__45`: BIGINT>>,`i_1` BIGINT") {
        SchemaScrubber.scrubSchema(df).schema.toDDL
      }
      assertResult("`b_2_2_2` ARRAY<STRUCT<`abc`: STRING, `c_1__45`: BIGINT>>,`i_1` BIGINT") {
        df.scrubSchema.schema.toDDL
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
      assertResult("`b_2_2_2` STRUCT<`abc`: STRING, `c_1__45`: BIGINT>,`i_1` BIGINT") {
        SchemaScrubber.scrubSchema(df).schema.toDDL
      }
      assertResult("`b_2_2_2` STRUCT<`abc`: STRING, `c_1__45`: BIGINT>,`i_1` BIGINT") {
        df.scrubSchema.schema.toDDL
      }
    }

    it("should scrub schema (struct) with exceptions") {
      val strings = Seq(
        """{"b-2-2-2":{"abc":"ttt","c_1-\\45":123},"i-1":1,"parent w space":"test","validParent":"winning",
          |"exception_parent":{"x.y.z":{"j.k.l":12,"good_col":10,
          |"other except":11,"# mixed":14,"%bad":15,"dup 1":16,"dup 1":17,"dup2":18,"dup2":"19"},
          |"dup 1":20,"dup2":21,"z.y.x":22}}""".stripMargin,
        """{"b-2-2-2":{"abc":"ttt","c_1-\\45":123},"i-1":1,"parent w space":"test","validParent":"winning",
          |"exception_parent":{"x.y.z":{"j.k.l":32,"good_col":32,
          |"other except":31,"# mixed":34,"%bad":35,"dup 1":36,"dup 1":37,"dup2":38,"dup2":"39"},
          |"dup 1":40,"dup2":41,"z.y.x":42}}""".stripMargin
      ).toDS
      val df = spark.read.json(strings)
      val propertiesScrubException = SanitizeFieldException(
        field = SchemaTools.colByName(df)("exception_parent"),
        rules = List(
          SanitizeRule("\\s", ""),
          SanitizeRule("\\.", ""),
          SanitizeRule("[^a-zA-Z0-9]", "_")
        ),
        recursive = true
      )
      val exceptionScrubber = SchemaScrubber(exceptions = Array(propertiesScrubException))

      val expectedResString = "`b_2_2_2` STRUCT<`abc`: STRING, `c_1__45`: BIGINT>,`exception_parent` " +
        "STRUCT<`dup1`: BIGINT, `dup2`: BIGINT, `xyz`: STRUCT<`_mixed`: BIGINT, `_bad`: BIGINT, " +
        "`dup1_UNIQUESUFFIX_95946320`: BIGINT, `dup1_UNIQUESUFFIX_95946320`: BIGINT, `dup2_UNIQUESUFFIX_3095059`: " +
        "BIGINT, `dup2_UNIQUESUFFIX_3095059`: STRING, `good_col`: BIGINT, `jkl`: BIGINT, `otherexcept`: BIGINT>, " +
        "`zyx`: BIGINT>,`i_1` BIGINT,`parentwspace` STRING,`validParent` STRING"
      val ddlFromLogic = df.scrubSchema(exceptionScrubber).schema.toDDL
      assertResult(expectedResString) {
        ddlFromLogic
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
