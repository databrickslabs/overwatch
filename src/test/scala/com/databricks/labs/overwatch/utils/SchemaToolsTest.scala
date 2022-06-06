package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{MapType, NullType, StringType, StructField, StructType}
import org.scalatest.GivenWhenThen
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.functions.{col, lit, size, struct}


class SchemaToolsTest extends AnyFunSpec with SparkSessionTestWrapper with GivenWhenThen {
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

  describe("Test cases for SchemaTools.structFromJson function") {
    it("should generate a struct from json string column - malformed json") {
      Given("below variables")
      val strings = Seq(
        "{\"c1\": \"r1\", \"c2\": \"r1\"}",
        "{\"c1\": \"r2\", \"c2\"}"
      ).toDS()
      val df = spark.read.json(strings)
      val c = "c1"

      When("function is called with given parameters and the schema is corrupt")

      Then("print a warning message and ")
      assertResult(StructType(Array(StructField("c3", StructType(Array(StructField("_corrupt_record", StringType, true))), true)))) (
        df.withColumn("c3", SchemaTools.structFromJson(spark, df, c)).select("c3").schema
      )
    }
    it("should throw an exception - column (top level) does not exist in the dataframe") {
      Given("below variables")
      val df = spark.createDataFrame(Seq(("c1", "c2"))).toDF("c1", "c2")
      val missingColumn = "c3"

      When("function is called with given parameters")

      Then("throws an exception - java.lang.IllegalArgumentException")
      assertThrows[java.lang.IllegalArgumentException](
        SchemaTools.structFromJson(spark = spark,
          df = df,
          c = missingColumn
        ))
    }
    it("should throw an exception - column is not a StringType") {
      Given("below variables")
      val strings = Seq(
        "{\"c1\": \"r1\", \"c2\": 1}",
        "{\"c1\": \"r2\", \"c2\": 2}"
      ).toDS()
      val df = spark.read.json(strings)
      val c = "c2"

      When("function is called with given parameters and column is not a StringType")

      Then("throws an exception - org.apache.spark.sql.AnalysisException")
      assertThrows[org.apache.spark.sql.AnalysisException](
        df.withColumn("c3", SchemaTools.structFromJson(spark, df, c))
      )
    }
    it("should return a null - column is StringType but empty") {
      Given("below variables")
      val strings = Seq(
        "{\"c1\": \"r1\", \"c2\": \"\"}",
        "{\"c1\": \"r2\", \"c2\": \"\"}"
      ).toDS()
      val df = spark.read.json(strings)
      val c = "c2"

      When("function is called with given parameters and column is not a StringType")

      Then("return a null")
      assertResult(StructType(Array(StructField("c3", NullType, true)))) (
        df.withColumn("c3", SchemaTools.structFromJson(spark, df, c)).select("c3").schema
      )
    }
  }

  describe("Test cases for SchemaTools.structToMap function") {
    it("should throw an exception - column is not a StructType") {
      Given("a dataframe without a struct column")
      val df = spark.range(1).withColumn("c1", lit("c1"))

      When("function is called on the given dataframe")

      Then("throws an exception")
      assertThrows[java.lang.Exception] (
        df.withColumn("mapCol", SchemaTools.structToMap(df, "c1"))
      )
    }
    it("should return an empty map<string, string> - null value column") {
      Given("a dataframe with a null column")
      val df = spark.range(1).withColumn("c1", lit(null))

      When("function is called on the given dataframe")

      Then("returns an empty MapType(StringType,StringType,true)")
      assertResult(MapType(StringType, StringType, true)) (
        df.withColumn("mapCol", SchemaTools.structToMap(df, "c1"))
          .select("mapCol").schema.fields.head.dataType
      )
    }
    it("should return an empty map<string, string> - input column does not exist in the df") {
      Given("a dataframe with a null column")
      val df = spark.range(1).withColumn("c1", lit("c1"))

      When("function is called on the given dataframe")

      Then("returns an empty MapType(StringType,StringType,true)")
      assertResult(MapType(StringType, StringType, true)) (
        df.withColumn("mapCol", SchemaTools.structToMap(df, "c2"))
          .select("mapCol").schema.fields.head.dataType
      )
      assertResult(null) (df.withColumn("mapCol", SchemaTools.structToMap(df, "c2"))
        .select("mapCol").first().get(0)
      )
    }
    /*
    it("should log warning and convert null struct column name to null_<randomString>") {
      /**
       * This test fails because of
       * mapCols.add(col(s"${colToConvert}.${field.name}").cast("string"))
       * Here, resultant column name is 'c3.'
       * Fails with error: syntax error in attribute name: c3.
       */
      Given("a dataframe with null/empty struct column name")
      val df = spark.range(1)
        .withColumn("c3", struct(lit("v1").alias("k1"), lit("v2").alias("")))

      When("function is called on a struct column")

      Then("")
      df.withColumn("mapCol", SchemaTools.structToMap(df, "c3"))
    }
    */
    it("should return same number of map-keys as columns in input struct - dropEmptyKeys = false") {
      /**
       * TODO - Once the above test case is resolved, this one has to be slightly modified to test null struct column
       * Changes include having a null/empty column name inside the struct
       */
      Given("a dataframe with a struct column")
      val df = spark.range(1)
        .withColumn("c3", struct(lit("v1").alias("k1"), lit("v2").alias("k2")))

      When("function is called on a struct column")

      Then("number of key-values in the converted map is same as number of key-values in the input struct")
      assertResult(df.select("c3.*").schema.fields.map(_.name).size) (
        df.withColumn("mapCol", SchemaTools.structToMap(df, "c3", false))
          .withColumn("sizeOfMapCol", size(col("mapCol")))
          .select("sizeOfMapCol").collect().head.get(0).asInstanceOf[Int]
      )
    }
    it("should return map keys only with non-null map values - dropEmptyKeys = true") {
      Given("a dataframe with a struct column")
      val df = spark.range(1)
        .withColumn("c3", struct(lit(null).alias("k1"), lit("v2").alias("k2")))

      When("function is called on a struct column and dropEmptyKeys = true")

      Then("return map keys only with non-null map values")
      assertResult(1) (
        df.withColumn("mapCol", SchemaTools.structToMap(df, "c3", true))
          .withColumn("sizeOfMapCol", size(col("mapCol")))
          .select("sizeOfMapCol").collect().head.get(0).asInstanceOf[Int]
      )
    }
    it("should return converted column with same name as colToConvert") {
      Given("a dataframe with struct column")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1"), lit("c2").alias("c2")))

      When("function is called on the struct column")

      Then("returns a map")
      assertResult("st") (
        df.withColumn("st", SchemaTools.structToMap(df, "st"))
          .select("st").schema.fields.head.name
      )
    }
    it("should return converted column (nested) with the name of all characters to the right of last period") {
      Given("a dataframe with nested struct column")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1"),
          struct(lit("c11").alias("c11"), lit("c12").alias("c12")).alias("st1"))
        )

      When("function is called on the nested struct column")


      Then("returns converted column for the nested struct")
      assertResult("c11,c12") (
        df.withColumn("mapCol", SchemaTools.structToMap(df, "st.st1"))
          .select("mapCol").collect().head.get(0).asInstanceOf[Map[String, String]].keys.mkString(",")
      )
    }
  }

  describe("Test cases for SchemaTools.modifyStruct") {
    it("should not make alterations to the resulting column if fieldName is not present in the changeInventory") {
      Given("a dataframe with struct column and a change inventory")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1"),
          struct(lit("c11").alias("c11"), lit("c12").alias("c12"), lit("c13").alias("c13")).alias("st1")
        ))

      val changeInventory = Map[String, Column]()

      When("function is call on the given df")

      Then("make no alterations to the resulting column")
      assertResult(df.select("st").schema) (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*).select("st").schema
      )
    }
    it("should make alterations to the struct columns as per the column logic") {
      Given("a dataframe with struct column and a changeInventory")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1"),
          struct(lit("c11").alias("c11"), lit("c12").alias("c12"), lit("c13").alias("c13")).alias("st1"),
          struct(lit("c21").alias("c21"), lit("c22").alias("c22"), lit("c23").alias("c23")).alias("st2")
        ))

      val changeInventory = Map[String, Column] (
        "st" -> SchemaTools.structToMap(df, "st"),
        "st.st1" -> SchemaTools.structToMap(df, "st.st1"),
        "st.st2" -> SchemaTools.structToMap(df, "st.st2")
      )

      When("function is called with given parameters")

      Then("make alterations to the struct fields as per column logic")
      assertResult(StructType(Array(StructField("st", MapType(StringType, StringType, true), true)))) (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*).select("st").schema
      )
    }
    it("should make alterations to the top level struct columns as per the changeInventory") {
      Given("a dataframe with struct column and a changeInventory")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1"),
          struct(lit("c11").alias("c11"), lit("c12").alias("c12"), lit("c13").alias("c13")).alias("st1"),
          struct(lit("c21").alias("c21"), lit("c22").alias("c22"), lit("c23").alias("c23")).alias("st2")
        ))

      val changeInventory = Map[String, Column] (
        "st" -> SchemaTools.structToMap(df, "st")
//        "st.st1" -> SchemaTools.structToMap(df, "st.st1"),
//        "st.st2" -> SchemaTools.structToMap(df, "st.st2")
      )

      When("function is called with given parameters")

      Then("make alterations to the struct fields as per column logic")
      assertResult(StructType(Array(StructField("st", MapType(StringType, StringType, true), true)))) (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*).select("st").schema
      )
    }
    it("should make alterations to the nested struct columns as per the changeInventory") {
      Given("a dataframe with struct column and a changeInventory")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1"),
          struct(lit("c11").alias("c11"), lit("c12").alias("c12"), lit("c13").alias("c13")).alias("st1"),
          struct(lit("c21").alias("c21"), lit("c22").alias("c22"), lit("c23").alias("c23")).alias("st2")
        ))

      val changeInventory = Map[String, Column] (
        "st.st1" -> SchemaTools.structToMap(df, "st.st1")
      )

      When("function is called with given parameters")

      Then("make alterations to the struct fields as per column logic")
      assertResult(StructType(Array(StructField("st1", MapType(StringType, StringType, true), true)))) (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*).select("st.st1").schema
      )
    }
    it("should return the modified struct with the same name as the original struct") {
      Given("a dataframe with struct column and a changeInventory")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1")))

      val changeInventory = Map[String, Column] (
        "st" -> SchemaTools.structToMap(df, "st")
      )

      When("function is called with given parameters")

      Then("modified struct has same name as the original struct")
      assertResult("st") (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*)
          .schema.fields.filter(_.name == "st")
          .filter(_.dataType == MapType(StringType, StringType, true)).map(_.name).head
      )
    }
    it("should make alteration to only the struct present in the changeInventory") {
      Given("a dataframe with struct column and a changeInventory")
      val df = spark.range(1)
        .withColumn("st", struct(lit("c1").alias("c1")))
        .withColumn("st1", struct(lit("c2").alias("c2")))

      val changeInventory = Map[String, Column] (
        "st" -> SchemaTools.structToMap(df, "st")
      )

      When("function is called with given parameters")

      Then("modified struct has same name as the original struct")
      assertResult(StructType(Array(StructField("st",MapType(StringType,StringType,true),true)))) (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*).select("st").schema
      )
      assertResult(df.select("st1").schema) (
        df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*).select("st1").schema
      )
    }
  }
}
