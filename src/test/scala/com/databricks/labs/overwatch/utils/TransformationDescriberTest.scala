package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.GivenWhenThen

class TransformationDescriberTest
    extends AnyFunSpec
    with GivenWhenThen
    with SparkSessionTestWrapper {

  import TransformationDescriber._
  import spark.implicits._
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val t = (df: DataFrame) => df.select( $"foo")

  val nt = NamedTransformation( t)

  describe( "A NamedTransformation") {

    it( "wraps a function literal") {

      assert( nt.transformation === t)

    }

    it( "knows its own name") {

      assert( nt.name === "nt")

    }

    Given( "a Spark `Dataset` (including `DataFrame`s)")

    val in = Seq( ("foo", "bar")).toDF( "foo", "bar")


    When( "a `NamedTransformation` is applied")

    val out = in.transformWithDescription( nt)


    Then( "the resulting Spark jobs have a matching description (pending)")

    // spark.sc.


    And( "the result of the transformation is correct")

    assertResult( "`foo` STRING") {
      out.schema.toDDL
    }

    assertResult( "foo") {
      out.first.getString(0)
    }


  }


}
