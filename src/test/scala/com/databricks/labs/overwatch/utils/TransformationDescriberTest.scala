package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.GivenWhenThen
import java.io.ByteArrayOutputStream

class TransformationDescriberTest
    extends AnyFunSpec
    with GivenWhenThen
    with SparkSessionTestWrapper {

  import TransformationDescriber._
  import spark.implicits._
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val t = (df: DataFrame) => df.select( $"foo")

  val nt = NamedTransformation( t)

  // TODO: replace use of `s` and `Console.withOut` with an abstraction

  val s = new ByteArrayOutputStream

  describe( "A NamedTransformation") {

    it( "wraps a function literal") {

      info( s"nt.transformation: ${nt.transformation}")

      assert( nt.transformation === t)

    }

    it( "knows its own name") {

      info( s"`nt.name`: ${nt.name}")
      info( s"`nt.toString`: ${nt.toString}")

      assert( nt.name === "nt")
      assert( nt.toString === "nt: NamedTransformation")

    }

    Given( "a Spark `Dataset` (including `DataFrame`s)")

    val in = Seq( ("foo", "bar")).toDF( "foo", "bar")

    Console.withOut( s) {
      in.show(numRows= 1, truncate= 0, vertical= true)
    }
    // info( s.toString)
    s.toString.linesIterator.foreach( info(_))
    s.reset

    When( "a `NamedTransformation` is applied")

    val out = in.transformWithDescription( nt)

    // val s = new ByteArrayOutputStream
    Console.withOut( s) {
      out.show(numRows= 1, truncate= 0, vertical= true)
    }
    // info( s.toString)
    s.toString.linesIterator.foreach( info(_))



    Then( "the resulting Spark jobs have a matching description (pending)")

    // info( s"""spark.jobGroup.id: ${out.sparkSession.sparkContext.getLocalProperty( "spark.jobGroup.id")}""")

    val sjd = out.sparkSession.sparkContext.getLocalProperty( "spark.job.description")

    info( s"spark.job.description: ${sjd}")

    assert( sjd === "nt: NamedTransformation")

    // info( s"""spark.callSite.short: ${out.sparkSession.sparkContext.getLocalProperty( "spark.callSite.short")}""")
    // info( s"""spark.callSite.long: ${out.sparkSession.sparkContext.getLocalProperty( "spark.callSite.long")}""")

    




    And( "the result of the transformation is correct")

    assertResult( "`foo` STRING") {
      out.schema.toDDL
    }

    assertResult( "foo") {
      out.first.getString(0)
    }


  }


}
