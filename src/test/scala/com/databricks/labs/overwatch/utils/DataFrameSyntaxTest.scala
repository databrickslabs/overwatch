package com.databricks.labs.overwatch.utils

// import com.databricks.labs.overwatch.SparkSessionTestWrapper
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.length
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.GivenWhenThen
import java.io.ByteArrayOutputStream
import org.scalatest.BeforeAndAfterEach

class DataFrameSyntaxTest
    extends AnyFunSpec
    with GivenWhenThen
    with BeforeAndAfterEach
    with SparkSessionTestWrapper
    with DataFrameSyntax[ SparkSessionTestWrapper] {


  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

   override def beforeEach(): Unit = {
    envInit( "WARN")
    // in case of any stackable traits:
    super.beforeEach()
  }

  Given( "any Spark `Dataset` (including `DataFrame`s)")

  val df: DataFrame = Seq(
    ( "1xxx5xxx10xxx15xxx20xxx25",
      "BARx5" * 5))
    .toDF(
      "foo",
      "bar")
    .withColumn( "length_foo", length( $"foo"))
    .withColumn( "length_bar", length( $"bar"))

  info( "df.schema.treeString =>")

  df.schema.treeString.linesIterator.foreach( info(_))


  describe( "implicit class `DataFrameShower`") {

    it( "implements `df.showLines()`") {

      info( "with defaults of `(numRows, truncate, vertical)` => `(20, 20, false)`")

      df.showLines().foreach( info(_))

      // assert( ???)

    }

    it( "implements `df.showLines( numRows, truncate, vertical)`") {

      df.showLines( 1, 2, true).foreach( info(_))

      // assert( ???)

    }

  }


  describe( "implicit class `DataFrameLogger`") {

    Given( "option `'overwatch.dataframelogger.level'` is set in Spark's runtime configuration to a level greater than or equal to the logger of the calling class")

    spark.conf.set( "overwatch.dataframelogger.level", "INFO")

    lazy val v = getDataFrameLoggerSparkConfValue.get
    lazy val l = getDataFrameLoggerLevel
    logger.setLevel( l)

    info( s"`'overwatch.dataframelogger.level'` is `'${v}'`")
    info( s"`log4j.Level` used by `DataFrameLogger` is `${l}`")
    info( s"`logger.getLevel` is `${logger.getLevel}`")

    assert( l.isGreaterOrEqual( logger.getLevel))


    it( "implements `df.log()` which produces truncated vertical output by default") {

      // info( df.showLines( vertical= true).mkString("\n"))

      df.log()

      // assert( ???)

    }

    it( "implements `df.log( false)`")  {

      info( "which produces vertical output without value truncation and a warning")

      // info( df.showLines( false).mkString("\n"))

      df.log( false)

      // assert( ???)

    }

    it( "implements `df.log( truncate= 0, vertical= false)`") {

      info( "which produces tabular output without value truncation accompanied by two warnings")

      // info( df.showLines( 1, 0, false).mkString("\n"))

      df.log( truncate= 0, vertical= false)

      // assert( ???)

    }

    it( "implements `df.log( numRows, truncate, vertical)`") {

      val (numRows, truncate, vertical) = ( 1, 10, true)

      info( s"(numRows, truncate, vertical) => ${(numRows, truncate, vertical)}")

      // info( df.showLines( numRows, truncate, vertical).mkString("\n"))

      df.log( numRows= numRows, truncate= truncate, vertical= vertical)

      // assert( ???)

    }


    ignore( "treats unrecognized logger levels as `OFF`") {

      spark.conf.unset( dataFrameLoggerSparkConfKey)

      spark.conf.set( dataFrameLoggerSparkConfKey, "FOO")

      assert( spark.sparkContext.appName == "SparkSessionTestWrapper")

      logger.setLevel( Level.INFO)

      val v = getDataFrameLoggerSparkConfValue().get
      val l = getDataFrameLoggerLevel()

      info( s"`'overwatch.dataframelogger.level'` is `'${v}'`")
      info( s"`logger.getLevel()` in `${this.getClass.getSimpleName}` is `${logger.getLevel}`")
      info( s"`DataFrameLogger` `Level` is `${l}`")

      assert( v == "FOO")
      assert( l == Level.OFF)

      df.log()


    }


    it( "treats unset logger level as `OFF`") {

      spark.conf.unset( dataFrameLoggerSparkConfKey)

      val v = getDataFrameLoggerSparkConfValue.getOrElse( "not set")
      val l = getDataFrameLoggerLevel()

      assert( v == "not set")

      info( s"`'overwatch.dataframelogger.level'` is ${v}")
      info( s"`logger.getLevel()` in `${this.getClass.getSimpleName}` is `${logger.getLevel}`")
      info( s"`DataFrameLogger` `Level` is `${l}`")

      assert( l == Level.OFF)


      df.log()

    }


    it( "respects the log level of the calling class") {

      spark.conf.set( "overwatch.dataframelogger.level", "DEBUG")

      // val l: Level = Level.toLevel( spark.conf.get( "overwatch.dataframelogger.level"))

      lazy val l = getDataFrameLoggerLevel()

      logger.setLevel( Level.INFO)

      val out =
        s"""| `'overwatch.dataframelogger.level'` is `'${l}'` (`${l.toInt}`);
            | `logger.getLevel()` in `${this.getClass.getSimpleName}`
            | is `${logger.getLevel}` (`${logger.getLevel.toInt}`)"""
          .stripMargin.linesIterator.mkString

      info( out)

      logger.info( out)

      assert(  logger.getLevel.isGreaterOrEqual( getDataFrameLoggerLevel))

      df.log()

     }


  }


}
