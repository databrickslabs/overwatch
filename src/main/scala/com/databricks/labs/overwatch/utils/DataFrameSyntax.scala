package com.databricks.labs.overwatch.utils


import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.Dataset
import java.io.ByteArrayOutputStream
import scala.util.{ Try, Success, Failure}

trait DataFrameSyntax[ SPARK <: SparkSessionWrapper] {

  self: SPARK =>

  /**
    * Spark's native `df.show()` produces formatted, tabular output on
    * the console but provides no ability to capture that output for
    * any programmatic manipulation.  This trait implements two
    * additional extension methods for Spark `Dataset[T]`s (including
    * `DataFrame`s):
    *
    * - `df.showLines()` returns an `Iterator[String]` suitable for
    *   any programmatic manipulation
    *
    * - `df.log()` uses `.showLines()` internally to redirect the
    *   formatted output to a logger
    *
    * These methods are overloaded in way that closely mimics the
    * behavior of Spark's built-in `Dataset.show()`.  The exception is
    * that `df.log()` defaults to vertical output, i.e. one line per
    * field per row.  Regular tabular output of wide
    * `DataFrame`/`Dataset`s are likely to overwhelm the logger and/or
    * impose severe performance penalties on the Overwatch
    * application.
    *
    * Examples:
    *
    * Given:
    * {{{
    * val debugDF: DataFrame = ???
    * }}}
    *
    * This produces "vertical" output on the console using Spark's native method:
    * }}}
    * debugDF.show( debugDF.count.toInt, 0, true)
    * }}}
    *
    * {{{
    * -RECORD 0---------------------------------
    *  organization_id       | 2753962522174656
    *  jobId                 | 903015066329560
    *  fromMS                | 1709419775381
    * . . .
    * -RECORD 1---------------------------------
    * . . .
    * }}}
    *
    * This captures the same text in an in-memory data structure:
    * {{{
    * val lines: Iterator[String] =
    *   debugDF.showLines( debugDF.count.toInt, 0, true)
    * }}}
    *
    *
    * This, when called from within the `overwatch.pipelines.Silver`
    * class, produces log entries according to the logging level
    * specified by the Spark conf:
    *
    * {{{
    * spark.conf.set( "overwatch.dataframelogger.level", "OFF")
    *
    * debugDF.log( debugDF.count.toInt, 0, true)
    * }}}
    *
    * {{{
    * <no output>
    * }}}
    *
    * {{{
    * spark.conf.set( "overwatch.dataframelogger.level", "INFO")
    *
    * debugDF.log( debugDF.count.toInt, 0, true)
    * }}}
    * 24/05/04 00:18:16 INFO Silver: -RECORD 0---------------------------------
    * 24/05/04 00:18:16 INFO Silver:  organization_id       | 2753962522174656
    * 24/05/04 00:18:16 INFO Silver:  jobId                 | 903015066329560
    * 24/05/04 00:18:16 INFO Silver:  fromMS                | 1709419775381
    * . . .
    * 24/05/04 00:18:16 INFO Silver: -RECORD 1---------------------------------
    * . . .
    * }}}
    *
    * This feature respects the logging level of the existing logger
    * in the calling class.  The value of
    * `overwatch.dataframelogger.level` must be less than or equal to
    * (in Log4J 1.x terms) that of the effective level of the logger
    * in scope, otherwise the logs will only contain an informative
    * message and no Spark action will occur.  See the test suite
    * implemntation below and log output for details.
    *
    */

  private val logger: Logger = Logger.getLogger(this.getClass)

  val dataFrameLoggerSparkConfKey = "overwatch.dataframelogger.level"

  implicit class DataFrameShower[T]( val df: Dataset[T]) {

    def showLines(): Iterator[String] =
      showLines( 20)

    def showLines( numRows: Int): Iterator[String] =
      showLines( numRows, truncate = true)

    def showLines( truncate: Boolean): Iterator[String] =
      showLines( 20, truncate)

    def showLines( numRows: Int, truncate: Boolean): Iterator[String] =
      showLines( numRows, if( truncate) 20 else 0, vertical= false)

    def showLines( numRows: Int, truncate: Int): Iterator[String] =
      showLines( numRows, truncate, vertical= false)

    def showLines(
      numRows: Int = 20,
      truncate: Int = 20,
      vertical: Boolean = false
    ): Iterator[String] = {
      val out = new ByteArrayOutputStream
      Console.withOut( out) {
        df.show( numRows, truncate, vertical) }
      val lines = out.toString.linesIterator
      out.reset()
      lines

    }
  }

  def getDataFrameLoggerSparkConfValue(): Option[ String] =
      spark.conf.getOption( dataFrameLoggerSparkConfKey)

  def getDataFrameLoggerLevel(): Level =  Try {
    getDataFrameLoggerSparkConfValue match {
      case Some( v) =>
        Level.toLevel( v)
      case None =>
        logger.log( Level.WARN,
          s"No ${dataFrameLoggerSparkConfKey} set in Spark conf;"
            + " default is OFF.")
        Level.OFF
    }
  } match {
    case Success( l) => {
      logger.log( Level.DEBUG, s"DataFrameLogger Level is ${l}")
      l
    }
    case Failure(_) => {
      logger.log( Level.WARN,
        s"${dataFrameLoggerSparkConfKey} value of ${getDataFrameLoggerSparkConfValue.get}"
          + " is not a valid logger level;"
          + " default is OFF.")
      Level.OFF
    }
  }


  implicit class DataFrameLogger[T]( val df: Dataset[T]) {

    // def log()(implicit level: Level): Unit =
    //   log( 20)

    def log( numRows: Int): Unit = //( implicit level: Level): Unit =
      log( numRows= numRows, truncate= true)

    def log( truncate: Boolean): Unit = //( implicit level: Level): Unit =
      log( truncate= if( truncate) 20 else 0)

    def log( numRows: Int, truncate: Boolean): Unit = //( implicit level: Level): Unit =
      log( numRows= numRows, truncate= if( truncate) 20 else 0)

    def log( numRows: Int, truncate: Int): Unit = //( implicit level: Level): Unit =
      log( numRows, truncate, vertical= true)

    // Not providing any signature with a `Level` for now.
    // Developers must get or set the Spark conf option above.
    //
    // def log( level: Level, numRows: Int = 20, truncate: Int = 20, vertical: Boolean = true) = ???

    def log(
      numRows : Int = 20,
      truncate: Int = 20,
      vertical: Boolean = true
    ): Unit = { // ( implicit level: Level): Unit = {

      // import DataFrameLogger._

      // @transient lazy val level = getDataFrameLoggerLevel()

      logger.log( Level.INFO,
        s"""| Overwatch DataFrame logger Spark configuration:
            |  `'${dataFrameLoggerSparkConfKey}'` ->
            | `'${getDataFrameLoggerSparkConfValue.getOrElse("")}'`"""
          .stripMargin.linesIterator.mkString)

      logger.log( Level.INFO,
        s"""| Overwatch DataFrame logger parameters:
            | `level= ${getDataFrameLoggerLevel()},
            | numRows= ${numRows},
            | truncate= ${truncate},
            | vertical= ${vertical}`"""
          .stripMargin.linesIterator.mkString)



      // level
      getDataFrameLoggerLevel() match {

        case Level.OFF =>

          logger.log( Level.INFO,
            s"""| Overwatch DataFrame logging is disabled; either set Spark
                | conf option "${dataFrameLoggerSparkConfKey}" to a defined logging level (e.g.
                | "WARN", "INFO", etc.), less than or equal to "${logger.getLevel}"
                | set in class `${this.getClass.getSimpleName}` or use Spark's
                | native `DataFrame.show()` method to produce console output."""
              .stripMargin.linesIterator.mkString)


        case l if logger.getLevel.isGreaterOrEqual( l) => {

          if( truncate == 0) logger.log( Level.WARN,
            """| Overwatch DataFrame logging is enabled but without value truncation;
               | long `STRING` columns and large/nested `MAP` or `STRUCT` columns are likely
               | to produce excessive logger output.  Please consider using truncation.  The
               | default is 20 characters per column."""
              .stripMargin.linesIterator.mkString)

          if( !vertical) logger.log( Level.WARN,
            """| Overwatch `DataFrame` logging is enabled but without vertical formatting;
               | tabular output of > 5 columns is likely to produce excessive logger output.
               | Please consider using vertical formatting (the default)."""
              .stripMargin.linesIterator.mkString)

          df.showLines( numRows, truncate, vertical)
            .foreach( logger.log( l, _))

        }

        case l =>

          logger.log( logger.getLevel,
            s"""| Overwatch DataFrame logging is enabled and set to ${l} but the
                | logger level ${logger.getLevel} set in class ${this.getClass.getSimpleName}
                | is higher; no records will appear in the logger output."""
              .stripMargin.linesIterator.mkString)


      }

    }

  }

}
