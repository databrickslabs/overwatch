package com.databricks.labs.overwatch.utils

import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.Dataset
import java.io.ByteArrayOutputStream

trait DataFrameSyntax {

  /**
    * Implements extension methods for Spark `Dataset[T]`s (including
    * `DataFrame`s) that mimic Spark's built-in `Dataset.show()`
    * family of overloaded methods.
    *
    * Instead of producing output on the console, they return an
    * `Iterator[String]` suitable for redirecting output to a logger,
    * for example, like this:
    *
    * {{{
    * debugDF
    *   .showLines( debugDF.count.toInt, 0, true)
    *   .foreach( logger.log( Level.INFO, _))
    * }}}
    *
    * Produces log entries according to your logger configuration:
    *
    * {{{
    * 24/05/04 00:18:16 INFO Silver: -RECORD 0---------------------------------
    * 24/05/04 00:18:16 INFO Silver:  organization_id       | 2753962522174656
    * 24/05/04 00:18:16 INFO Silver:  jobId                 | 903015066329560
    * 24/05/04 00:18:16 INFO Silver:  fromMS                | 1709419775381
    * . . .
    * 24/05/04 00:18:16 INFO Silver: -RECORD 1---------------------------------
    * . . .
    * }}}
    */

  implicit class DataFrameShower[T]( val df: Dataset[T]) {

    def showLines(): Iterator[String] =
      showLines( 20)

    def showLines( numRows: Int): Iterator[String] =
      showLines( numRows, truncate = true)

    def showLines( truncate: Boolean): Iterator[String] =
      showLines( 20, truncate)

    def showLines( numRows: Int, truncate: Boolean): Iterator[String] =
      if( truncate) {
        showLines( numRows, truncate = 20)
      } else {
        showLines( numRows, truncate = 0)
      }

    def showLines( numRows: Int, truncate: Int): Iterator[String] =
      showLines( numRows, truncate, vertical = false)

    def showLines( numRows: Int, truncate: Int, vertical: Boolean): Iterator[String] = {
      val out = new ByteArrayOutputStream
      Console.withOut( out) {
        df.show( numRows, truncate, vertical) }
      val lines = out.toString.linesIterator
      out.reset()
      lines

    }
  }
}

