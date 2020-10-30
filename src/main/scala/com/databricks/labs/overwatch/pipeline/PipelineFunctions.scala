package com.databricks.labs.overwatch.pipeline

import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{lit, date_add}

object PipelineFunctions {
  def addOneTick(ts: Column, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 + 1) / 1000).cast("timestamp")
      case _: DateType =>
        date_add(ts, 1)
      case _: DoubleType =>
        ts + 0.001d
      case _: LongType =>
        ts + 1
      case _: IntegerType =>
        ts + 1
      case _ => throw
        new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }
}
