package com.databricks.labs.overwatch.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.databricks.backend.common.rpc.CommandContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, from_unixtime, lit}

object Global {

  case class DBDetail()

  case class SparkDetail()

  case class GangliaDetail()

  case class TokenSecret(scope: String, key: String)

  case class DataTarget(databaseName: Option[String], databaseLocation: Option[String])

  case class OverwatchParams(tokenSecret: Option[TokenSecret],
                             dataTarget: Option[DataTarget])

  case class TimeTypes(asUnixTime: Long, asColumnTS: Column, asJavaDate: Date, asString: String)

  private var _fromTime: Long = _
  private var _pipelineSnapTime: Long = _
  private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private[overwatch] def setFromTime(value: Long): Boolean = ???
  private[overwatch] def setFromTime(value: Date): Boolean = ???
  private[overwatch] def setPipelineSnapTime(value: Long): Boolean = ???
  private[overwatch] def setPipelineSnapTime(value: Date): Boolean = ???
  private[overwatch] def fromTime: TimeTypes = {
    TimeTypes(
      _fromTime,
      from_unixtime(lit(_fromTime / 1000)),
      new Date(_fromTime),
      fmt.format(new Date(_fromTime))
    )
  }
  private[overwatch] def pipelineSnapTime: TimeTypes = {
    TimeTypes(
      _pipelineSnapTime,
      from_unixtime(lit(_pipelineSnapTime / 1000)),
      new Date(_pipelineSnapTime),
      fmt.format(new Date(_fromTime))
    )
  }


}
