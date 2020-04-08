package com.databricks.labs.overwatch.utils

import com.databricks.backend.common.rpc.CommandContext
import org.apache.spark.sql.Column

object GlobalStructures {

  case class DBDetail()

  case class SparkDetail()

  case class GangliaDetail()

  case class TokenSecret(scope: String, key: String)

  case class DataTarget(databaseName: Option[String], databaseLocation: Option[String])

  case class OverwatchParams(tokenSecret: Option[TokenSecret],
                             dataTarget: Option[DataTarget])

}
