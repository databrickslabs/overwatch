package com.databricks.labs.overwatch.utils

import com.databricks.backend.common.rpc.CommandContext

object GlobalStructures {



  case class DBDetail()

  case class SparkDetail()

  case class GangliaDetail()

  case class TokenSecret(scope: Option[String], key: Option[String])

  case class OverwatchParams(tokenSecret: Option[TokenSecret])

}
