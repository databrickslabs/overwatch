package com.databricks.labs.overwatch.utils

import com.databricks.backend.common.rpc.CommandContext
import org.apache.spark.sql.Column

object GlobalStructures {

//  case class ColDetails(origCol: Column, origColName: String, aggName: String, bounds: Bounds)
  case class RuleDetails(ruleName: String, origCol: Column, origColName: String, aggName: String, bounds: Bounds)

  case class Rule(ruleName: String, column: Column, aggFunc: Column => Column,
                  alias: String, boundaries: Bounds, by: Column*)

  case class Bounds(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity)

  case class Result(ruleName: String, colName: String, boundaries: Bounds, actualVal: Double, passed: Boolean)

  case class DBDetail()

  case class SparkDetail()

  case class GangliaDetail()

  case class TokenSecret(scope: String, key: String)

  case class DataTarget(databaseName: Option[String], databaseLocation: Option[String])

  case class OverwatchParams(tokenSecret: Option[TokenSecret],
                             dataTarget: Option[DataTarget])

}
