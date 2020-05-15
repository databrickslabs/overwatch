package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.OverwatchScope.OverwatchScope
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

case class DBDetail()

case class SparkDetail()

case class GangliaDetail()

case class TokenSecret(scope: String, key: String)

case class DataTarget(databaseName: Option[String], databaseLocation: Option[String])

case class ApiEnv(isLocal: Boolean, workspaceURL: String, encryptedToken: Array[Byte], cipher: Cipher)

case class OverwatchParams(tokenSecret: Option[TokenSecret],
                           dataTarget: Option[DataTarget],
                           auditLogPath: Option[String],
                           badRecordsPath: Option[String],
                           overwatchScope: Option[Seq[String]]
                          )

case class ParsedConfig(
                         overwatchScope: Seq[String],
                         tokenUsed: String, //TODO - Convert to enum
                         targetDatabase: String,
                         targetDatabaseLocation: String,
                         auditLogPath: Option[String],
                         passthroughLogPath: Option[String]
                       )

case class ModuleStatusReport(
                               moduleID: Int,
                               moduleName: String,
                               runStartTS: Long,
                               runEndTS: Long,
                               fromTS: Long,
                               untilTS: Long,
                               dataFrequency: String,
                               status: String,
                               recordsAppended: Long,
                               lastOptimizedTS: Long,
                               vacuumRetentionHours: Int,
                               inputConfig: OverwatchParams,
                               parsedConfig: ParsedConfig
                             )

object OverwatchScope extends Enumeration {
  type OverwatchScope = Value
  val jobs, jobRuns, clusters, clusterEvents, sparkEvents, pools, audit, iamPassthrough, profiles = Value
}

object Layer extends Enumeration {
  type Layer = Value
  val bronze, silver, gold, consumption = Value
}

object Frequency extends Enumeration {
  type Frequency = Value
  val milliSecond, daily = Value
}

private[overwatch] class NoNewDataException(s: String) extends Exception(s) {}
private[overwatch] class ApiCallFailure(s: String) extends Exception(s) {}

object OverwatchEncoders {
  implicit def overwatchScopeValues: org.apache.spark.sql.Encoder[Array[OverwatchScope.Value]] =
    org.apache.spark.sql.Encoders.kryo[Array[OverwatchScope.Value]]

  implicit def overwatchScope: org.apache.spark.sql.Encoder[OverwatchScope] =
    org.apache.spark.sql.Encoders.kryo[OverwatchScope]

  implicit def tokenSecret: org.apache.spark.sql.Encoder[TokenSecret] =
    org.apache.spark.sql.Encoders.kryo[TokenSecret]

  implicit def dataTarget: org.apache.spark.sql.Encoder[DataTarget] =
    org.apache.spark.sql.Encoders.kryo[DataTarget]

  implicit def parsedConfig: org.apache.spark.sql.Encoder[ParsedConfig] =
    org.apache.spark.sql.Encoders.kryo[ParsedConfig]

  implicit def overwatchParams: org.apache.spark.sql.Encoder[OverwatchParams] =
    org.apache.spark.sql.Encoders.kryo[OverwatchParams]

  implicit def frequency: org.apache.spark.sql.Encoder[Frequency] =
    org.apache.spark.sql.Encoders.kryo[Frequency]

  implicit def moduleStatusReport: org.apache.spark.sql.Encoder[ModuleStatusReport] =
    org.apache.spark.sql.Encoders.kryo[ModuleStatusReport]
}

object Schemas {

  final val reportSchema: StructType = ScalaReflection.schemaFor[ModuleStatusReport].dataType.asInstanceOf[StructType]

}