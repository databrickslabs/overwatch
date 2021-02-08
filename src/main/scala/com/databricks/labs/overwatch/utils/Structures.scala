package com.databricks.labs.overwatch.utils

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.Date

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.OverwatchScope.OverwatchScope
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

case class DBDetail()

case class SparkDetail()

case class GangliaDetail()

case class TokenSecret(scope: String, key: String)

case class DataTarget(databaseName: Option[String], databaseLocation: Option[String],
                      consumerDatabaseName: Option[String] = None, consumerDatabaseLocation: Option[String] = None)

case class DatabricksContractPrices(interactiveDBUCostUSD: Double, automatedDBUCostUSD: Double)

case class ApiEnv(isLocal: Boolean, workspaceURL: String, rawToken: String, encryptedToken: Array[Byte], cipher: Cipher)

// Todo Add Description
case class Module(moduleID: Int, moduleName: String)

object TimeTypesConstants {
  val dtStringFormat: String = "yyyy-MM-dd"
  val tsStringFormat: String = "yyyy-MM-dd HH:mm:ss"
  val tsFormat: SimpleDateFormat = new SimpleDateFormat(tsStringFormat)
  val dtFormat: SimpleDateFormat = new SimpleDateFormat(dtStringFormat)
}

// TODO: we need to try to get rid of the local date/time everywhere...
case class TimeTypes(asUnixTimeMilli: Long, asColumnTS: Column, asJavaDate: Date,
                     asUTCDateTime: ZonedDateTime, asLocalDateTime: LocalDateTime, asMidnightEpochMilli: Long) {

  lazy val asUnixTimeS: Long = asUnixTimeMilli / 1000
  lazy val asTSString: String = TimeTypesConstants.tsFormat.format(asJavaDate)
  lazy val asDTString: String = TimeTypesConstants.dtFormat.format(asJavaDate)
}

case class AzureAuditLogEventhubConfig(
                                        connectionString: String,
                                        eventHubName: String,
                                        auditRawEventsPrefix: String,
                                        maxEventsPerTrigger: Int = 10000,
                                        auditRawEventsChk: Option[String] = None,
                                        auditLogChk: Option[String] = None
                                      )

case class AuditLogConfig(rawAuditPath: Option[String] = None, azureAuditLogEventhubConfig: Option[AzureAuditLogEventhubConfig] = None)

case class OverwatchParams(auditLogConfig: AuditLogConfig,
                           tokenSecret: Option[TokenSecret] = None,
                           dataTarget: Option[DataTarget] = None,
                           badRecordsPath: Option[String] = None,
                           overwatchScope: Option[Seq[String]] = None,
                           maxDaysToLoad: Int = 60,
                           databricksContractPrices: DatabricksContractPrices = DatabricksContractPrices(0.56, 0.26),
                           primordialDateString: Option[String] = None
                          )

case class ParsedConfig(
                         auditLogConfig: AuditLogConfig,
                         overwatchScope: Seq[String],
                         tokenUsed: String, //TODO - Convert to enum
                         targetDatabase: String,
                         targetDatabaseLocation: String,
                         passthroughLogPath: Option[String]
                       )

case class ModuleStatusReport(
                               organization_id: String,
                               moduleID: Int,
                               moduleName: String,
                               primordialDateString: Option[String],
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

case class SimplifiedModuleStatusReport(
                                         organization_id: String,
                                         moduleID: Int,
                                         moduleName: String,
                                         primordialDateString: Option[String],
                                         runStartTS: Long,
                                         runEndTS: Long,
                                         fromTS: Long,
                                         untilTS: Long,
                                         dataFrequency: String,
                                         status: String,
                                         recordsAppended: Long,
                                         lastOptimizedTS: Long,
                                         vacuumRetentionHours: Int
                                       )

case class IncrementalFilter(cronColName: String, low: Column, high: Column)

object OverwatchScope extends Enumeration {
  type OverwatchScope = Value
  val jobs, clusters, clusterEvents, sparkEvents, audit, notebooks, accounts, pools = Value
  // TODO - iamPassthrough, profiles, pools
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

private[overwatch] class UnhandledException(s: String) extends Exception(s) {}

private[overwatch] class ApiCallFailure(s: String) extends Exception(s) {}

private[overwatch] class TokenError(s: String) extends Exception(s) {}

private[overwatch] class BadConfigException(s: String) extends Exception(s) {}

private[overwatch] class FailedModuleException(s: String) extends Exception(s) {}

private[overwatch] class UnsupportedTypeException(s: String) extends Exception(s) {}

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