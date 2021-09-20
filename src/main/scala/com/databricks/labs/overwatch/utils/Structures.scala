package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.pipeline.{Module, PipelineTable}
import com.databricks.labs.overwatch.utils.OverwatchScope.OverwatchScope
import com.databricks.labs.overwatch.validation.SnapReport
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.Date

case class DBDetail()

case class SparkDetail()

case class GangliaDetail()

case class TokenSecret(scope: String, key: String)

case class DataTarget(databaseName: Option[String], databaseLocation: Option[String], etlDataPathPrefix: Option[String],
                      consumerDatabaseName: Option[String] = None, consumerDatabaseLocation: Option[String] = None)

case class DatabricksContractPrices(
                                     interactiveDBUCostUSD: Double = 0.55,
                                     automatedDBUCostUSD: Double = 0.15,
                                     sqlComputeDBUCostUSD: Double = 0.22,
                                     jobsLightDBUCostUSD: Double = 0.10
                                   )

case class ApiEnv(isLocal: Boolean, workspaceURL: String, rawToken: String, packageVersion: String)

case class ValidatedColumn(
                            column: Column,
                            fieldToValidate: Option[StructField] = None,
                            requiredStructure: Option[StructField] = None
                          )

object TimeTypesConstants {
  val dtStringFormat: String = "yyyy-MM-dd"
  val tsStringFormat: String = "yyyy-MM-dd HH:mm:ss"
  val tsFormat: SimpleDateFormat = new SimpleDateFormat(tsStringFormat)
  val dtFormat: SimpleDateFormat = new SimpleDateFormat(dtStringFormat)
}

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

case class AuditLogConfig(
                           rawAuditPath: Option[String] = None,
                           auditLogFormat: String = "json",
                           azureAuditLogEventhubConfig: Option[AzureAuditLogEventhubConfig] = None
                         )

case class IntelligentScaling(enabled: Boolean = false, minimumCores: Int = 4, maximumCores: Int = 512, coeff: Double = 1.0)

case class OverwatchParams(auditLogConfig: AuditLogConfig,
                           tokenSecret: Option[TokenSecret] = None,
                           dataTarget: Option[DataTarget] = None,
                           badRecordsPath: Option[String] = None,
                           overwatchScope: Option[Seq[String]] = None,
                           maxDaysToLoad: Int = 60,
                           databricksContractPrices: DatabricksContractPrices = DatabricksContractPrices(),
                           primordialDateString: Option[String] = None,
                           intelligentScaling: IntelligentScaling = IntelligentScaling()
                          )

case class ParsedConfig(
                         auditLogConfig: AuditLogConfig,
                         overwatchScope: Seq[String],
                         tokenUsed: String, //TODO - Convert to enum
                         targetDatabase: String,
                         targetDatabaseLocation: String,
                         passthroughLogPath: Option[String],
                         packageVersion: String
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
                               status: String,
                               recordsAppended: Long,
                               lastOptimizedTS: Long,
                               vacuumRetentionHours: Int,
                               inputConfig: OverwatchParams,
                               parsedConfig: ParsedConfig
                             ) {
  def simple: SimplifiedModuleStatusReport = {
    SimplifiedModuleStatusReport(
      organization_id,
      moduleID,
      moduleName,
      primordialDateString,
      runStartTS,
      runEndTS,
      fromTS,
      untilTS,
      status,
      recordsAppended,
      lastOptimizedTS,
      vacuumRetentionHours
    )
  }
}

case class SimplifiedModuleStatusReport(
                                         organization_id: String,
                                         moduleID: Int,
                                         moduleName: String,
                                         primordialDateString: Option[String],
                                         runStartTS: Long,
                                         runEndTS: Long,
                                         fromTS: Long,
                                         untilTS: Long,
                                         status: String,
                                         recordsAppended: Long,
                                         lastOptimizedTS: Long,
                                         vacuumRetentionHours: Int
                                       )

case class IncrementalFilter(cronField: StructField, low: Column, high: Column)

case class UpgradeReport(db: String, tbl: String, errorMsg: Option[String])

object OverwatchScope extends Enumeration {
  type OverwatchScope = Value
  val jobs, clusters, clusterEvents, sparkEvents, audit, notebooks, accounts, pools = Value
  // Todo Issue_77
}

object WriteMode extends Enumeration {
  type WriteMode = Value
  val append: Value = Value("append")
  val overwrite: Value = Value("overwrite")
  val merge: Value = Value("merge")
}

// Todo Issue_56
private[overwatch] class NoNewDataException(s: String, val level: Level, val allowModuleProgression: Boolean = false) extends Exception(s) {}

private[overwatch] class UnhandledException(s: String) extends Exception(s) {}

private[overwatch] class IncompleteFilterException(s: String) extends Exception(s) {}

private[overwatch] class ApiCallFailure(s: String) extends Exception(s) {}

private[overwatch] class TokenError(s: String) extends Exception(s) {}

private[overwatch] class InvalidInstanceDetailsException(
                                                          instanceDetails: DataFrame,
                                                          dfCheck: DataFrame)
  extends Exception() with SparkSessionWrapper {

  import spark.implicits._
  private val erroredRecordsReport = instanceDetails
    .withColumn("API_name", lower(trim('API_name)))
    .join(
      dfCheck
        .select(
          lower(trim('API_name)).alias("API_name"),
          'rnk, 'rn, 'previousUntil,
          datediff('activeFrom, 'previousUntil).alias("daysBetweenCurrentAndPrevious")
        ),
      Seq("API_name")
    )
    .orderBy('API_name, 'activeFrom, 'activeUntil)

  println("InstanceDetails Error Report: ")
  erroredRecordsReport.show(numRows = 1000, false)

  private val badRecords = dfCheck.count()
  private val badRawKeys = dfCheck.select('API_name).as[String].collect().mkString(", ")
  private val errMsg = s"InstanceDetails Invalid: API_name keys with errors are: $badRawKeys. " +
    s"A total of $badRecords records were found in conflict. Each key (API_name) must be unique for a given time period " +
    s"(activeFrom --> activeUntil) AND the previous costs activeUntil must run through the previous date " +
    s"such that the function 'datediff' returns 1. Please correct the instanceDetails table before continuing " +
    s"with this module. "
  throw new BadConfigException(errMsg)
}

private[overwatch] class PipelineStateException(s: String, val target: Option[PipelineTable]) extends Exception(s) {}

private[overwatch] class BadConfigException(s: String, val failPipeline: Boolean = true) extends Exception(s) {}

private[overwatch] class FailedModuleException(s: String, val target: PipelineTable) extends Exception(s) {}

private[overwatch] class UnsupportedTypeException(s: String) extends Exception(s) {}

private[overwatch] class BadSchemaException(s: String) extends Exception(s) {}

private[overwatch] class UpgradeException(s: String, target: PipelineTable) extends Exception(s) {
  def getUpgradeReport: UpgradeReport = {
    UpgradeReport(target.databaseName, target.name, Some(s))
  }
}

private[overwatch] class BronzeSnapException(
                                              s: String,
                                              target: PipelineTable,
                                              module: Module
                                            ) extends Exception(s) {
  private val pipeline = module.pipeline
  private val emptyModule = pipeline.getModuleState(module.moduleId).isEmpty
  private val fromTime = if (emptyModule) {
    new Timestamp(pipeline.primordialTime.asUnixTimeMilli)
  } else {
    new Timestamp(module.fromTime.asUnixTimeMilli)
  }

  private val untilTime = if (emptyModule) {
    new Timestamp(pipeline.pipelineSnapTime.asUnixTimeMilli)
  } else {
    new Timestamp(module.untilTime.asUnixTimeMilli)
  }

  val errMsg = s"FAILED SNAP: ${target.tableFullName} --> MODULE: ${module.moduleName}: SNAP ERROR:\n$s"
  val snapReport: SnapReport = SnapReport(
    target.tableFullName,
    fromTime,
    untilTime,
    errMsg
  )
}

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

  implicit def moduleStatusReport: org.apache.spark.sql.Encoder[ModuleStatusReport] =
    org.apache.spark.sql.Encoders.kryo[ModuleStatusReport]
}

object Schemas {

  final val reportSchema: StructType = ScalaReflection.schemaFor[ModuleStatusReport].dataType.asInstanceOf[StructType]

}