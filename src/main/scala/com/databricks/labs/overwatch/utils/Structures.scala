package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.pipeline.{Module, PipelineFunctions, PipelineTable}
import com.databricks.labs.overwatch.utils.OverwatchScope.OverwatchScope
import com.databricks.labs.overwatch.validation.SnapReport
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import scalaj.http.HttpResponse

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, ZonedDateTime}
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
                                        minEventsPerTrigger: Int = 10,
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
                           intelligentScaling: IntelligentScaling = IntelligentScaling(),
                           workspace_name: Option[String] = None,
                           externalizeOptimize: Boolean = false,
                           tempWorkingDir: String = "" // will be set after data target validated if not overridden
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
                               workspace_name: String,
                               moduleID: Int,
                               moduleName: String,
                               primordialDateString: Option[String],
                               runStartTS: Long,
                               runEndTS: Long,
                               fromTS: Long,
                               untilTS: Long,
                               status: String,
                               writeOpsMetrics: Map[String, String],
                               lastOptimizedTS: Long,
                               vacuumRetentionHours: Int,
                               inputConfig: OverwatchParams,
                               parsedConfig: ParsedConfig,
                               externalizeOptimize: Boolean
                             ) {
  def simple: SimplifiedModuleStatusReport = {
    SimplifiedModuleStatusReport(
      organization_id,
      workspace_name,
      moduleID,
      moduleName,
      primordialDateString,
      runStartTS,
      runEndTS,
      fromTS,
      untilTS,
      status,
      writeOpsMetrics,
      lastOptimizedTS,
      vacuumRetentionHours,
      externalizeOptimize
    )
  }
}

case class SimplifiedModuleStatusReport(
                                         organization_id: String,
                                         workspace_name: String,
                                         moduleID: Int,
                                         moduleName: String,
                                         primordialDateString: Option[String],
                                         runStartTS: Long,
                                         runEndTS: Long,
                                         fromTS: Long,
                                         untilTS: Long,
                                         status: String,
                                         writeOpsMetrics: Map[String, String],
                                         lastOptimizedTS: Long,
                                         vacuumRetentionHours: Int,
                                         externalizeOptimize: Boolean
                                       )

case class IncrementalFilter(cronField: StructField, low: Column, high: Column)

case class DBUCostDetail(
                          organization_id: String,
                          sku: String,
                          contract_price: Double,
                          activeFrom: LocalDate,
                          activeUntil: Option[LocalDate],
                          isActive: Boolean
                        )

case class UpgradeReport(
                          db: String,
                          tbl: String,
                          statusMsg: Option[String] = None,
                          step: Option[String] = None,
                          failUpgrade: Boolean = false
                        )

case class WorkspaceDataset(path: String, name: String)

case class WorkspaceMetastoreRegistrationReport(workspaceDataset: WorkspaceDataset, registerStatement: String, status: String)

case class CloneDetail(source: String, target: String, asOfTS: Option[String] = None, cloneLevel: String = "DEEP")

case class CloneReport(cloneSpec: CloneDetail, cloneStatement: String, status: String)

case class OrgConfigDetail(organization_id: String, latestParams: OverwatchParams)

case class DeltaHistory(version: Long, timestamp: java.sql.Timestamp, operation: String, clusterId: String, operationMetrics: Map[String, String], userMetadata: String)

/**
 * Rule for sanitizing schemas.
 * @param from regex search string
 * @param to replace string
 */
case class SanitizeRule(from: String, to: String)

/**
 * Exception to global schema sanitization rules by field
 * @param field field to which different rules should be applied
 * @param rules ordered list of rules to be applied. The rules will be applied in order incrementing
 *              from the lowest index
 * @param recursive Whether or not to recurse down a struct and apply the parent rule to all children of the struct
 */
case class SanitizeFieldException(field: StructField, rules: List[SanitizeRule], recursive: Boolean)

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

private[overwatch] class ApiCallEmptyResponse(val apiCallDetail: String, val allowModuleProgression: Boolean) extends Exception(apiCallDetail)

private[overwatch] class ApiCallFailure(
                                         val httpResponse: HttpResponse[String],
                                         apiCallDetail: String,
                                         t: Throwable = null,
                                         debugFlag: Boolean = false
                                       ) extends Exception (httpResponse.body) {
  private val logger = Logger.getLogger("ApiCall")
  private val hardFailErrors = Array(401, 404, 407)
  var failPipeline: Boolean = false

  private def buildAPIErrorMessage(r: HttpResponse[String]): String = {
    s"API CALL FAILED: ErrorCode: ${r.code} Databricks Message: " +
      s"${r.headers.getOrElse("x-databricks-reason-phrase", Vector("NA")).reduce(_ + " " + _)} " +
      s"StatusLine: ${r.statusLine} " +
      s"\nHTML Response Body: -- ${r.body}\n" +
      s"API CALL DETAILS: \n " +
      s"$apiCallDetail"
  }

  if (hardFailErrors.contains(httpResponse.code)) failPipeline = true
  private val logLevel = if (failPipeline) Level.ERROR else Level.WARN
  val msg: String = buildAPIErrorMessage(httpResponse)
  if (debugFlag) println(msg)
  if (t != null) logger.log(logLevel, msg, t) else logger.log(logLevel, msg)
}

private[overwatch] class TokenError(s: String) extends Exception(s) {}

/**
 * Specialized error to
 *
 * @param target          Pipeline Source throwing the error
 * @param invalidReportDF details of keys causing the error
 * @param fromCol         column name of the active start date or time
 * @param untilCol        column name of the expiry date or time, null == active
 */
private[overwatch] class InvalidType2Input(
                                            target: PipelineTable,
                                            invalidReportDF: DataFrame,
                                            fromCol: String,
                                            untilCol: String
                                          )
  extends Exception() with SparkSessionWrapper {

  import spark.implicits._

  private val erroredRecordsReport = target.asDF
    .join(
      invalidReportDF,
      target.keys
    )
    .orderBy(target.keys.map(col) ++ Array(col(fromCol), col(untilCol)): _*)

  println("InstanceDetails Error Report: ")
  erroredRecordsReport.show(numRows = 1000, false)

  private val badRecords = invalidReportDF.count()
  private val badRawKeys = invalidReportDF.select(target.keys.map(col): _*).drop("organization_id").distinct()
    .as[String].collect().mkString(", ")
  private val errMsg = s"${target.tableFullName} Invalid: ${target.keys.mkString(", ")} keys with errors are: $badRawKeys. " +
    s"A total of $badRecords records were found in conflict. " +
    s"There cannot be any gaps or overlaps in time for a specific key such that " +
    s"datediff($fromCol --> $untilCol) returns 0 for all keys. Please correct the data in the table " +
    s"before continuing with Gold Pipeline."
  throw new BadConfigException(errMsg)
}

private[overwatch] class PipelineStateException(s: String, val target: Option[PipelineTable]) extends Exception(s) {
  println(s)
}

private[overwatch] class BadConfigException(s: String, val failPipeline: Boolean = true) extends Exception(s) {
  println(s)
}

private[overwatch] class FailedModuleException(s: String, val target: PipelineTable) extends Exception(s) {
  println(s)
}

private[overwatch] class UnsupportedTypeException(s: String) extends Exception(s) {
  println(s)
}

private[overwatch] class BadSchemaException(s: String) extends Exception(s) {
  println(s)
}

private[overwatch] class UpgradeException(s: String, target: PipelineTable, step: Option[String] = None, failUpgrade: Boolean = false) extends Exception(s) {
  def getUpgradeReport: UpgradeReport = {
    UpgradeReport(target.databaseName, target.name, Some(PipelineFunctions.appendStackStrace(this, s)), step, failUpgrade)
  }
}

private[overwatch] class SimplifiedUpgradeException(s: String, db: String, table: String, step: Option[String] = None, failUpgrade: Boolean = false) extends Exception(s) {
  def getUpgradeReport: UpgradeReport = {
    UpgradeReport(db, table, Some(PipelineFunctions.appendStackStrace(this, s)), step, failUpgrade)
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