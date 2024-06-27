package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Module, PipelineFunctions, PipelineTable}
import com.databricks.labs.overwatch.utils.OverwatchScope.OverwatchScope
import com.databricks.labs.overwatch.validation.SnapReport
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StructField, StructType}
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

case class DataTarget(databaseName: Option[String],
                      databaseLocation: Option[String],
                      etlDataPathPrefix: Option[String],
                      consumerDatabaseName: Option[String] = None,
                      consumerDatabaseLocation: Option[String] = None){
   private[overwatch] def deriveDatabaseName: String = {
    if(databaseName.count(_ == '.')>1) throw new Exception("Invalid Database name. Please check the number of '.' in the name" +
      "It should be either 0 or 1" )
    databaseName.get.split("\\.").last
  }

  private[overwatch] def deriveConsumerDatabaseName: String = {
    if(consumerDatabaseName.count(_ == '.')>1) throw new Exception("Invalid Database name. Please check the number of '.' in the name" +
      "It should be either 0 or 1" )
    consumerDatabaseName.get.split("\\.").last
  }

  private[overwatch] def deriveEtlCatalogName: String = {
    if(databaseName.count(_ == '.')>1) throw new Exception("Invalid Database name. Please check the number of '.' in the name" +
      "It should be either 0 or 1" )
    databaseName.get.split("\\.").head
  }

  private[overwatch] def deriveConsumerCatalogName: String = {
    if(consumerDatabaseName.count(_ == '.')>1) throw new Exception("Invalid Database name. Please check the number of '.' in the name" +
      "It should be either 0 or 1" )
    consumerDatabaseName.get.split("\\.").head
  }
}

case class DatabricksContractPrices(
                                     interactiveDBUCostUSD: Double = 0.55,
                                     automatedDBUCostUSD: Double = 0.15,
                                     sqlComputeDBUCostUSD: Double = 0.22,
                                     jobsLightDBUCostUSD: Double = 0.10
                                   )

case class ApiEnv(
                   isLocal: Boolean,
                   workspaceURL: String,
                   rawToken: String,
                   packageVersion: String,
                   successBatchSize: Int = 200,
                   errorBatchSize: Int = 500,
                   runID: String = "",
                   enableUnsafeSSL: Boolean = false,
                   threadPoolSize: Int = 4,
                   apiWaitingTime: Long = 300000,
                   proxyHost: Option[String] = None,
                   proxyPort: Option[Int] = None,
                   proxyUserName: Option[String] = None,
                   proxyPasswordScope: Option[String] = None,
                   proxyPasswordKey: Option[String] = None,
                   mountMappingPath: Option[String] = None
                 )


case class ApiEnvConfig(
                         successBatchSize: Int = 200,
                         errorBatchSize: Int = 500,
                         enableUnsafeSSL: Boolean = false,
                         threadPoolSize: Int = 4,
                         apiWaitingTime: Long = 300000,
                         apiProxyConfig: Option[ApiProxyConfig] = None,
                         mountMappingPath: Option[String] = None
                       )

case class ApiProxyConfig(
                           proxyHost: Option[String] = None,
                           proxyPort: Option[Int] = None,
                           proxyUserName: Option[String] = None,
                           proxyPasswordScope: Option[String] = None,
                           proxyPasswordKey: Option[String] = None
                         )

case class MultiWorkspaceConfig(workspace_name: String,
                                workspace_id: String,
                                workspace_url: String,
                                api_url: String,
                                cloud: String,
                                primordial_date: java.sql.Date,
                                storage_prefix: String,
                                etl_database_name: String,
                                consumer_database_name: String,
                                secret_scope: String,
                                secret_key_dbpat: String,
                                auditlogprefix_source_path: Option[String],
                                eh_name: Option[String],
                                eh_scope_key: Option[String],
                                interactive_dbu_price: Double,
                                automated_dbu_price: Double,
                                sql_compute_dbu_price: Double,
                                jobs_light_dbu_price: Double,
                                max_days: Int,
                                excluded_scopes: Option[String],
                                active: Boolean,
                                proxy_host: Option[String] = None,
                                proxy_port: Option[Int] = None,
                                proxy_user_name: Option[String] = None,
                                proxy_password_scope: Option[String] = None,
                                proxy_password_key: Option[String] = None,
                                success_batch_size: Option[Int] = None,
                                error_batch_size: Option[Int] = None,
                                enable_unsafe_SSL: Option[Boolean]= None,
                                thread_pool_size:  Option[Int] = None,
                                api_waiting_time:  Option[Long] = None,
                                mount_mapping_path: Option[String],
                                eh_conn_string: Option[String],
                                aad_tenant_id: Option[String],
                                aad_client_id: Option[String],
                                aad_client_secret_key: Option[String],
                                aad_authority_endpoint: Option[String],
                                deployment_id: String,
                                output_path: String,
                                temp_dir_path: Option[String],
                                sql_endpoint: Option[String] = None
                               )
case class RulesValidationResult(ruleName: String, passed: String, permitted: String, actual: String)

case class RulesValidationReport(deployment_id: String, workspace_id: String, result: RulesValidationResult)

case class ReconReport(
                        validated: Boolean = false,
                        workspaceId: String,
                        reconType: String,
                        sourceDB: String,
                        targetDB: String,
                        tableName: String,
                        sourceCount: Option[Long] = None,
                        targetCount: Option[Long] = None,
                        missingInSource: Option[Long] = None,
                        missingInTarget: Option[Long] = None,
                        commonDataCount: Option[Long] = None,
                        deviationPercentage: Option[Double] = None,
                        sourceQuery: Option[String] = None,
                        targetQuery: Option[String] = None,
                        errorMsg: Option[String] = None
                      )

object MultiWorkspaceConfigColumns extends Enumeration {
  val workspace_name, workspace_id, workspace_url, api_url, cloud, primordial_date,
  storage_prefix, etl_database_name, consumer_database_name, secret_scope,
  secret_key_dbpat, auditlogprefix_source_path, eh_name, eh_scope_key, scopes,
  interactive_dbu_price, automated_dbu_price, sql_compute_dbu_price, jobs_light_dbu_price,
  max_days, excluded_scopes, active, deploymentId, output_path = Value
}
case class MultiWorkspaceParams(
                                 args: String,
                                 apiUrl: String,
                                 workspaceId: String,
                                 deploymentId: String
                               )

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
                                        auditLogChk: Option[String] = None,
                                        azureClientId: Option[String] = None,
                                        azureClientSecret: Option[String] = None,
                                        azureTenantId: Option[String] = None,
                                        azureAuthEndpoint: String = "https://login.microsoftonline.com/"
                                      )

case class AuditLogConfig(
                           rawAuditPath: Option[String] = None,
                           auditLogFormat: String = "json",
                           azureAuditLogEventhubConfig: Option[AzureAuditLogEventhubConfig] = None,
                           systemTableName: Option[String] = None,
                           sqlEndpoint: Option[String] = None
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
                           apiEnvConfig: Option[ApiEnvConfig] = None,
                           tempWorkingDir: String = "", // will be set after data target validated if not overridden
                           sqlEndpoint: Option[String] = None
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

case class DeploymentValidationReport(
                                       validated: Boolean = false,
                                       simpleMsg:  String,
                                       validationRule: String,
                                       validationMsg: Option[String] = None,
                                       workspaceId: Option[String]
                                     )

case class MultiWSDeploymentReport(
                                    workspaceId: String,
                                    zone: String,
                                    workspaceDetails: Option[String],
                                    errorMsg: String,
                                    deploymentId: Option[String]
                                  )

case class WorkspaceMetastoreRegistrationReport(workspaceDataset: WorkspaceDataset, registerStatement: String, status: String)


case class CloneDetail(source: String,
                       target: String,
                       asOfTS: Option[String] = None,
                       cloneLevel: String = "DEEP",
                       immutableColumns:Array[String] = Array(),
                       mode: WriteMode.WriteMode = WriteMode.append)

case class CloneReport(cloneSpec: CloneDetail, cloneStatement: String, status: String)

case class OrgConfigDetail(organization_id: String, latestParams: OverwatchParams)

case class OrgWorkspace(organization_id: String, workspace: Workspace)

case class NamedColumn(fieldName: String, column: Column)

case class ModuleRollbackTS(organization_id: String, moduleId: Int, rollbackTS: Long, isAzure: Boolean)

case class TargetRollbackTS(organization_id: String, target: PipelineTable, rollbackTS: Long)

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
  val jobs, clusters, clusterEvents, sparkEvents, audit, notebooks, accounts,
  dbsql, pools, notebookCommands, warehouseEvents = Value
  // Todo Issue_77
  def toArray: Array[String] = values.map(_.toString).toArray
}

object WriteMode extends Enumeration {
  type WriteMode = Value
  val append: Value = Value("append")
  val overwrite: Value = Value("overwrite")
  val merge: Value = Value("merge")
}

/**
 * insertOnly = whenNotMatched --> Insert
 * updateOnly = whenMatched --> update
 * full = both insert and update
 */
object MergeScope extends Enumeration {
  type MergeScope = Value
  val full: Value = Value("full")
  val insertOnly: Value = Value("insertOnly")
  val updateOnly: Value = Value("updateOnly")
}

// Todo Issue_56
private[overwatch] class NoNewDataException(s: String, val level: Level, val allowModuleProgression: Boolean = false) extends Exception(s) {}

private[overwatch] class UnhandledException(s: String) extends Exception(s) {}

private[overwatch] class IncompleteFilterException(s: String) extends Exception(s) {}

private[overwatch] class ApiCallEmptyResponse(val apiCallDetail: String, val allowModuleProgression: Boolean) extends Exception(apiCallDetail)

private[overwatch] class ApiCallFailureV2(s: String) extends Exception(s) {}

private[overwatch] class ModuleDisabled(moduleId: Int, s: String) extends Exception(s) {
  private val logger = Logger.getLogger("ModuleDisabled")
  logger.log(Level.INFO, s"MODULE DISABLED: MODULE_ID: $moduleId -- SKIPPING")
}

private[overwatch] class ApiCallFailure(
                                         val httpResponse: HttpResponse[String],
                                         apiCallDetail: String,
                                         t: Throwable = null,
                                         responseWithMeta: String = null,
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

  val responseMeta = responseWithMeta //Contains the api request details

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