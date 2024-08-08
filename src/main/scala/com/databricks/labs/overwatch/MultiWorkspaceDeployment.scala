package com.databricks.labs.overwatch

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.validation.DeploymentValidation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps

/** *
 * MultiWorkspaceDeployment class is the main class which runs the deployment for multiple workspaces.
 *
 * @params configLocation: can be either a delta table or path of the delta table or fully qualified path of the csv file which contains the configuration,
 * @params tempOutputPath: location which will be used as a temp storage.It will be automatically cleaned after each run.
 * @params apiEnvConfig: configs related to api call.
 */
object MultiWorkspaceDeployment extends SparkSessionWrapper {


  def apply(configLocation: String): MultiWorkspaceDeployment = {
    apply(configLocation, "/mnt/tmp/overwatch")
  }

  /**
   *
   * @param configLocation can be either a delta table or path of the delta table or fully qualified path of the csv file which contains the configuration,
   * @param tempOutputPath location which will be used as a temp storage.It will be automatically cleaned after each run.
   * @return
   */
  def apply(configLocation: String, tempOutputPath: String) = {
    new MultiWorkspaceDeployment()
      .setConfigLocation(configLocation)
      .setOutputPath(tempOutputPath)
      .setPipelineSnapTime()
  }
}


class MultiWorkspaceDeployment extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _configLocation: String = _

  private var _outputPath: String = _

  private val _deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = ArrayBuffer()

  private val _deploymentId: String = java.util.UUID.randomUUID.toString

  private def deploymentId = _deploymentId

  private def deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = _deploymentReport

  private def outputPath: String = _outputPath

  protected def configLocation: String = _configLocation

  private var _pipelineSnapTime: Long = _

  private var _systemTableAudit: String = "system.access.audit"

  private def systemTableAudit: String = _systemTableAudit

  private def setConfigLocation(value: String): this.type = {
    _configLocation = value
    this
  }

  private def setOutputPath(value: String): this.type = {
    _outputPath = value
    this
  }

  private def pipelineSnapTime: TimeTypes = {
    Pipeline.createTimeDetail(_pipelineSnapTime)
  }

  private def setPipelineSnapTime(): this.type = {
    _pipelineSnapTime = LocalDateTime.now(Pipeline.systemZoneId).toInstant(Pipeline.systemZoneOffset).toEpochMilli
    logger.log(Level.INFO, s"INIT: Pipeline Snap TS: ${pipelineSnapTime.asUnixTimeMilli}-${pipelineSnapTime.asTSString}")
    this
  }


  /**
   * Creating overwatch parameters
   *
   * @param config each row of the csv config file
   * @return
   */
  private def buildParams(config: MultiWorkspaceConfig): MultiWorkspaceParams = {
    try {
      val dataTarget = DataTarget(
        Some(config.etl_database_name),
        Some(s"${config.storage_prefix}/${config.etl_database_name}.db"),
        Some(s"${config.storage_prefix}/global_share"),
        Some(s"${config.consumer_database_name}"),
        Some(s"${config.storage_prefix}/${config.consumer_database_name}.db")
      )
      val tokenSecret = TokenSecret(config.secret_scope, config.secret_key_dbpat)
      val badRecordsPath = s"${config.storage_prefix}/${config.workspace_id}/sparkEventsBadrecords"
      // TODO -- ISSUE 781 - quick fix to support non-json audit logs but needs to be added back to external parameters
      val auditLogConfig = getAuditlogConfigs(config)
      val interactiveDBUPrice: Double = config.interactive_dbu_price
      val automatedDBUPrice: Double = config.automated_dbu_price
      val sqlComputerDBUPrice: Double = config.sql_compute_dbu_price
      val jobsLightDBUPrice: Double = config.jobs_light_dbu_price
      val customWorkspaceName: String = config.workspace_name
      val standardScopes = OverwatchScope.toArray
      val scopesToExecute = (standardScopes.map(_.toLowerCase).toSet --
        config.excluded_scopes.getOrElse("").split(":").map(_.toLowerCase).toSet).toArray

      val maxDaysToLoad: Int = config.max_days
      val primordialDateString: Date = config.primordial_date
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val stringDate = dateFormat.format(primordialDateString)
      val apiEnvConfig = getProxyConfig(config)
      val temp_dir_path = config.temp_dir_path.getOrElse("")
      val sql_endpoint = config.sql_endpoint

      val params = OverwatchParams(
        auditLogConfig = auditLogConfig,
        dataTarget = Some(dataTarget),
        tokenSecret = Some(tokenSecret),
        badRecordsPath = Some(badRecordsPath),
        overwatchScope = Some(scopesToExecute),
        maxDaysToLoad = maxDaysToLoad,
        databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice, sqlComputerDBUPrice, jobsLightDBUPrice),
        primordialDateString = Some(stringDate),
        workspace_name = Some(customWorkspaceName),
        externalizeOptimize = true,
        apiEnvConfig = Some(apiEnvConfig),
        tempWorkingDir = temp_dir_path,
        sqlEndpoint = sql_endpoint
      )
      MultiWorkspaceParams(JsonUtils.objToJson(params).compactString,
        s"""${config.api_url}""",
        s"""${config.workspace_id}""",
        s"""${config.deployment_id}""")
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while building params")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(
          MultiWSDeploymentReport(s"""${config.workspace_id}""",
            "",
            Some(s"""WorkspaceId: ${config.workspace_id}"""),
            fullMsg,
            Some(config.deployment_id)
          ))
        null
    }
  }

  private def getAuditLogConfigForSystemTable(config: MultiWorkspaceConfig): AuditLogConfig = {
      if(config.sql_endpoint.getOrElse("").isEmpty) {
        val auditLogFormat = "delta"
        AuditLogConfig(rawAuditPath = config.auditlogprefix_source_path,
          auditLogFormat = auditLogFormat, systemTableName = Some(systemTableAudit))
      }
      else {
        val auditLogFormat = "delta"
        AuditLogConfig(rawAuditPath = config.auditlogprefix_source_path,
          auditLogFormat = auditLogFormat, systemTableName = Some(systemTableAudit),
          sqlEndpoint = config.sql_endpoint)
      }
    }

  private def getAuditLogConfigForAwsGcp(config: MultiWorkspaceConfig): AuditLogConfig = {
    val auditLogFormat = spark.conf.getOption("overwatch.auditlogformat").getOrElse("json")
    AuditLogConfig(rawAuditPath = config.auditlogprefix_source_path, auditLogFormat = auditLogFormat)
  }

  private def getAuditLogConfigForzure(config: MultiWorkspaceConfig): AuditLogConfig = {
    val ehStatePath = s"${config.storage_prefix}/${config.workspace_id}/ehState"
    val isAAD = config.aad_client_id.nonEmpty &&
      config.aad_tenant_id.nonEmpty &&
      config.aad_client_secret_key.nonEmpty &&
      config.eh_conn_string.nonEmpty
    val azureLogConfig = if (isAAD) {
      AzureAuditLogEventhubConfig(connectionString = config.eh_conn_string.get, eventHubName = config.eh_name.get
        , auditRawEventsPrefix = ehStatePath,
        azureClientId = Some(config.aad_client_id.get),
        azureClientSecret = Some(dbutils.secrets.get(config.secret_scope, key = config.aad_client_secret_key.get)),
        azureTenantId = Some(config.aad_tenant_id.get),
        azureAuthEndpoint = config.aad_authority_endpoint.getOrElse("https://login.microsoftonline.com/")
      )
    } else {
      val ehConnString = s"{{secrets/${config.secret_scope}/${config.eh_scope_key.get}}}"
      AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = config.eh_name.get, auditRawEventsPrefix = ehStatePath)
    }
    AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig))
  }

   private def getAuditlogConfigs(config: MultiWorkspaceConfig): AuditLogConfig = {
      if (config.auditlogprefix_source_path.getOrElse("").toLowerCase.equals("system")) {
        getAuditLogConfigForSystemTable(config)
      } else {
        if(s"${config.cloud.toLowerCase()}" != "azure") {
          getAuditLogConfigForAwsGcp(config)
        }
        else {
          getAuditLogConfigForzure(config)
        }
      }
    }


    private def getProxyConfig(config: MultiWorkspaceConfig): ApiEnvConfig = {
    val apiProxyConfig = ApiProxyConfig(config.proxy_host, config.proxy_port, config.proxy_user_name, config.proxy_password_scope, config.proxy_password_key)
    val apiEnvConfig = ApiEnvConfig(config.success_batch_size.getOrElse(200),
      config.error_batch_size.getOrElse(500),
      config.enable_unsafe_SSL.getOrElse(false),
      config.thread_pool_size.getOrElse(4),
      config.api_waiting_time.getOrElse(300000),
      Some(apiProxyConfig),
      config.mount_mapping_path)
    apiEnvConfig
  }

  private def startBronzeDeployment(workspace: Workspace, deploymentId: String): MultiWSDeploymentReport = {
    val workspaceId = workspace.getConfig.organizationId
    val args = JsonUtils.objToJson(workspace.getConfig.inputConfig)
    println(s"""************Bronze Deployment Started workspaceID:$workspaceId\nargs:${args.prettyString}**********  """)
    try {
      Bronze(workspace).run()
      println(s"""************Bronze Deployment Completed workspaceID:$workspaceId************ """)
      MultiWSDeploymentReport(workspaceId, "Bronze", Some(args.compactString),
        "SUCCESS",
        Some(deploymentId)
      )
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        MultiWSDeploymentReport(workspaceId, "Bronze", Some(args.compactString),
          fullMsg,
          Some(deploymentId)
        )
    } finally {
      clearThreadFromSessionsMap()
    }
  }

  private def startSilverDeployment(workspace: Workspace, deploymentId: String): MultiWSDeploymentReport = {
    val workspaceId = workspace.getConfig.organizationId
    val args = JsonUtils.objToJson(workspace.getConfig.inputConfig)
    try {
      println(s"""************Silver Deployment Started workspaceID:$workspaceId\nargs:${args.prettyString} ************""")

      Silver(workspace).run()
      println(s"""************Silver Deployment Completed workspaceID:$workspaceId************""")
      MultiWSDeploymentReport(workspaceId, "Silver", Some(args.compactString),
        "SUCCESS",
        Some(deploymentId)
      )
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        MultiWSDeploymentReport(workspaceId, "Silver", Some(args.compactString),
          fullMsg,
          Some(deploymentId)
        )
    } finally {
      clearThreadFromSessionsMap()
    }
  }

  private def startGoldDeployment(workspace: Workspace, deploymentId: String): MultiWSDeploymentReport = {
    val workspaceId = workspace.getConfig.organizationId
    val args = JsonUtils.objToJson(workspace.getConfig.inputConfig)
    try {
      println(s"""************Gold Deployment Started workspaceID:$workspaceId args:${args.prettyString} ************"""")

      Gold(workspace).run()
      println(s"""************Gold Deployment Completed workspaceID:$workspaceId************""")
      MultiWSDeploymentReport(workspaceId, "Gold", Some(args.compactString),
        "SUCCESS",
        Some(deploymentId)
      )
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        MultiWSDeploymentReport(workspaceId, "Gold", Some(args.compactString),
          fullMsg,
          Some(deploymentId)
        )
    }finally {
      clearThreadFromSessionsMap()
    }
  }

  /**
   * Takes a snapshot of the config and saves it to /report/configTable
   *
   * @param configDF
   */
  private def snapshotConfig(multiworkspaceConfigs: Array[MultiWorkspaceConfig]) = {
    var configWriteLocation = multiworkspaceConfigs.head.storage_prefix
    if (!configWriteLocation.startsWith("dbfs:")
      && !configWriteLocation.startsWith("s3")
      && !configWriteLocation.startsWith("abfss")
      && !configWriteLocation.startsWith("gs")) {
      configWriteLocation = s"""dbfs:${configWriteLocation}"""
    }
    multiworkspaceConfigs.toSeq.toDS().toDF()
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .withColumn("configFile", lit(configLocation))
      .write.format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(s"""${configWriteLocation}/report/configTable""")
  }

  /**
   * Takes a snapshot of the validation result
   *
   * @param validationDF
   * @param path
   * @param reportName
   */
  private def snapShotValidation(validationArray: Array[DeploymentValidationReport], path: String, reportName: String, deploymentId: String): Unit = {
    var validationPath = path
    if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss") && !path.startsWith("gs")) {
      validationPath = s"""dbfs:${path}"""
    }
    validationArray.toSeq.toDS().toDF()
      .withColumn("deployment_id", lit(deploymentId))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(s"""${validationPath}/report/${reportName}""")
    println("Validation report has been saved to " + s"""${validationPath}/report/${reportName}""")
  }

  /**
   * Saves the deployment report.
   *
   * @param validationDF
   * @param path
   * @param reportName
   */
  private def saveDeploymentReport(validationArray: Array[MultiWSDeploymentReport], path: String, reportName: String): Unit = {
    var reportPath = path
    if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss") && !path.startsWith("gs")) {
      reportPath = s"""dbfs:${path}"""
    }
    validationArray.toSeq.toDF()
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${reportPath}/report/${reportName}""")
    println("Deployment report has been saved to " + s"""${reportPath}/report/${reportName}""")
  }

  /**
   * Validates the config csv file existence.
   */
  private[overwatch] def validateFileExistence(configLocation: String): Boolean = {
    if (!Helpers.pathExists(configLocation)) {
      throw new BadConfigException("Unable to find config file in the given location:" + configLocation)
    }
    true
  }

  /**
   * Generate Config Dataframe from Config Location(csv, delta path or Delta Table)
   */
  def generateBaseConfig(configLocation: String): DataFrame = {
    val rawBaseConfigDF = try {
      if (configLocation.toLowerCase().endsWith(".csv")) { // CSV file
        println(s"Config source: csv path ${configLocation}")
        validateFileExistence(configLocation)
        spark.read.option("header", "true")
          .option("ignoreLeadingWhiteSpace", true)
          .option("ignoreTrailingWhiteSpace", true)
          .csv(configLocation)
      } else if (configLocation.contains("/")) { // delta path
        println(s"Config source: delta path ${configLocation}")
        validateFileExistence(configLocation)
        spark.read.format("delta").load(configLocation)
      } else { // delta table
        println(s"Config source: delta table ${configLocation}")
        if (!spark.catalog.tableExists(configLocation)) {
          throw new BadConfigException("Unable to find Delta table" + configLocation)
        }
        spark.read.table(configLocation)
      }
    } catch {
      case e: Exception =>
        println("Exception while reading config , please provide config csv path/config delta path/config delta table")
        throw e
    }

    val deploymentSelectsNoNullStrings = Schema.deployementMinimumSchema.fields.map(f => {
      when(trim(lower(col(f.name))) === "null", lit(null).cast(f.dataType)).otherwise(col(f.name)).alias(f.name)
    })

    checkStoragePrefixColumnName(rawBaseConfigDF)
    rawBaseConfigDF
      .verifyMinimumSchema(Schema.deployementMinimumSchema)
      .select(deploymentSelectsNoNullStrings: _*)

  }

  /**
   * For overwatch version < 0.7.2 We have etl_storage_prefix as a column in config.csv.
   * As we have completed Unity Catalogue integration on overwatch version 0.7.2 we are not supporting column etl_storage_prefix.
   * This column name has been changed to storage_prefix. This function checks whether the config.csv contains storage_prefix or not.
   *
   * @param config
   * @return
   */
  private def checkStoragePrefixColumnName(rawBaseConfigDF: DataFrame): Unit ={
    if (rawBaseConfigDF.hasFieldNamed("etl_storage_prefix")) {
      throw new BadConfigException("WE DO NOT support etl_storage_prefix column anymore please change the column name to  storage_prefix")
    }
  }


  private def generateMultiWorkspaceConfig(
                                            configLocation: String,
                                            deploymentId: String,
                                            outputPath: String = ""
                                          ): Array[MultiWorkspaceConfig] = { // Array[MultiWorkspaceConfig] = {
    try {
      val baseConfig = generateBaseConfig(configLocation)
      val multiWorkspaceConfig = baseConfig
        .withColumn("api_url", removeTrailingSlashes('api_url))
        .withColumn("deployment_id", lit(deploymentId))
        .withColumn("output_path", lit(outputPath))
        .as[MultiWorkspaceConfig]
        .filter(_.active)
        .collect()
      if (multiWorkspaceConfig.length < 1) {
        throw new BadConfigException("Config file has 0 record, config file:" + configLocation)
      }
      multiWorkspaceConfig
    } catch {
      case e: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(e, "Unable to create Config Dataframe")
        logger.log(Level.ERROR, fullMsg)
        throw e
    }
  }


  /**
   * crate pipeline executions as futures and return the deployment reports
   * @param deploymentParams deployment params for a specific workspace
   * @param medallions medallions to execute (bronze, silver, gold)
   * @param ec futures executionContext
   * @return future deployment report
   */
  private def executePipelines(
                                deploymentParams: MultiWorkspaceParams,
                                medallions: String,
                                ec: ExecutionContextExecutor
                              ): Future[Array[MultiWSDeploymentReport]] = {

    Future {
      val threadDeploymentReport = ArrayBuffer[MultiWSDeploymentReport]()
      val deploymentId = deploymentParams.deploymentId
      val workspace = Initializer(deploymentParams.args,
        apiURL = Some(deploymentParams.apiUrl),
        organizationID = Some(deploymentParams.workspaceId))

      val zonesLower = medallions.toLowerCase
      if (zonesLower.contains("bronze")) threadDeploymentReport.append(startBronzeDeployment(workspace, deploymentId))
      if (zonesLower.contains("silver")) threadDeploymentReport.append(startSilverDeployment(workspace, deploymentId))
      if (zonesLower.contains("gold")) threadDeploymentReport.append(startGoldDeployment(workspace, deploymentId))
      threadDeploymentReport.toArray
    }(ec)

  }


  /**
   * Performs the multi workspace deployment
   *
   * @param parallelism the number of threads which will run in parallel to perform the deployment
   * @param zones       the zone can be Bronze,Silver or Gold by default it will be Bronze
   */
  def deploy(parallelism: Int = 4, zones: String = "Bronze,Silver,Gold"): Unit = {
    val processingStartTime = System.currentTimeMillis();
    try {
      // initialize spark overrides for global spark conf
      // global overrides should be set BEFORE parSessionsOn is set to true
      PipelineFunctions.setSparkOverrides(spark(globalSession = true), SparkSessionWrapper.globalSparkConfOverrides)

      if (parallelism > 1) SparkSessionWrapper.parSessionsOn = true
      SparkSessionWrapper.sessionsMap.clear()
      SparkSessionWrapper.globalTableLock.clear()

      println("ParallelismLevel :" + parallelism)
      val multiWorkspaceConfig = generateMultiWorkspaceConfig(configLocation, deploymentId, outputPath)

      snapshotConfig(multiWorkspaceConfig)
      val params = DeploymentValidation
        .performMandatoryValidation(multiWorkspaceConfig, parallelism)
        .map(buildParams)
      println("Workspaces to be Deployed :" + params.length)
      val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))
      val deploymentReports = params.filter(param => param != null)
        .map(executePipelines(_, zones, ec))
        .flatMap(f => Await.result(f, Duration.Inf))

      deploymentReport.appendAll(deploymentReports)
      saveDeploymentReport(deploymentReport.toArray, multiWorkspaceConfig.head.storage_prefix, "deploymentReport")
    } catch {
      case e: Throwable =>
        val failMsg = s"FAILED DEPLOYMENT WITH EXCEPTION"
        println(failMsg)
        logger.log(Level.ERROR, failMsg, e)
        throw e
    } finally {
      SparkSessionWrapper.sessionsMap.clear()
      SparkSessionWrapper.globalTableLock.clear()
    }
    println(s"""Deployment completed in sec ${(System.currentTimeMillis() - processingStartTime) / 1000}""")
  }


  /**
   * Validates all the parameters provided in config csv file and generates a report which is stored at /etl_storrage_prefix/report/validationReport
   *
   *
   * @param parallelism number of threads which will be running in parallel to complete the task
   */
  def validate(parallelism: Int = 4): Unit = {
    val processingStartTime = System.currentTimeMillis()
    val multiWorkspaceConfig = generateMultiWorkspaceConfig(configLocation, deploymentId, outputPath)
    val currentOrgID = Initializer.getOrgId
    val validations = DeploymentValidation.performValidation(multiWorkspaceConfig, parallelism,currentOrgID)
    val notValidatedCount = validations.filterNot(_.validated).length

    snapShotValidation(validations, multiWorkspaceConfig.head.storage_prefix, "validationReport", deploymentId)
    val processingEndTime = System.currentTimeMillis();
    val msg =
      s"""Validation report details
         |Total validation count: ${validations.length}
         |Failed validation count:${notValidatedCount}
         |Report run duration in sec : ${(processingEndTime - processingStartTime) / 1000}
         |""".stripMargin
    println(msg)
  }

  /**
   * Returns the Overwatch parameters from config.
   * @param workspaceId
   * @return
   */
  def getParams(workspaceId: String = ""): Array[MultiWorkspaceParams] = {
    val overwatchParams = generateMultiWorkspaceConfig(configLocation, deploymentId, outputPath).map(buildParams)
    val returnParam = if (workspaceId != "") {
      overwatchParams.filter(_.workspaceId == workspaceId)
    } else {
      overwatchParams
    }
    returnParam
  }



}