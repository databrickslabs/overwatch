package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.SparkSessionWrapper.parSessionsOn
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.validation.DeploymentValidation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util
import java.util.concurrent.Executors
import java.util.{Collections, Date}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/** *
 * MultiWorkspaceDeployment class is the main class which runs the deployment for multiple workspaces.
 *
 * @params configCsvPath: path of the csv file which will contain the different configs for the workspaces.
 * @params tempOutputPath: location which will be used as a temp storage.It will be automatically cleaned after each run.
 * @params apiEnvConfig: configs related to api call.
 */
object MultiWorkspaceDeployment extends SparkSessionWrapper {


  def apply(configCsvPath: String): MultiWorkspaceDeployment = {
    apply(configCsvPath, "/mnt/tmp/overwatch")
  }

  def apply(configCsvPath: String, tempOutputPath: String) = {
    new MultiWorkspaceDeployment()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath(tempOutputPath)
      .setPipelineSnapTime()
  }
}


class MultiWorkspaceDeployment extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _configCsvPath: String = _

  private var _outputPath: String = _

  private val _deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = ArrayBuffer()

  private val _deploymentId: String = java.util.UUID.randomUUID.toString

  private def deploymentId = _deploymentId

  private def deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = _deploymentReport

  private def outputPath: String = _outputPath

  protected def configCsvPath: String = _configCsvPath

  private var _pipelineSnapTime: Long = _


  private def setConfigCsvPath(value: String): this.type = {
    _configCsvPath = value
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
        Some(s"${config.etl_storage_prefix}/${config.etl_database_name}.db"),
        Some(s"${config.etl_storage_prefix}/global_share"),
        Some(s"${config.consumer_database_name}"),
        Some(s"${config.etl_storage_prefix}/${config.consumer_database_name}.db")
      )
      val tokenSecret = TokenSecret(config.secret_scope, config.secret_key_dbpat)
      val ehConnString = s"{{secrets/${config.secret_scope}/${config.eh_scope_key}}}"
      val ehStatePath = s"${config.etl_storage_prefix}/${config.workspace_id}/ehState"
      val badRecordsPath = s"${config.etl_storage_prefix}/${config.workspace_id}/sparkEventsBadrecords"
      val auditLogConfig = if (s"${config.cloud}" == "AWS") {
        val awsAuditSourcePath = s"${config.auditlogprefix_source_aws}"
        AuditLogConfig(rawAuditPath = Some(awsAuditSourcePath))
      } else {
        val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = config.eh_name, auditRawEventsPrefix = ehStatePath)
        AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig))
      }
      val interactiveDBUPrice: Double = config.interactive_dbu_price
      val automatedDBUPrice: Double = config.automated_dbu_price
      val sqlComputerDBUPrice: Double = config.sql_compute_dbu_price
      val jobsLightDBUPrice: Double = config.jobs_light_dbu_price
      val customWorkspaceName: String = config.workspace_name
      val standardScopes = "audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts".split(",").toBuffer
      if (config.excluded_scopes != null) {
        config.excluded_scopes.split(":").foreach(scope => standardScopes -= scope)
      }

      val maxDaysToLoad: Int = config.max_days
      val primordialDateString: Date = config.primordial_date
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val stringDate = dateFormat.format(primordialDateString)
      val apiEnvConfig = getProxyConfig(config)

      val params = OverwatchParams(
        auditLogConfig = auditLogConfig,
        dataTarget = Some(dataTarget),
        tokenSecret = Some(tokenSecret),
        badRecordsPath = Some(badRecordsPath),
        overwatchScope = Some(standardScopes),
        maxDaysToLoad = maxDaysToLoad,
        databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice, sqlComputerDBUPrice, jobsLightDBUPrice),
        primordialDateString = Some(stringDate),
        workspace_name = Some(customWorkspaceName),
        externalizeOptimize = true,
        apiEnvConfig = Some(apiEnvConfig),
        tempWorkingDir = ""
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

  private def getProxyConfig(config: MultiWorkspaceConfig): ApiEnvConfig = {
    val apiProxyConfig = ApiProxyConfig(config.proxy_host, config.proxy_port, config.proxy_user_name, config.proxy_password_scope, config.proxy_password_key)
    val apiEnvConfig = ApiEnvConfig(config.success_batch_size.getOrElse(200),
      config.error_batch_size.getOrElse(500),
      config.enable_unsafe_SSL.getOrElse(false),
      config.thread_pool_size.getOrElse(4),
      config.api_waiting_time.getOrElse(300000),
      Some(apiProxyConfig))
    apiEnvConfig
  }

  private def startBronzeDeployment(multiWorkspaceParams: MultiWorkspaceParams) = {
    try {
      println(s"""************Bronze Deployment Started workspaceID:${multiWorkspaceParams.workspaceId}  args:${multiWorkspaceParams.args}**********  """)
      val workspace = Initializer(multiWorkspaceParams.args, debugFlag = false,
        apiURL = Some(multiWorkspaceParams.apiUrl),
        organizationID = Some(multiWorkspaceParams.workspaceId))

      Bronze(workspace).run()
      println(s"""************Bronze Deployment Completed workspaceID:${multiWorkspaceParams.workspaceId}************ """)
      deploymentReport.append(MultiWSDeploymentReport(multiWorkspaceParams.workspaceId, "Bronze", Some(multiWorkspaceParams.args),
        "SUCCESS",
        Some(multiWorkspaceParams.deploymentId)
      ))
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport(multiWorkspaceParams.workspaceId, "Bronze", Some(multiWorkspaceParams.args),
          fullMsg,
          Some(multiWorkspaceParams.deploymentId)
        ))
    } finally {
      clearThreadFromSessionsMap()
    }
  }

  private def startSilverDeployment(multiWorkspaceParams: MultiWorkspaceParams) = {
    try {
      println(s"""************Silver Deployment Started workspaceID:${multiWorkspaceParams.workspaceId} args:${multiWorkspaceParams.args} ************""")
      val workspace = Initializer(multiWorkspaceParams.args, debugFlag = false,
        apiURL = Some(multiWorkspaceParams.apiUrl),
        organizationID = Some(multiWorkspaceParams.workspaceId))

      Silver(workspace).run()
      deploymentReport.append(MultiWSDeploymentReport(multiWorkspaceParams.workspaceId, "Silver", Some(multiWorkspaceParams.args),
        "SUCCESS",
        Some(multiWorkspaceParams.deploymentId)
      ))
      println(s"""************Silver Deployment Completed workspaceID:${multiWorkspaceParams.workspaceId}************""")
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport(multiWorkspaceParams.workspaceId, "Silver", Some(multiWorkspaceParams.args),
          fullMsg,
          Some(multiWorkspaceParams.deploymentId)
        ))
    } finally {
      clearThreadFromSessionsMap()
    }
  }

  private def startGoldDeployment(multiWorkspaceParams: MultiWorkspaceParams) = {
    try {
      println(s"""************Gold Deployment Started workspaceID:${multiWorkspaceParams.workspaceId} args:${multiWorkspaceParams.args} ************"""")
      val workspace = Initializer(multiWorkspaceParams.args, debugFlag = false,
        apiURL = Some(multiWorkspaceParams.apiUrl),
        organizationID = Some(multiWorkspaceParams.workspaceId))

      Gold(workspace).run()
      deploymentReport.append(MultiWSDeploymentReport(multiWorkspaceParams.workspaceId, "Gold", Some(multiWorkspaceParams.args),
        "SUCCESS",
        Some(multiWorkspaceParams.deploymentId)
      ))
      println(s"""************Gold Deployment Completed workspaceID:${multiWorkspaceParams.workspaceId}************""")
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport(multiWorkspaceParams.workspaceId, "Gold", Some(multiWorkspaceParams.args),
          fullMsg,
          Some(multiWorkspaceParams.deploymentId)
        ))
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
    var configWriteLocation = multiworkspaceConfigs.head.etl_storage_prefix
    if (!configWriteLocation.startsWith("dbfs:") && !configWriteLocation.startsWith("s3") && !configWriteLocation.startsWith("abfss")) {
      configWriteLocation = s"""dbfs:${configWriteLocation}"""
    }
    multiworkspaceConfigs.toSeq.toDS().toDF()
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .withColumn("configFile", lit(configCsvPath))
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
    if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss")) {
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
  private def saveDeploymentReport(validationArray: ArrayBuffer[MultiWSDeploymentReport], path: String, reportName: String): Unit = {
    var reportPath = path
    if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss")) {
      reportPath = s"""dbfs:${path}"""
    }
    validationArray.toDS().toDF()
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${reportPath}/report/${reportName}""")
  }

  /**
   * Validates the config csv file existence.
   */
  private[overwatch] def validateFileExistence(configCsvPath: String): Boolean = {
    if (!Helpers.pathExists(configCsvPath)) {
      throw new BadConfigException("Unable to find config file in the given location:" + configCsvPath)
    }
    true
  }

  private def generateMultiWorkspaceConfig(
                                            configCsvPath: String,
                                            deploymentId: String,
                                            outputPath: String = ""
                                          ): Array[MultiWorkspaceConfig] = { // Array[MultiWorkspaceConfig] = {
    try {
      validateFileExistence(configCsvPath)
      val multiWorkspaceConfig = spark.read.option("header", "true")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .csv(configCsvPath)
        .scrubSchema
        .verifyMinimumSchema(Schema.deployementMinimumSchema)
        .filter(MultiWorkspaceConfigColumns.active.toString)
        .withColumn("deployment_id", lit(deploymentId))
        .withColumn("output_path", lit(outputPath))
        .as[MultiWorkspaceConfig]
        .collect()
      if(multiWorkspaceConfig.size<1){
        throw new BadConfigException("Config file has 0 record, config file:" + configCsvPath)
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
   * Performs the multi workspace deployment
   *
   * @param parallelism the number of threads which will run in parallel to perform the deployment
   * @param zones       the zone can be Bronze,Silver or Gold by default it will be Bronze
   */
  def deploy(parallelism: Int = 4, zones: String = "Bronze,Silver,Gold"): Unit = {
    val processingStartTime = System.currentTimeMillis();
    try {
      println("ParallelismLevel :" + parallelism)
      val multiWorkspaceConfig = generateMultiWorkspaceConfig(configCsvPath, deploymentId, outputPath)
      snapshotConfig(multiWorkspaceConfig)
      val params = DeploymentValidation
        .performMandatoryValidation(multiWorkspaceConfig, parallelism)
        .map(buildParams)
      println("Workspace to be Deployed :" + params.size)
      SparkSessionWrapper.parSessionsOn = true
      val zoneArray = zones.split(",")
      zoneArray.foreach(zone => {
        val responseCounter = Collections.synchronizedList(new util.ArrayList[Int]())
        implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))
        params.foreach(deploymentParams => {
          val future = Future {
            zone.toLowerCase match {
              case "bronze" =>
                startBronzeDeployment(deploymentParams)
              case "silver" =>
                startSilverDeployment(deploymentParams)
              case "gold" =>
                startGoldDeployment(deploymentParams)
            }
          }
          future.onComplete {
            case _ =>
              responseCounter.add(1)
          }
        })
        while (responseCounter.size() < params.length) {
          Thread.sleep(5000)
        }
      })
      saveDeploymentReport(deploymentReport, multiWorkspaceConfig.head.etl_storage_prefix, "deploymentReport")
    } catch {
      case e: Exception => throw e
    } finally {
      SparkSessionWrapper.sessionsMap.clear()
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
    val multiWorkspaceConfig = generateMultiWorkspaceConfig(configCsvPath, deploymentId, outputPath)
    val validations = DeploymentValidation.performValidation(multiWorkspaceConfig, parallelism)
    val notValidatedCount = validations.filterNot(_.validated).length

    snapShotValidation(validations, multiWorkspaceConfig.head.etl_storage_prefix, "validationReport", deploymentId)
    val processingEndTime = System.currentTimeMillis();
    val msg =
      s"""Validation report details
         |Total validation count: ${validations.length}
         |Failed validation count:${notValidatedCount}
         |Report run duration in sec : ${(processingEndTime - processingStartTime) / 1000}
         |""".stripMargin
    println(msg)
  }

}
