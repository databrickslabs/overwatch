package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.validation.{ConfigColumns, DeploymentValidation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.util.concurrent.Executors
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


  def apply(configCsvPath: String, tempOutputPath: String, apiEnvConfig: ApiEnvConfig): MultiWorkspaceDeployment = {
    new MultiWorkspaceDeployment()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath(tempOutputPath)
      .setApiEnvConfig(apiEnvConfig)
      .initDeployment()
  }


  def apply(configCsvPath: String, tempOutputPath: String): MultiWorkspaceDeployment = {
    new MultiWorkspaceDeployment()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath(tempOutputPath)
      .initDeployment()
  }

  def apply(configCsvPath: String): MultiWorkspaceDeployment = {
    new MultiWorkspaceDeployment()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath("/mnt/tmp/overwatch")
      .initDeployment()
  }
}


class MultiWorkspaceDeployment extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _configCsvPath: String = _

  private var _deploymentId: String = _

  private var _outputPath: String = _

  private var _deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = _

  private var _parallelism: Int = _

  private var _inputDataFrame: DataFrame = _

  private var _apiEnvConfig: ApiEnvConfig = _

  protected def apiEnvConfig: ApiEnvConfig = _apiEnvConfig

  protected def inputDataFrame: DataFrame = _inputDataFrame

  protected def parallelism: Int = _parallelism

  protected def deploymentId: String = _deploymentId

  protected def deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = _deploymentReport

  protected def outputPath: String = _outputPath

  protected def configCsvPath: String = _configCsvPath

  private var _pipelineSnapTime: Long = _

  private[overwatch] def setApiEnvConfig(value: ApiEnvConfig): this.type = {
    _apiEnvConfig = value
    this
  }

  private[overwatch] def setInputDataFrame(value: DataFrame): this.type = {
    _inputDataFrame = value
    this
  }

  private def initDeployment(): this.type = {
    setDeploymentReport(ArrayBuffer())
    setDeploymentId(java.util.UUID.randomUUID.toString)
    setPipelineSnapTime()
    this
  }

  private def setParallelism(value: Int): this.type = {
    _parallelism = value
    this
  }

  private def setDeploymentId(value: String): this.type = {
    _deploymentId = value
    this
  }

  private def setDeploymentReport(value: ArrayBuffer[MultiWSDeploymentReport]): this.type = {
    _deploymentReport = value
    this
  }


  private[overwatch] def setConfigCsvPath(value: String): this.type = {
    _configCsvPath = value
    this
  }

  private[overwatch] def setOutputPath(value: String): this.type = {
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
   * Validates all the parameters provided in config csv file and generates a report which is stored at /etl_storrage_prefix/report/validationReport
   * @param parallelism number of threads which will be running parallely to complete the task
   */
  def validate(parallelism: Int = 4): Unit = {
    val processingStartTime = System.currentTimeMillis();
    val deploymentValidation = DeploymentValidation(configCsvPath, outputPath, parallelism, deploymentId)
    val report = deploymentValidation.performValidation
    val notValidatedCount = report.filter(x => {
      !x.validated
    }).count()
    snapShotValidation(report, deploymentValidation.makeDataFrame().head().getAs(ConfigColumns.etl_storage_prefix.toString), "validationReport")

    val processingEndTime = System.currentTimeMillis();
    val msg =
      s"""Validation report details
         |Total validation count: ${report.count()}
         |Failed validation count:${notValidatedCount}
         |Report run duration in sec : ${(processingEndTime - processingStartTime) / 1000}
         |""".stripMargin
    println(msg)
  }

  /**
   * Creating overwatch parameters
   * @param config each row of the csv config file
   * @return
   */
  private def buildParams(config: Row): (String, String) = {
    try {
      val dataTarget = DataTarget(
        Some(config.getAs(ConfigColumns.etl_database_name.toString)),
        Some(s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/${config.getAs(ConfigColumns.etl_database_name.toString)}.db"),
        Some(s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/global_share"),
        Some(s"${config.getAs(ConfigColumns.consumer_database_name.toString)}"),
        Some(s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/${config.getAs(ConfigColumns.consumer_database_name.toString)}.db")
      )
      val tokenSecret = TokenSecret(config.getAs(ConfigColumns.secret_scope.toString), config.getAs(ConfigColumns.secret_key_dbpat.toString))
      val ehConnString = s"{{secrets/${config.getAs(ConfigColumns.secret_scope.toString)}/${config.getAs(ConfigColumns.eh_scope_key.toString)}}}"
      val ehStatePath = s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/${config.getAs(ConfigColumns.workspace_id.toString)}/ehState"
      val badRecordsPath = s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/${config.getAs(ConfigColumns.workspace_id.toString)}/sparkEventsBadrecords"
      val auditLogConfig = if (s"${config.getAs(ConfigColumns.cloud.toString)}" == "AWS") {
        val awsAuditSourcePath = s"${config.getAs(ConfigColumns.auditlogprefix_source_aws.toString)}"
        AuditLogConfig(rawAuditPath = Some(awsAuditSourcePath))
      } else {
        val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = config.getAs(ConfigColumns.eh_name.toString), auditRawEventsPrefix = ehStatePath)
        AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig))
      }
      val interactiveDBUPrice: Double = config.getAs(ConfigColumns.interactive_dbu_price.toString)
      val automatedDBUPrice: Double = config.getAs(ConfigColumns.automated_dbu_price.toString)
      val sqlComputerDBUPrice: Double = config.getAs(ConfigColumns.sql_compute_dbu_price.toString)
      val jobsLightDBUPrice: Double = config.getAs(ConfigColumns.jobs_light_dbu_price.toString)
      val customWorkspaceName: String = config.getAs(ConfigColumns.workspace_name.toString)
      val standardScopes = "audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts".split(",").toBuffer //TODO add scope exclusing and active flag
      if (config.getAs(ConfigColumns.excluded_scopes.toString) != null) {
        config.getAs(ConfigColumns.excluded_scopes.toString).toString.split(":").foreach(scope => standardScopes -= scope)
      }

      val maxDaysToLoad: Int = config.getAs(ConfigColumns.max_days.toString)
      val primordialDateString: Date = config.getAs(ConfigColumns.primordial_date.toString)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val stringDate = dateFormat.format(primordialDateString)
      if (apiEnvConfig == null) {
        setApiEnvConfig(ApiEnvConfig())
      }
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
        apiURL = Some(config.getAs(ConfigColumns.workspace_url.toString)),
        organizationID = Some(config.getAs(ConfigColumns.workspace_id.toString)),
        tempWorkingDir = ""
      )
      (JsonUtils.objToJson(params).compactString, s"""${config.getAs(ConfigColumns.workspace_id.toString)}""")
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while building params")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(
          MultiWSDeploymentReport(s"""${config.getAs(ConfigColumns.workspace_id.toString)}""",
            "",
            Some(s"""WorkspaceId: ${config.getAs(ConfigColumns.workspace_id.toString).toString}"""),
            fullMsg,
            Some(deploymentId)
          ))
        null
    }
  }

  private def startBronzeDeployment(args: String, workspaceID: String) = {
    try {
      println(s"""Bronze Deployment Started workspaceID:${workspaceID} """)
      val workspace = Initializer(args, debugFlag = true, isMultiworkspaceDeployment = true)
      Bronze(workspace).run()
      println(s"""Bronze Deployment Completed workspaceID:${workspaceID} """)
      deploymentReport.append(MultiWSDeploymentReport(workspaceID, "Bronze", Some(args),
        "SUCCESS",
        Some(deploymentId)
      ))
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport(workspaceID, "Bronze", Some(args),
          fullMsg,
          Some(deploymentId)
        ))
    }
  }

  private def startSilverDeployment(args: String, workspaceID: String) = {
    try {
      println(s"""Silver Deployment Started workspaceID:${workspaceID}""" )
      val workspace = Initializer(args, debugFlag = true, isMultiworkspaceDeployment = true)
      Silver(workspace).run()
      deploymentReport.append(MultiWSDeploymentReport(workspaceID, "Silver", Some(args),
        "SUCCESS",
        Some(deploymentId)
      ))
      println(s"""Silver Deployment Completed workspaceID:${workspaceID}""" )
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport(workspaceID, "Silver", Some(args),
          fullMsg,
          Some(deploymentId)
        ))
    }
  }

  private def startGoldDeployment(args: String, workspaceID: String) = {
    try {
      println(s"""Gold Deployment Started workspaceID:${workspaceID}""")
      val workspace = Initializer(args, debugFlag = true, isMultiworkspaceDeployment = true)
      Gold(workspace).run()
      deploymentReport.append(MultiWSDeploymentReport(workspaceID, "Gold", Some(args),
        "SUCCESS",
        Some(deploymentId)
      ))
      println(s"""Gold Deployment Completed workspaceID:${workspaceID}""")
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport(workspaceID, "Gold", Some(args),
          fullMsg,
          Some(deploymentId)
        ))
    }
  }

  /**
   * Takes a snapshot of the config and saves it to /report/configTable
   * @param configDF
   */
  private def snapshotConfig(configDF: DataFrame) = {
    var configWriteLocation = configDF.head().getAs(ConfigColumns.etl_storage_prefix.toString).toString
    if (!configWriteLocation.startsWith("dbfs:") && !configWriteLocation.startsWith("s3") && !configWriteLocation.startsWith("abfss")) {
      configWriteLocation = s"""dbfs:${configWriteLocation}"""
    }
    configDF
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .withColumn("configFile", lit(configCsvPath))
      .write.format("delta").mode("append").save(s"""${configWriteLocation}/report/configTable""")
  }

  /**
   * Takes a snapshot of the validation result
   * @param validationDF
   * @param path
   * @param reportName
   */
  private def snapShotValidation(validationDF: Dataset[DeploymentValidationReport], path: String, reportName: String): Unit = {
    var validationPath = path
    if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss")) {
      validationPath = s"""dbfs:${path}"""
    }
    validationDF
      .withColumn("deploymentId", lit(deploymentId))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${validationPath}/report/${reportName}""")
    println("Validation report has been saved to " + s"""${validationPath}/report/${reportName}""")
  }

  /**
   * Saves the deployment report.
   * @param validationDF
   * @param path
   * @param reportName
   */
  private def saveDeploymentReport(validationDF: Dataset[MultiWSDeploymentReport], path: String, reportName: String): Unit = {
    var reportPath = path
    if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss")) {
      reportPath = s"""dbfs:${path}"""
    }
    validationDF
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${reportPath}/report/${reportName}""")
  }

  /**
   * Performs the multi workspace deployment
   *
   * @param parallelism the number of threads which will run in parallel to perform the deployment
   * @param zones       the zone can be Bronze,Silver or Gold by default it will be Bronze
   */
  def deploy(parallelism: Int = 4, zones: String = "Bronze"): Unit = {
    val processingStartTime = System.currentTimeMillis();
    setParallelism(parallelism)
    println("ParallelismLevel :" + parallelism)
    val deploymentValidation = DeploymentValidation(configCsvPath, parallelism, deploymentId)
    deploymentValidation.performMandatoryValidation
    val dataFrame = deploymentValidation.makeDataFrame()
    setInputDataFrame(dataFrame)
    snapshotConfig(dataFrame)
    val prams = dataFrame.collect().map(buildParams).filter(args => args != null)
    val zoneArray = zones.split(",")
    zoneArray.foreach(zone => {
      var responseCounter = 0
      implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))
      prams.foreach(compactString => {
        val future = Future {
          zone.toLowerCase match {
            case "bronze" =>
              startBronzeDeployment(compactString._1, compactString._2)
            case "silver" =>
              startSilverDeployment(compactString._1, compactString._2)
            case "gold" =>
              startGoldDeployment(compactString._1, compactString._2)
          }
        }
        future.onComplete {
          case _ =>
            responseCounter = responseCounter + 1
        }
      })
      while (responseCounter < prams.length) {
        Thread.sleep(5000)
      }
    })
    saveDeploymentReport(deploymentReport.toDS, deploymentValidation.makeDataFrame().head().getAs(ConfigColumns.etl_storage_prefix.toString), "deploymentReport")
    println(s"""Deployment completed in sec ${(System.currentTimeMillis() - processingStartTime) / 1000}""")

  }

}
