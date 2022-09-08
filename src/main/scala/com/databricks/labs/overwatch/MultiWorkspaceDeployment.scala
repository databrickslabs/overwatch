package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.validation.{ConfigColumns, DeploymentValidation}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object MultiWorkspaceDeployment extends SparkSessionWrapper {

  def apply(configCsvPath: String, reportOutputPath: String): MultiWorkspaceDeployment = {
    new MultiWorkspaceDeployment()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath(reportOutputPath)
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

  protected def inputDataFrame: DataFrame = _inputDataFrame

  protected def parallelism: Int = _parallelism

  protected def deploymentId: String = _deploymentId

  protected def deploymentReport: ArrayBuffer[MultiWSDeploymentReport] = _deploymentReport

  protected def outputPath: String = _outputPath

  protected def configCsvPath: String = _configCsvPath

  private var _pipelineSnapTime: Long = _

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

  private def buildParams(config: Row): String = {
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
      //private val ehConnString = dbutils.secrets.get(secretsScope, ehKey)

      val ehStatePath = s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/${config.getAs(ConfigColumns.workspace_id.toString)}/ehState"
      val badRecordsPath = s"${config.getAs(ConfigColumns.etl_storage_prefix.toString)}/${config.getAs(ConfigColumns.workspace_id.toString)}/sparkEventsBadrecords"
      val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = config.getAs(ConfigColumns.eh_name.toString), auditRawEventsPrefix = ehStatePath)
      val interactiveDBUPrice: Double = config.getAs(ConfigColumns.interactive_dbu_price.toString)
      val automatedDBUPrice: Double = config.getAs(ConfigColumns.automated_dbu_price.toString)
      val customWorkspaceName: String = config.getAs(ConfigColumns.workspace_name.toString)
      val standardScopes = "audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts".split(",").toBuffer //TODO add scope exclusing and active flag
      if (config.getAs(ConfigColumns.excluded_scopes.toString) != null) {
        config.getAs(ConfigColumns.excluded_scopes.toString).toString.split(":").foreach(scope => standardScopes -= scope)
      }


      val maxDaysToLoad: Int = config.getAs(ConfigColumns.max_days.toString)
      val primordialDateString: Date = config.getAs(ConfigColumns.primordial_date.toString)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val stringDate = dateFormat.format(primordialDateString)
      //val apiEnv = ApiEnv()
      val params = OverwatchParams(
        auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
        dataTarget = Some(dataTarget),
        tokenSecret = Some(tokenSecret),
        badRecordsPath = Some(badRecordsPath),
        overwatchScope = Some(standardScopes),
        maxDaysToLoad = maxDaysToLoad,
        databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice),
        primordialDateString = Some(stringDate),
        workspace_name = Some(customWorkspaceName),
        externalizeOptimize = true,
        apiURL = Some(config.getAs(ConfigColumns.workspace_url.toString)),
        organizationID = Some(config.getAs(ConfigColumns.workspace_id.toString)),
        tempWorkingDir = ""
      )
      JsonUtils.objToJson(params).compactString
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while building params")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport("", Some(s"""WorkspaceId: ${config.getAs(ConfigColumns.workspace_id.toString).toString}"""),
          fullMsg,
          Some(deploymentId)
        ))
        null
    }
  }

  private def startBronzeDeployment(args: String) = {

    try {
      println(s"""Bronze Deployment started config params: ${args} , ThreadName: """ + Thread.currentThread().getName)
      val workspace = Initializer(args, debugFlag = true, isMultiworkspaceDeployment = true)
      Bronze(workspace).run()
      println(s"""Bronze Deployment Completed******* config , ThreadName: """ + Thread.currentThread().getName)
      deploymentReport.append(MultiWSDeploymentReport("Bronze", Some(args),
        "SUCCESS",
        Some(deploymentId)
      ))

    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport("Bronze", Some(args),
          fullMsg,
          Some(deploymentId)
        ))
    }
  }

  private def startSilverDeployment(args: String) = {

    try {
      println(s"""Silver Deployment started config params: ${args} , ThreadName: """ + Thread.currentThread().getName)
      val workspace = Initializer(args, debugFlag = true, isMultiworkspaceDeployment = true)
      Silver(workspace).run()
      deploymentReport.append(MultiWSDeploymentReport("Silver", Some(args),
        "SUCCESS",
        Some(deploymentId)
      ))
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport("Silver", Some(args),
          fullMsg,
          Some(deploymentId)
        ))
    }
  }

  private def startGoldDeployment(args: String) = {

    try {
      println(s"""Gold Deployment started config params: ${args} , ThreadName: """ + Thread.currentThread().getName)
      val workspace = Initializer(args, debugFlag = true, isMultiworkspaceDeployment = true)
      Gold(workspace).run()
      deploymentReport.append(MultiWSDeploymentReport("Gold", Some(args),
        "SUCCESS",
        Some(deploymentId)
      ))
    } catch {
      case exception: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(exception, "Got Exception while Deploying,")
        logger.log(Level.ERROR, fullMsg)
        deploymentReport.append(MultiWSDeploymentReport("Gold", Some(args),
          fullMsg,
          Some(deploymentId)
        ))
    }
  }

  private def getWorkspace(row: Row): Workspace = {
    Initializer(buildParams(row), debugFlag = true, disableValidations = true, isMultiworkspaceDeployment = true)
  }


  private def snapshotConfig(configDF: DataFrame) = {
    var configWriteLocation = configDF.head().getAs(ConfigColumns.etl_storage_prefix.toString).toString
    if (!configWriteLocation.startsWith("dbfs:")) {
      configWriteLocation = s"""dbfs:${configWriteLocation}"""
    }
    configDF
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${configWriteLocation}/report/configTable""")
  }

  private def snapShotValidation(validationDF: Dataset[DeploymentValidationReport], path: String, reportName: String): Unit = {
    var validationPath = path
    if (!path.startsWith("dbfs:")) {
      validationPath = s"""dbfs:${path}"""
    }
    validationDF
      .withColumn("deploymentId", lit(deploymentId))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${validationPath}/report/${reportName}""")
    println("Validation report has been saved to " + s"""${validationPath}/report/${reportName}""")
  }

  private def saveDeploymentReport(validationDF: Dataset[MultiWSDeploymentReport], path: String, reportName: String): Unit = {
    var reportPath = path
    if (!path.startsWith("dbfs:")) {
      reportPath = s"""dbfs:${path}"""
    }
    validationDF
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta").mode("append").save(s"""${reportPath}/report/${reportName}""")
  }




  def deploy(parallelism: Int = 4, zones: String = "Bronze"): Unit = {
    val processingStartTime = System.currentTimeMillis();
    setParallelism(parallelism)
    println("Pallalesism :" + parallelism)
    val deploymentValidation = DeploymentValidation(configCsvPath, outputPath, parallelism, deploymentId)
    deploymentValidation.performMandatoryValidation
    val dataFrame = deploymentValidation.makeDataFrame()
    setInputDataFrame(dataFrame)
    snapshotConfig(dataFrame)
    val workspace = getWorkspace(dataFrame.head())
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    val mapParams = dataFrame.collect().map(buildParams).filter(args => args != null)
    val prams =mapParams.par//building arguments
    prams.tasksupport = taskSupport
    val zoneArray = zones.split(",")
    zoneArray.foreach(zone => {
      zone match {
        case "Bronze" =>
          println("*************Deploying BRONZE***********************")
          prams.foreach(startBronzeDeployment)
          println("*************BRONZE Deployment Completed***********************")
        case "Silver" =>
          println("*************Deploying SILVER***********************")
          prams.filter(args => args != null).map(startSilverDeployment)
          println("*************SILVER Deployment Completed***********************")
        case "Gold" =>
          println("*************Deploying GOLD***********************")
          prams.filter(args => args != null).map(startGoldDeployment)
          println("*************GOLD Deployment Completed***********************")
      }
    })
    saveDeploymentReport(deploymentReport.toDS, deploymentValidation.makeDataFrame().head().getAs(ConfigColumns.etl_storage_prefix.toString), "deploymentReport")
    println(s"""Deployment completed in sec ${(System.currentTimeMillis() - processingStartTime) / 1000}""")

  }

}
