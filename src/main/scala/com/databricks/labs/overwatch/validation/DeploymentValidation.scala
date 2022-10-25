package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ApiCallV2
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.{Pipeline, PipelineFunctions, Schema}
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.validation.{Rule, RuleSet}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * Contains the utilities to do pre-checks before performing the deployment.
 */
object DeploymentValidation extends SparkSessionWrapper {

  def apply(configCsvPath: String, outputPath: String, parallelism: Int, deploymentId: String) = {
    new DeploymentValidation()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath(outputPath)
      .setParallelism(parallelism)
      .setDeploymentId(deploymentId)
  }

  def apply(configCsvPath: String, parallelism: Int, deploymentId: String) = {
    new DeploymentValidation()
      .setConfigCsvPath(configCsvPath)
      .setParallelism(parallelism)
      .setDeploymentId(deploymentId)
  }

}

class DeploymentValidation() extends SparkSessionWrapper {

  import spark.implicits._


  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _inputDataFrame: DataFrame = _

  private var _cloudProvider: String = _

  private var _parallelism: Int = _

  private var _outputPath: String = _

  private var _validationStatus: ArrayBuffer[DeploymentValidationReport] = new ArrayBuffer[DeploymentValidationReport]()

  private var _deploymentId: String = _

  private var _configCsvPath: String = _


  protected def configCsvPath: String = _configCsvPath

  protected def outputPath: String = _outputPath

  protected def deploymentId: String = _deploymentId


  protected def parallelism: Int = _parallelism

  protected def cloudProvider: String = _cloudProvider

  protected def inputDataFrame: DataFrame = _inputDataFrame

  protected def validationStatus: ArrayBuffer[DeploymentValidationReport] = _validationStatus


  private[overwatch] def setConfigCsvPath(value: String): this.type = {
    _configCsvPath = value
    this
  }

  private[overwatch] def setOutputPath(value: String): this.type = {
    _outputPath = value
    this
  }

  private[overwatch] def setParallelism(value: Int): this.type = {
    _parallelism = value
    this
  }

  private[overwatch] def setDeploymentId(value: String): this.type = {
    _deploymentId = value
    this
  }


  private[overwatch] def setCloudProvider(value: String): this.type = {
    _cloudProvider = value
    this
  }

  private[overwatch] def setInputDataFrame(value: DataFrame): this.type = {
    _inputDataFrame = value
    this
  }

  /**
   * Validates the config csv file existence.
   */
  private def validateFileExistence() = {
    if (!Helpers.pathExists(configCsvPath)) {
      throw new BadConfigException("Unable to find config file in the given location:" + configCsvPath)
    }
  }

  /**
   * Create a DataFrame from provided csv file.
   * @return
   */
  private[overwatch] def makeDataFrame(): DataFrame = {
    try {
      var df = spark.read.option("header", "true")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .csv(configCsvPath)
        .scrubSchema
        .verifyMinimumSchema(Schema.deployementMinimumSchema)
        .filter(ConfigColumns.active.toString)
        .withColumn("deployment_id", lit(deploymentId))
      df.show(false)
      setInputDataFrame(df.persist(StorageLevel.MEMORY_AND_DISK))
      df
    } catch {
      case e: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(e, "Unable to create Config Dataframe")
        logger.log(Level.ERROR, fullMsg)
        throw e
    }

  }

  /**
   * Check the access of etl_storage_prefix
   *
   * @param row
   * @param fastFail if true it will throw the exception and exit the process
   *                 if false it will register the exception in the validation report.
   */
  private def storagePrefixAccessValidation(row: Row, fastFail: Boolean = false): Unit = {
    val storagePrefix: String = row.getAs(ConfigColumns.etl_storage_prefix.toString).toString
    val workspaceId: String = row.getAs(ConfigColumns.workspace_id.toString).toString
    val testDetails = s"""StorageAccessTest storage : ${storagePrefix}"""
    try {
      dbutils.fs.mkdirs(s"""${storagePrefix}/test_access""")
      dbutils.fs.put(s"""${storagePrefix}/test_access/testwrite""", "This is a file in cloud storage.")
      dbutils.fs.head(s"""${storagePrefix}/test_access/testwrite""")
      dbutils.fs.rm(s"""${storagePrefix}/test_access""", true)
      updateValidationStatus(DeploymentValidationReport(true,
        getSimpleMsg("Storage_Access"),
        testDetails,
        Some("SUCCESS"),
        Some(workspaceId)
      ))
    }
    catch {
      case exception: Exception =>
        val msg = s"""Unable to read/write/create file in the provided etl storage prefix"""
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        updateValidationStatus(DeploymentValidationReport(false,
          getSimpleMsg("Storage_Access"),
          testDetails,
          Some(fullMsg),
          Some(workspaceId)
        ))
        if (fastFail) {
          throw new BadConfigException(fullMsg)
        }


    }
  }

  /**
   * Performs clusters/list api call to check the access to the workspace
   *
   * @param row
   */
  private def validateApiUrl(row: Row): Unit = {
    val url = row.getAs(ConfigColumns.api_url.toString).toString
    val scope = row.getAs(ConfigColumns.secret_scope.toString).toString
    val patKey = row.getAs(ConfigColumns.secret_key_dbpat.toString).toString
    val workspaceId = row.getAs(ConfigColumns.workspace_id.toString).toString
    val testDetails = s"""WorkSpaceURLConnectivityTest APIURL:${url} DBPATWorkspaceScope:${scope} SecretKey_DBPAT:${patKey}"""
    try {
      val patToken = dbutils.secrets.get(scope = scope, key = patKey)
      val apiEnv = ApiEnv(false, url, patToken, getClass.getPackage.getImplementationVersion)
      val endPoint = "clusters/list"
      ApiCallV2(apiEnv, endPoint).execute().asDF()
      updateValidationStatus(DeploymentValidationReport(true,
        getSimpleMsg("APIURL_Connectivity"),
        testDetails,
        Some("SUCCESS"),
        Some(workspaceId)
      ))
    } catch {
      case exception: Exception =>
        val msg = s"""No Data retrieved workspaceId:${workspaceId} APIURL:${url} DBPATWorkspaceScope:${scope} SecretKey_DBPAT:${patKey}"""
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        updateValidationStatus(DeploymentValidationReport(false,
          getSimpleMsg("APIURL_Connectivity"),
          testDetails,
          Some(fullMsg),
          Some(workspaceId)
        ))

    }

  }

  /**
   * Checks for distinct values in the provided column.
   * @param ruleName
   * @param configColumns
   * @return
   */
  private def validateDistinct(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, countDistinct(configColumns), lit(1))
  }


  /**
   * Cloud should be either AWS or Azure.
   * @return
   */
  private def validateCloud(): Rule = {
    Rule("Valid_Cloud_providers", lower(col(ConfigColumns.cloud.toString)), Array("aws", "azure"))
  }


  /**
   * PrimordialDate should be less than current date and should be in yyyy-MM-dd format.
   * @return
   */
  private def validatePrimordialDate(): Rule = {
    Rule("Valid_PrimordialDate", to_date(col(ConfigColumns.primordial_date.toString), "yyyy-MM-dd") <= current_date() && to_date(col(ConfigColumns.primordial_date.toString), "yyyy-MM-dd").isNotNull)
  }

  /**
   * Check for not null values in the provided column
   * @param ruleName
   * @param configColumns
   * @return
   */
  private def validateNotNull(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, col(configColumns).isNotNull)
  }

  /**
   *Max days should be greater then 0 and should be not null.
   * @return
   */
  private def validateMaxDays(): Rule = {
    Rule("Valid_MaxDays", col(ConfigColumns.max_days.toString) >= 0 && col(ConfigColumns.max_days.toString).isNotNull)
  }

  /**
   * Scopes for Overwatch should be audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts,dbsql
   * @return
   */
  private def validateOWScope(): Rule = {
    Rule("Valid_Excluded_Scopes", lower(col(ConfigColumns.excluded_scopes.toString)), Array("audit", "sparkEvents", "jobs", "clusters", "clusterEvents", "notebooks", "pools", "accounts", "dbsql","",null))
  }

  /**
   * Validate audit log data for Aws and Azure
   * @param row
   */
  private def cloudSpecificValidation(row: Row): Unit = {

    row.getAs(ConfigColumns.cloud.toString).toString.toLowerCase match {
      case "aws" =>
        validateAuditLog(row.getAs(ConfigColumns.workspace_id.toString), row.getAs(ConfigColumns.auditlogprefix_source_aws.toString), row.getAs(ConfigColumns.primordial_date.toString), row.getAs(ConfigColumns.max_days.toString))
      case "azure" =>
        validateEventHub(row.getAs(ConfigColumns.workspace_id.toString), row.getAs(ConfigColumns.secret_scope.toString), row.getAs(ConfigColumns.eh_scope_key.toString), row.getAs(ConfigColumns.eh_name.toString))
    }

  }

  /**
   * Performs validation on audit log data for AWS.
   *
   * @param workspace_id
   * @param auditlogprefix_source_aws
   * @param primordial_date
   * @param maxDate
   */
  private def validateAuditLog(workspace_id: String, auditlogprefix_source_aws: String, primordial_date: Date, maxDate: Int): Unit = {
    try {
      val fromDT = new java.sql.Date(primordial_date.getTime()).toLocalDate()
      var untilDT = fromDT.plusDays(maxDate)
      val dateCompare = untilDT.compareTo(LocalDate.now())
      if (dateCompare > 0) { //Changing the max date to current date
        untilDT = LocalDate.now().minusDays(1)
      }
      val daysBetween = ChronoUnit.DAYS.between(fromDT, untilDT)
      var validationFlag = false
      if (daysBetween == 0) {
        validationFlag = Helpers.pathExists(s"${auditlogprefix_source_aws}/date=${fromDT.toString}")
      } else {
        val presentPaths = datesStream(fromDT).takeWhile(_.isBefore(untilDT)).toArray
          .map(dt => s"${auditlogprefix_source_aws}/date=${dt}")
          .filter(Helpers.pathExists)
        if (presentPaths.length == daysBetween) {
          validationFlag = true
        }
      }
      if (validationFlag) {
        updateValidationStatus(DeploymentValidationReport(true,
          getSimpleMsg("Validate_AuditLogPrefix"),
          s"""Folder should be present from :${fromDT} to:${untilDT} """,
          Some("SUCCESS"),
          Some(workspace_id)
        ))
      } else {
        val msg =
          s"""ReValidate the folder existence
             | Make sure folder with required date folder exist inside ${auditlogprefix_source_aws}/workspaceId=${workspace_id}
             |, primordial_date:${primordial_date}
             |, maxDate:${maxDate} """.stripMargin
        logger.log(Level.ERROR, msg)
        updateValidationStatus(DeploymentValidationReport(false,
          getSimpleMsg("Validate_AuditLogPrefix"),
          s"""Folder should be present from :${fromDT} to:${untilDT} """,
          Some(msg),
          Some(workspace_id)
        ))
      }

    } catch {
      case exception: Exception =>
        val msg =
          s"""AuditLogPrefixTest workspace_id:${workspace_id}
             | Make sure folder with required date folder exist inside ${auditlogprefix_source_aws}
             |, primordial_date:${primordial_date}
             |, maxDate:${maxDate} """.stripMargin
        logger.log(Level.ERROR, msg)
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        updateValidationStatus(DeploymentValidationReport(false,
          getSimpleMsg("Validate_AuditLogPrefix"),
          "Validating folders",
          Some(fullMsg),
          Some(workspace_id)
        ))
    }
  }

  /**
   * Performs validation of event hub data for Azure
   *
   * @param workspace_id
   * @param scope
   * @param key
   * @param ehName
   */
  private def validateEventHub(workspace_id: String, scope: String, key: String, ehName: String): Unit = {
    val testDetails = s"""Connectivity test with ehName:${ehName} scope:${scope} SecretKey_DBPAT:${key}"""
    try {
      import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
      val ehConn = dbutils.secrets.get(scope = scope, key)


      val connectionString = ConnectionStringBuilder(
        PipelineFunctions.parseAndValidateEHConnectionString(ehConn, false))
        .setEventHubName(ehName)
        .build

      val ehConf = EventHubsConf(connectionString)
        .setMaxEventsPerTrigger(100)
        .setStartingPosition(EventPosition.fromStartOfStream)

      val rawEHDF = spark.readStream
        .format("eventhubs")
        .options(ehConf.toMap)
        .load()
        .withColumn("deserializedBody", 'body.cast("string"))
      val timestamp = LocalDateTime.now(Pipeline.systemZoneId).toInstant(Pipeline.systemZoneOffset).toEpochMilli
      val tmpPersistDFPath = s"""$outputPath/${ehName.replaceAll("-", "")}/$timestamp/ehTest/rawDataDF"""
      val tempPersistChk = s"""$outputPath/${ehName.replaceAll("-", "")}/$timestamp/ehTest/rawChkPoint"""


      rawEHDF
        .writeStream
        .trigger(Trigger.Once)
        .option("checkpointLocation", tempPersistChk)
        .format("delta")
        .start(tmpPersistDFPath)
        .awaitTermination()

      val rawBodyLookup = spark.read.format("delta").load(tmpPersistDFPath)
      val schemaBuilders = rawBodyLookup
        .withColumn("parsedBody", structFromJson(spark, rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"))
        .selectExpr("streamRecord.*")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .withColumn("organization_id", lit("2222170229861029"))
        .select('resourceId, 'category, 'version, 'timestamp, 'date, 'properties, 'organization_id, 'identity.alias("userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")

      val parsedEHDF = rawBodyLookup
        .withColumn("parsedBody", structFromJson(spark, rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"))
        .selectExpr("streamRecord.*")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .withColumn("organization_id", lit("2222170229861029"))
        .select('resourceId, 'category, 'version, 'timestamp, 'date, 'properties, 'organization_id, 'identity.alias("userIdentity"))
        .withColumn("userIdentity", structFromJson(spark, schemaBuilders, "userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")
        .withColumn("requestParams", structFromJson(spark, schemaBuilders, "requestParams"))

      parsedEHDF
        .drop("response")
        .verifyMinimumSchema(Schema.auditMasterSchema)

      dbutils.fs.rm(s"""${tmpPersistDFPath}""", true)
      dbutils.fs.rm(s"""${tempPersistChk}""", true)

      updateValidationStatus(DeploymentValidationReport(true,
        getSimpleMsg("Validate_EventHub"),
        testDetails,
        Some("SUCCESS"),
        Some(workspace_id)
      ))
    } catch {
      case exception: Exception =>
        val msg = s"""Unable to retrieve data from ehName:${ehName} scope:${scope} SecretKey_DBPAT:${key}"""
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        updateValidationStatus(DeploymentValidationReport(false,
          getSimpleMsg("Validate_EventHub"),
          testDetails,
          Some(fullMsg),
          Some(workspace_id)
        ))
    }

  }

  private def updateValidationStatus(deploymentValidationReport: DeploymentValidationReport): Unit ={
    synchronized {
      validationStatus.append(deploymentValidationReport)
    }
  }

  /**
   * Transforms output of the rules engine to DeploymentValidationReport
   * @param ruleSet
   */
  private def validateRuleAndUpdateStatus(ruleSet: RuleSet): Unit = {
    val validation = ruleSet.validate()
    val columns = ruleSet.getRules.map(x => x.ruleName)
    val completeReportDF = if (!validation.completeReport.columns.contains("workspace_id")) {
      validation.completeReport.withColumn("workspace_id", lit(""))
    } else {
      validation.completeReport
    }
     val resultDF = completeReportDF.withColumn("concat_columns", array(columns map col: _*))
      .withColumn("result", explode(col("concat_columns")))
      .select("deployment_id", "workspace_id", "result")


    val validationReport = resultDF.as[WorkSpaceValidationReport].collect().par
    validationReport.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    validationReport.foreach(validation => {
      var validationMsg = "SUCCESS"
      val resultFlag: Boolean = validation.result.passed==null || validation.result.passed.toBoolean
      if (!resultFlag) {
        validationMsg = s"""Validation failed:  ${validation.result}"""
      }
      val validationDetails = s""" ${validation.result.ruleName}"""
      updateValidationStatus(DeploymentValidationReport(resultFlag, getSimpleMsg(validation.result.ruleName), validationDetails, Some(validationMsg), Some(validation.workspace_id)))

     })

  }

  /**
   * Provides human readable message for each validation.
   *
   * @param ruleName
   * @return
   */
  private  def getSimpleMsg(ruleName: String): String = {
    ruleName match {
      case "Common_ETLStoragePrefix" => "ETL Storage Prefix should be common across the workspaces."
      case "Common_ETLDatabase" => "Workspaces should have a common ETL Database Name."
      case "Common_ConsumerDatabaseName" => "Workspaces should have a common Consumer Database Name."
      case "Valid_Cloud_providers" => "Cloud provider can be either AWS or Azure."
      case "NOTNULL_APIURL" => "API URL should not be empty."
      case "NOTNULL_SecretScope" => "Secrete scope should not be empty."
      case "NOTNULL_SecretKey_DBPAT" => "PAT key should not be empty."
      case "Valid_PrimordialDate" => "Primordial Date should in yyyy-MM-dd format(Ex:2022-01-30) and should be less than current date."
      case "Valid_MaxDays" => "Max Days should be a number."
      case "APIURL_Connectivity" => "API URL should give some response with provided scope and key."
      case "Validate_AuditLogPrefix" => "Folder with Primordial date should be present inside AuditLogPrefix."
      case "Validate_EventHub" => "Consuming data from EventHub."
      case "Valid_Excluded_Scopes" => "Excluded scope can be audit:sparkEvents:jobs:clusters:clusterEvents:notebooks:pools:accounts."
      case "Storage_Access" => "ETL_STORAGE_PREFIX should have read,write and create access"
    }
  }

  /**
   * Performs mandatory validation before each deployment.
   */
  private[overwatch] def performMandatoryValidation() = {
    logger.log(Level.INFO, "Performing mandatory validation")
    validateFileExistence()
    makeDataFrame()
    val groupedRuleSet = RuleSet(inputDataFrame, by = "deployment_id")
    groupedRuleSet.add(validateDistinct("Common_ETLStoragePrefix",ConfigColumns.etl_storage_prefix.toString))
    validateRuleAndUpdateStatus(groupedRuleSet)
    if (validationStatus.toDS().filter("validated==false").count > 0) { //Checks for failed validations
      throw new BadConfigException(getSimpleMsg("Common_ETLStoragePrefix"))
    }
    storagePrefixAccessValidation(inputDataFrame.head(), true)
  }

  /**
   * Entry point of the validation.
   *
   * @return
   */
  private[overwatch] def performValidation(): Dataset[DeploymentValidationReport] = {
    //Primary validation //
    validateFileExistence()
    makeDataFrame()
    println("Parallelism :" + parallelism)

    //csv data validation
    val processingStartTime = System.currentTimeMillis();
    val groupedRuleSet = RuleSet(inputDataFrame, by = "deployment_id")
    val gropedRules = Seq[Rule](
      validateDistinct("Common_ETLStoragePrefix",ConfigColumns.etl_storage_prefix.toString),
      validateDistinct("Common_ETLDatabase",ConfigColumns.etl_database_name.toString),
      validateDistinct("Common_ConsumerDatabaseName",ConfigColumns.consumer_database_name.toString)
    )
    groupedRuleSet.add(gropedRules)
    validateRuleAndUpdateStatus(groupedRuleSet)

    val nonGroupedRuleSet = RuleSet(inputDataFrame)
    val nonGroupedRules = Seq[Rule](
      validateNotNull("NOTNULL_APIURL",ConfigColumns.api_url.toString),
      validateNotNull("NOTNULL_SecretScope",ConfigColumns.secret_scope.toString),
      validateNotNull("NOTNULL_SecretKey_DBPAT",ConfigColumns.secret_key_dbpat.toString),
      validateCloud(),
      validatePrimordialDate(),
      validateMaxDays(),
      validateOWScope()
    )
    nonGroupedRuleSet.add(nonGroupedRules)
    validateRuleAndUpdateStatus(nonGroupedRuleSet)
  val processingEndTime = System.currentTimeMillis();
    println("Rule engine validation Duration in sec :" + (processingEndTime - processingStartTime) / 1000)

    //Connectivity validation
    val inputRow = inputDataFrame.collect().par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    inputRow.tasksupport = taskSupport
    inputRow.map(validateApiUrl)
    val processingEtURL = System.currentTimeMillis();
    println("URL validation Duration in sec :" + (processingEtURL - processingEndTime) / 1000)

    //Cloud specific validation s3/EH
    inputRow.map(cloudSpecificValidation)
    val processingCould = System.currentTimeMillis();
    println("Cloud specific validation Duration in sec :" + (processingCould - processingEtURL) / 1000)
    storagePrefixAccessValidation(inputRow.head)

    inputDataFrame.unpersist()
    validationStatus.toDS()
  }

}

object ConfigColumns extends Enumeration {
  val workspace_name, workspace_id, workspace_url, api_url, cloud, primordial_date,
  etl_storage_prefix, etl_database_name, consumer_database_name, secret_scope,
  secret_key_dbpat, auditlogprefix_source_aws, eh_name, eh_scope_key, scopes,
  interactive_dbu_price, automated_dbu_price, sql_compute_dbu_price, jobs_light_dbu_price, max_days, excluded_scopes, active, deploymentId = Value
}