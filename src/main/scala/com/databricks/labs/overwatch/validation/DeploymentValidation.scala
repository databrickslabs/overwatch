package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ApiCallV2
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.{Pipeline, PipelineFunctions, Schema}
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils.{BadConfigException, _}
import com.databricks.labs.validation.{Rule, RuleSet}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import java.time.{LocalDate, LocalDateTime}
import java.time.temporal.ChronoUnit
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object DeploymentValidation extends SparkSessionWrapper {

  def apply(configCsvPath: String, outputPath: String, parallelism: Int,deploymentId:String) = {
    new DeploymentValidation()
      .setConfigCsvPath(configCsvPath)
      .setOutputPath(outputPath)
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

  private var _validationStatus: ArrayBuffer[DeploymentValidationReport] = _

  private var _deploymentId: String = _

  private var _configCsvPath: String = _

  private var _isDelta: Boolean = _


  protected def configCsvPath: String = _configCsvPath

  protected def outputPath: String = _outputPath

  protected def deploymentId: String = _deploymentId


  protected def parallelism: Int = _parallelism

  protected def cloudProvider: String = _cloudProvider

  protected def inputDataFrame: DataFrame = _inputDataFrame

  protected def validationStatus: ArrayBuffer[DeploymentValidationReport] = _validationStatus

  private[overwatch] def setIsDelta(value: Boolean): this.type = {
    _isDelta = value
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

  private[overwatch] def setParallelism(value: Int): this.type = {
    _parallelism = value
    this
  }

  private[overwatch] def setDeploymentId(value: String): this.type = {
    _deploymentId = value
    this
  }

  private[overwatch] def setValidationStatus(value: ArrayBuffer[DeploymentValidationReport]): this.type = {
    _validationStatus = value
    this
  }


  private[overwatch] def setCloudProvider(value: String): this.type = {
    _cloudProvider = value
    this
  }

  private[overwatch] def setInputDataFrame(value: DataFrame): this.type = {
    _inputDataFrame = value
    _inputDataFrame.persist(StorageLevel.MEMORY_AND_DISK)
    this
  }

  private[overwatch] def validateFileExistance() = {
    if (!Helpers.pathExists(configCsvPath)) {
      throw new BadConfigException("Unable to find config file in the given location:" + configCsvPath)
    }
  }

  def makeDataFrame(): DataFrame = {
    try {
      var df = spark.read.option("header", "true")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .csv(configCsvPath)
        .verifyMinimumSchema(Schema.deployementMinimumSchema, enforceNonNullCols = false)
        .filter(ConfigColumns.active.toString)
        .withColumn("deploymentId", lit(deploymentId))
      val columns = df.columns
      columns.foreach(columnName =>
        df = df.withColumnRenamed(columnName, columnName.replaceAll(" ", ""))
      )
      setInputDataFrame(df)
      setValidationStatus(ArrayBuffer())
      df
    }catch{
      case e:Exception=>
        val fullMsg = PipelineFunctions.appendStackStrace(e, "Unable to create Config Dataframe")
        logger.log(Level.ERROR, fullMsg)
        throw e
    }

  }

  private[overwatch] def storagePrefixAccessValidation(row: Row,fastFail:Boolean=false): Unit = {
    val storagePrefix = row.getAs(ConfigColumns.etl_storage_prefix.toString).toString
    val workspaceId = row.getAs(ConfigColumns.workspace_id.toString).toString
    val testDetails = s"""StorageAccessTest storage : ${storagePrefix}"""
    try{
      dbutils.fs.mkdirs(s"""${storagePrefix}/test_access""")
      dbutils.fs.put(s"""${storagePrefix}/test_access/testwrite""", "This is a file in cloud storage.")
      dbutils.fs.head(s"""${storagePrefix}/test_access/testwrite""")
      dbutils.fs.rm(s"""${storagePrefix}/test_access""",true)
      validationStatus.append(DeploymentValidationReport(true,
        getSimpleMsg("Storage_Access"),
        testDetails,
        Some("SUCCESS"),
        Some(workspaceId)
      ))
    }
    catch {
      case exception:Exception =>
        val msg = s"""Unable to read/write/create file in the provided etl storage prefix"""
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        validationStatus.append(DeploymentValidationReport(false,
          getSimpleMsg("Storage_Access"),
          testDetails,
          Some(fullMsg),
          Some(workspaceId)
        ))
        if(fastFail){
          throw new BadConfigException(fullMsg)
        }


    }
  }

  private[overwatch] def validateApiUrl(row: Row): Unit = {
    val url = row.getAs(ConfigColumns.api_url.toString).toString
    val scope = row.getAs(ConfigColumns.secret_scope.toString).toString
    val patKey = row.getAs(ConfigColumns.secret_key_dbpat.toString).toString
    val workspaceId = row.getAs(ConfigColumns.workspace_id.toString).toString
    val testDetails = s"""WorkSpaceURLConnectivityTest APIURL:${url} DBPATWorkspaceScope:${scope} SecretKey_DBPAT:${patKey}"""
    try {
      val patToken = dbutils.secrets.get(scope = scope, key = patKey)
      val apiEnv = ApiEnv(false, url, patToken, "6.1.2.0")
      val endPoint = "clusters/list"
      ApiCallV2(apiEnv, endPoint).execute().asDF()
      validationStatus.append(DeploymentValidationReport(true,
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
        validationStatus.append(DeploymentValidationReport(false,
          getSimpleMsg("APIURL_Connectivity"),
          testDetails,
          Some(fullMsg),
          Some(workspaceId)
        ))

    }

  }

  private[overwatch] def validateEtlStorage(): Rule = {
    Rule("Common_ETLStoragePrefix", countDistinct(ConfigColumns.etl_storage_prefix.toString), lit(1))
  }

  private[overwatch] def validateETLDatabaseName(): Rule = {
    Rule("Common_ETLDatabase", countDistinct(ConfigColumns.etl_database_name.toString), lit(1))
  }

  private[overwatch] def validateConsumerDatabaseName(): Rule = {
    Rule("Common_ConsumerDatabaseName", countDistinct(ConfigColumns.consumer_database_name.toString), lit(1))
  }

  private[overwatch] def validateCloud(): Rule = {
    Rule("Valid_Cloud_providers", col(ConfigColumns.cloud.toString), Array("AWS", "Azure"))
  }

  private[overwatch] def validateSecretScope(): Rule = {
    Rule("NOTNULL_SecretScope", col(ConfigColumns.secret_scope.toString).isNotNull)
  }

  private[overwatch] def validateSecreteKeyDBPAT(): Rule = {
    Rule("NOTNULL_SecretKey_DBPAT", col(ConfigColumns.secret_key_dbpat.toString).isNotNull)
  }

  private[overwatch] def validateAPIURL(): Rule = {
    Rule("NOTNULL_APIURL", col(ConfigColumns.api_url.toString).isNotNull)
  }

  private[overwatch] def validatePrimordialDate(): Rule = {
    Rule("Valid_PrimordialDate", to_date(col(ConfigColumns.primordial_date.toString),"yyyy-MM-dd") <= current_date() && to_date(col(ConfigColumns.primordial_date.toString),"yyyy-MM-dd").isNotNull)
  }

  private[overwatch] def validateMaxDays(): Rule = {
    Rule("Valid_MaxDays", col(ConfigColumns.max_days.toString) >= 0 && col(ConfigColumns.max_days.toString).isNotNull)
  }

  private[overwatch] def validateOWScope(): Rule = {
    Rule("Valid_Excluded_Scopes", col(ConfigColumns.excluded_scopes.toString), Array("audit", "sparkEvents", "jobs", "clusters", "clusterEvents", "notebooks", "pools", "accounts", "", null))
  }


  private[overwatch] def cloudSpecificValidation(row: Row): Unit = {

    row.getAs(ConfigColumns.cloud.toString).toString match {
      case "AWS" =>
        validateAuditLog(row.getAs(ConfigColumns.workspace_id.toString), row.getAs(ConfigColumns.auditlogprefix_source_aws.toString), row.getAs(ConfigColumns.primordial_date.toString), row.getAs(ConfigColumns.max_days.toString))
      case "Azure" =>
        validateEventHub(row.getAs(ConfigColumns.workspace_id.toString), row.getAs(ConfigColumns.secret_scope.toString), row.getAs(ConfigColumns.eh_scope_key.toString), row.getAs(ConfigColumns.eh_name.toString))
    }

  }

  private[overwatch] def validateAuditLog(workspace_id: String, auditlogprefix_source_aws: String, primordial_date: Date, maxDate: Int): Unit = {
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
        validationStatus.append(DeploymentValidationReport(true,
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
        validationStatus.append(DeploymentValidationReport(false,
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
        validationStatus.append(DeploymentValidationReport(false,
          getSimpleMsg("Validate_AuditLogPrefix"),
          "Validating folders",
          Some(fullMsg),
          Some(workspace_id)
        ))
    }
  }

  private[overwatch] def validateEventHub(workspace_id: String, scope: String, key: String, ehName: String): Unit = {
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

      dbutils.fs.rm(s"""${tmpPersistDFPath}""",true)
      dbutils.fs.rm(s"""${tempPersistChk}""",true)

      validationStatus.append(DeploymentValidationReport(true,
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
        validationStatus.append(DeploymentValidationReport(false,
          getSimpleMsg("Validate_EventHub"),
          testDetails,
          Some(fullMsg),
          Some(workspace_id)
        ))
    }

  }


  private[overwatch] def validateRuleAndUpdateStatus(ruleSet: RuleSet, groupedRule: Boolean): Unit = {
    val validation = ruleSet.validate()
    val inputRow = ruleSet.getRules.par
    inputRow.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    ruleSet.getRules.foreach(rule => { //TODO case class / higher order function
      if (groupedRule) {
        validation.completeReport.select(rule.ruleName).collect().foreach(reportRow => {
          val validationRow = reportRow.getAs(0).toString
          val validationResult = validationRow.contains("true")
          var validationMsg = "SUCCESS"
          if (!validationResult) {
            validationMsg = s"""Validation failed:  ${validationRow}"""
          }
          val validationDetails = s"""Grouped Test: ${validationRow}"""
          validationStatus.append(DeploymentValidationReport(validationResult, getSimpleMsg(rule.ruleName), validationDetails, Some(validationMsg), Some("")))
        })
      } else {
        validation.completeReport.select(rule.ruleName, ConfigColumns.workspace_id.toString).collect().foreach(reportRow => {
          val validationRow = reportRow.getAs(0).toString
          val dataArray = validationRow.split(",")
          val validationResult = validationRow.contains("true") || dataArray(1) == "null"
          val workspace_id = reportRow.getAs(1).toString
          var validationMsg = "SUCCESS"
          if (!validationResult) {
            validationMsg = s"""Validation failed: ${validationRow}"""
          }
          val validationDetails = s"""Test :${validationRow}, workspace_id:${workspace_id}"""
          validationStatus.append(DeploymentValidationReport(validationResult, getSimpleMsg(rule.ruleName), validationDetails, Some(validationMsg), Some(workspace_id)))
        })

      }

    })
  }

  private[overwatch] def getSimpleMsg(ruleName: String): String = {
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


 def performMandatoryValidation = {
   println("Performing mandatory validation")
   validateFileExistance
   makeDataFrame
   val groupedRuleSet = RuleSet(inputDataFrame, by = "deploymentId")
   groupedRuleSet.add(validateEtlStorage)
   validateRuleAndUpdateStatus(groupedRuleSet, true)
  if(validationStatus.toDS().filter("validated==false").count>0) {
    throw new BadConfigException(getSimpleMsg("Common_ETLStoragePrefix"))
  }
   storagePrefixAccessValidation(inputDataFrame.head(),true)
 }

  def performValidation: Dataset[DeploymentValidationReport] = {
    //Primary validation //
    validateFileExistance
    makeDataFrame
    println("Parallelism :" + parallelism)

    //csv data validation
    val processingStartTime = System.currentTimeMillis();
    val groupedRuleSet = RuleSet(inputDataFrame, by = "deploymentId")
    groupedRuleSet.add(validateEtlStorage)
    groupedRuleSet.add(validateETLDatabaseName)
    groupedRuleSet.add(validateConsumerDatabaseName)
    validateRuleAndUpdateStatus(groupedRuleSet, true)

    val nonGroupedRuleSet = RuleSet(inputDataFrame)
    nonGroupedRuleSet.add(validateCloud)
    nonGroupedRuleSet.add(validateAPIURL)
    nonGroupedRuleSet.add(validateSecretScope)
    nonGroupedRuleSet.add(validateSecreteKeyDBPAT)
    nonGroupedRuleSet.add(validatePrimordialDate)
    nonGroupedRuleSet.add(validateMaxDays)
    nonGroupedRuleSet.add(validateOWScope())
    validateRuleAndUpdateStatus(nonGroupedRuleSet, false)
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
  interactive_dbu_price, automated_dbu_price,sql_compute_dbu_price,jobs_light_dbu_price, max_days, excluded_scopes, active, deploymentId = Value
}