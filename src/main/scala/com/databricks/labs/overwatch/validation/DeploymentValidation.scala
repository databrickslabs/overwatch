package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.api.ApiCallV2
import com.databricks.labs.overwatch.eventhubs.AadAuthInstance
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.{Initializer, Pipeline, PipelineFunctions, Schema}
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.validation.{Rule, RuleSet}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

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


  import spark.implicits._


  private val logger: Logger = Logger.getLogger(this.getClass)



  /**
   * Create a DataFrame from provided csv file.
   * @return
   */
  private[overwatch] def makeDataFrame(configCsvPath: String,deploymentId: String,outputPath: String = ""): Dataset[MultiWorkspaceConfig] = {
    try {
      val df = spark.read.option("header", "true")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .csv(configCsvPath)
        .scrubSchema
        .verifyMinimumSchema(Schema.deployementMinimumSchema)
        .filter(MultiWorkspaceConfigColumns.active.toString)
        .withColumn("deployment_id", lit(deploymentId))
        .withColumn("output_path", lit(outputPath))
     val multiWorkspaceConfig = df.as[MultiWorkspaceConfig]
      multiWorkspaceConfig
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
  private def storagePrefixAccessValidation(config: MultiWorkspaceConfig, fastFail: Boolean = false): DeploymentValidationReport  = {
    val testDetails = s"""StorageAccessTest storage : ${config.storage_prefix}"""
    try {
      dbutils.fs.mkdirs(s"""${config.storage_prefix}/test_access""")
      dbutils.fs.put(s"""${config.storage_prefix}/test_access/testwrite""", "This is a file in cloud storage.")
      dbutils.fs.head(s"""${config.storage_prefix}/test_access/testwrite""")
      dbutils.fs.rm(s"""${config.storage_prefix}/test_access""", true)
      DeploymentValidationReport(true,
        getSimpleMsg("Storage_Access"),
        testDetails,
        Some("SUCCESS"),
        Some(config.workspace_id)
      )
    }
    catch {
      case exception: Exception =>
        val msg = s"""Unable to read/write/create file in the provided etl storage prefix"""
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        if (fastFail) {
          throw new BadConfigException(fullMsg)
        }
        DeploymentValidationReport(false,
          getSimpleMsg("Storage_Access"),
          testDetails,
          Some(fullMsg),
          Some(config.workspace_id)
        )
    }
  }

  /**
   * Validates the content of provided mount_mapping_path csv file.Below are the validation rules.
   * 1)validate for file existence.
   * 2)validate the provided csv file belongs to the provided workspace_id.
   * 3)validate the provided csv file contains columns "mountPoint", "source","workspace_id" and has some values in it.
   *
   * @param conf
   * @return
   */
  private def validateMountMappingPath(conf: MultiWorkspaceConfig): DeploymentValidationReport = {

    // get fine here -- already verified non-empty in calling function
    val path = conf.mount_mapping_path.get.trim
    val testDetails =
      s"""WorkSpaceMountTest
         |mount_mapping_path:${path}
         """.stripMargin

    try {
      if (!Helpers.pathExists(path)) {
        DeploymentValidationReport(false,
          getSimpleMsg("Validate_Mount"),
          testDetails,
          Some("Unable to find the provided csv: " + path),
          Some(conf.workspace_id)
        )
      } else {
        val inputDf = spark.read.option("header", "true")
          .option("ignoreLeadingWhiteSpace", true)
          .option("ignoreTrailingWhiteSpace", true)
          .csv(path)
          .filter('source.isNotNull)
          .verifyMinimumSchema(Schema.mountMinimumSchema)
          .select("mountPoint", "source","workspace_id")
          .filter('workspace_id === conf.workspace_id)


        val dataCount = inputDf.count()
        if (dataCount > 0) {
          DeploymentValidationReport(true,
            getSimpleMsg("Validate_Mount"),
            s"""WorkSpaceMountTest
               |mount_mapping_path:${path}
               |mount points found:${dataCount}
               """.stripMargin,
            Some("SUCCESS"),
            Some(conf.workspace_id)
          )
        } else {
          DeploymentValidationReport(false,
            getSimpleMsg("Validate_Mount"),
            testDetails,
            Some(s"""No data found for workspace_id: ${conf.workspace_id} in provided csv: ${path}"""),
            Some(conf.workspace_id)
          )
        }
      }
    } catch {
      case e: Exception =>
        val fullMsg = PipelineFunctions.appendStackStrace(e, s"""Exception while reading the mount_mapping_path :${path}""")
        logger.log(Level.ERROR, fullMsg)
        DeploymentValidationReport(false,
          getSimpleMsg("Validate_Mount"),
          testDetails,
          Some(fullMsg),
          Some(conf.workspace_id)
        )
    }

  }
  private def validateMountCount(conf: MultiWorkspaceConfig,currentOrgID:String): DeploymentValidationReport = {

    val isAzure = conf.cloud.toLowerCase == "azure" //Mount-point validation is only done for Azure
    val isRemoteWorkspace = conf.workspace_id.trim != currentOrgID// No need to perform mount-point validation for driver workspace.
    val isMountMappingPathProvided = conf.mount_mapping_path.nonEmpty

    if (isAzure && isRemoteWorkspace) { //Performing mount test
      if (isMountMappingPathProvided) {
        validateMountMappingPath(conf)
      } else {
        val testDetails =
          s"""WorkSpaceMountTest
             |APIURL:${conf.api_url}
             |DBPATWorkspaceScope:${conf.secret_scope}
             |SecretKey_DBPAT:${conf.secret_key_dbpat}""".stripMargin
        try {
          val patToken = dbutils.secrets.get(scope = conf.secret_scope, key = conf.secret_key_dbpat)
          val apiEnv = ApiEnv(false, conf.api_url, patToken, getClass.getPackage.getImplementationVersion)
          val endPoint = "dbfs/search-mounts"
          val mountCount = ApiCallV2(apiEnv, endPoint).execute().asDF().count()
          if (mountCount < 50) {
            DeploymentValidationReport(true,
              getSimpleMsg("Validate_Mount"),
              testDetails,
              Some("SUCCESS"),
              Some(conf.workspace_id)
            )
          } else {
            DeploymentValidationReport(false,
              getSimpleMsg("Validate_Mount"),
              testDetails,
              Some("Number of mounts found in workspace is more than 50"),
              Some(conf.workspace_id)
            )
          }

        } catch {
          case exception: Exception =>
            val msg =
              s"""No Data retrieved
                 |WorkspaceId:${conf.workspace_id}
                 |APIURL:${conf.api_url}
                 | DBPATWorkspaceScope:${conf.secret_scope}
                 | SecretKey_DBPAT:${conf.secret_key_dbpat}""".stripMargin
            val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
            logger.log(Level.ERROR, fullMsg)
            DeploymentValidationReport(false,
              getSimpleMsg("Validate_Mount"),
              testDetails,
              Some(fullMsg),
              Some(conf.workspace_id)
            )

        }
      }
    } else {
      DeploymentValidationReport(true,
        getSimpleMsg("Validate_Mount"),
        "Skipping mount point check",
        Some("SUCCESS"),
        Some(conf.workspace_id)
      )
    }


  }

  /**
   * Performs clusters/list api call to check the access to the workspace
   * @param conf
   * @return
   */
  private def validateApiUrlConnectivity(conf: MultiWorkspaceConfig): DeploymentValidationReport = {

    val apiUrl = if (conf.api_url.trim.charAt(conf.api_url.length - 1) == '/') {
      conf.api_url.trim.substring(0, conf.api_url.length - 1)
    } else {
      conf.api_url.trim
    }
    val testDetails =
      s"""WorkSpaceURLConnectivityTest
         |APIURL:${apiUrl}
         |DBPATWorkspaceScope:${conf.secret_scope}
         |SecretKey_DBPAT:${conf.secret_key_dbpat}""".stripMargin
    try {
      val patToken = dbutils.secrets.get(scope = conf.secret_scope, key = conf.secret_key_dbpat)
      val apiProxyConfig = ApiProxyConfig(conf.proxy_host, conf.proxy_port, conf.proxy_user_name, conf.proxy_password_scope, conf.proxy_password_key)
      val derivedApiEnvConfig = ApiEnvConfig(conf.success_batch_size.getOrElse(200),
        conf.error_batch_size.getOrElse(500),
        conf.enable_unsafe_SSL.getOrElse(false),
        conf.thread_pool_size.getOrElse(4),
        conf.api_waiting_time.getOrElse(300000),
        Some(apiProxyConfig))

      val derivedApiProxy = derivedApiEnvConfig.apiProxyConfig.getOrElse(ApiProxyConfig())
      val apiEnv = ApiEnv(false, apiUrl, patToken, getClass.getPackage.getImplementationVersion, derivedApiEnvConfig.successBatchSize,
        derivedApiEnvConfig.errorBatchSize, "runID", derivedApiEnvConfig.enableUnsafeSSL, derivedApiEnvConfig.threadPoolSize,
        derivedApiEnvConfig.apiWaitingTime, derivedApiProxy.proxyHost, derivedApiProxy.proxyPort,
        derivedApiProxy.proxyUserName, derivedApiProxy.proxyPasswordScope, derivedApiProxy.proxyPasswordKey
      )



      val endPoint = "clusters/list"
      ApiCallV2(apiEnv, endPoint).execute().asDF()
      DeploymentValidationReport(true,
        getSimpleMsg("APIURL_Connectivity"),
        testDetails,
        Some("SUCCESS"),
        Some(conf.workspace_id)
      )
    } catch {
      case exception: Exception =>
        val msg =
          s"""No Data retrieved
             |WorkspaceId:${conf.workspace_id}
             |APIURL:${apiUrl}
             | DBPATWorkspaceScope:${conf.secret_scope}
             | SecretKey_DBPAT:${conf.secret_key_dbpat}""".stripMargin
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        DeploymentValidationReport(false,
          getSimpleMsg("APIURL_Connectivity"),
          testDetails,
          Some(fullMsg),
          Some(conf.workspace_id)
        )

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
    Rule("Valid_Cloud_providers", lower(col(MultiWorkspaceConfigColumns.cloud.toString)), Array("aws", "azure","gcp"))
  }


  /**
   * PrimordialDate should be less than current date and should be in yyyy-MM-dd format.
   * @return
   */
  private def validatePrimordialDate(): Rule = {
    Rule("Valid_PrimordialDate", to_date(col(MultiWorkspaceConfigColumns.primordial_date.toString), "yyyy-MM-dd") <= current_date() && to_date(col(MultiWorkspaceConfigColumns.primordial_date.toString), "yyyy-MM-dd").isNotNull)
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
    Rule("Valid_MaxDays", col(MultiWorkspaceConfigColumns.max_days.toString) >= 0 && col(MultiWorkspaceConfigColumns.max_days.toString).isNotNull)
  }

  /**
   * Scopes for Overwatch should be audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts,dbsql
   * @return
   */
  private def validateOWScope(): Rule = {
    Rule("Valid_Excluded_Scopes",
      lower(col(MultiWorkspaceConfigColumns.excluded_scopes.toString)),
      Array("audit", "sparkEvents", "jobs", "clusters", "clusterEvents", "notebooks", "pools", "accounts", "dbsql","",null))
  }

  /**
   * Validate audit log data for Aws and Azure
   * @param config
   * @return
   */
  private def cloudSpecificValidation(config: MultiWorkspaceConfig): DeploymentValidationReport = {

    config.cloud.toLowerCase match {
      case cloudType if cloudType == "aws" || cloudType == "gcp" =>
        validateAuditLog(
          config.workspace_id,
          config.auditlogprefix_source_path,
          config.primordial_date,
          config.max_days
        )
      case "azure" =>
        validateEventHub(
         config)
    }

  }

  /**
   *For overwatch version < 0.7.2 We have auditlogprefix_source_AWS as a column in config.csv.
   * As we have completed GCP integration on overwatch version 0.7.2 we are not supporting column auditlogprefix_source_AWS.
   * This column name has been changed to auditlogprefix_source_PATH. This function checks whether the config.csv contains auditlogprefix_source_PATH or not.
   * @param config
   * @return
   */
  private def validateAuditLogColumnName(config: MultiWorkspaceConfig) = {
    config.auditlogprefix_source_path.getOrElse(
      throw new BadConfigException("auditlogprefix_source_path cannot be null when cloud is AWS/GCP," +
        "WE DO NOT support auditlogprefix_source_aws column anymore please change the column name to  auditlogprefix_source_path.")
    )
  }

  /**
   * Performs validation on audit log data for AWS.
   *
   * @param workspace_id
   * @param auditlogprefix_source_aws
   * @param primordial_date
   * @param maxDate
   */
  private def validateAuditLog(workspace_id: String, auditlogprefix_source_path: Option[String], primordial_date: Date, maxDate: Int): DeploymentValidationReport = {
    try {
      if (auditlogprefix_source_path.isEmpty) throw new BadConfigException(
        "auditlogprefix_source_path cannot be null when cloud is AWS/GCP,WE DO NOT support auditlogprefix_source_aws column anymore please change the column name to  auditlogprefix_source_path")
      val auditLogPrefix = auditlogprefix_source_path.get
      val fromDT = new java.sql.Date(primordial_date.getTime).toLocalDate
      var untilDT = fromDT.plusDays(maxDate.toLong)
      val dateCompare = untilDT.compareTo(LocalDate.now())
      val msgBuffer = new StringBuffer()
      if (dateCompare > 0) { //Changing the max date to current date
        untilDT = LocalDate.now()
      }
      val daysBetween = ChronoUnit.DAYS.between(fromDT, untilDT)
      var validationFlag = false
      if (daysBetween == 0) {
        validationFlag = Helpers.pathExists(s"${auditLogPrefix}/date=${fromDT.toString}")
      } else {
        val pathsToCheck = datesStream(fromDT).takeWhile(_.isBefore(untilDT)).toArray
          .map(dt => s"${auditLogPrefix}/date=${dt}")
        val presentPaths = datesStream(fromDT).takeWhile(_.isBefore(untilDT)).toArray
          .map(dt => s"${auditLogPrefix}/date=${dt}")
          .filter(Helpers.pathExists)
        if (presentPaths.length == daysBetween) {
          validationFlag = true
        }
        else if(presentPaths.length >0){
         val presentPathSet = presentPaths.toSet
          msgBuffer.append("Warning: unable to find below paths: ")
          pathsToCheck.filterNot(presentPathSet).foreach(path=>msgBuffer.append(path+","))
        }else{
          msgBuffer.append("Exception: audit logs not found ")
        }

      }
      if (validationFlag) {
        DeploymentValidationReport(true,
          getSimpleMsg("Validate_AuditLogPrefix"),
          s"""Audit log folders should be present from :${fromDT} to:${untilDT} """,
          Some("SUCCESS"),
          Some(workspace_id)
        )
      } else {
        val msg =
          s"""ReValidate the folder existence
             | Make sure audit log with required date folder exist inside ${auditlogprefix_source_path.getOrElse("EMPTY")}
             |, primordial_date:${primordial_date}
             |, maxDate:${maxDate} """.stripMargin

        msgBuffer.append(msg)
        logger.log(Level.ERROR, msg)
        DeploymentValidationReport(false,
          getSimpleMsg("Validate_AuditLogPrefix"),
          s"""Audit log folders should be present from :${fromDT} to:${untilDT} """,
          Some(msgBuffer.toString),
          Some(workspace_id)
        )
      }

    } catch {
      case exception: Exception =>
        val msg =
          s"""AuditLogPrefixTest workspace_id:${workspace_id}
             | Make sure audit log with required date folder exist inside ${auditlogprefix_source_path.getOrElse("EMPTY")}
             |, primordial_date:${primordial_date}
             |, maxDate:${maxDate} """.stripMargin
        logger.log(Level.ERROR, msg)
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        DeploymentValidationReport(false,
          getSimpleMsg("Validate_AuditLogPrefix"),
          "Validating folders",
          Some(fullMsg),
          Some(workspace_id)
        )
    }
  }

  /**
   * Consumes the data from Event Hub.
   * @param config
   * @param isAAD If true it will use aad to authenticate and consume the data.
   * @param tempPersistDFPath Path in which consumed events will be stored.
   * @param tempPersistChk Used for EH checkpoint.
   */
  private def consumeEHData(config: MultiWorkspaceConfig, isAAD: Boolean,tempPersistDFPath: String ,tempPersistChk: String ):Unit = {

    val ehConn = if (isAAD) config.eh_conn_string.get else dbutils.secrets.get(scope = config.secret_scope, config.eh_scope_key.get)
    val connectionString = ConnectionStringBuilder(
      PipelineFunctions.parseAndValidateEHConnectionString(ehConn, false))
      .setEventHubName(config.eh_name.get)
      .build
    val ehConf = EventHubsConf(connectionString)
      .setMaxEventsPerTrigger(5000)
      .setStartingPosition(EventPosition.fromStartOfStream)
    val rawEHDF = if (isAAD) {
      val aadParams = Map("aad_tenant_id" -> config.aad_tenant_id.get,
        "aad_client_id" -> config.aad_client_id.get,
        "aad_client_secret" -> dbutils.secrets.get(scope = config.secret_scope, key = config.aad_client_secret_key.get),
        "aad_authority_endpoint" -> config.aad_authority_endpoint.getOrElse("https://login.microsoftonline.com/"))
      spark.readStream
        .format("eventhubs")
        .options(AadAuthInstance.addAadAuthParams(ehConf, aadParams).toMap)
        .load()
    } else {
      spark.readStream
        .format("eventhubs")
        .options(ehConf.toMap)
        .load()
    }
    rawEHDF
      .withColumn("deserializedBody", 'body.cast("string"))
      .writeStream
      .trigger(Trigger.Once)
      .option("checkpointLocation", tempPersistChk)
      .format("delta")
      .start(tempPersistDFPath)
      .awaitTermination()

  }


  /**
   * Decides if the provided configuration is AAD or not.
   *
   * @param config
   * @return
   */
  private def checkAAD(config: MultiWorkspaceConfig): Boolean = {
    if (config.eh_name.isEmpty)
      throw new BadConfigException("eh_name should be nonempty, please check the configuration.")

    return if (config.aad_client_id.nonEmpty && //Check the mandatory field for AAD connection.
      config.aad_tenant_id.nonEmpty &&
      config.aad_client_secret_key.nonEmpty &&
      config.eh_conn_string.nonEmpty) {
      if (config.eh_scope_key.isEmpty) { //eh_scope_key should be empty for AAD.
        true
      } else {
        throw new BadConfigException("For AAD eh_scope_key should be empty")
      }
    } else if (config.eh_scope_key.nonEmpty) { //Checking for legacy connection.
      if (config.aad_client_id.isEmpty &&
        config.aad_tenant_id.isEmpty &&
        config.aad_client_secret_key.isEmpty &&
        config.eh_conn_string.isEmpty) {
        false
      } else {
        throw new BadConfigException("For NON AAD aad_client_id,aad_tenant_id,aad_client_secret_key,eh_conn_string should be empty")
      }
    } else {
      throw new BadConfigException("EXCEPTION: Please check Event Hub configuration eh_name,eh_scope_key .For AAD check aad_client_id,aad_tenant_id,aad_client_secret_key,eh_conn_string")
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
  private def validateEventHub(
                               config: MultiWorkspaceConfig
                              ): DeploymentValidationReport = {

    val isAAD = checkAAD(config)
    if (config.eh_scope_key.isEmpty && config.eh_name.isEmpty && !isAAD) throw new BadConfigException("When cloud is Azure, the eh_name and " +
      "eh_scope_key are required fields but they were empty in the config. For AAD clinetID,tenentID and clientSecretKey are required fields but they were empty in the config")
    // using gets here because if they were empty from above check, exception would already be thrown
    val ehName = config.eh_name.get
    val testDetails = s"""Connectivity test for ehName:${ehName} """
    try {
      val timestamp = LocalDateTime.now(Pipeline.systemZoneId).toInstant(Pipeline.systemZoneOffset).toEpochMilli
      val tempPersistDFPath = s"""${config.output_path}/${config.eh_name.get.replaceAll("-", "")}/$timestamp/ehTest/rawDataDF"""
      val tempPersistChk = s"""${config.output_path}/${config.eh_name.get.replaceAll("-", "")}/$timestamp/ehTest/rawChkPoint"""
      consumeEHData(config, isAAD,tempPersistDFPath,tempPersistChk)
      val rawBodyLookup = spark.read.format("delta").load(tempPersistDFPath)
      val schemaBuilders = rawBodyLookup
        .withColumn("parsedBody", structFromJson(spark, rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"))
        .selectExpr("streamRecord.*")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .withColumn("organization_id", lit(config.workspace_id))
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
        .withColumn("organization_id", lit(config.workspace_id))
        .select('resourceId, 'category, 'version, 'timestamp, 'date, 'properties, 'organization_id, 'identity.alias("userIdentity"))
        .withColumn("userIdentity", structFromJson(spark, schemaBuilders, "userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")
        .withColumn("requestParams", structFromJson(spark, schemaBuilders, "requestParams"))

      parsedEHDF
        .drop("response")
        .verifyMinimumSchema(Schema.auditMasterSchema)

      dbutils.fs.rm(s"""${tempPersistDFPath}""", true)
      dbutils.fs.rm(s"""${tempPersistChk}""", true)

      DeploymentValidationReport(true,
        getSimpleMsg("Validate_EventHub"),
        testDetails,
        Some("SUCCESS"),
        Some(config.workspace_id)
      )
    } catch {
      case exception: Throwable =>
        val msg = if(isAAD){
          s"""Using AAD unable to retrieve data from ehName:${ehName}
             | eh_conn_string:${config.eh_conn_string}
             | aad_client_id:${config.aad_client_id}
             | aad_tenant_id:${config.aad_tenant_id}
             | aad_client_secret_key:${config.aad_client_secret_key}""".stripMargin
        }
        else {
          s"""Unable to retrieve data from ehName:${ehName} scope:${config.secret_scope} eh_scope_key:${config.eh_scope_key.get}"""
        }
        val fullMsg = PipelineFunctions.appendStackStrace(exception, msg)
        logger.log(Level.ERROR, fullMsg)
        DeploymentValidationReport(false,
          getSimpleMsg("Validate_EventHub"),
          testDetails,
          Some(fullMsg),
          Some(config.workspace_id)
        )
    }

  }



  /**
   * Transforms output of the rules engine to DeploymentValidationReport
   * @param ruleSet
   */
  private def validateRuleAndUpdateStatus(ruleSet: RuleSet,parallelism: Int): ArrayBuffer[DeploymentValidationReport] = {
    val validationStatus: ArrayBuffer[DeploymentValidationReport] = new ArrayBuffer[DeploymentValidationReport]()
    val validation = ruleSet.validate()
    val columns = ruleSet.getRules.map(_.ruleName)
    val completeReportDF = if (!validation.completeReport.columns.contains("workspace_id")) {
      validation.completeReport.withColumn("workspace_id", lit(""))
    } else {
      validation.completeReport
    }
     val resultDF = completeReportDF.withColumn("concat_rule_columns", array(columns map col: _*))
      .withColumn("result", explode(col("concat_rule_columns")))
      .select("deployment_id", "workspace_id", "result")


    val validationReport = resultDF.as[RulesValidationReport].collect().par
    validationReport.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    validationReport.foreach(validation => {
      var validationMsg = "SUCCESS"
      val resultFlag: Boolean = validation.result.passed==null || validation.result.passed.toBoolean
      if (!resultFlag) {
        validationMsg = s"""Validation failed:  ${validation.result}"""
      }
      val validationDetails = s""" ${validation.result.ruleName}"""
      synchronized {
        validationStatus.append(DeploymentValidationReport(resultFlag, getSimpleMsg(validation.result.ruleName), validationDetails, Some(validationMsg), Some(validation.workspace_id)))
      }
     })
    validationStatus

  }

  /**
   * Provides human readable message for each validation.
   *
   * @param ruleName
   * @return
   */
  private  def getSimpleMsg(ruleName: String): String = {
    ruleName match {
      case "Common_StoragePrefix" => "Storage Prefix should be common across the workspaces."
      case "Common_ETLDatabase" => "Workspaces should have a common ETL Database Name."
      case "Common_ConsumerDatabaseName" => "Workspaces should have a common Consumer Database Name."
      case "Valid_Cloud_providers" => "Cloud provider can be either AWS or Azure."
      case "NOTNULL_APIURL" => "API URL should not be empty."
      case "NOTNULL_SecretScope" => "Secrete scope should not be empty."
      case "NOTNULL_SecretKey_DBPAT" => "PAT key should not be empty."
      case "Valid_PrimordialDate" => "Primordial Date should in yyyy-MM-dd format(Ex:2022-01-30) and should be less than current date."
      case "Valid_MaxDays" => "Max Days should be a number."
      case "APIURL_Connectivity" => "API URL should give some response with provided scope and key."
      case "Validate_AuditLogPrefix" => "Folder with audit logs should be present inside AuditLogPrefix."
      case "Validate_EventHub" => "Consuming data from EventHub."
      case "Valid_Excluded_Scopes" => "Excluded scope can be audit:sparkEvents:jobs:clusters:clusterEvents:notebooks:pools:accounts."
      case "Storage_Access" => "ETL_STORAGE_PREFIX should have read,write and create access"
      case "Validate_Mount" => "Number of mount points in the workspace should not exceed 50"
    }
  }

  /**
   * Performs common_etl_storasgsePrefix check and read write access check for storage prefix before each deployment.
   * @param multiWorkspaceConfig
   * @param parallelism
   * @return
   */
  private[overwatch] def performMandatoryValidation( multiWorkspaceConfig: Array[MultiWorkspaceConfig],parallelism: Int):Array[MultiWorkspaceConfig]  = {
    logger.log(Level.INFO, "Performing mandatory validation")
    val commonEtlStorageRuleSet = RuleSet(multiWorkspaceConfig.toSeq.toDS.toDF(), by = "deployment_id")//Check common etl_storage_prefix
    commonEtlStorageRuleSet.add(validateDistinct("Common_StoragePrefix", MultiWorkspaceConfigColumns.storage_prefix.toString))
    val validationStatus = validateRuleAndUpdateStatus(commonEtlStorageRuleSet, parallelism)
    if (!validationStatus.forall(_.validated)) {
      throw new BadConfigException(getSimpleMsg("Common_StoragePrefix"))
    }
    storagePrefixAccessValidation(multiWorkspaceConfig.head, fastFail = true) //Check read write access for etl_storage_prefix
    multiWorkspaceConfig.filter(_.cloud.toLowerCase() != "azure").map(config=>validateAuditLogColumnName(config))
    multiWorkspaceConfig.filter(_.cloud.toLowerCase() == "azure").map(checkAAD)
    multiWorkspaceConfig
    }

  /**
   * Entry point of the validation.
   *
   * @return
   */
  private[overwatch] def performValidation(
                                            multiWorkspaceConfig: Array[MultiWorkspaceConfig],
                                            parallelism: Int,
                                            currentOrgID:String,
                                          ): Array[DeploymentValidationReport] = {
    //Primary validation //
    val configDF = multiWorkspaceConfig.toSeq.toDS.toDF.repartition(getTotalCores).cache()
    println("Parallelism :" + parallelism + " Number of input rows :" + configDF.count())

    var validationStatus: ArrayBuffer[DeploymentValidationReport] = new ArrayBuffer[DeploymentValidationReport]()
    //csv data validation


    val gropedRules = Seq[Rule](
      validateDistinct("Common_StoragePrefix",MultiWorkspaceConfigColumns.storage_prefix.toString),
      validateDistinct("Common_ETLDatabase",MultiWorkspaceConfigColumns.etl_database_name.toString),
      validateDistinct("Common_ConsumerDatabaseName",MultiWorkspaceConfigColumns.consumer_database_name.toString)
    )
    val groupedRuleSet = RuleSet(configDF, by = "deployment_id").add(gropedRules)

    val nonGroupedRules = Seq[Rule](
      validateNotNull("NOTNULL_APIURL",MultiWorkspaceConfigColumns.api_url.toString),
      validateNotNull("NOTNULL_SecretScope",MultiWorkspaceConfigColumns.secret_scope.toString),
      validateNotNull("NOTNULL_SecretKey_DBPAT",MultiWorkspaceConfigColumns.secret_key_dbpat.toString),
      validateCloud(),
      validatePrimordialDate(),
      validateMaxDays(),
      validateOWScope()
    )
    val nonGroupedRuleSet = RuleSet(configDF).add(nonGroupedRules)


    //Connectivity validation
    val configToValidate = multiWorkspaceConfig.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    configToValidate.tasksupport = taskSupport
    validationStatus =
        validateRuleAndUpdateStatus(groupedRuleSet,parallelism) ++
        validateRuleAndUpdateStatus(nonGroupedRuleSet,parallelism)++
          configToValidate.map(validateApiUrlConnectivity) ++ //Make request to each API and check the response
          configToValidate.map(cloudSpecificValidation)++ //Connection check for audit logs s3/EH
          configToValidate.map(validateMountCount(_,currentOrgID))
    //Access validation for etl_storage_prefix
    validationStatus.append(storagePrefixAccessValidation(configToValidate.head)) //Check read/write/create/list access for etl_storage_prefix

    configDF.unpersist()
    validationStatus.toArray
  }

}

