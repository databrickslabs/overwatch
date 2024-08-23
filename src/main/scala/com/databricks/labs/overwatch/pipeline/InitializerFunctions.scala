package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.Helpers.{getCurrentCatalogName, setCurrentCatalog, spark}
import com.databricks.labs.overwatch.utils.OverwatchScope.{OverwatchScope, _}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, DslSymbol}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, rank, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait InitializerFunctions
  extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _config: Config = _
  private var _disableValidations: Boolean = false

  protected def setConfig(value: Config): this.type = {
    _config = value
    this
  }

  protected def setDisableValidation(value: Boolean): this.type = {
    _disableValidations = value
    this
  }

  private def config: Config = _config

  private def disableValidations: Boolean = _disableValidations


  /**
   * Convert the args brought in as JSON string into the paramters object "OverwatchParams".
   * Validate the config and the environment readiness for the run based on the configs and environment state
   *
   * @param rawParams JSON string of input args from input into main class.
   * @return
   */
  private[overwatch] def validateAndRegisterArgs(rawParams: OverwatchParams): this.type = {

    // Now that the input parameters have been parsed -- set them in the config
    config.setInputConfig(rawParams)

    val overwatchFriendlyName = rawParams.workspace_name.getOrElse(config.organizationId)
    config.setWorkspaceName(overwatchFriendlyName)

    /**
     * PVC: HARD OVERRIDE FOR PVC
     * Each time PVC deploys a newer version the derived organization_id changes due to the load balancer id
     * and/or URL to access the workspace changes. There are no identifiable, equal values that can be found
     * between one deployment and the next. To resolve this, PVC customers will be REQUIRED to provide a canonical
     * name for each workspace (i.e. workspaceName) to provide consistency across deployments.
     */

    if (isPVC && !config.isMultiworkspaceDeployment) pvcOverrideOrganizationId()

    // Set external optimize if customer specified otherwise use default
    config.setExternalizeOptimize(rawParams.externalizeOptimize)

    /** Retrieve scope from user inputs, validate it, and add it to the config */
    val overwatchScope = rawParams.overwatchScope.getOrElse(Seq("all"))

    if (overwatchScope.head == "all") {
      config.setOverwatchScope(config.orderedOverwatchScope)
    } else {
      config.setOverwatchScope(validateScope(overwatchScope))
    }

    /** Retrieve raw token secret, validate it and add it to the config if valid */
    val tokenSecret = rawParams.tokenSecret
    val validatedTokenSecret = validateTokenSecret(tokenSecret)

    /** Build and validate workspace meta including API ENV */
    val rawApiEnv = config.buildApiEnv(validatedTokenSecret, rawParams.apiEnvConfig)
    val validatedApiEnv = validateApiEnv(rawApiEnv)
    config.setApiEnv(validatedApiEnv)

    /** Validate and set the data target details */
    val rawDataTarget = rawParams.dataTarget.getOrElse(
      DataTarget(Some("overwatch"), Some("dbfs:/user/hive/warehouse/overwatch.db"), None)
    )
    validateAndSetDataTarget(rawDataTarget)


    /** Set Databricks Contract Prices from Config */
    config.setContractInteractiveDBUPrice(rawParams.databricksContractPrices.interactiveDBUCostUSD)
    config.setContractAutomatedDBUPrice(rawParams.databricksContractPrices.automatedDBUCostUSD)
    config.setContractSQLComputeDBUPrice(rawParams.databricksContractPrices.sqlComputeDBUCostUSD)
    config.setContractJobsLightDBUPrice(rawParams.databricksContractPrices.jobsLightDBUCostUSD)

    // Set Primordial Date
    config.setPrimordialDateString(rawParams.primordialDateString)

    // Audit logs are required and paramount to Overwatch delivery -- they must be present and valid
    /** Validate and set Audit Log Configs */
    val rawAuditLogConfig = rawParams.auditLogConfig

    val validatedAuditLogConfig = validateAuditLogConfigs(rawAuditLogConfig, config.organizationId,
        config.workspaceURL, config.sqlEndpoint, validatedTokenSecret)
    config.setAuditLogConfig(validatedAuditLogConfig)

    // check if it is a valid migration from cloud to system table
    checkSystemTableMigrationValidity(config)

    // must happen AFTER data target validation
    // persistent location for corrupted spark event log files
    /** Validate and set Bad Records Path */
    val rawBadRecordsPath = rawParams.badRecordsPath
    val badRecordsPath = rawBadRecordsPath.getOrElse(
      s"${config.etlDataPathPrefix}/spark_events_bad_records_files/${config.organizationId}"
    )
    config.setBadRecordsPath(badRecordsPath)

    // must happen AFTER data target is set validation since the etlDataPrefix must already be set in the config
    /** Validate and set Temp Working Dir */
    val rawTempWorkingDir = rawParams.tempWorkingDir
    val validatedTempWorkingDir = prepTempWorkingDir(rawTempWorkingDir)
    config.setTempWorkingDir(validatedTempWorkingDir)

    /** Set Max Days */
    if (rawParams.maxDaysToLoad <= 0) {
      throw new BadConfigException(s"maxDaysToLoad should be greater than 0.")
    } else {
      config.setMaxDays(rawParams.maxDaysToLoad)
    }

    /** Validate and set Intelligent Scaling Params */
    val rawISConfig = rawParams.intelligentScaling
    val validatedISConfig = validateIntelligentScaling(rawISConfig)
    config.setIntelligentScaling(validatedISConfig)

    // as of 0711
    val disabledModulesString = spark(globalSession = true).conf.getOption("overwatch.modules.disabled").getOrElse("0")
    config.registerDisabledModules(disabledModulesString)

    this
  }

  /**
   * Determine whether or not the workspace is configured on a PVC environment
   *
   * @param config OW Config
   * @return
   */
  def isPVC: Boolean = {
    val lowerOrgId = config.organizationId.toLowerCase
    if (lowerOrgId.contains("ilb") || lowerOrgId.contains("elb")) {
      val pvcDetectedMsg = "Databricks PVC: PVC WORKSPACE DETECTED!"
      config.setIsPVC(true)
      if (config.debugFlag) println(pvcDetectedMsg)
      logger.log(Level.INFO, pvcDetectedMsg)
      require(config.workspaceName.toLowerCase != config.organizationId.toLowerCase,
        s"PVC workspaces require the 'workspaceName' to be configured in the Overwatch configs for " +
          s"data continuity. Please choose a friendly workspace name and add it to the configuration and try to " +
          s"run the pipeline again."
      )
      true
    } else false
  }

  /**
   * Alter the org_id appropriately when the workspace is PVC
   *
   * @param config OW Config
   */
  def pvcOverrideOrganizationId(): Unit = {
    val overrideMsg = s"Databricks PVC: Overriding organization_id from ${config.organizationId} to " +
      s"${config.workspaceName} to accommodate data continuity across deployments"
    if (config.debugFlag) println(overrideMsg)
    logger.log(Level.INFO, overrideMsg)
    config.setOrganizationId(config.workspaceName)
  }

  /**
   * Some scopes are dependent on others. In these cases Overwatch attempts to make it very clear which modules
   * must work together. This will be made clear in the documentation.
   * Assuming all checks pass, this function converts the list of strings into an enum and stores it in the config
   * to be referenced throughout the package
   *
   * @param scopes List of modules to be executed during the run as a string.
   * @throws com.databricks.labs.overwatch.utils.BadConfigException
   * @return
   */
  @throws(classOf[BadConfigException])
  def validateScope(scopes: Seq[String]): Seq[OverwatchScope.OverwatchScope] = {
    val lcScopes = scopes.map(_.toLowerCase)

    if ((lcScopes.contains("clusterevents") || lcScopes.contains("clusters")) && !lcScopes.contains("audit")) {
      println(s"WARNING: Cluster data without audit will result in loss of granularity. It's recommended to configure" +
        s"the audit module.")
    }

    if (lcScopes.contains("clusterevents") || lcScopes.contains("sparkevents") || lcScopes.contains("jobs")) {
      require(lcScopes.contains("clusters"), "sparkEvents, clusterEvents, and jobs scopes require clusters scope to " +
        "also be enabled as cluster metadata is used to build these scopes.")
    }

    if (lcScopes.contains("jobs") && !lcScopes.contains("audit")) {
      println(s"WARNING: Jobs / JobRuns without audit will result in loss of granularity. It's recommended to configure" +
        s"the audit module.")
    }

    // Build the scope enum
    lcScopes.map {
      case "jobs" => jobs
      case "clusters" => clusters
      case "clusterevents" => clusterEvents
      case "sparkevents" => sparkEvents
      case "notebooks" => notebooks
      case "pools" => pools
      case "audit" => audit
      case "accounts" => accounts
      case "dbsql" => dbsql
      case "notebookcommands" => notebookCommands
      case "warehouseevents" => warehouseEvents
      case scope => {
        val supportedScopes = s"${OverwatchScope.values.mkString(", ")}, all"
        throw new BadConfigException(s"Scope $scope is not supported. Supported scopes include: " +
          s"${supportedScopes}.")
      }
    }
  }

  /**
   * validate the tokenSecret provided is acceptable
   *
   * @param tokenSecret        OW TokenSecret to be validated
   * @param disableValidations Whether or not the Initializer validations are disabled
   * @return
   */
  def validateTokenSecret(tokenSecret: Option[TokenSecret]): Option[TokenSecret] = {
    // validate token secret requirements
    // TODO - Validate if token has access to necessary assets. Warn/Fail if not
    if (tokenSecret.nonEmpty && !disableValidations) {
      if (tokenSecret.get.scope.isEmpty || tokenSecret.get.key.isEmpty) {
        throw new BadConfigException(s"Secret AND Key must be provided together or neither of them. " +
          s"Either supply both or neither.")
      }
      val scopeCheck = dbutils.secrets.listScopes().map(_.getName()).toArray.filter(_ == tokenSecret.get.scope)
      if (scopeCheck.length == 0) throw new BadConfigException(s"Scope ${tokenSecret.get.scope} does not exist " +
        s"in this workspace. Please provide a scope available and accessible to this account.")
      val scopeName = scopeCheck.head

      val keyCheck = dbutils.secrets.list(scopeName).toArray.filter(_.key == tokenSecret.get.key)
      if (keyCheck.length == 0) throw new BadConfigException(s"Key ${tokenSecret.get.key} does not exist " +
        s"within the provided scope: ${tokenSecret.get.scope}. Please provide a scope and key " +
        s"available and accessible to this account.")
      Some(TokenSecret(scopeName, keyCheck.head.key))
    } else None //config.registerWorkspaceMeta(None,None)
  }

  /**
   * Ensure api env overrides are within acceptable bounds
   *
   * @param apiEnv OW ApiEnv
   * @return
   */
  def validateApiEnv(apiEnv: ApiEnv): ApiEnv = {
    if (apiEnv.threadPoolSize < 1 || apiEnv.threadPoolSize > 20) {
      throw new BadConfigException("ThreadPoolSize should be a valid number between 0 to 20")
    }
    if (apiEnv.apiWaitingTime < 60000 || apiEnv.apiWaitingTime > 900000) { // 60000ms = 1 mint,900000ms = 15mint
      throw new BadConfigException("ApiWaiting time should be between 60000ms and 900000ms")
    }
    if (apiEnv.errorBatchSize < 1 || apiEnv.errorBatchSize > 1000) {
      throw new BadConfigException("ErrorBatchSize should be between 1 to 1000")
    }
    if (apiEnv.successBatchSize < 1 || apiEnv.successBatchSize > 1000) {
      throw new BadConfigException("SuccessBatchSize should be between 1 to 1000")
    }
    if (apiEnv.proxyHost.nonEmpty) {
      if (apiEnv.proxyPort.isEmpty) {
        throw new BadConfigException("Proxy host and port should be defined")
      }
    }
    if (apiEnv.proxyUserName.nonEmpty) {
      if (apiEnv.proxyPasswordKey.isEmpty || apiEnv.proxyPasswordScope.isEmpty) {
        throw new BadConfigException("Please define ProxyUseName,ProxyPasswordScope and ProxyPasswordKey")
      }
    }
    apiEnv
  }

  /**
   * It's critical to ensure that the database Overwatch is interacting with is truly an Overwatch database as it can be
   * very dangerous to interact with the wrong database. This function validates that the DB was actually created
   * by this process, that the db default paths match as well as the schema versions are ==
   *
   * @param dataTarget data target as parsed into OverwatchParams
   * @throws java.lang.IllegalArgumentException
   * @return
   */
  @throws(classOf[IllegalArgumentException])
  private[overwatch] def dataTargetIsValid(dataTarget: DataTarget): Boolean = {
    val dbName = dataTarget.databaseName.getOrElse("overwatch")
    val rawDBLocation = dataTarget.databaseLocation.getOrElse(s"/user/hive/warehouse/${dbName}.db")
    val dbLocation = PipelineFunctions.cleansePathURI(rawDBLocation)
    val rawETLDataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)
    val etlDataLocation = PipelineFunctions.cleansePathURI(rawETLDataLocation)
    var switch = true
    if (spark.catalog.databaseExists(dbName)) {
      val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
      val dbProperties = dbMeta.properties
      val existingDBLocation = dbMeta.locationUri.toString
      if (existingDBLocation != dbLocation) {
        switch = false
        throw new BadConfigException(s"The DB: $dbName exists " +
          s"at location $existingDBLocation which is different than the location entered in the config. Ensure " +
          s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
          s"dbfs:/...")
      }

      val isOverwatchDB = dbProperties.getOrElse("OVERWATCHDB", "FALSE") == "TRUE"
      if (!isOverwatchDB) {
        switch = false
        throw new BadConfigException(s"The Database: $dbName was not created by overwatch. Specify a " +
          s"database name that does not exist or was created by Overwatch.")
      }
    } else { // Database does not exist
      if (!Helpers.pathExists(dbLocation)) { // db path does not already exist -- valid
        logger.log(Level.INFO, s"Target location " +
          s"is valid: will create database: $dbName at location: ${dbLocation}")
      } else { // db does not exist AND path already exists
        switch = false
        throw new BadConfigException(
          s"""The target database location: ${dbLocation}
          already exists. Please specify a path that doesn't yet exist. If attempting to launch Overwatch on a secondary
          workspace, please choose a unique location for the database on this workspace and use the "etlDataPathPrefix"
          to reference the shared physical data location.""".stripMargin)
      }
    }

    if (Helpers.pathExists(etlDataLocation)) println(s"\n\nWARNING!! The ETL Data Prefix exists. Verify that only " +
      s"Overwatch data exists in this path.")

    // todo - refactor away duplicity
    /**
     * Many of the validation above are required for the consumer DB but the consumer DB will only contain
     * views. It's important that the basic db checks are completed but the checks don't need to be as extensive
     * since there's no chance of data corruption given only creating views. This section needs to be refactored
     * to remove duplicity while still enabling control between which checks are done for which DataTarget.
     */
    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
    val rawConsumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
    val consumerDBLocation = PipelineFunctions.cleansePathURI(rawConsumerDBLocation)
    if (consumerDBName != dbName) { // separate consumer db
      if (spark.catalog.databaseExists(consumerDBName)) {
        val consumerDBMeta = spark.sessionState.catalog.getDatabaseMetadata(consumerDBName)
        val existingConsumerDBLocation = consumerDBMeta.locationUri.toString
        if (existingConsumerDBLocation != consumerDBLocation) { // separated consumer DB but same location FAIL
          switch = false
          throw new BadConfigException(s"The Consumer DB: $consumerDBName exists" +
            s"at location $existingConsumerDBLocation which is different than the location entered in the config. Ensure" +
            s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
            s"dbfs:/...")
        }
      } else { // consumer DB is different from ETL DB AND db does not exist
        if (!Helpers.pathExists(consumerDBLocation)) { // consumer db path is empty
          logger.log(Level.INFO, s"Consumer DB location " +
            s"is valid: will create database: $consumerDBName at location: ${consumerDBLocation}")
        } else {
          switch = false
          throw new BadConfigException(
            s"""The consumer database location: ${dbLocation}
          already exists. Please specify a path that doesn't yet exist. If attempting to launch Overwatch on a secondary
          workspace, please choose a unique location for the database on this workspace.""".stripMargin)
        }
      }

      if (consumerDBLocation == dbLocation && consumerDBName != dbName) { // separate db AND same location ERROR
        switch = false
        throw new BadConfigException("Consumer DB Name cannot differ from ETL DB Name while having the same location.")
      }
    } else { // same consumer db as etl db
      if (consumerDBName == dbName && consumerDBLocation != dbLocation) { // same db AND DIFFERENT location ERROR
        switch = false
        throw new BadConfigException("Consumer DB cannot match ETL DB Name while having different locations.")
      }
    }

    switch
  }

  private def quickBuildAuditLogConfig(auditLogConfig: AuditLogConfig): AuditLogConfig = {
    if (auditLogConfig.rawAuditPath.nonEmpty) {
      val auditLogPath = auditLogConfig.rawAuditPath.get
      val auditLogFormat = auditLogConfig.auditLogFormat.toLowerCase.trim

      val finalAuditLogPath = if (auditLogPath.endsWith("/")) auditLogPath.dropRight(1) else auditLogPath
      auditLogConfig.copy(rawAuditPath = Some(finalAuditLogPath), auditLogFormat = auditLogFormat)
    } else if (auditLogConfig.azureAuditLogEventhubConfig.nonEmpty) {
      val ehConfig = auditLogConfig.azureAuditLogEventhubConfig.get
      val ehPrefix = ehConfig.auditRawEventsPrefix
      val cleanPrefix = if (ehPrefix.endsWith("/")) ehPrefix.dropRight(1) else ehPrefix
      val rawEventsCheckpoint = ehConfig.auditRawEventsChk.getOrElse(s"${cleanPrefix}/rawEventsCheckpoint")
      val auditLogBronzeChk = ehConfig.auditLogChk.getOrElse(s"${cleanPrefix}/auditLogBronzeCheckpoint")
      val ehFinalConfig = auditLogConfig.azureAuditLogEventhubConfig.get.copy(
        auditRawEventsPrefix = cleanPrefix,
        auditRawEventsChk = Some(rawEventsCheckpoint),
        auditLogChk = Some(auditLogBronzeChk)
      )
      auditLogConfig.copy(azureAuditLogEventhubConfig = Some(ehFinalConfig))
    } else throw new BadConfigException("Audit Configuration Failed")

  }

  @throws(classOf[BadConfigException])
  def validateAuditLogConfigs(auditLogConfig: AuditLogConfig,
                              organization_id: String,
                              workspace_url: String,
                              sql_endpoint: String,
                              token: Option[TokenSecret]): AuditLogConfig = {
    if (disableValidations) { //need to double check this
      quickBuildAuditLogConfig(auditLogConfig)
    } else {
      if (ifFetchFromSystemTable(auditLogConfig)){
        validateAuditLogConfigsFromSystemTable(auditLogConfig,organization_id, workspace_url, token)
      } else {
        validateAuditLogConfigsFromCloud(auditLogConfig)
      }
    }
  }

  def ifFetchFromSystemTable(auditLogConfig: AuditLogConfig): Boolean = {
    auditLogConfig.rawAuditPath.getOrElse("").toLowerCase.equals("system")
  }

  /**
   * defaults temp working dir to etlTargetPath/organizationId
   * this is important to minimize bandwidth issues
   * also setting temp target to be within the etl target minimizes liklihood for read/write permissions
   * issues. Can be overridden in config
   *
   * @param tempPath defaults to "" (nullstring) in the config
   */
  def prepTempWorkingDir(tempPath: String): String = {
    val storagePrefix = config.etlDataPathPrefix.split("/").dropRight(1).mkString("/")
    val defaultTempPath = s"$storagePrefix/tempworkingdir/${config.organizationId}"

    if (disableValidations) {
      // if not validated -- don't drop data but do append millis in case data dir is not empty
      // prevent dropping data on accidental registration of temp dir
      val currentMillis = System.currentTimeMillis()
      if (tempPath == "") s"$defaultTempPath/$currentMillis" else s"$tempPath/$currentMillis"
    } else {
      val workspaceTempWorkingDir = if (tempPath == "") { // default null string
        defaultTempPath
      }
      else if (tempPath.split("/").takeRight(1).headOption.getOrElse("") == config.organizationId) { // if user defined value already ends in orgid don't append it
        tempPath
      } else { // if user configured value doesn't end with org id append it
        s"$tempPath/${config.organizationId}"
      }
      val hadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
      if (Helpers.pathExists(workspaceTempWorkingDir)) { // if temp path exists clean it
        Helpers.fastrm(Helpers.parListFiles(workspaceTempWorkingDir, hadoopConf))
      }
      // ensure path exists at init
      dbutils.fs.mkdirs(workspaceTempWorkingDir)
      workspaceTempWorkingDir
    }
  }

  def validateIntelligentScaling(intelligentScaling: IntelligentScaling): IntelligentScaling = {
    if (disableValidations) {
      intelligentScaling
    } else {
      if (intelligentScaling.enabled) {
        if (intelligentScaling.minimumCores < 1)
          throw new BadConfigException(s"Intelligent Scaling: Minimum cores must be > 0. Set to ${intelligentScaling.minimumCores}")
        if (intelligentScaling.minimumCores > intelligentScaling.maximumCores)
          throw new BadConfigException(s"Intelligent Scaling: Minimum cores must be > 0. \n" +
            s"Minimum = ${intelligentScaling.minimumCores}\nMaximum = ${intelligentScaling.maximumCores}")
        if (intelligentScaling.coeff >= 10.0 || intelligentScaling.coeff <= 0.0)
          throw new BadConfigException(s"Intelligent Scaling: Scaling Coeff must be between 0.0 and 10.0 (exclusive). \n" +
            s"coeff configured at = ${intelligentScaling.coeff}")
      }

      intelligentScaling
    }
  }

  /**
   * Initialize the "Database" object
   * If creating the database special properties will be created to allow overwatch to identify that the db was
   * created through this process. Additionally, the schema version will be noded. This allows for upgrades based on
   * version of Overwatch being executed.
   *
   * @return
   */
  //    def initializeDatabase(): Database = ???
  def initializeDatabase(): Database

  /**
   * validate and set the DataTarget for Overwatch
   *
   * @param dataTarget OW DataTarget
   */
  def validateAndSetDataTarget(dataTarget: DataTarget): Unit

  def validateSqlEndpoint(sqlEndpoint: String, organizationId: String): String = {
    val pattern = """/sql/1\.0/warehouses/.*""".r
    sqlEndpoint.trim match {
      case pattern(_*) => sqlEndpoint
      case "" => sqlEndpoint
      case _ => throw new Exception(s"Invalid sqlEndpoint for organizationId: $organizationId ")
    }
  }
  def validateAuditLogConfigsFromSystemTable(auditLogConfig: AuditLogConfig,
                                             organizationId: String,
                                             workspace_url: String,
                                             token: Option[TokenSecret]): AuditLogConfig = {
    val auditLogFormat = "delta"
    val systemTableName = auditLogConfig.systemTableName.get

    val sqlEndpoint = validateSqlEndpoint(auditLogConfig.sqlEndpoint.getOrElse(""),organizationId)
    if(sqlEndpoint.isEmpty) {
      val systemTableNameDf = spark.table(systemTableName).filter(s"workspace_id = '$organizationId'").limit(1)
      if (systemTableNameDf.isEmpty)
        throw new Exception(s"No data found in ${systemTableName} for organizationId: $organizationId ")
      auditLogConfig.copy(auditLogFormat=auditLogFormat,systemTableName = Some(systemTableName))
    }
    else {
      val host = workspace_url.stripPrefix("https://").stripSuffix("/")
      val scope = token.get.scope
      val key = token.get.key
      val rawToken = dbutils.secrets.get(scope, key)
      val systemTableNameDf = spark.read
        .format("databricks")
        .option("host", host)
        .option("httpPath", sqlEndpoint)
        .option("personalAccessToken", rawToken)
        .option("query", s"select * from ${systemTableName} where workspace_id='${organizationId}' limit 1")
        .load()
      if (systemTableNameDf.isEmpty)
        throw new Exception(s"No data found in ${systemTableName} for organizationId: $organizationId ")
      auditLogConfig.copy(auditLogFormat=auditLogFormat,systemTableName = Some(systemTableName),sqlEndpoint = Some(sqlEndpoint))
    }
  }

  def validateAuditLogConfigsFromCloud(auditLogConfig: AuditLogConfig): AuditLogConfig = {
      if (config.cloudProvider != "azure") {

        val auditLogPath = auditLogConfig.rawAuditPath
        val auditLogFormat = auditLogConfig.auditLogFormat.toLowerCase.trim
        if (config.overwatchScope.contains(audit) && auditLogPath.isEmpty) {
          throw new BadConfigException("Audit cannot be in scope without the 'auditLogPath' being set. ")
        }

        if (auditLogPath.nonEmpty)
          dbutils.fs.ls(auditLogPath.get).foreach(auditFolder => {
            if (auditFolder.isDir) require(auditFolder.name.startsWith("date="), s"Audit directory must contain " +
              s"partitioned date folders in the format of ${auditLogPath.get}/date=. Received ${auditFolder} instead.")
          })

        val supportedAuditLogFormats = Array("json", "parquet", "delta")
        if (!supportedAuditLogFormats.contains(auditLogFormat)) {
          throw new BadConfigException(s"Audit Log Format: Supported formats are ${supportedAuditLogFormats.mkString(",")} " +
            s"but $auditLogFormat was placed in teh configuration. Please select a supported audit log format.")
        }

        val finalAuditLogPath = if (auditLogPath.get.endsWith("/")) auditLogPath.get.dropRight(1) else auditLogPath.get

        // return validated audit log config for aws
        auditLogConfig.copy(rawAuditPath = Some(finalAuditLogPath), auditLogFormat = auditLogFormat)

      } else {
        val ehConfigOp = auditLogConfig.azureAuditLogEventhubConfig
        require(ehConfigOp.nonEmpty, "When using Azure, an Eventhub must be configured for audit log retrieval")
        val ehConfig = ehConfigOp.get
        val ehPrefix = ehConfig.auditRawEventsPrefix

        val cleanPrefix = if (ehPrefix.endsWith("/")) ehPrefix.dropRight(1) else ehPrefix
        val rawEventsCheckpoint = ehConfig.auditRawEventsChk.getOrElse(s"${ehPrefix}/rawEventsCheckpoint")
        // TODO -- Audit log bronze is no longer streaming target -- remove this path
        val auditLogBronzeChk = ehConfig.auditLogChk.getOrElse(s"${ehPrefix}/auditLogBronzeCheckpoint")

        if (config.debugFlag) {
          println("DEBUG FROM Init")
          println(s"cleanPrefix = ${cleanPrefix}")
          println(s"rawEventsCheck = ${rawEventsCheckpoint}")
          println(s"auditLogsBronzeChk = ${auditLogBronzeChk}")
          println(s"ehPrefix = ${ehPrefix}")
        }

        val ehFinalConfig = ehConfig.copy(
          auditRawEventsPrefix = cleanPrefix,
          auditRawEventsChk = Some(rawEventsCheckpoint),
          auditLogChk = Some(auditLogBronzeChk)
        )

        // parse the connection string to validate format
        PipelineFunctions.parseAndValidateEHConnectionString(ehFinalConfig.connectionString, ehFinalConfig.azureClientId.isEmpty)
        // return validated auditLogConfig for Azure
        auditLogConfig.copy(azureAuditLogEventhubConfig = Some(ehFinalConfig))
      }
    }

    def checkSystemTableMigrationValidity(config: Config): Boolean = {
      val workspaceID = config.organizationId
      val latestConfigByOrg = Window.partitionBy(col("organization_id")).orderBy(col("Pipeline_SnapTS").desc)
      val etlDB = config.databaseName
      val initialCatalog = getCurrentCatalogName(spark)
      val etlDBWithOutCatalog = if(etlDB.contains(".")){
        setCurrentCatalog(spark, etlDB.split("\\.").head)
        etlDB.split("\\.").last
      } else etlDB

      if(!spark.catalog.tableExists(s"${etlDBWithOutCatalog}.pipeline_report")) {
        logger.log(Level.INFO, s"Since it is a first run no need to check for migration validity")
        setCurrentCatalog(spark, initialCatalog)
        return true
      }

      val lastValue = spark.table(s"${etlDBWithOutCatalog}.pipeline_report")
        .filter(col("status") === "SUCCESS")
        .withColumn("rnk", rank().over(latestConfigByOrg))
        .withColumn("rn", row_number().over(latestConfigByOrg))
        .filter(col("rnk") === 1 && col("rn") === 1)
        .filter(col("organization_id") === workspaceID)
        .select("inputConfig.auditLogConfig.rawAuditPath").collect.map(x=>x(0)).mkString

      setCurrentCatalog(spark, initialCatalog)
      val currentValue = config.auditLogConfig.rawAuditPath.getOrElse("")
      if( lastValue == "system" && currentValue !="system" )
        throw new Exception(s"Cannot migrate from system table to cloud for organization_id - ${workspaceID}" +
          s" Please use the same configuration as the last run")
      else
        true
    }

}
