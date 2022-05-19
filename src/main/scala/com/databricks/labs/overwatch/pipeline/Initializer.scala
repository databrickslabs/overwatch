package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ParamDeserializer
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.OverwatchScope._
import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.SerializableConfiguration

/**
 * Take the config and validate the setup
 * If valid, initialize the environment
 *
 * @param config
 */
class Initializer(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _isSnap: Boolean = false
  private var _disableValidations: Boolean = false

  private def setIsSnap(value: Boolean): this.type = {
    _isSnap = value
    this
  }

  private def isSnap: Boolean = _isSnap

  private def setDisableValidations(value: Boolean): this.type = {
    _disableValidations = value
    this
  }

  private def disableValidations: Boolean = _disableValidations

  /**
   * Initialize the "Database" object
   * If creating the database special properties will be created to allow overwatch to identify that the db was
   * created through this process. Additionally, the schema version will be noded. This allows for upgrades based on
   * version of Overwatch being executed.
   *
   * @return
   */
  private def initializeDatabase(): Database = {
    // TODO -- Add metadata table
    // TODO -- refactor and clean up duplicity
    val dbMeta = if (isSnap) {
      logger.log(Level.INFO, "Initializing Snap Database")
      "OVERWATCHDB='TRUE',SNAPDB='TRUE'"
    } else {
      logger.log(Level.INFO, "Initializing ETL Database")
      "OVERWATCHDB='TRUE'"
    }
    if (!spark.catalog.databaseExists(config.databaseName)) {
      logger.log(Level.INFO, s"Database ${config.databaseName} not found, creating it at " +
        s"${config.databaseLocation}.")
      val createDBIfNotExists = if (!config.isLocalTesting) {
        s"create database if not exists ${config.databaseName} location '" +
          s"${config.databaseLocation}' WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"
      } else {
        s"create database if not exists ${config.databaseName} " +
          s"WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"
      }
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Successfully created database. $createDBIfNotExists")
    } else {
      // TODO -- get schema version of each table and perform upgrade if necessary
      logger.log(Level.INFO, s"Database ${config.databaseName} already exists, using append mode.")
    }

    // Create consumer database if one is configured
    if (config.consumerDatabaseName != config.databaseName) {
      logger.log(Level.INFO, "Initializing Consumer Database")
      if (!spark.catalog.databaseExists(config.consumerDatabaseName)) {
        val createConsumerDBSTMT = s"create database if not exists ${config.consumerDatabaseName} " +
          s"location '${config.consumerDatabaseLocation}'"
        spark.sql(createConsumerDBSTMT)
        logger.log(Level.INFO, s"Successfully created database. $createConsumerDBSTMT")
      }
    }

    Database(config)
  }

  private def validateIntelligentScaling(intelligentScaling: IntelligentScaling): IntelligentScaling = {
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
      val rawEventsCheckpoint = ehConfig.auditRawEventsChk.getOrElse(s"${ehPrefix}/rawEventsCheckpoint")
      val auditLogBronzeChk = ehConfig.auditLogChk.getOrElse(s"${ehPrefix}/auditLogBronzeCheckpoint")
      val ehFinalConfig = auditLogConfig.azureAuditLogEventhubConfig.get.copy(
        auditRawEventsPrefix = cleanPrefix,
        auditRawEventsChk = Some(rawEventsCheckpoint),
        auditLogChk = Some(auditLogBronzeChk)
      )
      auditLogConfig.copy(azureAuditLogEventhubConfig = Some(ehFinalConfig))
    } else throw new BadConfigException("Audit Configuration Failed")

  }

  @throws(classOf[BadConfigException])
  private def validateAuditLogConfigs(auditLogConfig: AuditLogConfig): AuditLogConfig = {

    if (config.cloudProvider == "aws") {

      val auditLogPath = auditLogConfig.rawAuditPath
      val auditLogFormat = auditLogConfig.auditLogFormat.toLowerCase.trim
      if (config.overwatchScope.contains(audit) && auditLogPath.isEmpty) {
        throw new BadConfigException("Audit cannot be in scope without the 'auditLogPath' being set. ")
      }

      if (auditLogPath.nonEmpty && !config.isLocalTesting)
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

      val ehFinalConfig = auditLogConfig.azureAuditLogEventhubConfig.get.copy(
        auditRawEventsPrefix = cleanPrefix,
        auditRawEventsChk = Some(rawEventsCheckpoint),
        auditLogChk = Some(auditLogBronzeChk)
      )

      // parse the connection string to validate format
      PipelineFunctions.parseEHConnectionString(ehFinalConfig.connectionString)
      // return validated auditLogConfig for Azure
      auditLogConfig.copy(azureAuditLogEventhubConfig = Some(ehFinalConfig))
    }
  }

  private def isPVC: Boolean = {
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

  private def pvcOverrideOrganizationId: Unit = {
    val overrideMsg = s"Databricks PVC: Overriding organization_id from ${config.organizationId} to " +
      s"${config.workspaceName} to accommodate data continuity across deployments"
    if (config.debugFlag) println(overrideMsg)
    logger.log(Level.INFO, overrideMsg)
    config.setOrganizationId(config.workspaceName)
  }

  /**
   * defaults temp working dir to etlTargetPath/organizationId
   * this is important to minimize bandwidth issues
   * also setting temp target to be within the etl target minimizes liklihood for read/write permissions
   * issues. Can be overridden in config
   * @param tempPath defaults to "" (nullstring) in the config
   * @param etlTargetPath target path for all overwatch data
   */
  private def prepAndSetTempWorkingDir(tempPath: String, etlTargetPath: String): Unit = {
    val defaultTempPath = s"$etlTargetPath/tempWorkingDir/${config.organizationId}"
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
    config.setTempWorkingDir(workspaceTempWorkingDir)
  }

  /**
   * Convert the args brought in as JSON string into the paramters object "OverwatchParams".
   * Validate the config and the environment readiness for the run based on the configs and environment state
   *
   * @param overwatchArgs JSON string of input args from input into main class.
   * @return
   */
  private def validateAndRegisterArgs(overwatchArgs: String): this.type = {

    /**
     * Register custom deserializer to create OverwatchParams object
     */
    val paramModule: SimpleModule = new SimpleModule()
      .addDeserializer(classOf[OverwatchParams], new ParamDeserializer)
    val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .registerModule(paramModule)
      .asInstanceOf[ObjectMapper with ScalaObjectMapper]

    /**
     * if isLocalTesting -- Allow for local testing
     * Either all parameters can be hard coded in the config object or a mach args json string can be returned from
     * the config object. Returning the config args is a more complete method for integration testing just be sure
     * to hard code the configuration json string WITHOUT escaped parenthesis even though escaped are necessary
     * when coming from Datbaricks jobs ui since it has to be escaped to go through the api calls.
     *
     * Otherwise -- read from the args passed in and serialize into OverwatchParams
     */
    val rawParams = if (config.isLocalTesting) {
      //      config.buildLocalOverwatchParams()
      //      val synthArgs = config.buildLocalOverwatchParams()
      //      mapper.readValue[OverwatchParams](synthArgs)
      mapper.readValue[OverwatchParams](overwatchArgs)
    } else {
      logger.log(Level.INFO, "Validating Input Parameters")
      mapper.readValue[OverwatchParams](overwatchArgs)
    }

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
    if (isPVC) pvcOverrideOrganizationId

    config.setExternalizeOptimize(rawParams.externalizeOptimize)

    val overwatchScope = rawParams.overwatchScope.getOrElse(Seq("all"))
    val tokenSecret = rawParams.tokenSecret
    // TODO -- PRIORITY -- If data target is null -- default table gets dbfs:/null
    val dataTarget = rawParams.dataTarget.getOrElse(
      DataTarget(Some("overwatch"), Some("dbfs:/user/hive/warehouse/overwatch.db"), None))
    val auditLogConfig = rawParams.auditLogConfig

    if (overwatchScope.head == "all") config.setOverwatchScope(config.orderedOverwatchScope)
    else config.setOverwatchScope(validateScope(overwatchScope))

    // validate token secret requirements
    // TODO - Validate if token has access to necessary assets. Warn/Fail if not
    if (tokenSecret.nonEmpty && !disableValidations && !config.isLocalTesting) {
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

      config.registerWorkspaceMeta(Some(TokenSecret(scopeName, keyCheck.head.key)))
    } else config.registerWorkspaceMeta(None)

    // Validate data Target
    if (!disableValidations && !config.isLocalTesting) dataTargetIsValid(dataTarget)

    // If data target is valid get db name and location and set it
    val dbName = dataTarget.databaseName.get
    val dbLocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")
    val dataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)
    config.setDatabaseNameAndLoc(dbName, dbLocation, dataLocation)

    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
    val consumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
    config.setConsumerDatabaseNameandLoc(consumerDBName, consumerDBLocation)

    // Set Databricks Contract Prices from Config
    config.setContractInteractiveDBUPrice(rawParams.databricksContractPrices.interactiveDBUCostUSD)
    config.setContractAutomatedDBUPrice(rawParams.databricksContractPrices.automatedDBUCostUSD)
    config.setContractSQLComputeDBUPrice(rawParams.databricksContractPrices.sqlComputeDBUCostUSD)
    config.setContractJobsLightDBUPrice(rawParams.databricksContractPrices.jobsLightDBUCostUSD)

    // Set Primordial Date
    config.setPrimordialDateString(rawParams.primordialDateString)

    // Audit logs are required and paramount to Overwatch delivery -- they must be present and valid
    if (!disableValidations) {
      config.setAuditLogConfig(validateAuditLogConfigs(auditLogConfig))
    } else {
      config.setAuditLogConfig(quickBuildAuditLogConfig(auditLogConfig))
    }


    // must happen AFTER data target validation
    // persistent location for corrupted spark event log files
    val badRecordsPath = rawParams.badRecordsPath
    config.setBadRecordsPath(badRecordsPath.getOrElse(
      s"${dataTarget.etlDataPathPrefix}/spark_events_bad_records_files/${config.organizationId}")
    )

    // must happen AFTER data target validation
    if (!disableValidations) { // temp working dir is not necessary for disabled validations as pipelines cannot be
    // executed without validations
      prepAndSetTempWorkingDir(rawParams.tempWorkingDir, config.etlDataPathPrefix)
    }

    config.setMaxDays(rawParams.maxDaysToLoad)

    if (!disableValidations) validateIntelligentScaling(rawParams.intelligentScaling)
    config.setIntelligentScaling(rawParams.intelligentScaling)

    this
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
  private def dataTargetIsValid(dataTarget: DataTarget): Boolean = {
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
  private def validateScope(scopes: Seq[String]): Seq[OverwatchScope.OverwatchScope] = {
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


    // TODO -- Check to see if audit scope was used previously and not now. If so, throw warning and require
    //  override parameter to ensure the user understands the data corruption/loss
    //  for example if user disables clusterevents, ok, but this will disallow certain basic measures and
    //  the data will not be recoverable once it's expires from end of life (as per DB API)

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
      //      case "iampassthrough" => iamPassthrough
      //      case "profiles" => profiles
      case scope => {
        val supportedScopes = s"${OverwatchScope.values.mkString(", ")}, all"
        throw new BadConfigException(s"Scope $scope is not supported. Supported scopes include: " +
          s"${supportedScopes}.")
      }
    }
  }

}

// Todo - do a quick touch to audit logs location and bad records path to validate access
object Initializer extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  // Init the SparkSessionWrapper with envVars
  envInit()

  def getOrgId: String = {
    if (dbutils.notebook.getContext.tags("orgId") == "0") {
      dbutils.notebook.getContext.apiUrl.get.split("\\.")(0).split("/").last
    } else dbutils.notebook.getContext.tags("orgId")
  }

  private def initConfigState(debugFlag: Boolean): Config = {
    logger.log(Level.INFO, "Initializing Config")
    val config = new Config()
    val orgId = if (config.isLocalTesting) System.getenv("ORGID") else {
      getOrgId
    }
    config.setOrganizationId(orgId)
    config.registerInitialSparkConf(spark.conf.getAll)
    config.setInitialWorkerCount(getNumberOfWorkerNodes)
    config.setInitialShuffleParts(spark.conf.get("spark.sql.shuffle.partitions").toInt)
    if (debugFlag) {
      envInit("DEBUG")
      config.setDebugFlag(debugFlag)
    }
    config
  }

  /**
   * Companion object to validate environment initialize the config for the run.
   * Takes input of raw arg strings into the main class, parses and validates them,
   * checks the environement for run readiness, sets up the scope based on modules selected,
   * and checks for avoidable issues. The initializer is also responsible for identifying any errors in the
   * configuration that can be identified before the runs begins to enable fail fast.
   *
   * @param overwatchArgs Json string of args -- When passing into args in Databricks job UI, the json string must
   *                      be passed in as an escaped Json String. Use JsonUtils in Tools to build and extract the string
   *                      to be used here.
   * @param debugFlag     manual Boolean setter to enable the debug flag. This is different than the log4j DEBUG Level
   *                      When setting this to true it does enable the log4j DEBUG level but throughout the code there
   *                      is more robust output when debug is enabled.
   * @return
   */
  def apply(overwatchArgs: String): Workspace = {
    apply(
      overwatchArgs,
      debugFlag = false,
      isSnap = false,
      disableValidations = false
    )
  }
  def apply(overwatchArgs: String, debugFlag: Boolean): Workspace = {
    apply(
      overwatchArgs,
      debugFlag,
      isSnap = false,
      disableValidations = false
    )
  }

  /**
   *
   * @param overwatchArgs Json string of args -- When passing into args in Databricks job UI, the json string must
   *                      be passed in as an escaped Json String. Use JsonUtils in Tools to build and extract the string
   *                      to be used here.
   * @param debugFlag manual Boolean setter to enable the debug flag. This is different than the log4j DEBUG Level
   *                      When setting this to true it does enable the log4j DEBUG level but throughout the code there
   *                      is more robust output when debug is enabled.
   * @param isSnap internal only param to add specific snap metadata to initialized dataset
   * @param disableValidations internal only whether or not to validate the parameters for the local Databricks workspace
   *                           if this is set to true, pipelines cannot be run as they are set to read only mode
   * @param initializeDatabase internal only parameter to disable the automatic creation of a database upon workspace
   *                           init.
   * @return
   */
  private[overwatch] def apply(
                                overwatchArgs: String,
                                debugFlag: Boolean = false,
                                isSnap: Boolean = false,
                                disableValidations: Boolean = false,
                                initializeDatabase: Boolean = true
                              ): Workspace = {

    val config = initConfigState(debugFlag)

    logger.log(Level.INFO, "Initializing Environment")
    val initializer = new Initializer(config)
      .setIsSnap(isSnap)
      .setDisableValidations(disableValidations) // if true, will result in a read only pipeline
      .validateAndRegisterArgs(overwatchArgs)

    val database = if (initializeDatabase) initializer.initializeDatabase() else Database(config)

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace(database, config)
      .setValidated(!disableValidations)

    workspace
  }


}

