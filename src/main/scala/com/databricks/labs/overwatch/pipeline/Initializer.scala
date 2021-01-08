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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, row_number}

/**
 * Take the config and validate the setup
 * If valid, initialize the environment
 *
 * @param config
 */
class Initializer(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private def showRangeReport(lastRunDetail: Array[SimplifiedModuleStatusReport]): Unit = {
    val rangeReport = lastRunDetail.map(lr =>
      (
        lr.moduleID,
        lr.moduleName,
        config.fromTime(lr.moduleID).asTSString,
        config.untilTime(lr.moduleID).asTSString,
        config.pipelineSnapTime.asTSString
      )
    )

    rangeReport.toSeq.toDF("moduleID", "moduleName", "fromTS", "untilTS", "snapTS")
      .orderBy('snapTS.desc, 'moduleId)
      .show(false)
  }

  /**
   * initialize the pipeline run
   * Identify the timestamps to use by module and set them
   *
   * @return
   */
  private def initPipelineRun(): this.type = {
    val rangeDetail = if (spark.catalog.databaseExists(config.databaseName) &&
      spark.catalog.tableExists(config.databaseName, "pipeline_report")) {
      val w = Window.partitionBy('moduleID).orderBy('Pipeline_SnapTS.desc)
      val lastRunDetail = spark.table(s"${config.databaseName}.pipeline_report")
        .filter('Status.isin("SUCCESS", "EMPTY"))
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter('rnk === 1 && 'rn === 1)
        .drop("inputConfig", "parsedConfig")
        .as[SimplifiedModuleStatusReport]
        .collect()
      config.setLastRunDetail(lastRunDetail)
      lastRunDetail
    } else {
      config.setIsFirstRun(true)
      Array[SimplifiedModuleStatusReport]()
    }
    config.setPipelineSnapTime()
    if (config.debugFlag) showRangeReport(rangeDetail)
    this
  }

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
    logger.log(Level.INFO, "Initializing ETL Database")
    if (!spark.catalog.databaseExists(config.databaseName)) {
      logger.log(Level.INFO, s"Database ${config.databaseName} not found, creating it at " +
        s"${config.databaseLocation}.")
      val createDBIfNotExists = if (!config.isLocalTesting) {
        s"create database if not exists ${config.databaseName} location '" +
          s"${config.databaseLocation}' WITH DBPROPERTIES (OVERWATCHDB='TRUE',SCHEMA=${config.overwatchSchemaVersion})"
      } else {
        s"create database if not exists ${config.databaseName} " +
          s"WITH DBPROPERTIES (OVERWATCHDB='TRUE',SCHEMA=${config.overwatchSchemaVersion})"
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

  /**
   * Ensure all static datasets exist in the newly initialized Database. This function must be called after
   * the database has been initialized.
   *
   * @return
   */
  private def loadStaticDatasets: this.type = {
    if (config.isFirstRun || !spark.catalog.tableExists(config.consumerDatabaseName, "instanceDetails")) {
      val instanceDetailsDF = config.cloudProvider match {
        case "aws" =>
          InitializerFunctions.loadLocalCSVResource(spark, "/AWS_Instance_Details.csv")
        case "azure" =>
          InitializerFunctions.loadLocalCSVResource(spark, "/Azure_Instance_Details.csv")
        case _ =>
          throw new IllegalArgumentException("Overwatch only supports cloud providers, AWS and Azure.")
      }

      instanceDetailsDF
        .write.format("delta")
        .saveAsTable(s"${config.consumerDatabaseName}.instanceDetails")
    }
    this
  }

  @throws(classOf[BadConfigException])
  private def validateAuditLogConfigs(auditLogConfig: AuditLogConfig): this.type = {

    if (config.cloudProvider == "aws") {

      val auditLogPath = auditLogConfig.rawAuditPath
      if (config.overwatchScope.contains(audit) && auditLogPath.isEmpty) {
        throw new BadConfigException("Audit cannot be in scope without the 'auditLogPath' being set. ")
      }

      if (auditLogPath.nonEmpty && !config.isLocalTesting)
        dbutils.fs.ls(auditLogPath.get).foreach(auditFolder => {
          if (auditFolder.isDir) require(auditFolder.name.startsWith("date="), s"Audit directory must contain " +
            s"partitioned date folders in the format of ${auditLogPath.get}/date=. Received ${auditFolder} instead.")
        })

      val finalAuditLogPath = if (auditLogPath.get.endsWith("/")) auditLogPath.get.dropRight(1) else auditLogPath.get

      config.setAuditLogConfig(
        auditLogConfig.copy(rawAuditPath = Some(finalAuditLogPath), None)
      )

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

      config.setAuditLogConfig(
        auditLogConfig.copy(
          None, Some(ehFinalConfig)
        )
      )

    }
    this
  }

  /**
   * Convert the args brought in as JSON string into the paramters object "OverwatchParams".
   * Validate the config and the environment readiness for the run based on the configs and environment state
   *
   * @param args JSON string of input args from input into main class.
   * @return
   */
  private def validateAndRegisterArgs(args: Array[String]): this.type = {

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
      val synthArgs = config.buildLocalOverwatchParams()
      mapper.readValue[OverwatchParams](synthArgs)
    } else {
      logger.log(Level.INFO, "Validating Input Parameters")
      mapper.readValue[OverwatchParams](args(0))
    }

    // Now that the input parameters have been parsed -- set them in the config
    config.setInputConfig(rawParams)


    val overwatchScope = rawParams.overwatchScope.getOrElse(Seq("all"))
    val tokenSecret = rawParams.tokenSecret
    // TODO -- PRIORITY -- If data target is null -- default table gets dbfs:/null
    val dataTarget = rawParams.dataTarget.getOrElse(
      DataTarget(Some("overwatch"), Some("dbfs:/user/hive/warehouse/overwatch.db")))
    val auditLogConfig = rawParams.auditLogConfig
    val badRecordsPath = rawParams.badRecordsPath

    if (overwatchScope.head == "all") config.setOverwatchScope(config.orderedOverwatchScope)
    else config.setOverwatchScope(validateScope(overwatchScope))

    // validate token secret requirements
    // TODO - Validate if token has access to necessary assets. Warn/Fail if not
    if (tokenSecret.nonEmpty && !config.isLocalTesting) {
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

      config.registeredEncryptedToken(Some(TokenSecret(scopeName, keyCheck.head.key)))
    } else config.registeredEncryptedToken(None)

    // Validate data Target
    if (!config.isLocalTesting) dataTargetIsValid(dataTarget)

    // If data target is valid get db name and location and set it
    val dbName = dataTarget.databaseName.get
    val dbLocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")
    config.setDatabaseNameandLoc(dbName, dbLocation)

    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
    val consumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
    config.setConsumerDatabaseNameandLoc(consumerDBName, consumerDBLocation)

    // Set Databricks Contract Prices from Config
    // Defaulted to 0.56 interactive and 0.26 automated
    config.setContractInteractiveDBUPrice(rawParams.databricksContractPrices.interactiveDBUCostUSD)
    config.setContractAutomatedDBUPrice(rawParams.databricksContractPrices.automatedDBUCostUSD)

    // Audit logs are required and paramount to Overwatch delivery -- they must be present and valid
    validateAuditLogConfigs(auditLogConfig)

    // Todo -- add validation to badRecordsPath
    config.setBadRecordsPath(badRecordsPath.getOrElse("/tmp/overwatch/badRecordsPath"))

    config.setMaxDays(rawParams.maxDaysToLoad)

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
    val dbLocation = dataTarget.databaseLocation.getOrElse(s"/user/hive/warehouse/${dbName}.db")
    var switch = true
    if (spark.catalog.databaseExists(dbName)) {
      val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
      val dbProperties = dbMeta.properties
      val existingDBLocation = dbMeta.locationUri.toString
      if (existingDBLocation != dbLocation) {
        switch = false
        throw new BadConfigException(s"The DB: $dbName exists" +
          s"at location $existingDBLocation which is different than the location entered in the config. Ensure" +
          s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
          s"dbfs:/...")
      }

      val isOverwatchDB = dbProperties.getOrElse("OVERWATCHDB", "FALSE") == "TRUE"
      if (!isOverwatchDB) {
        switch = false
        throw new BadConfigException(s"The Database: $dbName was not created by overwatch. Specify a " +
          s"database name that does not exist or was created by Overwatch.")
      }
      val overwatchSchemaVersion = dbProperties.getOrElse("SCHEMA", "BAD_SCHEMA")
      if (overwatchSchemaVersion != config.overwatchSchemaVersion) {
        switch = false
        throw new BadConfigException(s"The overwatch DB Schema version is: $overwatchSchemaVersion but this" +
          s" version of Overwatch requires ${config.overwatchSchemaVersion}. Upgrade Overwatch Schema to proceed " +
          s"or drop existing database and allow Overwatch to recreate.")
      }
    } else { // Database does not exist
      try { // If the fs.ls below doesn't not throw a FileNotFound exception the target is not empty and will fail the run
        dbutils.fs.ls(dbLocation)
        switch = false
        throw new BadConfigException(s"The target database location: ${dbLocation} " +
          s"already exists but does not appear to be associated with the Overwatch database name, $dbName " +
          s"specified. Specify a path that doesn't already exist or choose an existing overwatch database AND " +
          s"database its associated location.")
      } catch { // If the fs.ls throws fileNotFound exception, the db target does not exist and the db will be created
        case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Target location " +
          s"is valid: will create database: $dbName at location: ${dbLocation}", e)
      }
    }

    // todo - refactor away duplicity
    /**
     * Many of the validation above are required for the consumer DB but the consumer DB will only contain
     * views. It's important that the basic db checks are completed but the checks don't need to be as extensive
     * since there's no chance of data corruption given only creating views. This section needs to be refactored
     * to remove duplicity while still enabling control between which checks are done for which DataTarget.
     */
    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
    val consumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
    if (consumerDBName != dbName) { // separate consumer db
      if (spark.catalog.databaseExists(consumerDBName)) {
        val consumerDBMeta = spark.sessionState.catalog.getDatabaseMetadata(consumerDBName)
        val existingConsumerDBLocation = consumerDBMeta.locationUri.toString
        if (existingConsumerDBLocation != consumerDBLocation) {
          switch = false
          throw new BadConfigException(s"The DB: $dbName exists" +
            s"at location $existingConsumerDBLocation which is different than the location entered in the config. Ensure" +
            s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
            s"dbfs:/...")
        }
      } else { // consumer DB is different from ETL DB AND db does not exist
        try { // If the fs.ls below doesn't not throw a FileNotFound exception the target is not empty and will fail the run
          dbutils.fs.ls(consumerDBLocation)
          switch = false
          throw new BadConfigException(s"The target database location: ${consumerDBLocation} " +
            s"already exists but does not appear to be associated with the Overwatch database name, $consumerDBName " +
            s"specified. Specify a path that doesn't already exist or choose an existing overwatch database AND " +
            s"database its associated location.")
        } catch { // If the fs.ls throws fileNotFound exception, the db target does not exist and the db will be created
          case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Target location " +
            s"is valid: will create database: $consumerDBName at location: ${consumerDBLocation}", e)
        }
      }

      if (consumerDBLocation == dbLocation) { // separate db AND same location ERROR
        switch = false
        throw new BadConfigException("Consumer DB Name cannot differ from ETL DB Name while having the same location.")
      }
    } else { // same consumer db as etl db
      if (consumerDBLocation != dbLocation) { // same db AND DIFFERENT location ERROR
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

  /**
   * Companion object to validate environment initialize the config for the run.
   * Takes input of raw arg strings into the main class, parses and validates them,
   * checks the environement for run readiness, sets up the scope based on modules selected,
   * and checks for avoidable issues. The initializer is also responsible for identifying any errors in the
   * configuration that can be identified before the runs begins to enable fail fast.
   *
   * @param args      Json string of args -- When passing into args in Databricks job UI, the json string must
   *                  be passed in as an escaped Json String. Use JsonUtils in Tools to build and extract the string
   *                  to be used here.
   * @param debugFlag manual Boolean setter to enable the debug flag. This is different than the log4j DEBUG Level
   *                  When setting this to true it does enable the log4j DEBUG level but throughout the code there
   *                  is more robust output when debug is enabled.
   * @return
   */
  def apply(args: Array[String], debugFlag: Boolean = false): Workspace = {

    logger.log(Level.INFO, "Initializing Config")
    val config = new Config()
    config.registerInitialSparkConf(spark.conf.getAll)
    config.setInitialShuffleParts(spark.conf.get("spark.sql.shuffle.partitions").toInt)
    if (debugFlag) {
      envInit("DEBUG")
      config.setDebugFlag(debugFlag)
    }

    logger.log(Level.INFO, "Initializing Environment")
    val initializer = new Initializer(config)
    val database = initializer
      .validateAndRegisterArgs(args)
      .initPipelineRun()
      .initializeDatabase()

    initializer.loadStaticDatasets

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace(database, config)

    workspace
  }
}

