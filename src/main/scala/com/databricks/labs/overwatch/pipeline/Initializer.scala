package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ParamDeserializer
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.OverwatchScope._
import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{current_timestamp, lit, max, rank, row_number}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class Initializer(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  // TODO -- look for max incremental in all tables to build out module last run times instead of deriving it from
  //  pipeline_report as this clearly can cause issues
  def initPipelineRun(): this.type = {
    if (spark.catalog.databaseExists(config.databaseName) &&
      spark.catalog.tableExists(config.databaseName, "pipeline_report")) {
      val w = Window.partitionBy('moduleID).orderBy('Pipeline_SnapTS.desc)
      val lastRunDetail = spark.table(s"${config.databaseName}.pipeline_report")
        .filter('Status === "SUCCESS")
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter('rnk === 1 && 'rn === 1)
        .as[ModuleStatusReport]
        .collect()
      config.setLastRunDetail(lastRunDetail)
    } else {
      config.setIsFirstRun(true)
    }
    config.setPipelineSnapTime()
    this
  }

  def initializeDatabase(): Database = {
    // TODO -- Add metadata table
    logger.log(Level.INFO, "Initializing Database")
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
      logger.log(Level.INFO, s"Sucessfully created database. $createDBIfNotExists")
      Database(config)
    } else {
      // TODO -- get schema version of each table and perform upgrade if necessary
      logger.log(Level.INFO, s"Database ${config.databaseName} already exists, using append mode.")
      Database(config)
    }
  }

  @throws(classOf[BadConfigException])
  def validateAuditLogConfigs(auditLogConfig: AuditLogConfig): this.type = {

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

      val finalAuditLogPath = auditLogPath.get.replaceAll("//", "/")

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

      if (config.debugFlag){
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

  def validateAndRegisterArgs(args: Array[String]): this.type = {

    val paramModule: SimpleModule = new SimpleModule()
      .addDeserializer(classOf[OverwatchParams], new ParamDeserializer)
    val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .registerModule(paramModule)
      .asInstanceOf[ObjectMapper with ScalaObjectMapper]

    // Allow for local testing
    val rawParams = if (config.isLocalTesting) {
//      config.buildLocalOverwatchParams()
      val synthArgs = config.buildLocalOverwatchParams()
      mapper.readValue[OverwatchParams](synthArgs)
    } else {
      logger.log(Level.INFO, "Validating Input Parameters")
      mapper.readValue[OverwatchParams](args(0))
    }
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

      if (!config.isLocalTesting) dataTargetIsValid(dataTarget)

      val dbName = dataTarget.databaseName.get
      val dbLocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")

      config.setDatabaseNameandLoc(dbName, dbLocation)

      validateAuditLogConfigs(auditLogConfig)

      // Todo -- add validation to badRecordsPath
      config.setBadRecordsPath(badRecordsPath.getOrElse("/tmp/overwatch/badRecordsPath"))

    this
  }

  @throws(classOf[IllegalArgumentException])
  private def dataTargetIsValid(dataTarget: DataTarget): Boolean = {
    val dbName = dataTarget.databaseName.getOrElse("overwatch")
    val dblocation = dataTarget.databaseLocation.getOrElse(s"/user/hive/warehouse/${dbName}.db")
    var switch = true
    if (spark.catalog.databaseExists(dbName)) {
      val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
      val dbProperties = dbMeta.properties
      val existingDBLocaion = dbMeta.locationUri.toString
      if (existingDBLocaion != dblocation) {
        switch = false
        throw new BadConfigException(s"The DB: $dbName exists" +
          s"at location $existingDBLocaion which is different than the location entered in the config. Ensure" +
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
      try {
        dbutils.fs.ls(dblocation)
        switch = false
        throw new BadConfigException(s"The target database location: ${dblocation} " +
          s"already exists but does not appear to be associated with the Overwatch database name, $dbName " +
          s"specified. Specify a path that doesn't already exist or choose an existing overwatch database AND " +
          s"database its associated location.")
      } catch {
        case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Target location " +
          s"is valid: will create database: $dbName at location: ${dblocation}")
      }
    }

    switch
  }

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

    if ((lcScopes.contains("clusterevents") || lcScopes.contains("clusters") || lcScopes.contains("jobs"))
      && !lcScopes.contains("audit")) {
      println(s"WARNING: JobRuns without audit will result in loss of granularity. It's recommended to configure" +
        s"the audit module.")
    }


    // TODO -- Check to see if audit scope was used previous and not now. If so, throw warning and require
    //  override parameter to ensure the user understands the data corruption/loss
    lcScopes.map {
      case "jobs" => jobs
      case "clusters" => clusters
      case "clusterevents" => clusterEvents
      case "sparkevents" => sparkEvents
      case "notebooks" => notebooks
//      case "pools" => pools
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

  def apply(args: Array[String], debugFlag: Boolean = false): Workspace = {

    logger.log(Level.INFO, "Initializing Config")
    val config = new Config()
    config.registerInitialSparkConf(spark.conf.getAll)
    config.setInitialShuffleParts(spark.conf.get("spark.sql.shuffle.partitions").toInt)
    config.setDebugFlag(debugFlag)

    // Todo - Move timestamp init to correct location
    logger.log(Level.INFO, "Initializing Environment")
    val database = new Initializer(config)
      .validateAndRegisterArgs(args)
      .initPipelineRun()
      .initializeDatabase()

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace(database, config)

    workspace
  }

}