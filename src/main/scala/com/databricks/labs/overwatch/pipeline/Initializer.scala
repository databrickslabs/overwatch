package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ParamDeserializer
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.OverwatchScope._
import com.databricks.labs.overwatch.utils.{Config, DataTarget, OverwatchParams, OverwatchScope, SparkSessionWrapper, TokenSecret}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, row_number, current_timestamp, max, lit}

import scala.collection.JavaConverters._

class Initializer extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  def initializeTimestamps(): this.type = {
    val fromTimeByModuleID = if (spark.catalog.databaseExists(Config.databaseName)) {
      // Determine if >= 7 days since last pipeline run, if so, set postProcessing Flag
      Config.setPostProcessingFlag(spark.table(s"${Config.databaseName}.pipeline_report")
        .select(
          (current_timestamp.cast("long") -
            max('Pipeline_SnapTS).cast("long")) >= lit(604800)).as[Boolean]
        .collect()(0))

      val w = Window.partitionBy('moduleID).orderBy('untilTS)
      spark.table(s"${Config.databaseName}.pipeline_report")
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter('rnk === 1 && 'rn === 1)
        .select('moduleID, 'untilTS)
        .rdd.map(r => (r.getInt(0), r.getLong(1)))
        .collectAsMap().toMap
    } else {
      Config.setPostProcessingFlag(true)
      Map(0 -> Config.fromTime(0).asUnixTime)
    }
    Config.setFromTime(fromTimeByModuleID)
    Config.setPipelineSnapTime()
    this
  }

  def initializeDatabase(): Database = {
    // TODO -- Add metadata table
    if (!spark.catalog.databaseExists(Config.databaseName)) {
      logger.log(Level.INFO, s"Database ${Config.databaseName} not found, creating it at " +
        s"${Config.databaseLocation}.")
      val createDBIfNotExists = s"create database if not exists ${Config.databaseName} location '" +
        s"${Config.databaseLocation}' WITH DBPROPERTIES (OVERWATCHDB='TRUE',SCHEMA=${Config.overwatchSchemaVersion})"
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Sucessfully created database. $createDBIfNotExists")
      Database(Config.databaseName)
    } else {
      logger.log(Level.INFO, s"Databsae ${Config.databaseName} already exists, using append mode.")
      Database(Config.databaseName)
    }
  }

}

object Initializer extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  // Init the SparkSessionWrapper with envVars
  envInit()

  def apply(args: Array[String]): Workspace = {

    logger.log(Level.INFO, "Initializing Environment")
    validateAndRegisterArgs(args)

    // Todo - Move timestamp init to correct location
    val initializer = new Initializer()
      .initializeTimestamps()

    logger.log(Level.INFO, "Initializing Database")
    val database = initializer.initializeDatabase()

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace(database)

    workspace
  }

  @throws(classOf[IllegalArgumentException])
  private def validateScope(scopes: Array[String]): Array[OverwatchScope.OverwatchScope] = {
    val lcScopes = scopes.map(_.toLowerCase)
    if (lcScopes.contains("jobruns")) {
      require(lcScopes.contains("jobs"), "When jobruns are in scope for log capture, jobs must also be in scope " +
        "as jobruns depend on jobIDs captured from jobs scope.")
    }

    if (lcScopes.contains("clusterevents") || lcScopes.contains("sparkevents")) {
      require(lcScopes.contains("clusters"), "sparkEvents and clusterEvents scopes both require clusters scope to " +
        "also be enabled as clusterID is a requirement for these scopes.")
    }

    lcScopes.map {
      case "jobs" => jobs
      case "jobruns" => jobRuns
      case "clusters" => clusters
      case "clusterevents" => clusterEvents
      case "sparkevents" => sparkEvents
      case "pools" => pools
      case "audit" => audit
      case "iampassthrough" => iamPassthrough
      case "profiles" => profiles
      case scope => {
        val supportedScopes = s"${OverwatchScope.values.mkString(", ")}, all"
        throw new IllegalArgumentException(s"Scope $scope is not supported. Supported scopes include: " +
          s"${supportedScopes}.")
      }
    }
  }

  @throws(classOf[IllegalArgumentException])
  private def dataTargetIsValid(dataTarget: DataTarget): Boolean = {
    val dbName = dataTarget.databaseName.getOrElse("overwatch")
    val dblocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")
    var switch = true
    if (spark.catalog.databaseExists(dbName)) {
      val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
      val dbProperties = dbMeta.properties
      val existingDBLocaion = dbMeta.locationUri.toString
      if (existingDBLocaion != dblocation) {
        switch = false
        throw new IllegalArgumentException(s"The DB: $dbName exists" +
          s"at location $existingDBLocaion which is different than the location entered in the config. Ensure" +
          s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
          s"dbfs:/...")
      }

      val isOverwatchDB = dbProperties.getOrElse("OVERWATCHDB", "FALSE") == "TRUE"
      if (!isOverwatchDB) {
        switch = false
        throw new IllegalArgumentException(s"The Database: $dbName was not created by overwatch. Specify a " +
          s"database name that does not exist or was created by Overwatch.")
      }
      val overwatchSchemaVersion = dbProperties.getOrElse("SCHEMA", "BAD_SCHEMA")
      if (overwatchSchemaVersion != Config.overwatchSchemaVersion) {
        switch = false
        throw new IllegalArgumentException(s"The overwatch DB Schema version is: $overwatchSchemaVersion but this" +
          s" version of Overwatch requires ${Config.overwatchSchemaVersion}. Upgrade Overwatch Schema to proceed " +
          s"or drop existing database and allow Overwatch to recreate.")
      }
    } else { // Database does not exist
      try {
        dbutils.fs.ls(dblocation)
        switch = false
        throw new IllegalArgumentException(s"The target database location: ${dblocation} " +
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

  def validateAndRegisterArgs(args: Array[String]): Unit = {

    val paramModule: SimpleModule = new SimpleModule()
      .addDeserializer(classOf[OverwatchParams], new ParamDeserializer)
    val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .registerModule(paramModule)
      .asInstanceOf[ObjectMapper with ScalaObjectMapper]

    // Allow for local testing
    if (Config.isLocalTesting) {
      Config.buildLocalOverwatchParams()
      println("Built Local Override Parameters")
    } else {
      logger.log(Level.INFO, "Validating Input Parameters")
      val rawParams = mapper.readValue[OverwatchParams](args(0))
      Config.setInputConfig(rawParams)
      val overwatchScope = rawParams.overwatchScope.getOrElse(Array("all"))
      val tokenSecret = rawParams.tokenSecret
      val dataTarget = rawParams.dataTarget.getOrElse(
        DataTarget(Some("overwatch"), Some("dbfs:/user/hive/warehouse/overwatch.db")))
      val auditLogPath = rawParams.auditLogPath
      val eventLogPrefix = rawParams.eventLogPrefix
      val badRecordsPath = rawParams.badRecordsPath

      // validate token secret requirements
      // TODO - Validate if token has access to necessary assets. Warn/Fail if not
      if (tokenSecret.nonEmpty) {
        if (tokenSecret.get.scope.isEmpty || tokenSecret.get.key.isEmpty) {
          throw new IllegalArgumentException(s"Secret AND Key must be provided together or neither of them. " +
            s"Either supply both or neither.")
        }
        val scopeCheck = dbutils.secrets.listScopes().map(_.getName()).toArray.filter(_ == tokenSecret.get.scope)
        if (scopeCheck.length == 0) throw new NullPointerException(s"Scope ${tokenSecret.get.scope} does not exist " +
          s"in this workspace. Please provide a scope available and accessible to this account.")
        val scopeName = scopeCheck.head

        val keyCheck = dbutils.secrets.list(scopeName).toArray.filter(_.key == tokenSecret.get.key)
        if (keyCheck.length == 0) throw new NullPointerException(s"Key ${tokenSecret.get.key} does not exist " +
          s"within the provided scope: ${tokenSecret.get.scope}. Please provide a scope and key " +
          s"available and accessible to this account.")

        Config.registeredEncryptedToken(Some(TokenSecret(scopeName, keyCheck.head.key)))
      } else Config.registeredEncryptedToken(None)

      // Todo -- validate database name and location
      // Todo -- If Name exists allow existing location and validate correct Overwatch DB and DB version
      dataTargetIsValid(dataTarget)

      val dbName = dataTarget.databaseName.get
      val dbLocation = dataTarget.databaseLocation.get

      Config.setDatabaseNameandLoc(dbName, dbLocation)
      Config.setAuditLogPath(auditLogPath)
      Config.setEventLogPrefix(eventLogPrefix)

      // Todo -- add validation to badRecordsPath
      Config.setBadRecordsPath(badRecordsPath.getOrElse("/tmp/overwatch/badRecordsPath"))

      if (overwatchScope(0) == "all") Config.setOverwatchScope(OverwatchScope.values.toArray)
      else Config.setOverwatchScope(validateScope(overwatchScope))
    }
  }
}