package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ParamDeserializer
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchParams, TokenSecret, SparkSessionWrapper}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._

class Initializer extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def initializeTimestamps(value: Long): this.type = {
    // TODO - Pull from state - Currently hardcoded to Jan 1 2020
    Config.setFromTime(1577836800000L)
    Config.setPipelineSnapTime(value)
    this
  }

  def initializeDatabase(): Database = {
    if (!spark.catalog.databaseExists(Config.databaseName)) {
      logger.log(Level.INFO, s"Database ${Config.databaseName} not found, creating it at " +
        s"${Config.databaseLocation}.")
      val createDBIfNotExists = s"create database if not exists ${Config.databaseName} location '" +
        s"${Config.databaseLocation}'"
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Sucessfully created database. $createDBIfNotExists")
      Database(Config.databaseName)
    } else {
      logger.log(Level.INFO, s"Databsae ${Config.databaseName} already exists, using append mode.")
      Database(Config.databaseName)
    }
  }

}

object Initializer {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def apply(args: Array[String]): (Workspace, Database, Pipeline) = {

    logger.log(Level.INFO, "Initializing Environment")
    validateAndRegisterArgs(args)

    // Todo - Move timestamp init to correct location
    val initializer = new Initializer()
      .initializeTimestamps(System.currentTimeMillis())

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace()

    logger.log(Level.INFO, "Initializing Database")
    val database = initializer.initializeDatabase()

    logger.log(Level.INFO, "Initializing Pipeline")
    val pipeline = Pipeline(workspace, database)

    (workspace, database, pipeline)
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
    } else {
      try {
        logger.log(Level.INFO, "Validating Input Parameters")
        val rawParams = mapper.readValue[OverwatchParams](args(0))
        val tokenSecret = rawParams.tokenSecret
        val dataTarget = rawParams.dataTarget
        val auditLogPath = rawParams.auditLogPath

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

        val dbName = if (dataTarget.nonEmpty && dataTarget.get.databaseName.nonEmpty) dataTarget.get.databaseName.get
        else "OverWatch"

        val dbLoc = if (dataTarget.nonEmpty && dataTarget.get.databaseLocation.nonEmpty) dataTarget.get.databaseLocation.get
        else s"dbfs:/user/hive/warehouse/${dbName}.db"

        Config.setDatabaseNameandLoc(dbName, dbLoc)
        Config.setAuditLogPath(auditLogPath)


        //        // validate data target requirements
        //        val (dbName, dbLocation) = if (dataTarget.nonEmpty) {
        //          // Database location validation
        //          if (dataTarget.get.databaseLocation.nonEmpty) {
        //            // Require On DBFS
        //            require(dataTarget.get.databaseLocation.get.take(6) == "dbfs:/", "Specified database location " +
        //              "must be on dbfs. If direct third party storage is required, please mount that to Databricks and " +
        //              "specificy the mount location, dbfs:/mnt/...")
        //            try {
        //              dbutils.fs.ls(dataTarget.get.databaseLocation.get)
        //              throw new IllegalArgumentException(s"The target database location: ${dataTarget.get.databaseLocation.get} " +
        //                s"already exists. Specify a path that doesn't already exist.")
        //            } catch {
        //              case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Target location is valid: " +
        //                s"will create database at ${dataTarget.get.databaseLocation.get}", e)
        //            }
        //          }
        //        }
        //        try {
        //          dbutils.fs.ls(dbDefaultLocation)
        //          throw new IllegalArgumentException(s"The default target database location: ${dbDefaultLocation} " +
        //            s"already exists. Specify a path that doesn't already exist.")
        //        } catch {
        //          case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Default Target location " +
        //            s"is valid: will create database at ${dbDefaultLocation}")
        //        }
        //
        ////        val dbName = dataTarget.get.databaseName.getOrElse("Overwatch")
        ////        val dbDefaultLocation = dataTarget.get s"dbfs:/user/hive/warehouse/${dbName}.db"
        //        val validatedDataTarget: DataTarget = DataTarget(Some(dbName),
        //          Some(dataTarget.get.databaseLocation.getOrElse(dbDefaultLocation)))
        //
        //        OverwatchParams(validatedTokenSecret, Some(validatedDataTarget), auditLogPath)
              } catch {
                case e: Throwable => {
                  logger.log(Level.FATAL, s"Input parameters could not be validated. " +
                    s"Failing to avoid workspace contamination. \n $e")
                }
              }
      }
    }
  }