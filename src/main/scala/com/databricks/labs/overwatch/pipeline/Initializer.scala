package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ParamDeserializer
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.GlobalStructures.{DataTarget, OverwatchParams, TokenSecret}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._

class Initializer extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _
  private var _databaseLocation: String = _
  private final var _snapUnixStartTimeMillis: Long = _
  private var _snapUnixPreviousEndTimeMillis: Long = _

  def initializeTimestamps(value: Long): this.type = {
    _snapUnixStartTimeMillis = value
    // TODO - Pull from state - Currently hardcoded to Jan 1 2020
    _snapUnixPreviousEndTimeMillis = 1577836800000L
    this
  }

  def setBaseParams(value: OverwatchParams): this.type = {
    _databaseName = value.dataTarget.get.databaseName.get
    _databaseLocation = value.dataTarget.get.databaseLocation.get
    this
  }

  def databaseName: String = _databaseName
  def databaseLocation: String = _databaseLocation
  def startTime: Long = _snapUnixStartTimeMillis
  def previousEndTime: Long = _snapUnixPreviousEndTimeMillis

  def initializeDatabase(): Database = {
    if (!spark.catalog.databaseExists(databaseName)) {
      logger.log(Level.INFO, s"Database ${databaseName} not found, creating it at " +
        s"${databaseLocation}.")
      val createDBIfNotExists = s"create database if not exists ${databaseName} location '" +
        s"${databaseLocation}'"
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Sucessfully created database. $createDBIfNotExists")
      Database(databaseName)
    } else {
      logger.log(Level.INFO, s"Databsae ${databaseName} already exists, using append mode.")
      Database(databaseName)
    }
  }

}

object Initializer {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def apply(params: OverwatchParams): Initializer = {
    new Initializer()
      .setBaseParams(params)
      .initializeTimestamps(System.currentTimeMillis())
  }

  def buildLocalOverwatchParams: OverwatchParams = {
    val tokenSecret = Some(TokenSecret("LOCALTESTING", "TOKEN"))
    val dataTarget = Some(DataTarget(Some("Overwatch"), Some("/mnt/tomesdata/data/overwatch/overwatch.db")))
    OverwatchParams(
      tokenSecret,
      dataTarget
    )
  }

  def validateBatchParams(args: Array[String]): OverwatchParams = {

    val paramModule: SimpleModule = new SimpleModule()
      .addDeserializer(classOf[OverwatchParams], new ParamDeserializer)
    val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .registerModule(paramModule)
      .asInstanceOf[ObjectMapper with ScalaObjectMapper]

    // Allow for local testing
    if (System.getenv("OVERWATCH").equals("LOCAL")) {
      buildLocalOverwatchParams
    } else {
      try {
        logger.log(Level.INFO, "Validating Input Parameters")
        val rawParams = mapper.readValue[OverwatchParams](args(0))
        val tokenSecret = rawParams.tokenSecret
        val dataTarget = rawParams.dataTarget

        // validate token secret requirements
        val validatedTokenSecret: Option[TokenSecret] = if (tokenSecret.nonEmpty) {
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
          Some(TokenSecret(scopeName, keyCheck.head.key))
        } else None
        // validate data target requirements
        if (dataTarget.nonEmpty) {
          if (dataTarget.get.databaseLocation.nonEmpty) {
            require(dataTarget.get.databaseLocation.get.take(6) == "dbfs:/", "Specified database location " +
              "must be on dbfs. If direct third party storage is required, please mount that to Databricks and " +
              "specificy the mount location, dbfs:/mnt/...")
            try {
              dbutils.fs.ls(dataTarget.get.databaseLocation.get)
              throw new IllegalArgumentException(s"The target database location: ${dataTarget.get.databaseLocation.get} " +
                s"already exists. Specify a path that doesn't already exist.")
            } catch {
              case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Target location is valid: " +
                s"will create database at ${dataTarget.get.databaseLocation.get}")
            }
          }
        }
        val dbName = dataTarget.get.databaseName.getOrElse("Overwatch")
        val dbDefaultLocation = s"dbfs:/user/hive/warehouse/${dbName}.db"
        try {
          dbutils.fs.ls(dbDefaultLocation)
          throw new IllegalArgumentException(s"The default target database location: ${dbDefaultLocation} " +
            s"already exists. Specify a path that doesn't already exist.")
        } catch {
          case e: java.io.FileNotFoundException => logger.log(Level.INFO, s"Default Target location " +
            s"is valid: will create database at ${dbDefaultLocation}")
        }
        val validatedDataTarget: DataTarget = DataTarget(Some(dbName),
          Some(dataTarget.get.databaseLocation.getOrElse(dbDefaultLocation)))

        OverwatchParams(validatedTokenSecret, Some(validatedDataTarget))
      } catch {
        case e: Throwable => {
          logger.log(Level.FATAL, s"Input parameters could not be validated. " +
            s"Failing to avoid workspace contamination. \n $e")
          OverwatchParams(tokenSecret = None, dataTarget = None)
        }
      }
    }
  }
}