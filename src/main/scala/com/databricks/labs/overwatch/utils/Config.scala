package com.databricks.labs.overwatch.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, lit}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.log4j.{Level, Logger}

object Config {

  case class TimeTypes(asUnixTime: Long, asColumnTS: Column, asJavaDate: Date, asString: String)

  private val _isLocalTesting: Boolean = System.getenv("OVERWATCH") == "LOCAL"
  private var _fromTime: Long = _
  private var _pipelineSnapTime: Long = _
  private var _databaseName: String = ""
  private var _databaseLocation: String = ""
  private var _workspaceUrl: String = ""
  private var _token: Array[Byte] = _
  private var _auditLogsPath: Option[String] = None

  final private val cipher = new Cipher
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private[overwatch] def setFromTime(value: Long): this.type = {
    _fromTime = value;
    this
  }

  private[overwatch] def setFromTime(value: Date): this.type = {
    _fromTime = value.getTime / 1000;
    this
  }

  private[overwatch] def setPipelineSnapTime(value: Long): this.type = {
    _pipelineSnapTime = value;
    this
  }

  private[overwatch] def setPipelineSnapTime(value: Date): this.type = {
    _pipelineSnapTime = value.getTime / 1000;
    this
  }

  private[overwatch] def fromTime: TimeTypes = {
    TimeTypes(
      _fromTime,
      from_unixtime(lit(_fromTime / 1000)),
      new Date(_fromTime),
      fmt.format(new Date(_fromTime))
    )
  }

  private[overwatch] def pipelineSnapTime: TimeTypes = {
    TimeTypes(
      _pipelineSnapTime,
      from_unixtime(lit(_pipelineSnapTime / 1000)),
      new Date(_pipelineSnapTime),
      fmt.format(new Date(_fromTime))
    )
  }

  private[overwatch] def isLocalTesting: Boolean = _isLocalTesting

  private[overwatch] def databaseName: String = _databaseName

  private[overwatch] def databaseLocation: String = _databaseLocation

  private[overwatch] def workspaceURL: String = _workspaceUrl

  private[overwatch] def token: String = cipher.decrypt(_token)
  private[overwatch] def encryptedToken: Array[Byte] = _token

  private[overwatch] def auditLogPath: Option[String] = _auditLogsPath

  def buildLocalOverwatchParams(): this.type = {

    registeredEncryptedToken(None)
    _databaseName = "Overwatch"
    _databaseLocation = "/Dev/git/Databricks--Overwatch/spark-warehouse/overwatch.db"
    _auditLogsPath = if (System.getenv("AUDIT_LOG_PATH") == "") None
    else Some(System.getenv("AUDIT_LOG_PATH"))
    this

  }

  private[overwatch] def registeredEncryptedToken(tokenSecret: Option[TokenSecret]): this.type = {
    try {
      // Token secrets not supported in local testing
      if (tokenSecret.nonEmpty && !_isLocalTesting) { // not local testing and secret passed
        _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
        val scope = tokenSecret.get.scope
        val key = tokenSecret.get.key
        _token = cipher.encrypt(dbutils.secrets.get(scope, key))
        val authMessage = s"Valid Secret Identified: Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMessage)
      } else {
        if (_isLocalTesting) { // Local testing env vars
          _workspaceUrl = System.getenv("OVERWATCH_ENV")
          _token = cipher.encrypt(System.getenv("OVERWATCH_TOKEN"))
        } else { // Use default token for job owner
          _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
          _token = cipher.encrypt(dbutils.notebook.getContext().apiToken.get)
          val authMessage = "No secret parameters provided: attempting to continue with job owner's token."
          logger.log(Level.WARN, authMessage)
          println(authMessage)
        }
      }
      this
    } catch {
      case e: Throwable => logger.log(Level.FATAL, "No valid credentials and/or Databricks URI", e);this
    }
  }

  private[overwatch] def setDatabaseNameandLoc(dbName: String, dbLocation: String): this.type = {
    _databaseLocation = dbLocation
    _databaseName = dbName
    println(s"DEBUG: Database Name and Locations set to ${_databaseName} and ${_databaseLocation}")
    this
  }

  private[overwatch] def setAuditLogPath(auditLogPath: Option[String]): this.type = {
    _auditLogsPath = auditLogPath
    this
  }

}
