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
  private var _databaseName: String = _
  private var _databaseLocation: String = _
  private var _workspaceUrl: String = _
  private var _token: Array[Byte] = _
  private var _auditLogsPath: Option[String] = _

  final private val cipher = new Cipher
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private[overwatch] def setFromTime(value: Long): Boolean = {
    _fromTime = value;
    true
  }

  private[overwatch] def setFromTime(value: Date): Boolean = {
    _fromTime = value.getTime / 1000;
    true
  }

  private[overwatch] def setPipelineSnapTime(value: Long): Boolean = {
    _pipelineSnapTime = value;
    true
  }

  private[overwatch] def setPipelineSnapTime(value: Date): Boolean = {
    _pipelineSnapTime = value.getTime / 1000;
    true
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

  def buildLocalOverwatchParams(): Unit = {

    registeredEncryptedToken(None)
    _databaseName = "Overwatch"
    _databaseLocation = "/Dev/git/Databricks--Overwatch/spark-warehouse/overwatch.db"
    _auditLogsPath = if (System.getenv("AUDIT_LOG_PATH") == "") None
    else Some(System.getenv("AUDIT_LOG_PATH"))

  }

  private[overwatch] def registeredEncryptedToken(tokenSecret: Option[TokenSecret]): Unit = {
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
    } catch {
      case e: Throwable => logger.log(Level.FATAL, "No valid credentials and/or Databricks URI", e)
    }
  }

  private[overwatch] def setDatabaseNameandLoc(dbName: String, dbLocation: String): Unit = {
    _databaseLocation = dbLocation
    _databaseName = dbName
  }

  private[overwatch] def setAuditLogPath(auditLogPath: Option[String]): Unit = {
    _auditLogsPath = auditLogPath
  }

}
