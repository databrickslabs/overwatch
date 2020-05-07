package com.databricks.labs.overwatch.utils

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, LocalTime, ZoneId, ZoneOffset}
import java.util.{Calendar, Date, TimeZone, UUID}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, lit}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.log4j.{Level, Logger}

object Config {

  case class TimeTypes(asUnixTime: Long, asColumnTS: Column, asJavaDate: Date,
                       asUTCDateTime: LocalDateTime, asMidnightEpochMilli: Long,
                       asTSString: String, asDTString: String)

  private final val _overwatchSchemaVersion = "0.1"
  private final val _runID = UUID.randomUUID().toString.replace("-","")
  private val _isLocalTesting: Boolean = System.getenv("OVERWATCH") == "LOCAL"
  private var _fromTime: Map[Int, Long] = Map(0 -> primordealEpoch)
  private var _pipelineSnapTime: Long = _
  private var _databaseName: String = _
  private var _databaseLocation: String = _
  private var _workspaceUrl: String = _
  private var _token: Array[Byte] = _
  private var _tokenType: String = _
  private var _eventLogPrefix: Option[String] = None
  private var _auditLogPath: Option[String] = None
  private var _badRecordsPath: String = _
  private var _passthroughLogPath: Option[String] = None
  private var _inputConfig: OverwatchParams = _
  private var _parsedConfig: ParsedConfig = _
  private var _overwatchScope: Array[OverwatchScope.Value] = OverwatchScope.values.toArray

  final private val cipher = new Cipher
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // TODO - Set Global timezone to UTC
  private def primordealEpoch: Long = {
    LocalDateTime.now(ZoneId.of("Etc/UTC")).minusDays(60).toInstant(ZoneOffset.UTC)
      .truncatedTo(ChronoUnit.DAYS)
      .toEpochMilli
  }

//  private[overwatch] def setFromTime(value: Long): this.type = {
//    _fromTime_OLD = value;
//    this
//  }
//
//  private[overwatch] def setFromTime(value: Date): this.type = {
//    _fromTime_OLD = value.getTime;
//    this
//  }

  // FromTime by ModuleID
  private[overwatch] def setFromTime(value: Map[Int, Long]): this.type = {
    _fromTime = value
    this
  }

  private[overwatch] def setPipelineSnapTime(): this.type = {
    _pipelineSnapTime = LocalDateTime.now(ZoneId.of("Etc/UTC")).toInstant(ZoneOffset.UTC).toEpochMilli
    this
  }

  // Inclusive when used as fromTS logic will be >= NOT >
  private[overwatch] def fromTime(moduleID: Int): TimeTypes = {
    val fromTime = _fromTime.getOrElse(moduleID, primordealEpoch)
    val dt = new Date(fromTime)
    val localDT = LocalDateTime.ofInstant(dt.toInstant, ZoneId.of("Etc/UTC"))
    TimeTypes(
      fromTime,
      from_unixtime(lit(fromTime / 1000)),
      dt,
      localDT,
      localDT.toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toEpochMilli,
      tsFormat.format(new Date(fromTime)),
      dtFormat.format(new Date(fromTime))
    )
  }

  // Exclusive when used as untilTS logic will be < NOT <=
  private[overwatch] def pipelineSnapTime: TimeTypes = {
    val dt = new Date(_pipelineSnapTime)
    val localDT = LocalDateTime.ofInstant(dt.toInstant, ZoneId.of("Etc/UTC"))
    TimeTypes(
      _pipelineSnapTime,
      from_unixtime(lit(_pipelineSnapTime / 1000)),
      dt,
      localDT,
      localDT.toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toEpochMilli,
      tsFormat.format(new Date(_pipelineSnapTime)),
      dtFormat.format(new Date(_pipelineSnapTime))
    )
  }

  private[overwatch] def setOverwatchScope(value: Array[OverwatchScope.Value]): this.type = {
    _overwatchScope = value
    this
  }

  private[overwatch] def overwatchSchemaVersion: String = _overwatchSchemaVersion

  private[overwatch] def isLocalTesting: Boolean = _isLocalTesting

  private[overwatch] def databaseName: String = _databaseName

  private[overwatch] def databaseLocation: String = _databaseLocation

  private[overwatch] def workspaceURL: String = _workspaceUrl

  private[overwatch] def token: String = cipher.decrypt(_token)

  private[overwatch] def encryptedToken: Array[Byte] = _token

  private[overwatch] def auditLogPath: Option[String] = _auditLogPath

  private[overwatch] def eventLogPrefix: Option[String] = _eventLogPrefix

  private[overwatch] def badRecordsPath: String = _badRecordsPath

  private[overwatch] def passthroughLogPath: Option[String] = _passthroughLogPath

  private[overwatch] def inputConfig: OverwatchParams = _inputConfig

  private[overwatch] def runID: String = _runID

  private[overwatch] def overwatchScope: Array[OverwatchScope.Value] = _overwatchScope

  private[overwatch] def cal: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

  private[overwatch] def tsFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private[overwatch] def dtFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def buildLocalOverwatchParams(): this.type = {

    registeredEncryptedToken(None)
    _overwatchScope = Array(OverwatchScope.clusters, OverwatchScope.jobs,
      OverwatchScope.jobRuns, OverwatchScope.clusterEvents)
    _databaseName = "Overwatch"
    _databaseLocation = "/Dev/git/Databricks--Overwatch/spark-warehouse/overwatch.db"
    _auditLogPath = if (System.getenv("AUDIT_LOG_PATH") == "") None
    else Some(System.getenv("AUDIT_LOG_PATH"))
    this

  }

  def setInputConfig(value: OverwatchParams): this.type = {
    _inputConfig = value
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
        _tokenType = "Secret"
      } else {
        if (_isLocalTesting) { // Local testing env vars
          _workspaceUrl = System.getenv("OVERWATCH_ENV")
          _token = cipher.encrypt(System.getenv("OVERWATCH_TOKEN"))
          _tokenType = "Environment"
        } else { // Use default token for job owner
          _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
          _token = cipher.encrypt(dbutils.notebook.getContext().apiToken.get)
          val authMessage = "No secret parameters provided: attempting to continue with job owner's token."
          logger.log(Level.WARN, authMessage)
          println(authMessage)
          _tokenType = "Owner"
        }
      }
      this
    } catch {
      case e: Throwable => logger.log(Level.FATAL, "No valid credentials and/or Databricks URI", e); this
    }
  }

  private[overwatch] def setDatabaseNameandLoc(dbName: String, dbLocation: String): this.type = {
    _databaseLocation = dbLocation
    _databaseName = dbName
    println(s"DEBUG: Database Name and Locations set to ${_databaseName} and ${_databaseLocation}")
    this
  }

  private[overwatch] def setAuditLogPath(value: Option[String]): this.type = {
    _auditLogPath = value
    this
  }

  private[overwatch] def setPassthroughLogPath(value: Option[String]): this.type = {
    _passthroughLogPath = value
    this
  }

  private[overwatch] def setEventLogPrefix(value: Option[String]): this.type = {
    _eventLogPrefix = value
    this
  }

  private[overwatch] def setBadRecordsPath(value: String): this.type = {
    _badRecordsPath = value
    this
  }

  private[overwatch] def parsedConfig: ParsedConfig = {
    ParsedConfig(
      overwatchScope = overwatchScope.map(_.toString),
      tokenUsed = _tokenType,
      targetDatabase = databaseName,
      targetDatabaseLocation = databaseLocation,
      auditLogPath = auditLogPath,
      passthroughLogPath = passthroughLogPath,
      eventLogPefix = eventLogPrefix
    )
  }

}
