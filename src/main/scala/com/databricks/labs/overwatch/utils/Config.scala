package com.databricks.labs.overwatch.utils

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, LocalTime, ZoneId, ZoneOffset}
import java.util.{Calendar, Date, TimeZone, UUID}

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{from_unixtime, lit}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.log4j.{Level, Logger}

class Config() {

  private final val _overwatchSchemaVersion = "0.1"
  private final val _runID = UUID.randomUUID().toString.replace("-","")
  private final val cipherKey = UUID.randomUUID().toString
  private val _isLocalTesting: Boolean = System.getenv("OVERWATCH") == "LOCAL"
  private val _isDBConnect: Boolean = System.getenv("DBCONNECT") == "TRUE"
  private var _isFirstRun: Boolean = false
  private var _debugFlag: Boolean = false
  private var _lastRunDetail: Array[ModuleStatusReport] = Array[ModuleStatusReport]()
  private var _pipelineSnapTime: Long = _
  private var _databaseName: String = _
  private var _databaseLocation: String = _
  private var _workspaceUrl: String = _
  private var _cloudProvider: String = _
  private var _token: Array[Byte] = _
  private var _tokenType: String = _
  private var _apiEnv: ApiEnv = _
  private var _auditLogPath: Option[String] = None
  private var _badRecordsPath: String = _
  private var _passthroughLogPath: Option[String] = None
  private var _inputConfig: OverwatchParams = _
  private var _parsedConfig: ParsedConfig = _
  private var _overwatchScope: Seq[OverwatchScope.Value] = OverwatchScope.values.toSeq
  private var _initialSparkConf: Map[String, String] = Map()
  private var _intialShuffleParts: Int = 200

  final private val cipher = new Cipher(cipherKey)
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // TODO - Set Global timezone to UTC
  private def primordealEpoch: Long = {
    LocalDateTime.now(ZoneId.of("Etc/UTC")).minusDays(60).toInstant(ZoneOffset.UTC)
      .truncatedTo(ChronoUnit.DAYS)
      .toEpochMilli
  }

  private[overwatch] def setPipelineSnapTime(): this.type = {
    _pipelineSnapTime = LocalDateTime.now(ZoneId.of("Etc/UTC")).toInstant(ZoneOffset.UTC).toEpochMilli
    this
  }

  // TODO -- This is for local testing only
  def setPipelineSnapTime(tsMilli: Long): this.type = {
//    _pipelineSnapTime = LocalDateTime.now(ZoneId.of("Etc/UTC")).toInstant(ZoneOffset.UTC).toEpochMilli
    _pipelineSnapTime = tsMilli
    this
  }

  private[overwatch] def setInitialShuffleParts(value: Int): this.type = {
    _intialShuffleParts = value
    this
  }

  @throws(classOf[NoSuchElementException])
  def fromTime(moduleID: Int): TimeTypes = {
    val lastRunStatus = if (!isFirstRun) lastRunDetail.filter(_.moduleID == moduleID) else lastRunDetail
    require(lastRunStatus.length <= 1, "More than one start time identified from pipeline_report.")
    val fromTime = if (lastRunStatus.length != 1) primordealEpoch else lastRunStatus.head.untilTS
    createTimeDetail(fromTime)
  }

  // Exclusive when used as untilTS logic will be < NOT <=
  def pipelineSnapTime: TimeTypes = {
    createTimeDetail(_pipelineSnapTime)
  }

//  case class TimeTypes(asUnixTimeMilli: Long, asUnixTimeS: Long, asColumnTS: Column, asJavaDate: Date,
//                       asUTCDateTime: LocalDateTime, asMidnightEpochMilli: Long,
//                       asTSString: String, asDTString: String)
  private[overwatch] def createTimeDetail(tsMilli: Long): TimeTypes = {
    val dt = new Date(tsMilli)
    val localDT = LocalDateTime.ofInstant(dt.toInstant, ZoneId.of("Etc/UTC"))
    TimeTypes(
      tsMilli, // asUnixTimeMilli
      tsMilli / 1000, // asUnixTimeS -- NOTE: THIS IS LOSSY
      from_unixtime(lit(tsMilli).cast("double") / 1000).cast("timestamp"), // asColumnTS
      dt, // asJavaDate
      localDT, // asUTCDateTime
      localDT.toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toEpochMilli, // asMidnightEpochMilli
      tsFormat.format(new Date(tsMilli)), // asTSString
      dtFormat.format(new Date(tsMilli)) // asDTString
    )
  }

  private[overwatch] def setOverwatchScope(value: Seq[OverwatchScope.Value]): this.type = {
    _overwatchScope = value
    this
  }

  private[overwatch] def setDebugFlag(value: Boolean): this.type = {
    _debugFlag = value
    this
  }

  private[overwatch] def setIsFirstRun(value: Boolean): this.type = {
    _isFirstRun = value
    if (_isFirstRun) {
      println(s"WARNING! First runs often require a large cluster for some modules and long API runs for others. " +
        s"Below is the suggested method for running the first historical load. \n \nFirst Run --> With a sufficiently " +
        s"large cluster load modules: audit, clusters, jobs, notebooks, pools, sparkEvents.\n\nFollowup Run --> " +
        s"With a small cluster add the additional desired modules. For example, given the run above as the first run " +
        s"the following would be a suggested second run (and future) module config:\n" +
        s"audit, clusters, jobs, notebooks, pools, sparkEvents, clusterEvents, jobRuns")
      if (overwatchScope.contains(OverwatchScope.jobRuns)) {
        println(s"WARNING! It's not recommended to combine API modules and audit/events modules on the first run.")
      }
    }
    this
  }

  private[overwatch] def setLastRunDetail(value: Array[ModuleStatusReport]): this.type = {
    // Todo -- Add assertion --> number of rows <= number of modules or something to that effect
    _lastRunDetail = value
    this
  }

  private def setApiEnv(value: ApiEnv): this.type = {
    _apiEnv = value
    this
  }

  private[overwatch] def overwatchSchemaVersion: String = _overwatchSchemaVersion

  private[overwatch] def lastRunDetail: Array[ModuleStatusReport] = _lastRunDetail

  private[overwatch] def isLocalTesting: Boolean = _isLocalTesting

  private[overwatch] def isDBConnect: Boolean = _isDBConnect

  private[overwatch] def isFirstRun: Boolean = _isFirstRun

  private[overwatch] def debugFlag: Boolean = _debugFlag

  private[overwatch] def cloudProvider: String = _cloudProvider

  private[overwatch] def initialShuffleParts: Int = _intialShuffleParts

  private[overwatch] def databaseName: String = _databaseName

  private[overwatch] def databaseLocation: String = _databaseLocation

  private[overwatch] def workspaceURL: String = _workspaceUrl

  private[overwatch] def token: String = cipher.decrypt(_token)

  private[overwatch] def apiEnv: ApiEnv = _apiEnv

  private[overwatch] def encryptedToken: Array[Byte] = _token

  private[overwatch] def auditLogPath: Option[String] = _auditLogPath

  private[overwatch] def badRecordsPath: String = _badRecordsPath

  private[overwatch] def passthroughLogPath: Option[String] = _passthroughLogPath

  private[overwatch] def inputConfig: OverwatchParams = _inputConfig

  private[overwatch] def runID: String = _runID

  private[overwatch] def overwatchScope: Seq[OverwatchScope.Value] = _overwatchScope

  private[overwatch] def cal: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

  private[overwatch] def tsFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private[overwatch] def dtFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  private[overwatch] def orderedOverwatchScope: Seq[OverwatchScope.Value] = {
    import OverwatchScope._
//    jobs, jobRuns, clusters, clusterEvents, sparkEvents, pools, audit, iamPassthrough, profiles
    Seq(audit, jobs, jobRuns, clusters, clusterEvents, sparkEvents, notebooks)
  }

  private[overwatch] def registerInitialSparkConf(value: Map[String, String]): this.type = {
    _initialSparkConf = value
    this
  }

  private[overwatch] def initialSparkConf(): Map[String, String] = {
    _initialSparkConf
  }

  // Set scope for local testing
  def buildLocalOverwatchParams(): this.type = {

    registeredEncryptedToken(None)
    _overwatchScope = Array(OverwatchScope.audit, OverwatchScope.clusters, OverwatchScope.clusterEvents,
      OverwatchScope.jobs, OverwatchScope.jobRuns)
    _databaseName = "overwatch_local"
    _badRecordsPath = "/tmp/tomes/overwatch/sparkEventsBadrecords"
//    _databaseLocation = "/Dev/git/Databricks--Overwatch/spark-warehouse/overwatch.db"
    _auditLogPath = Some("/mnt/tomesdata/logs/field_training_audit/")
    _cloudProvider = "aws"
    this

  }

  def setInputConfig(value: OverwatchParams): this.type = {
    _inputConfig = value
    this
  }

  // TODO - figure out why the decryption is failing and properly encrypt
  private[overwatch] def registeredEncryptedToken(tokenSecret: Option[TokenSecret]): this.type = {
    var rawToken = ""
    try {
      // Token secrets not supported in local testing
      if (tokenSecret.nonEmpty && !_isLocalTesting) { // not local testing and secret passed
        _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
        _cloudProvider = if (_workspaceUrl.toLowerCase().contains("azure")) "azure" else "aws"
        val scope = tokenSecret.get.scope
        val key = tokenSecret.get.key
        rawToken = dbutils.secrets.get(scope, key)
        _token = cipher.encrypt(dbutils.secrets.get(scope, key))
        val authMessage = s"Valid Secret Identified: Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMessage)
        _tokenType = "Secret"
      } else {
        if (_isLocalTesting) { // Local testing env vars
          _workspaceUrl = System.getenv("OVERWATCH_ENV")
          rawToken = System.getenv("OVERWATCH_TOKEN")
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
      setApiEnv(ApiEnv(isLocalTesting, workspaceURL, rawToken, _token, cipher))
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
      passthroughLogPath = passthroughLogPath
    )
  }

}
