package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.PipelineFunctions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import java.util.UUID

class Config() {

  private final val _overwatchSchemaVersion = "0.1"
  private final val _runID = UUID.randomUUID().toString.replace("-", "")
  private final val packageVersion: String = getClass.getPackage.getImplementationVersion
  private val _isLocalTesting: Boolean = System.getenv("OVERWATCH") == "LOCAL"
  private val _isDBConnect: Boolean = System.getenv("DBCONNECT") == "TRUE"
  private var _isFirstRun: Boolean = false
  private var _debugFlag: Boolean = false
  private var _organizationId: String = _
  private var _databaseName: String = _
  private var _databaseLocation: String = _
  private var _etlDataPathPrefix: String = _
  private var _consumerDatabaseName: String = _
  private var _consumerDatabaseLocation: String = _
  private var _workspaceUrl: String = _
  private var _cloudProvider: String = _
  private var _tokenType: String = _
  private var _apiEnv: ApiEnv = _
  private var _auditLogConfig: AuditLogConfig = _
  private var _badRecordsPath: String = _
  private var _primordialDateString: Option[String] = None
  private var _maxDays: Int = 60
  private var _passthroughLogPath: Option[String] = None
  private var _inputConfig: OverwatchParams = _
  private var _overwatchScope: Seq[OverwatchScope.Value] = OverwatchScope.values.toSeq
  private var _initialSparkConf: Map[String, String] = Map()
  private var _intialShuffleParts: Int = 200
  private var _contractInteractiveDBUPrice: Double = 0.56
  private var _contractAutomatedDBUPrice: Double = 0.26

  private val logger: Logger = Logger.getLogger(this.getClass)
  /**
   * BEGIN GETTERS
   * The next section is getters that provide access to local configuration variables. Only adding details where
   * the getter may be obscure or more complicated.
   */

  private[overwatch] def overwatchSchemaVersion: String = _overwatchSchemaVersion

  private[overwatch] def isLocalTesting: Boolean = _isLocalTesting

  private[overwatch] def isDBConnect: Boolean = _isDBConnect

  private[overwatch] def isFirstRun: Boolean = _isFirstRun

  private[overwatch] def debugFlag: Boolean = _debugFlag

  private[overwatch] def organizationId: String = _organizationId

  private[overwatch] def cloudProvider: String = _cloudProvider

  private[overwatch] def initialShuffleParts: Int = _intialShuffleParts

  private[overwatch] def maxDays: Int = _maxDays

  private[overwatch] def databaseName: String = _databaseName

  private[overwatch] def databaseLocation: String = _databaseLocation

  private[overwatch] def etlDataPathPrefix: String = _etlDataPathPrefix

  private[overwatch] def consumerDatabaseName: String = _consumerDatabaseName

  private[overwatch] def consumerDatabaseLocation: String = _consumerDatabaseLocation

  private[overwatch] def workspaceURL: String = _workspaceUrl

  private[overwatch] def apiEnv: ApiEnv = _apiEnv

  private[overwatch] def auditLogConfig: AuditLogConfig = _auditLogConfig

  private[overwatch] def badRecordsPath: String = _badRecordsPath

  private[overwatch] def passthroughLogPath: Option[String] = _passthroughLogPath

  private[overwatch] def inputConfig: OverwatchParams = _inputConfig

  private[overwatch] def runID: String = _runID

  private[overwatch] def contractInteractiveDBUPrice: Double = _contractInteractiveDBUPrice

  private[overwatch] def contractAutomatedDBUPrice: Double = _contractAutomatedDBUPrice

  private[overwatch] def primordialDateString: Option[String] = _primordialDateString

  private[overwatch] def globalFilters: Seq[Column] = {
    Seq(
      col("organization_id") === organizationId
    )
  }

  /**
   * OverwatchScope defines the modules active for the current run
   * Some have been disabled for the moment in a sprint to v1.0 release but this acts as the
   * cononical module inventory
   *
   * @return
   */
  private[overwatch] def orderedOverwatchScope: Seq[OverwatchScope.Value] = {
    import OverwatchScope._
    //    jobs, clusters, clusterEvents, sparkEvents, pools, audit, passthrough, profiles
    Seq(audit, notebooks, accounts, pools, clusters, clusterEvents, sparkEvents, jobs)
  }

  private[overwatch] def overwatchScope: Seq[OverwatchScope.Value] = _overwatchScope

  private[overwatch] def registerInitialSparkConf(value: Map[String, String]): this.type = {
    val manualOverrides = Map(
      "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" ->
        value.getOrElse("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "false"),
      "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" ->
        value.getOrElse("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false"),
      "spark.databricks.delta.optimize.maxFileSize" ->
        value.getOrElse("spark.databricks.delta.optimize.maxFileSize", (1024 * 1024 * 128).toString),
      "spark.databricks.delta.retentionDurationCheck.enabled" ->
        value.getOrElse("spark.databricks.delta.retentionDurationCheck.enabled", "true"),
      "spark.databricks.delta.optimizeWrite.numShuffleBlocks" ->
        value.getOrElse("spark.databricks.delta.optimizeWrite.numShuffleBlocks", "50000"),
      "spark.databricks.delta.optimizeWrite.binSize" ->
        value.getOrElse("spark.databricks.delta.optimizeWrite.binSize", "512"),
      "spark.sql.caseSensitive" -> "false"
    )
    _initialSparkConf = value ++ manualOverrides
    this
  }

  private[overwatch] def initialSparkConf: Map[String, String] = {
    _initialSparkConf
  }

  private[overwatch] def parsedConfig: ParsedConfig = {
    ParsedConfig(
      auditLogConfig = auditLogConfig,
      overwatchScope = overwatchScope.map(_.toString),
      tokenUsed = _tokenType,
      targetDatabase = databaseName,
      targetDatabaseLocation = databaseLocation,
      passthroughLogPath = passthroughLogPath
    )
  }

  /**
   * BEGIN SETTERS
   */

  /**
   * Identify the initial value before overwatch for shuffle partitions. This value gets modified a lot through
   * this process but should be set back to the same as the value before Overwatch process when Overwatch finishes
   * its work
   *
   * @param value number of shuffle partitions to be set
   * @return
   */
  private[overwatch] def setInitialShuffleParts(value: Int): this.type = {
    _intialShuffleParts = value
    this
  }

  private[overwatch] def setMaxDays(value: Int): this.type = {
    _maxDays = value
    this
  }

  private[overwatch] def setPrimordialDateString(value: Option[String]): this.type = {
    if (value.nonEmpty) {
      logger.log(Level.INFO, s"CONFIG SET: Primordial Date String = ${value.get}")
    } else {
      logger.log(Level.INFO, "CONFIG NOT SET: Primordial Date String")
    }
    _primordialDateString = value
    this
  }

  /**
   * Set the Overwatch Scope in the correct order as per the ordered Seq. This is important for processing the
   * modules in the correct order inside the pipeline
   * @param value
   * @return
   */
  private[overwatch] def setOverwatchScope(value: Seq[OverwatchScope.Value]): this.type = {
    val orderedScope = orderedOverwatchScope.filter(scope => value.contains(scope))
    _overwatchScope = orderedScope
    this
  }

  def setDebugFlag(value: Boolean): this.type = {
    _debugFlag = value
    this
  }

  private[overwatch] def setIsFirstRun(value: Boolean): this.type = {
    _isFirstRun = value
    logger.log(Level.INFO, s"IS FIRST RUN: ${value.toString}")
    this
  }

  private[overwatch] def setContractInteractiveDBUPrice(value: Double): this.type = {
    _contractInteractiveDBUPrice = value
    this
  }

  private[overwatch] def setContractAutomatedDBUPrice(value: Double): this.type = {
    _contractAutomatedDBUPrice = value
    this
  }

  private[overwatch] def setOrganizationId(value: String): this.type = {
    if (debugFlag) println(s"organization ID set to ${value}")
    _organizationId = value
    this
  }
  private def setApiEnv(value: ApiEnv): this.type = {
    _apiEnv = value
    this
  }

  /**
   * After the input parameters have been serialized into the OverwatchParams object, set it
   *
   * @param value
   * @return
   */

  def setInputConfig(value: OverwatchParams): this.type = {
    _inputConfig = value
    this
  }

  /**
   * This function wraps all requisite parameters required for API calls and sets the complex type: APIEnv.
   * Also sets all required details to enable API calls.
   * An additional review should be completed to determine if all setters can be removed except the APIEnv registration.
   *
   * @param tokenSecret Optional input of the Token Secret. If left null, the token secret will be initialized
   *                    as the job owner or notebook user (if called from notebook)
   * @return
   */
  private[overwatch] def registerWorkspaceMeta(tokenSecret: Option[TokenSecret]): this.type = {
    var rawToken = ""
    try {
      // Token secrets not supported in local testing
      if (tokenSecret.nonEmpty && !_isLocalTesting) { // not local testing and secret passed
        _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
        _cloudProvider = if (_workspaceUrl.toLowerCase().contains("azure")) "azure" else "aws"
        val scope = tokenSecret.get.scope
        val key = tokenSecret.get.key
        rawToken = dbutils.secrets.get(scope, key)
        val authMessage = s"Valid Secret Identified: Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMessage)
        _tokenType = "Secret"
      } else {
        if (_isLocalTesting) { // Local testing env vars
          _workspaceUrl = System.getenv("OVERWATCH_ENV")
          //_cloudProvider setter not necessary -- done in local setup
          rawToken = System.getenv("OVERWATCH_TOKEN")
          _tokenType = "Environment"
        } else { // Use default token for job owner
          _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
          _cloudProvider = if (_workspaceUrl.toLowerCase().contains("azure")) "azure" else "aws"
          rawToken = dbutils.notebook.getContext().apiToken.get
          val authMessage = "No secret parameters provided: attempting to continue with job owner's token."
          logger.log(Level.WARN, authMessage)
          println(authMessage)
          _tokenType = "Owner"
        }
      }
      setApiEnv(ApiEnv(isLocalTesting, workspaceURL, rawToken, packageVersion))
      this
    } catch {
      case e: Throwable => logger.log(Level.FATAL, "No valid credentials and/or Databricks URI", e); this
    }
  }

  /**
   * Set Overwatch DB and location
   *
   * @param dbName
   * @param dbLocation
   * @return
   */
  private[overwatch] def setDatabaseNameAndLoc(dbName: String, dbLocation: String, dataLocation: String): this.type = {
    val cleanDBLocation = PipelineFunctions.cleansePathURI(dbLocation)
    val cleanETLDataLocation = PipelineFunctions.cleansePathURI(dataLocation)
    _databaseLocation = cleanDBLocation
    _databaseName = dbName
    _etlDataPathPrefix = dataLocation
    println(s"DEBUG: Database Name and Location set to ${_databaseName} and ${_databaseLocation}.\n\n " +
      s"DATA Prefix set to: $dataLocation")
    if (dbLocation.contains("/user/hive/warehouse/")) println("\n\nWARNING!! You have chosen a database location in " +
      "/user/hive/warehouse prefix. While the tables are created as external tables this still presents a risk that " +
      "'drop database cascade' command will permanently delete all data for all Overwatch workspaces." +
      "It's strongly recommended to specify a database outside of the /user/hive/warehouse prefix to prevent this.")
    if (cleanDBLocation.toLowerCase == cleanETLDataLocation.toLowerCase) println("\n\nWARNING!! The ETL Database " +
      "AND ETL Data Prefix locations " +
      "are equal. If ETL database is accidentally dropped ALL data from all workspaces will be lost in spite of the " +
      "tables being external. Specify a separate prefix for the ETL data to avoid this from happening.")
    this
  }

  private[overwatch] def setConsumerDatabaseNameandLoc(consumerDBName: String, consumerDBLocation: String): this.type = {
    val cleanConsumerDBLocation = PipelineFunctions.cleansePathURI(consumerDBLocation)
    _consumerDatabaseLocation = cleanConsumerDBLocation
    _consumerDatabaseName = consumerDBName
    println(s"DEBUG: Consumer Database Name and Location set to ${_consumerDatabaseName} and ${_consumerDatabaseLocation}")
    this
  }

  /**
   * Sets audit log config
   *
   * @param value
   * @return
   */
  private[overwatch] def setAuditLogConfig(value: AuditLogConfig): this.type = {
    _auditLogConfig = value
    this
  }

  /**
   * Sets Passthrough log path
   * Passthrough logs will be enabled post v1.0.
   *
   * @param value
   * @return
   */
  private[overwatch] def setPassthroughLogPath(value: Option[String]): this.type = {
    _passthroughLogPath = value
    this
  }

  /**
   * Some input files have corrupted records, this sets the quarantine path for all such scenarios
   *
   * @param value
   * @return
   */
  private[overwatch] def setBadRecordsPath(value: String): this.type = {
    _badRecordsPath = value
    this
  }

  /**
   * Manual setters for DB Remote and Local Testing. This is not used if "isLocalTesting" == false
   * This function allows for hard coded parameters for rapid integration testing and prototyping
   *
   * @return
   */
  def buildLocalOverwatchParams(): String = {

    registerWorkspaceMeta(None)
    _overwatchScope = Array(OverwatchScope.audit, OverwatchScope.clusters)
    _databaseName = "overwatch_local"
    _badRecordsPath = "/tmp/tomes/overwatch/sparkEventsBadrecords"
    //    _databaseLocation = "/Dev/git/Databricks--Overwatch/spark-warehouse/overwatch.db"

    // AWS TEST
    _cloudProvider = "azure"
    """
      |{String for testing. Run string without escape chars. Do not commit secrets to git}
      |""".stripMargin

  }
}
