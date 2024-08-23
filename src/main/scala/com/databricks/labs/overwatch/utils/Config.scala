package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.PipelineFunctions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import com.databricks.labs.overwatch.utils.Helpers.spark
import java.util.UUID

class Config() {

  private final val _runID = UUID.randomUUID().toString.replace("-", "")
  private final val packageVersion: String = getClass.getPackage.getImplementationVersion
  private val _isLocalTesting: Boolean = System.getenv("OVERWATCH") == "LOCAL"
  private var _megaFactor: Int = spark.conf.getOption("overwatch.mega.factor").getOrElse("1").toInt
  private var _debugFlag: Boolean = false
  private var _overwatchSchemaVersion = "0.700"
  private var _organizationId: String = _
  private var _workspaceName: String = _
  private var _tempWorkingDir: String = _
  private var _isPVC: Boolean = false
  private var _externalizeOptimize: Boolean = false
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
  private var _disabledModules: Array[Int] = Array[Int]()
  private var _initialWorkerCount: Int = _
  private var _intelligentScaling: IntelligentScaling = IntelligentScaling()
  private var _passthroughLogPath: Option[String] = None
  private var _inputConfig: OverwatchParams = _
  private var _overwatchScope: Seq[OverwatchScope.Value] = OverwatchScope.values.toSeq
  private var _initialSparkConf: Map[String, String] = Map()
  private var _contractInteractiveDBUPrice: Double = _
  private var _contractAutomatedDBUPrice: Double = _
  private var _contractSQLComputeDBUPrice: Double = _
  private var _contractJobsLightDBUPrice: Double = _
  private var _isMultiworkspaceDeployment: Boolean = false
  private var _apiUrl: Option[String] = None
  private var _deploymentType: String = _
  private var _etlCatalogName: String = _
  private var _consumerCatalogName: String = _
  private val _systemTableAudit: String = "system.access.audit"
  private var _sqlEndpoint: String = _


  private val logger: Logger = Logger.getLogger(this.getClass)

  assert(megaFactor <= 10 && megaFactor > 0,
    s"""
       |ERROR: overwatch.mega.factor must be between 1 and 10. The current value of $megaFactor is not supported.
       |""".stripMargin)

  /**
   * BEGIN GETTERS
   * The next section is getters that provide access to local configuration variables. Only adding details where
   * the getter may be obscure or more complicated.
   */

  // as of 0711
  def disabledModules: Array[Int] = _disabledModules
  def isMultiworkspaceDeployment: Boolean = _isMultiworkspaceDeployment

  def apiUrl: Option[String] = _apiUrl

  def overwatchSchemaVersion: String = _overwatchSchemaVersion

  def isLocalTesting: Boolean = _isLocalTesting

  def debugFlag: Boolean = _debugFlag

  def organizationId: String = _organizationId

  def workspaceName: String = _workspaceName

  def tempWorkingDir: String = _tempWorkingDir

  def isPVC: Boolean = _isPVC

  def externalizeOptimize: Boolean = _externalizeOptimize

  def cloudProvider: String = _cloudProvider

  def maxDays: Int = _maxDays

  def initialWorkerCount: Int = _initialWorkerCount

  def megaFactor: Int = _megaFactor

  def databaseName: String = _databaseName

  def databaseLocation: String = _databaseLocation

  def etlDataPathPrefix: String = _etlDataPathPrefix

  def consumerDatabaseName: String = _consumerDatabaseName

  def consumerDatabaseLocation: String = _consumerDatabaseLocation

  def workspaceURL: String = _workspaceUrl

  def apiEnv: ApiEnv = _apiEnv

  def auditLogConfig: AuditLogConfig = _auditLogConfig

  def badRecordsPath: String = _badRecordsPath

  def passthroughLogPath: Option[String] = _passthroughLogPath

  def inputConfig: OverwatchParams = _inputConfig

  def runID: String = _runID

  def contractInteractiveDBUPrice: Double = _contractInteractiveDBUPrice

  def contractAutomatedDBUPrice: Double = _contractAutomatedDBUPrice

  def contractSQLComputeDBUPrice: Double = _contractSQLComputeDBUPrice

  def contractJobsLightDBUPrice: Double = _contractJobsLightDBUPrice

  def primordialDateString: Option[String] = _primordialDateString

  def intelligentScaling: IntelligentScaling = _intelligentScaling

  def globalFilters: Seq[Column] = {
    Seq(
      col("organization_id") === organizationId
    )
  }

  def deploymentType: String = _deploymentType

//  def catalogName: String = _catalogName
  def etlCatalogName: String = _etlCatalogName

  def consumerCatalogName: String = _consumerCatalogName

  def systemTableAudit: String = _systemTableAudit

  def sqlEndpoint: String = _sqlEndpoint

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
    Seq(audit, notebooks, accounts, pools, clusters, clusterEvents, sparkEvents, jobs,
      dbsql, warehouseEvents, notebookCommands)
  }

  def overwatchScope: Seq[OverwatchScope.Value] = _overwatchScope

  /**
   * override spark confs with Overwatch global overrides
   * meant to be used only from Initializer to ensure at the beginning of a PipelineRun all the spark Confs are set
   * correctly
   * @param value
   * @return
   */
  private[overwatch] def registerInitialSparkConf(value: Map[String, String]): this.type = {
    val isMegaOverrides = if (megaFactor > 1) { // disable autoBroadcastJoin during mega loads
      val driverMaxResultSize = spark.conf.getOption("spark.driver.maxResultSize").getOrElse("NOT SET")
      logger.log(Level.INFO, s"Mega Factor Set: $megaFactor")
      println(s"MEGA RUN DETECTED! Be sure to use a spark.driver.maxResultSize >= 32g to handle the task metadata " +
        s"and a driver with at least 64g of memory.\nspark.driver.maxResultSize currently set to: $driverMaxResultSize")
      Map(
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1"
      )
    } else Map[String, String]()

    val sparkOverrides = SparkSessionWrapper.globalSparkConfOverrides ++ isMegaOverrides
    logger.log(Level.INFO, s"Config Initialized with Spark Overrides of:\n" +
      s"$sparkOverrides")
    _initialSparkConf = value ++ sparkOverrides
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
      passthroughLogPath = passthroughLogPath,
      packageVersion = packageVersion
    )
  }

  /**
   * BEGIN SETTERS
   */

  // as of 0711
  private[overwatch] def registerDisabledModules(value: String): this.type = {
    val disabledModulesArray = value.replaceAll("\\s", "").split(",").map(_.toInt)
    _disabledModules = disabledModulesArray
    logger.log(Level.INFO, s"DISABLING MODULES: ${disabledModulesArray.mkString(", ")}")
    this
  }
  private[overwatch] def setMaxDays(value: Int): this.type = {
    _maxDays = value
    this
  }

  private[overwatch] def setIsMultiworkspaceDeployment(value: Boolean): this.type = {
    _isMultiworkspaceDeployment = value
    this
  }

  private[overwatch] def setApiUrl(value: Option[String]): this.type = {
    _apiUrl = value
    this
  }

  private[overwatch] def setInitialWorkerCount(value: Int): this.type = {
    _initialWorkerCount = value
    this
  }

  private[overwatch] def setCloudProvider(value: String): this.type = {
    _cloudProvider = value
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
   *
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

  private[overwatch] def setOverwatchSchemaVersion(value: String): this.type = {
    _overwatchSchemaVersion = value
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

  private[overwatch] def setContractSQLComputeDBUPrice(value: Double): this.type = {
    _contractSQLComputeDBUPrice = value
    this
  }

  private[overwatch] def setContractJobsLightDBUPrice(value: Double): this.type = {
    _contractJobsLightDBUPrice = value
    this
  }

  private[overwatch] def setIntelligentScaling(value: IntelligentScaling): this.type = {
    _intelligentScaling = value
    this
  }

  private[overwatch] def setOrganizationId(value: String): this.type = {
    val msg = s"organization ID set to $value"
    logger.log(Level.INFO, msg)
    if (debugFlag) println(msg)
    _organizationId = value
    this
  }

  private[overwatch] def setWorkspaceName(value: String): this.type = {
    val msg = s"workspaceName set to $value"
    logger.log(Level.INFO, msg)
    if (debugFlag) println(msg)
    _workspaceName = value
    this
  }

  private[overwatch] def setTempWorkingDir(value: String): this.type = {
    _tempWorkingDir = value
    this
  }

  private[overwatch] def setIsPVC(value: Boolean): this.type = {
    _isPVC = value
    this
  }

  private[overwatch] def setExternalizeOptimize(value: Boolean): this.type = {
    val msg = s"Pipeline Optimize DISABLED: You have elected to complete the optimizations externally. It's " +
      s"imperative that you schedule a periodic optimization job using the Optimize main class. See documentation " +
      s"for further instructions. If you do not complete optimizations outside or insdie the pipeline, the " +
      s"performance will degrade."
    if (value) logger.log(Level.INFO, msg)
    if (debugFlag && value) println(msg)
    _externalizeOptimize = value
    this
  }

   def setApiEnv(value: ApiEnv): this.type = {
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

  private[overwatch] def setWorkspaceURL(value: String): this.type = {
    _workspaceUrl = value
    this
  }

  private[overwatch] def setTokenType(value: String): this.type = {
    _tokenType = value
    this
  }

  private[overwatch] def overrideMegaFactor(value: Int): this.type = {
    _megaFactor = value
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
  private[overwatch] def registerWorkspaceMeta(tokenSecret: Option[TokenSecret],apiEnvConfig: Option[ApiEnvConfig]): this.type = {
    var rawToken = ""
    var scope = ""
    var key = ""
    try {
      // Token secrets not supported in local testing
      if (tokenSecret.nonEmpty && !_isLocalTesting) { // not local testing and secret passed
        if (isMultiworkspaceDeployment && apiUrl.nonEmpty) {
          _workspaceUrl = apiUrl.get
          logger.log(Level.INFO, "Multiworkspace Deployment setting the workspaceURL :" + _workspaceUrl)
        } else {
          _workspaceUrl = dbutils.notebook.getContext().apiUrl.get
        }
        _cloudProvider = if (_workspaceUrl.toLowerCase().contains("azure")) "azure" else "aws"
        scope = tokenSecret.get.scope
        key = tokenSecret.get.key
        rawToken = dbutils.secrets.get(scope, key)
        val authMessage = s"Valid Secret Identified: Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMessage)
        _tokenType = "Secret"
      } else {
        if (_isLocalTesting) { // Local testing env vars
          _workspaceUrl = System.getenv("OVERWATCH_ENV")
          _cloudProvider = if (_workspaceUrl.toLowerCase().contains("azure")) "azure" else "aws"
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
      if (!rawToken.matches("^(dapi|dkea)[a-zA-Z0-9-]*$")) throw new BadConfigException(s"contents of secret " +
        s"at scope:key $scope:$key is not in a valid format. Please validate the contents of your secret. It must be " +
        s"a user access token. It should start with 'dapi' ")
      val derivedApiEnvConfig = apiEnvConfig.getOrElse(ApiEnvConfig())
      val derivedApiProxy = derivedApiEnvConfig.apiProxyConfig.getOrElse(ApiProxyConfig())
      setApiEnv(ApiEnv(isLocalTesting, workspaceURL, rawToken, packageVersion, derivedApiEnvConfig.successBatchSize,
        derivedApiEnvConfig.errorBatchSize, runID, derivedApiEnvConfig.enableUnsafeSSL, derivedApiEnvConfig.threadPoolSize,
        derivedApiEnvConfig.apiWaitingTime, derivedApiProxy.proxyHost, derivedApiProxy.proxyPort,
        derivedApiProxy.proxyUserName, derivedApiProxy.proxyPasswordScope, derivedApiProxy.proxyPasswordKey ,
        derivedApiEnvConfig.mountMappingPath
      ))

      this
    } catch {
      case e: IllegalArgumentException if e.getMessage.toLowerCase.contains("secret does not exist with scope") =>
        throw new BadConfigException(e.getMessage, failPipeline = true)
      case e: Throwable =>
        logger.log(Level.FATAL, "No valid credentials and/or Databricks URI", e)
        throw new BadConfigException(e.getMessage, failPipeline = true)
        this
    }
  }

  /**
   * Function derives the cloud from workspaceUrl.
   * @return
   */
  private def deriveCloudProvider(): String = {
    if(isMultiworkspaceDeployment) {
      if (isPVC) {
        "aws"
      } else {
        workspaceURL.toLowerCase match {
          case cloudType if cloudType.contains("azure") => "azure"
          case cloudType if cloudType.contains("gcp") => "gcp"
          case _ => "aws"
        }
      }
    } else spark.conf.get("spark.databricks.cloudProvider").toLowerCase
  }

  private[overwatch] def buildApiEnv(tokenSecret: Option[TokenSecret], apiEnvConfig: Option[ApiEnvConfig]): ApiEnv = {
    var rawToken = ""
    var scope = ""
    var key = ""
    if (isMultiworkspaceDeployment && apiUrl.nonEmpty) {
      setWorkspaceURL(apiUrl.get)
      logger.log(Level.INFO, "Multiworkspace Deployment setting the workspaceURL :" + _workspaceUrl)
    } else {
      setWorkspaceURL(dbutils.notebook.getContext().apiUrl.get)
    }
    setCloudProvider(deriveCloudProvider())
    try {
      // Token secrets not supported in local testing
      if (tokenSecret.nonEmpty) { // not local testing and secret passed
        scope = tokenSecret.get.scope
        key = tokenSecret.get.key
        rawToken = dbutils.secrets.get(scope, key)
        val authMessage = s"Valid Secret Identified: Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMessage)
        setTokenType("Secret")
      } else { // Use default token for job owner
        rawToken = dbutils.notebook.getContext().apiToken.get
        val authMessage = "No secret parameters provided: attempting to continue with job owner's token."
        logger.log(Level.WARN, authMessage)
        println(authMessage)
        setTokenType("Owner")
      }
      if (!rawToken.matches("^(dapi|dkea)[a-zA-Z0-9-]*$")) throw new BadConfigException(s"contents of secret " +
        s"at scope:key $scope:$key is not in a valid format. Please validate the contents of your secret. It must be " +
        s"a user access token. It should start with 'dapi' ")
      val derivedApiEnvConfig = apiEnvConfig.getOrElse(ApiEnvConfig())
      val derivedApiProxy = derivedApiEnvConfig.apiProxyConfig.getOrElse(ApiProxyConfig())
      ApiEnv(
        isLocalTesting,
        workspaceURL,
        rawToken,
        packageVersion,
        derivedApiEnvConfig.successBatchSize,
        derivedApiEnvConfig.errorBatchSize,
        runID,
        derivedApiEnvConfig.enableUnsafeSSL,
        derivedApiEnvConfig.threadPoolSize,
        derivedApiEnvConfig.apiWaitingTime,
        derivedApiProxy.proxyHost,
        derivedApiProxy.proxyPort,
        derivedApiProxy.proxyUserName,
        derivedApiProxy.proxyPasswordScope,
        derivedApiProxy.proxyPasswordKey,
        derivedApiEnvConfig.mountMappingPath
      )
    } catch {
      case e: IllegalArgumentException if e.getMessage.toLowerCase.contains("secret does not exist with scope") =>
        throw new BadConfigException(e.getMessage, failPipeline = true)
      case e: Throwable =>
        logger.log(Level.FATAL, "No valid credentials and/or Databricks URI", e)
        throw new BadConfigException(e.getMessage, failPipeline = true)
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

  private[overwatch] def setDeploymentType(value: String): this.type = {
    _deploymentType = value
    this
  }

  private[overwatch] def setCatalogName(etlCatalogName: String, consumerCatalogName: String): this.type = {
    _etlCatalogName = etlCatalogName
    _consumerCatalogName = consumerCatalogName
    this
  }

}
