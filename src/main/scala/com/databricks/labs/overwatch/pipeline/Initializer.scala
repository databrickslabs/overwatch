package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ParamDeserializer
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}

class Initializer(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  extends InitializerFunctions(config, disableValidations, isSnap, initDB)
    with SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Convert the args brought in as JSON string into the paramters object "OverwatchParams".
   * Validate the config and the environment readiness for the run based on the configs and environment state
   *
   * @param overwatchArgs JSON string of input args from input into main class.
   * @return
   */
  private def validateAndRegisterArgs(overwatchArgs: String): this.type = {

    /**
     * Register custom deserializer to create OverwatchParams object
     */
    val paramModule: SimpleModule = new SimpleModule()
      .addDeserializer(classOf[OverwatchParams], new ParamDeserializer)
    val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .registerModule(paramModule)
      .asInstanceOf[ObjectMapper with ScalaObjectMapper]

    /**
     * if isLocalTesting -- Allow for local testing
     * Either all parameters can be hard coded in the config object or a mach args json string can be returned from
     * the config object. Returning the config args is a more complete method for integration testing just be sure
     * to hard code the configuration json string WITHOUT escaped parenthesis even though escaped are necessary
     * when coming from Datbaricks jobs ui since it has to be escaped to go through the api calls.
     *
     * Otherwise -- read from the args passed in and serialize into OverwatchParams
     */
    logger.log(Level.INFO, "Validating Input Parameters")
    val rawParams = mapper.readValue[OverwatchParams](overwatchArgs)


    // Now that the input parameters have been parsed -- set them in the config
    config.setInputConfig(rawParams)

    // Add the workspace name (friendly) if provided by the customer
    val overwatchFriendlyName = rawParams.workspace_name.getOrElse(config.organizationId)
    config.setWorkspaceName(overwatchFriendlyName)

    /**
     * PVC: HARD OVERRIDE FOR PVC
     * Each time PVC deploys a newer version the derived organization_id changes due to the load balancer id
     * and/or URL to access the workspace changes. There are no identifiable, equal values that can be found
     * between one deployment and the next. To resolve this, PVC customers will be REQUIRED to provide a canonical
     * name for each workspace (i.e. workspaceName) to provide consistency across deployments.
     */

    if (Init.isPVC && !config.isMultiworkspaceDeployment) Init.pvcOverrideOrganizationId()

    // Set external optimize if customer specified otherwise use default
    config.setExternalizeOptimize(rawParams.externalizeOptimize)

    /** Retrieve scope from user inputs, validate it, and add it to the config */
    val overwatchScope = rawParams.overwatchScope.getOrElse(Seq("all"))

    if (overwatchScope.head == "all") {
      config.setOverwatchScope(config.orderedOverwatchScope)
    } else {
      config.setOverwatchScope(Init.validateScope(overwatchScope))
    }

    /** Retrieve raw token secret, validate it and add it to the config if valid */
    val tokenSecret = rawParams.tokenSecret
    val validatedTokenSecret = Init.validateTokenSecret(tokenSecret)

    /** Build and validate workspace meta including API ENV */
    val rawApiEnv = config.buildApiEnv(validatedTokenSecret,rawParams.apiEnvConfig)
    val validatedApiEnv = Init.validateApiEnv(rawApiEnv)
    config.setApiEnv(validatedApiEnv)

    /** Validate and set the data target details */
    val rawDataTarget = rawParams.dataTarget.getOrElse(
      DataTarget(Some("overwatch"), Some("dbfs:/user/hive/warehouse/overwatch.db"), None)
    )
    Init.validateAndSetDataTarget(rawDataTarget)


    /** Set Databricks Contract Prices from Config */
    config.setContractInteractiveDBUPrice(rawParams.databricksContractPrices.interactiveDBUCostUSD)
    config.setContractAutomatedDBUPrice(rawParams.databricksContractPrices.automatedDBUCostUSD)
    config.setContractSQLComputeDBUPrice(rawParams.databricksContractPrices.sqlComputeDBUCostUSD)
    config.setContractJobsLightDBUPrice(rawParams.databricksContractPrices.jobsLightDBUCostUSD)

    // Set Primordial Date
    config.setPrimordialDateString(rawParams.primordialDateString)

    // Audit logs are required and paramount to Overwatch delivery -- they must be present and valid
    /** Validate and set Audit Log Configs */
    val rawAuditLogConfig = rawParams.auditLogConfig
    val validatedAuditLogConfig = Init.validateAuditLogConfigs(rawAuditLogConfig)
    config.setAuditLogConfig(validatedAuditLogConfig)

    // must happen AFTER data target validation
    // persistent location for corrupted spark event log files
    /** Validate and set Bad Records Path */
    val rawBadRecordsPath = rawParams.badRecordsPath
    val badRecordsPath = rawBadRecordsPath.getOrElse(
      s"${config.etlDataPathPrefix}/spark_events_bad_records_files/${config.organizationId}"
    )
    config.setBadRecordsPath(badRecordsPath)

    // must happen AFTER data target is set validation since the etlDataPrefix must already be set in the config
    /** Validate and set Temp Working Dir */
    val rawTempWorkingDir = rawParams.tempWorkingDir
    val validatedTempWorkingDir = Init.prepTempWorkingDir(rawTempWorkingDir)
    config.setTempWorkingDir(validatedTempWorkingDir)

    /** Set Max Days */
    if (rawParams.maxDaysToLoad <= 0) {
      throw new BadConfigException(s"maxDaysToLoad should be greater than 0.")
    }else {
      config.setMaxDays(rawParams.maxDaysToLoad)
    }

    /** Validate and set Intelligent Scaling Params */
    val rawISConfig = rawParams.intelligentScaling
    val validatedISConfig = Init.validateIntelligentScaling(rawISConfig)
    config.setIntelligentScaling(validatedISConfig)

    // as of 0711
    val disabledModulesString = spark(globalSession = true).conf.getOption("overwatch.modules.disabled").getOrElse("0")
    config.registerDisabledModules(disabledModulesString)

    this
  }

  private def initializeDatabase(): Database = {
    if (initDB) {
      Init.initializeDatabase()
    } else {
      Database(config)
    }
  }

}

object Initializer extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  // Init the SparkSessionWrapper with envVars
  envInit()

  def getOrgId: String = {
    val clusterOwnerOrgID = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    if (clusterOwnerOrgID == " " || clusterOwnerOrgID == "0") {
      dbutils.notebook.getContext.apiUrl.get.split("\\.")(0).split("/").last
    } else clusterOwnerOrgID
  }

  private def initConfigState(debugFlag: Boolean,organizationID: Option[String],apiUrl: Option[String]): Config = {
    logger.log(Level.INFO, "Initializing Config")
    val config = new Config()

    // If MW take orgID from arg
    if(organizationID.isEmpty) {
      config.setOrganizationId(getOrgId)
    }else{ // is multiWorkspace deployment since orgID is passed
      logger.log(Level.INFO, "Setting multiworkspace deployment")
      config.setOrganizationId(organizationID.get)
      if (apiUrl.nonEmpty) {
        config.setApiUrl(apiUrl)
      }
      config.setIsMultiworkspaceDeployment(true)
    }
    // set spark overrides in scoped spark session and override the necessary values for Pipeline Run
    config.registerInitialSparkConf(spark(globalSession = true).conf.getAll)
    config.setInitialWorkerCount(getNumberOfWorkerNodes)
    if (debugFlag) {
      envInit("DEBUG")
      config.setDebugFlag(debugFlag)
    }
    config
  }

  /**
   * Companion object to validate environment initialize the config for the run.
   * Takes input of raw arg strings into the main class, parses and validates them,
   * checks the environement for run readiness, sets up the scope based on modules selected,
   * and checks for avoidable issues. The initializer is also responsible for identifying any errors in the
   * configuration that can be identified before the runs begins to enable fail fast.
   *
   * @param overwatchArgs Json string of args -- When passing into args in Databricks job UI, the json string must
   *                      be passed in as an escaped Json String. Use JsonUtils in Tools to build and extract the string
   *                      to be used here.
   * @param debugFlag     manual Boolean setter to enable the debug flag. This is different than the log4j DEBUG Level
   *                      When setting this to true it does enable the log4j DEBUG level but throughout the code there
   *                      is more robust output when debug is enabled.
   * @return
   */
  def apply(overwatchArgs: String): Workspace = {
    apply(
      overwatchArgs,
      debugFlag = false,
      isSnap = false,
      disableValidations = false
    )
  }

  def apply(overwatchArgs: String, debugFlag: Boolean): Workspace = {
    apply(
      overwatchArgs,
      debugFlag,
      isSnap = false,
      disableValidations = false
    )
  }

  /**
   *
   * @param overwatchArgs Json string of args -- When passing into args in Databricks job UI, the json string must
   *                      be passed in as an escaped Json String. Use JsonUtils in Tools to build and extract the string
   *                      to be used here.
   * @param debugFlag manual Boolean setter to enable the debug flag. This is different than the log4j DEBUG Level
   *                      When setting this to true it does enable the log4j DEBUG level but throughout the code there
   *                      is more robust output when debug is enabled.
   * @param isSnap internal only param to add specific snap metadata to initialized dataset
   * @param disableValidations internal only whether or not to validate the parameters for the local Databricks workspace
   *                           if this is set to true, pipelines cannot be run as they are set to read only mode
   * @param initializeDatabase internal only parameter to disable the automatic creation of a database upon workspace
   *                           init.
   * @return
   */
  private[overwatch] def apply(
                                overwatchArgs: String,
                                debugFlag: Boolean = false,
                                isSnap: Boolean = false,
                                disableValidations: Boolean = false,
                                initializeDatabase: Boolean = true,
                                apiURL: Option[String] = None ,
                                organizationID: Option[String] = None
                              ): Workspace = {


    val config = initConfigState(debugFlag,organizationID,apiURL)

    logger.log(Level.INFO, "Initializing Environment")

    val initializer = new Initializer(config, disableValidations, isSnap, initializeDatabase)
      .validateAndRegisterArgs(overwatchArgs)

    val database = if (initializeDatabase) initializer.initializeDatabase() else Database(config)

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace(database, config)
      .setValidated(!disableValidations)

    workspace
  }

}