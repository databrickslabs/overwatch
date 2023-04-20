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
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Initializer(config: Config, disableValidations: Boolean)
  extends InitializerFunctions
    with SparkSessionWrapper {

  setConfig(config)
  setDisableValidation(disableValidations)

}

object Initializer extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  // Init the SparkSessionWrapper with envVars
  envInit()

  /**
   * Returns the local workspace orgID
   * @return
   */
  def getOrgId: String = getOrgId(None)

  /**
   * Returns the local workspace orgID, or the orgID contained in the input apiURL
   * When getOrgId is called from a Future the apiURL MUST be passed to prevent failure to get orgID
   * since dbutils.notebook.getContext.apiUrl.get returns None and None.get fails
   * @param apiUrl apiURL of the workspace to get the orgId
   * @return
   */
  def getOrgId(apiUrlInput: Option[String]): String = {
    val clusterOwnerOrgID = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    if (clusterOwnerOrgID == " " || clusterOwnerOrgID == "0") {
      val apiURL = getApiURL(apiUrlInput)

      apiURL.split("\\.")(0).split("/").lastOption match {
        case Some(orgID) => orgID
        case None => throw new BadConfigException("ORG ID cannot be determined")
      }
    } else clusterOwnerOrgID
  }

  private def getApiURL(apiUrlInput: Option[String]): String = {
    apiUrlInput match {
      case Some(apiURL) => apiURL
      case None => dbutils.notebook.getContext.apiUrl match {
        case Some(apiURL) => apiURL
        case None => throw new BadConfigException("API URL cannot be determined")
      }
    }
  }


  private def initConfigState(debugFlag: Boolean,organizationID: Option[String],
                              apiUrl: Option[String]
                             ): Config = {
    logger.log(Level.INFO, "Initializing Config")
    val config = new Config()

    // If MW take orgID from arg
    if(organizationID.isEmpty) {
      config.setOrganizationId(getOrgId(apiUrl))
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

  def deserializeArgs(overwatchArgs: String): OverwatchParams = {
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
    rawParams
  }

  def getDeploymentType(overwatchParams: OverwatchParams): String ={
    logger.debug(s"database ------ ${overwatchParams.dataTarget.get.databaseName.get}")
    if (overwatchParams.dataTarget.get.databaseName.get.contains("."))
       "uce"
    else
       "default"
  }

  /**
   * Load text file
   *
   * @param path path to the local resource
   * @return sequence of lines read from the file
   */
  def loadLocalResource(path: String): Seq[String] = {
    val fileLocation = getClass.getResourceAsStream(path)
    if (fileLocation == null)
      throw new RuntimeException(s"There is no resource at path: $path")
    val source = scala.io.Source.fromInputStream(fileLocation).mkString.stripMargin
    source.split("\\r?\\n").filter(_.trim.nonEmpty).toSeq
  }

  /**
   * Load database for cloud provider node details
   *
   * @param path path to the local resource
   * @return
   */
  def loadLocalCSVResource(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val csvData = loadLocalResource(path).toDS()
    spark.read.option("header", "true").option("inferSchema", "true").csv(csvData).coalesce(1)
  }

  def buildInitializer(
                        config: Config,
                        disableValidations: Boolean = false,
                        isSnap: Boolean = false,
                        initializeDatabase: Boolean = true): Initializer = {
    config.deploymentType.toLowerCase.trim match {
      case "uce" => new InitializerFunctionsUCE(config, disableValidations, isSnap, initializeDatabase)
      case "default" => new InitializerFunctionsDefault(config, disableValidations, isSnap, initializeDatabase)
      case _ => throw new UnsupportedOperationException(s"The Deployment Type for ${config.deploymentType} is not supported.")
    }
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
    val overwatchParams = deserializeArgs(overwatchArgs)

    val deployment = getDeploymentType(overwatchParams)
    logger.log(Level.INFO,s"deployment type is - ${deployment}")

    config.setDeploymentType(deployment)

    val initializer = buildInitializer(config, disableValidations, isSnap, initializeDatabase)

    initializer.validateAndRegisterArgs(overwatchParams)

    val database = if (initializeDatabase) initializer.initializeDatabase() else Database(config)

    logger.log(Level.INFO, "Initializing Workspace")
    val workspace = Workspace(database, config)
      .setValidated(!disableValidations)

    workspace
  }

}