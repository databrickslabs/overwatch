package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.{BadConfigException, DataTarget, Helpers, JsonUtils, OverwatchParams, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Optimizer extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  /**
   * Identify the latest successful runs by module
   * @param overwatchETLDB name of the Overwtch ETL database
   * @return
   */
  private[overwatch] def getLatestSuccessState(overwatchETLDB: String,orgId: String): DataFrame = {
    val lastSuccessByModuleW = Window.partitionBy('moduleID).orderBy('Pipeline_SnapTS.desc)
    val orgIDFilter = if (orgId == "") lit(true) else 'organization_id === orgId
    spark.table(s"${overwatchETLDB}.pipeline_report")
      .filter(orgIDFilter)
      .filter('status === "SUCCESS" || 'status.like("EMPTY:%"))
      .withColumn("rnk", rank.over(lastSuccessByModuleW))
      .withColumn("rn", row_number.over(lastSuccessByModuleW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
      .orderBy('Pipeline_SnapTS.desc)
  }

  /**
   * This method is used to get the API URL from config table.
   *
   * @param orgID      The organization ID as a string.
   * @param dataTarget The data target object.
   * @return The API URL as a string.
   *
   *         The method works as follows:
   *         1. It retrieves the ETL storage prefix from the data target and trims any leading or trailing spaces.
   *         2. It constructs the report location by appending "report/configTable" to the last 12 characters of the ETL storage prefix.
   *         3. It defines a window function that partitions by workspace_id and orders by snapTS in descending order.
   *         4. It reads the data from the report location and selects the workspace_id, api_url, and snapTS columns.
   *         5. It filters the data to only include rows where the workspace_id matches the provided organization ID.
   *         6. It adds a new column "rnk" which is the rank of each row within its partition, as defined by the window function.
   *         7. It filters the data to only include rows where "rnk" is 1, i.e., the row with the latest snapTS for each workspace_id.
   *         8. It returns the api_url of the first row in the resulting DataFrame.
   */
  private def getApiURL(orgID: String,dataTarget: DataTarget): String = {
    val etlStoragePrefix = dataTarget.etlDataPathPrefix.get.trim
    val reportLocation = etlStoragePrefix.substring(0,etlStoragePrefix.length-12)+"report/configTable" //Removing global prefix and adding report location
    if(Helpers.pathExists(reportLocation)) { //If the config table is found at the specified location, proceed with legacy optimization.
      val latestRunWindow = Window.partitionBy('workspace_id).orderBy('snapTS.desc)
      spark.read.load(reportLocation).select("workspace_id", "api_url", "snapTS")
        .filter('workspace_id === orgID)
        .withColumn("rnk", rank.over(latestRunWindow))
        .filter('rnk === 1)
        .first().getString(1)
    } else {  // If the config table is not found at the specified location, throw an exception.
      throw new BadConfigException(s"Config table not found at $reportLocation")
    }

  }


  /**
   * Derive the workspace from the config supplied in the last run
   * @param overwatchETLDB name of the Overwtch ETL database
   * @return workspace object
   */
  private def getLatestWorkspace(overwatchETLDB: String,orgId: String): Workspace = {
   val latestSuccessState = getLatestSuccessState(overwatchETLDB,orgId).limit(1)
   val orgID = latestSuccessState.select("organization_id").collect().map(x => x(0).toString)
    val params = latestSuccessState
      .selectExpr("inputConfig.*")
      .as[OverwatchParams]
      .first

    val args = JsonUtils.objToJson(params).compactString
    val prettyArgs = JsonUtils.objToJson(params).prettyString
    logger.log(Level.INFO, s"ARGS: Identified config string is: \n$prettyArgs")
    try{
      val apiURL = getApiURL(orgID(0),params.dataTarget.get)
      logger.log(Level.INFO,"Running optimization for Multiworkspace deployment")
      Initializer(args,apiURL=Some(apiURL), organizationID = Some(orgID(0)))
    }catch {
      case e: Exception => { //Unable to retrive api URL performing legacy Optimization
        logger.log(Level.INFO,"Running optimization for single workspace deployment")
        Initializer(args)
      }
    }

   }

  /**
   * pass in the overwatch ETL database name to optimize overwatch in parallel
   * @param args(0)   Overwatch ETL Database.
   * @param args(1)   Organization_ID for which optimization need to be performed.(Optional). By default optimizer will run for all
   *                  the organization ID in pipeline_report. If args(1) is not provided then run optimization for whole database.
   */

  def main(args: Array[String]): Unit = {


    val (overwatchETLDB, orgID) = if (args.length == 1) {
      val dbName = args(0)
      (dbName, "")
    } else if (args.length == 2) {
      val dbName = args(0)
      val org_id = args(1)
      val cntOrgID = spark.table(s"${dbName}.pipeline_report").select("organization_id").filter('organization_id === org_id).distinct.count()
      if (cntOrgID > 0) {
        (dbName, org_id)
      } else {
        throw new BadConfigException("Input Organization_ID is not part of the Overwatch Deployment for which you want to run the optimizer")
      }
    } else {
      throw new BadConfigException(s"Main class requires at least 1 but less than 3 arguments. Received ${args.length} " +
        s"arguments. Please review the docs to compose the input arguments appropriately.")
    }

    val workspace = getLatestWorkspace(overwatchETLDB,orgID)
    val config = workspace.getConfig
    if (config.debugFlag) println(JsonUtils.objToJson(config.inputConfig).compactString)
    val bronze = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
    val silver = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
    val gold = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)

    val optimizationCandidates = bronze.getAllTargets ++ silver.getAllTargets ++ gold.getAllTargets :+ bronze.pipelineStateTarget
    val postProcessor = new PostProcessor(config)
    val orgIdList  = spark.table(s"${overwatchETLDB}.pipeline_report").select("organization_id").distinct().collect().map(x => x(0).toString)
    postProcessor.optimizeOverwatch(spark, optimizationCandidates,orgID,orgIdList)

  }

}
