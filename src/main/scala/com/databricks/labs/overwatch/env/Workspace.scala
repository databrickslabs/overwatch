package com.databricks.labs.overwatch.env

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.api.ApiCallV2
import com.databricks.labs.overwatch.pipeline.{PipelineFunctions, Schema}
import com.databricks.labs.overwatch.utils.Helpers.deriveRawApiResponseDF
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.databricks.labs.overwatch.pipeline.TransformFunctions._

import java.util
import java.util.Collections
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Future
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


/**
 * The Workspace class gets instantiated once per run per Databricks workspace. THe need for this class evolved
 * over time and is being restructured as part of an outstanding refactor branch which is likely not to be completed
 * until after v1.0 release.
 *
 * @param config
 */
class Workspace(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _database: Database = _
  private var _validated: Boolean = false
  private[overwatch] val overwatchRunClusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

  private[overwatch] def database: Database = _database

  private def setDatabase(value: Database): this.type = {
    _database = value
    this
  }

  private[overwatch] def setValidated(value: Boolean): this.type = {
    _validated = value
    this
  }

  def copy(_config: Config = config): Workspace = {
    val db = this.database
    Workspace(db, _config)
  }

  def isValidated: Boolean = _validated

  /**
   * Most of the jobs data comes from the audit logs but there are several edge cases that result in incomplete
   * jobs log data in the audit logs (same true for cluster specs). As such, each time the jobs module executes
   * a snapshot of actively defined jobs is captured and used to fill in the blanks in the silver+ layers.
   *
   * @return
   */
  def getJobsDF(apiTempPath: String): DataFrame = {
    val jobsEndpoint = "jobs/list"
    val query = Map(
      "limit" -> "25",
      "expand_tasks" -> "true"
    )
    ApiCallV2(config.apiEnv, jobsEndpoint,query,2.1)
      .setSuccessTempPath(apiTempPath)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Exposed config as a public getter to enable access to config for testing. This should not be public facing
   * public function.
   *
   * @return
   */
  def getConfig: Config = config

  def getClustersDF(tempApiDir: String): DataFrame = {
    val clustersEndpoint = "clusters/list"
    ApiCallV2(config.apiEnv, clustersEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * For future development
   *
   * @param dbfsPath
   * @return
   */
  def getDBFSPaths(dbfsPath: String): DataFrame = {
    val dbfsEndpoint = "dbfs/list"
    val jsonQuery = s"""{"path":"${dbfsPath}"}"""
    ApiCallV2(config.apiEnv, dbfsEndpoint, jsonQuery)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Placeholder for future dev
   *
   * @return
   */
  def getPoolsDF(tempApiDir: String): DataFrame = {
    val poolsEndpoint = "instance-pools/list"
    ApiCallV2(config.apiEnv, poolsEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Placeholder for future dev
   *
   * @return
   */
  def getProfilesDF(tempApiDir: String): DataFrame = {
    val profilesEndpoint = "instance-profiles/list"
    ApiCallV2(config.apiEnv, profilesEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF().withColumn("organization_id", lit(config.organizationId))

  }

  /**
   * Placeholder for future dev
   *
   * @return
   */
  def getWorkspaceUsersDF: DataFrame = {
    val workspaceEndpoint = "workspace/list"
    ApiCallV2(config.apiEnv, workspaceEndpoint).execute().asDF().withColumn("organization_id", lit(config.organizationId))
  }

  def getSqlQueryHistoryDF(fromTime: TimeTypes, untilTime: TimeTypes): DataFrame = {
    val sqlQueryHistoryEndpoint = "sql/history/queries"
    val acc = sc.longAccumulator("sqlQueryHistoryAccumulator")
    val startTime = fromTime.asUnixTimeMilli - (1000 * 60 * 60 * 24 * 2) // subtract 2 days for running query merge
    val jsonQuery = Map(
      "max_results" -> "50",
      "include_metrics" -> "true",
      "filter_by.query_start_time_range.start_time_ms" ->  s"$startTime",
      "filter_by.query_start_time_range.end_time_ms" ->  s"${untilTime.asUnixTimeMilli}"
      )
    ApiCallV2(
      config.apiEnv,
      sqlQueryHistoryEndpoint,
      jsonQuery,
      tempSuccessPath = s"${config.tempWorkingDir}/sqlqueryhistory_silver/${System.currentTimeMillis()}"
    )
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  def getSqlQueryHistoryParallelDF(fromTime: TimeTypes,
                                   untilTime: TimeTypes,
                                   pipelineSnapTime: TimeTypes,
                                   tmpSqlHistorySuccessPath: String,
                                   tmpSqlHistoryErrorPath: String): DataFrame = {
    val sqlQueryHistoryEndpoint = "sql/history/queries"
    val untilTimeMs = untilTime.asUnixTimeMilli
    val fromTimeMs = fromTime.asUnixTimeMilli - (1000*60*60*24*2)  //subtracting 2 days for running query merge
    val finalResponseCount = scala.math.ceil((untilTimeMs - fromTimeMs).toDouble/(1000*60*60)).toLong// Total no. of API Calls

    // creating Json input for parallel API calls
    val jsonInput = Map(
      "start_value" -> s"${fromTimeMs}",
      "end_value" -> s"${untilTimeMs}",
      "increment_counter" -> "3600000",
      "final_response_count" -> s"${finalResponseCount}",
      "result_key" -> "res",
      "tmp_success_path" -> tmpSqlHistorySuccessPath,
      "tmp_error_path" -> tmpSqlHistoryErrorPath
    )

    // calling function to make parallel API calls
    val apiCallV2Obj = new ApiCallV2(config.apiEnv)
    val tmpSqlQueryHistorySuccessPath= apiCallV2Obj.makeParallelApiCalls(sqlQueryHistoryEndpoint, jsonInput, pipelineSnapTime.asUnixTimeMilli,config)
    logger.log(Level.INFO, " sql query history landing completed")

    if(Helpers.pathExists(tmpSqlQueryHistorySuccessPath)) {
      try {
        val rawDF = deriveRawApiResponseDF(spark.read.json(tmpSqlQueryHistorySuccessPath))
        if (rawDF.columns.contains("res")) {
          rawDF.select(explode(col("res")).alias("res")).select(col("res" + ".*"))
            .withColumn("organization_id", lit(config.organizationId))
        } else {
          logger.log(Level.INFO, s"""No Data is present for sql/query/history from - ${fromTimeMs} to - ${untilTimeMs}, res column not found in dataset""")
          spark.emptyDataFrame
        }
      } catch {
        case e: Throwable =>
          throw new Exception(e)
      }
    } else {
      println(s"""No Data is present for sql/query/history from - ${fromTimeMs} to - ${untilTimeMs}""")
      logger.log(Level.INFO,s"""No Data is present for sql/query/history from - ${fromTimeMs} to - ${untilTimeMs}""")
      spark.emptyDataFrame
    }
  }

  def resizeCluster(apiEnv: ApiEnv, numWorkers: Int): Unit = {
    val endpoint = "clusters/resize"
    val jsonQuery = s"""{"cluster_id":"${overwatchRunClusterId}","num_workers":${numWorkers}}"""
    try {
      ApiCallV2(apiEnv, endpoint, jsonQuery).execute()
    } catch {
      case e: ApiCallFailure if e.httpResponse.code == 400 &&
        e.httpResponse.body.contains("cannot transition from Reconfiguring to Reconfiguring") =>
        val resizeErrorAttemptMsg = s"The Overwatch cluster cannot resize to $numWorkers nodes at this time " +
          s"as it is still resizing from a previous module. For smaller workspaces and daily runs " +
          s"intelligent scaling may not be necessary."
        if (config.debugFlag) println(resizeErrorAttemptMsg)
        logger.warn(resizeErrorAttemptMsg)
    }
  }

  /**
   * get EXISTING dataset[s] metadata within the configured Overwatch workspace
   *
   * @return Seq[WorkspaceDataset]
   */
  def getWorkspaceDatasets: Seq[WorkspaceDataset] = {
    dbutils.fs.ls(config.etlDataPathPrefix)
      .filter(_.isDir)
      .map(dataset => {
        val path = dataset.path
        val uri = Helpers.getURI(path)
        val name = if (dataset.name.endsWith("/")) dataset.name.dropRight(1) else dataset.name
        WorkspaceDataset(uri.getPath, name)
      })
  }

  def getClusterLibraries(tempApiDir: String): DataFrame = {
    val libsEndpoint = "libraries/all-cluster-statuses"
    ApiCallV2(config.apiEnv, libsEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  def getClusterPolicies(tempApiDir: String): DataFrame = {
    val policiesEndpoint = "policies/clusters/list"
    ApiCallV2(config.apiEnv, policiesEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  def getTokens(tempApiDir: String): DataFrame = {
    val tokenEndpoint = "token/list"
    ApiCallV2(config.apiEnv, tokenEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  def getGlobalInitScripts(tempApiDir: String): DataFrame = {
    val globalInitScEndpoint = "global-init-scripts"
    ApiCallV2(config.apiEnv, globalInitScEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Function to get the the list of Job Runs
   * @return
   */
  def getJobRunsDF(fromTime: TimeTypes, untilTime: TimeTypes,tempWorkingDir: String): DataFrame = {
    val jobsRunsEndpoint = "jobs/runs/list"
    val jsonQuery = Map(
      "limit" -> "25",
      "expand_tasks" -> "true",
      "start_time_from" -> s"${fromTime.asUnixTimeMilli}",
      "start_time_to" -> s"${untilTime.asUnixTimeMilli}"
    )
    val acc = sc.longAccumulator("sqlQueryHistoryAccumulator")
    var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())

    val apiObj = ApiCallV2(config.apiEnv,
      jobsRunsEndpoint,
      jsonQuery,
      tempSuccessPath = tempWorkingDir,
      2.1).executeMultiThread(acc)

    apiObj.forEach(
      obj => if (obj.contains("job_id")) {
        apiResponseArray.add(obj)
      }
    )

    if (apiResponseArray.size() > 0) { //In case of response array didn't hit the batch-size as a final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tempWorkingDir, apiResponseArray.toString)
    }

    if(Helpers.pathExists(tempWorkingDir)) {
      try {
        spark.conf.set("spark.sql.caseSensitive", "true")
        val baseDF = spark.read.json(tempWorkingDir)
        val df = deriveRawApiResponseDF(baseDF)
          .select(explode(col("runs")).alias("runs")).select(col("runs" + ".*"))
          .withColumn("organization_id", lit(config.organizationId))
        spark.conf.set("spark.sql.caseSensitive", "false")
        df
      } catch {
        case e: Throwable =>
          throw new Exception(e)
      }
    } else {
      println(s"""No Data is present for jobs/runs/list from - ${fromTime.asUnixTimeMilli} to - ${untilTime.asUnixTimeMilli}""")
      logger.log(Level.INFO,s"""No Data is present for jobs/runs/list from - ${fromTime.asUnixTimeMilli} to - ${untilTime.asUnixTimeMilli}""")
      spark.emptyDataFrame
    }

  }

  /**
   * Most of the warehouse data comes from the audit logs but there are several edge cases that result in incomplete
   * warehouses log data in the audit logs (same true for cluster specs). As such, each time the warehouse module executes
   * a snapshot of actively defined warehouses is captured and used to fill in the blanks in the silver+ layers.
   * @return
   */
  def getWarehousesDF(tempApiDir: String): DataFrame = {
    val warehousesEndpoint = "sql/warehouses"
    ApiCallV2(config.apiEnv, warehousesEndpoint)
      .setSuccessTempPath(tempApiDir)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param targetPrefix prefix of path target to send the snap
   * @param cloneLevel   Deep or Shallow
   * @param asOfTS       appends asOfTimestamp option to Delta reader to limit data on clone. This will only go back as
   *                     far as the latest vacuum by design.
   * @param excludes     Array of table names to exclude from the snapshot
   *                     this is the table name only - without the database prefix
   * @return
   */
  def snap(
            targetPrefix: String,
            cloneLevel: String = "DEEP",
            asOfTS: Option[String] = None,
            excludes: Array[String] = Array()
          ): Seq[CloneReport] = {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val sourcesToSnap = getWorkspaceDatasets
      .filterNot(dataset => excludes.map(_.toLowerCase).contains(dataset.name.toLowerCase))
    val cloneSpecs = sourcesToSnap.map(dataset => {
      val sourceName = dataset.name
      val sourcePath = dataset.path
      val targetPath = if (targetPrefix.takeRight(1) == "/") s"$targetPrefix$sourceName" else s"$targetPrefix/$sourceName"
      CloneDetail(sourcePath, targetPath, asOfTS, cloneLevel,Array(),WriteMode.append)
    }).toArray.toSeq
    Helpers.parClone(cloneSpecs)
  }

  /**
   * add existing tables to the metastore in the configured database.
   *
   * @return Seq[WorkspaceMetastoreRegistrationReport]
   */
  def addToMetastore(): Seq[WorkspaceMetastoreRegistrationReport] = {

    require(Helpers.pathExists(config.etlDataPathPrefix), s"This function can only register the Overwatch data tables " +
      s"to the Data Target configured in Overwatch. The location ${config.etlDataPathPrefix} does not exist.")

    val datasets = getWorkspaceDatasets.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(getDriverCores * 2))
    datasets.tasksupport = taskSupport
    logger.log(Level.INFO, s"BEGINNING METASTORE REGISTRATION: Database Name ${config.databaseName}")
    val addReport = datasets.map(dataset => {
      val fullTableName = s"${config.databaseName}.${dataset.name}"
      val stmt = s"CREATE TABLE $fullTableName USING DELTA LOCATION '${dataset.path}'"
      logger.log(Level.INFO, stmt)
      try {
        if (spark.catalog.tableExists(fullTableName)) throw new BadConfigException(s"TABLE EXISTS: SKIPPING")
        if (dataset.name == "tempworkingdir") throw new UnsupportedTypeException(s"Can not Create table using '${dataset.path}'")
        else {
          spark.sql(stmt)
//          if (workspacesAllowed.nonEmpty){
//            if (spark.catalog.databaseExists(config.databaseName)) spark.sql(s"Drop Database ${config.databaseName} cascade")
//          }
        }

        WorkspaceMetastoreRegistrationReport(dataset, stmt, "SUCCESS")

      } catch {
        case e: BadConfigException =>
          WorkspaceMetastoreRegistrationReport(dataset, stmt, e.getMessage)
        case e: UnsupportedTypeException =>
          WorkspaceMetastoreRegistrationReport(dataset, stmt, e.getMessage)
        case e: Throwable =>
          val msg = s"TABLE REGISTRATION FAILED: ${e.getMessage}"
          logger.log(Level.ERROR, msg)
          WorkspaceMetastoreRegistrationReport(dataset, stmt, msg)
      }
    }).toArray.toSeq
    if (spark.catalog.databaseExists(config.databaseName)) spark.sql(s"refresh ${config.databaseName}")
    addReport
  }

  /**
   * Fetch the warehouse event data from system.compute.warehouse_events
   * @param fromTime : from time to fetch the data
   * @param untilTime: until time to fetch the data
   * @param maxHistoryDays: maximum history days to fetch the data
   * @return
   */
  def getWarehousesEventDF(fromTime: TimeTypes,
                           untilTime: TimeTypes,
                           maxHistoryDays: Int = 30
                           ): DataFrame = {
    val sysTableFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val moduleFromTime = fromTime.asLocalDateTime.format(sysTableFormat)
    val moduleUntilTime = untilTime.asLocalDateTime.format(sysTableFormat)
    spark.sql(s"""
        select * from system.compute.warehouse_events
        WHERE event_time >= DATE_SUB('${moduleFromTime}', ${maxHistoryDays})
        and event_time <= '${moduleUntilTime}'
        """)
      .withColumnRenamed("event_type","state")
      .withColumnRenamed("workspace_id","organization_id")
      .withColumnRenamed("event_time","timestamp")
  }
}


object Workspace {
  /**
   * Workspace companion object initializer.
   */
  def apply(database: Database, config: Config): Workspace = {
    new Workspace(config).setDatabase(database)
  }

}
