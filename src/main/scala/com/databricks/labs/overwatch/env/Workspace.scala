package com.databricks.labs.overwatch.env

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ApiCallV2
import com.databricks.labs.overwatch.pipeline.PipelineFunctions
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.util
import java.util.Collections
import java.util.concurrent.Executors
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success}


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
  def getJobsDF: DataFrame = {

    val jobsEndpoint = "jobs/list"
    val query = Map(
      "limit" -> "25",
      "expand_tasks" -> "true",
      "offset" -> "0"
    )
    ApiCallV2(config.apiEnv, jobsEndpoint,query,2.1)
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

  def getClustersDF: DataFrame = {
    val clustersEndpoint = "clusters/list"
    ApiCallV2(config.apiEnv, clustersEndpoint)
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
  def getPoolsDF: DataFrame = {
    val poolsEndpoint = "instance-pools/list"
    ApiCallV2(config.apiEnv, poolsEndpoint)
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Placeholder for future dev
   *
   * @return
   */
  def getProfilesDF: DataFrame = {
    val profilesEndpoint = "instance-profiles/list"
    ApiCallV2(config.apiEnv, profilesEndpoint).execute().asDF().withColumn("organization_id", lit(config.organizationId))

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
      tempSuccessPath = s"${config.tempWorkingDir}/sqlqueryhistory_silver/${System.currentTimeMillis()}",
      accumulator = acc
    )
      .execute()
      .asDF()
      .withColumn("organization_id", lit(config.organizationId))
  }

  def getSqlQueryHistoryParallelDF(fromTime: TimeTypes, untilTime: TimeTypes): DataFrame = {
    val sqlQueryHistoryEndpoint = "sql/history/queries"
    val acc = sc.longAccumulator("sqlQueryHistoryAccumulator")
    var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
    var apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
    val apiResponseCounter = Collections.synchronizedList(new util.ArrayList[Int]())
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(config.apiEnv.threadPoolSize))
    val tmpSqlQueryHistorySuccessPath = s"${config.tempWorkingDir}/sqlqueryhistory_silver/${System.currentTimeMillis()}"
    val tmpSqlQueryHistoryErrorPath = s"${config.tempWorkingDir}/errors/sqlqueryhistory_silver/${System.currentTimeMillis()}"
    val untilTimeMs = untilTime.asUnixTimeMilli
    var fromTimeMs = fromTime.asUnixTimeMilli - (1000*60*60*24*2)  //subtracting 2 days for running query merge
    val finalResponseCount = scala.math.ceil((untilTimeMs - fromTimeMs).toDouble/(1000*60*60)) // Total no. of API Calls
    while (fromTimeMs < untilTimeMs){
      val (startTime, endTime) = if ((untilTimeMs- fromTimeMs)/(1000*60*60) > 1) {
        (fromTimeMs,
          fromTimeMs+(1000*60*60))
      }
      else{
        (fromTimeMs,
          untilTimeMs)
      }
      //create payload for the API calls
      val jsonQuery = Map(
        "max_results" -> "50",
        "include_metrics" -> "true",
        "filter_by.query_start_time_range.start_time_ms" ->  s"$startTime",
        "filter_by.query_start_time_range.end_time_ms" ->  s"$endTime"
      )
      /**TODO:
       * Refactor the below code to make it more generic
       */
      //call future
      val future = Future {
        val apiObj = ApiCallV2(
          config.apiEnv,
          sqlQueryHistoryEndpoint,
          jsonQuery,
          tempSuccessPath = tmpSqlQueryHistorySuccessPath,
          accumulator = acc
        ).executeMultiThread()

        synchronized {
          apiObj.forEach(
            obj=>if(obj.contains("res")){
              apiResponseArray.add(obj)
            }
          )
          if (apiResponseArray.size() >= config.apiEnv.successBatchSize) {
            PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistorySuccessPath, apiResponseArray.toString)
            apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
          }
        }
      }
      future.onComplete {
        case Success(_) =>
          apiResponseCounter.add(1)

        case Failure(e) =>
          if (e.isInstanceOf[ApiCallFailureV2]) {
            synchronized {
              apiErrorArray.add(e.getMessage)
              if (apiErrorArray.size() >= config.apiEnv.errorBatchSize) {
                PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistoryErrorPath, apiErrorArray.toString)
                apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
              }
            }
            logger.log(Level.ERROR, "Future failure message: " + e.getMessage, e)
          }
          apiResponseCounter.add(1)
      }
      fromTimeMs = fromTimeMs+(1000*60*60)
    }

    val timeoutThreshold = config.apiEnv.apiWaitingTime // 5 minutes
    var currentSleepTime = 0
    var accumulatorCountWhileSleeping = acc.value
    while (apiResponseCounter.size() < finalResponseCount && currentSleepTime < timeoutThreshold) {
      //As we are using Futures and running 4 threads in parallel, We are checking if all the treads has completed
      // the execution or not. If we have not received the response from all the threads then we are waiting for 5
      // seconds and again revalidating the count.
      if (currentSleepTime > 120000) //printing the waiting message only if the waiting time is more than 2 minutes.
      {
        println(
          s"""Waiting for other queued API Calls to complete; cumulative wait time ${currentSleepTime / 1000}
             |seconds; Api response yet to receive ${finalResponseCount - apiResponseCounter.size()}""".stripMargin)
      }
      Thread.sleep(5000)
      currentSleepTime += 5000
      if (accumulatorCountWhileSleeping < acc.value) { //new API response received while waiting.
        currentSleepTime = 0 //resetting the sleep time.
        accumulatorCountWhileSleeping = acc.value
      }
    }
    if (apiResponseCounter.size() != finalResponseCount) { // Checking whether all the api responses has been received or not.
      logger.log(Level.ERROR,
        s"""Unable to receive all the sql/history/queries api responses; Api response
           |received ${apiResponseCounter.size()};Api response not
           |received ${finalResponseCount - apiResponseCounter.size()}""".stripMargin)
      throw new Exception(
        s"""Unable to receive all the sql/history/queries api responses; Api response received
           |${apiResponseCounter.size()};Api response not received ${finalResponseCount - apiResponseCounter.size()}""".stripMargin)
    }
    if (apiResponseArray.size() > 0) { //In case of response array didn't hit the batch-size as a final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistorySuccessPath, apiResponseArray.toString)
      apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
    }
    if (apiErrorArray.size() > 0) { //In case of error array didn't hit the batch-size as a final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistorySuccessPath, apiErrorArray.toString)
      apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
    }
    logger.log(Level.INFO, " sql query history landing completed")
    if(Helpers.pathExists(tmpSqlQueryHistorySuccessPath)) {
      try {
        spark.read.json(tmpSqlQueryHistorySuccessPath)
          .select(explode(col("res")).alias("res")).select(col("res" + ".*"))
          .withColumn("organization_id", lit(config.organizationId))
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
      CloneDetail(sourcePath, targetPath, asOfTS, cloneLevel)
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
        case e: Throwable =>
          val msg = s"TABLE REGISTRATION FAILED: ${e.getMessage}"
          logger.log(Level.ERROR, msg)
          WorkspaceMetastoreRegistrationReport(dataset, stmt, msg)
      }
    }).toArray.toSeq
    spark.sql(s"refresh ${config.databaseName}")
    addReport
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
