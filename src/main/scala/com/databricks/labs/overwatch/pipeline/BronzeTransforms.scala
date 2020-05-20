package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{OverwatchScope, SparkSessionWrapper}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window


trait BronzeTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _jobIDs: Array[Long] = Array()
  private var _newDataRetrieved: Boolean = true
  private var _clusterIDs: Array[String] = Array()
  private var _eventLogGlob: DataFrame = _

  private def setJobIDs(value: Array[Long]): this.type = {
    _jobIDs = value
    this
  }

  protected def setNewDataRetrievedFlag(value: Boolean): this.type = {
    _newDataRetrieved = value
    this
  }

  protected def setClusterIDs(value: Array[String]): this.type = {
    _clusterIDs = value
    this
  }

  protected def setEventLogGlob(value: DataFrame): this.type = {
    _eventLogGlob = value
    this
  }

  protected def sparkEventsLogGlob: DataFrame = _eventLogGlob

  protected def clusterIDs: Array[String] = _clusterIDs

  protected def newDataRetrieved: Boolean = _newDataRetrieved

  private def jobIDs: Array[Long] = _jobIDs

  @throws(classOf[NoNewDataException])
  private def apiByID[T](endpoint: String, apiEnv: ApiEnv,
                         apiType: String,
                         ids: Array[T], idsKey: String,
                         extraQuery: Option[Map[String, Any]] = None): Array[String] = {
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(4))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport
    //    DEBUG
    //    val idsPar = Array("0827-194754-tithe1")
    // removing parallelization for now to see if it fixes some weird errors
    // CONFIRMED -- Parallelizing this breaks the token cipher
    val results = ids.flatMap(id => {
      val rawQuery = Map(
        idsKey -> id
      )

      val query = if (extraQuery.nonEmpty) {
        extraQuery.get ++ rawQuery
      } else rawQuery

      val apiCall = ApiCall(endpoint, apiEnv, Some(query))
      if (apiType == "post") apiCall.executePost().asStrings
      else apiCall.executeGet().asStrings
    }) //.toArray
    if (results.isEmpty) {
      setNewDataRetrievedFlag(false)
      //      throw new NoNewDataException(s"${endpoint} returned no new data, skipping") // TODO -- BUBBLE THIS UP
      Array()
    }
    else results
  }

  protected def getAuditLogsDF(auditLogPath: String): DataFrame = {
    spark.read.json(auditLogPath)
  }

  // TODO -- Get from audit if exists
  protected def collectJobsIDs()(df: DataFrame): DataFrame = {
    setJobIDs(df.select('job_id).distinct().as[Long].collect())
    df
  }

  // TODO -- get from audit
  // TODO -- add assertion that df count == total count from API CALL
  protected def prepJobRunsDF(apiEnv: ApiEnv): DataFrame = {
    val extraQuery = Map("completed_only" -> true)
    val jobRuns = apiByID("jobs/runs/list", apiEnv,
      "get", jobIDs, "job_id", Some(extraQuery))

    if (newDataRetrieved) {
      // TODO -- add filter to remove runs before fromTS
      spark.read.json(Seq(jobRuns: _*).toDS()).select(explode('runs).alias("runs"))
        .select(col("runs.*"))
    } else jobRuns.toSeq.toDF("FAILURE")
  }

  protected def getNewJobRuns(apiEnv: ApiEnv, fromTimeCol: Column)(df: DataFrame): DataFrame = {
    val runIDs = df
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .filter('actionName.isin("runSucceeded", "runFailed"))
      .filter('date > fromTimeCol.cast("date"))
      .select('runId.cast("int")).distinct.as[Int].collect()
    val jobRuns = apiByID("jobs/runs/get", apiEnv,
      "get", runIDs, "run_id", None)

    if (newDataRetrieved) {
      // TODO -- add filter to remove runs before fromTS
      spark.read.json(Seq(jobRuns: _*).toDS())
    } else jobRuns.toSeq.toDF("FAILURE")
  }

  protected def collectClusterIDs()(df: DataFrame): DataFrame = {
    setClusterIDs(df.select('cluster_id).distinct().as[String].collect())
    df
  }

  protected def collectClusterIDs(fromTimeCol: Column)(df: DataFrame): DataFrame = {
    // Get clusterIDs for clusters with activity since last run
    setClusterIDs(
      df
        .selectExpr("*", "requestParams.*")
        .filter('serviceName === "clusters" && 'cluster_id.isNotNull)
        .filter('date > fromTimeCol.cast("date")) // Partition Filter
        .select('cluster_id).distinct().as[String].collect()
    )
    df
  }

  protected def prepClusterEventLogs(start_time: Long, end_time: Long,
                                     apiEnv: ApiEnv): DataFrame = {

    val extraQuery = Map(
      "start_time" -> start_time, // 1588935326000L, //
      "end_time" -> end_time //1589021726000L //
    )

    // TODO -- add assertion that df count == total count from API CALL
    val clusterEvents = apiByID("clusters/events", apiEnv, "post",
      clusterIDs, "cluster_id", Some(extraQuery))

    if (newDataRetrieved) {
      spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
        .select(col("events.*"))
    } else clusterEvents.toSeq.toDF("FAILURE")

  }

  // Todo -- Put back to private
  def getUniqueSparkEventsFiles(badRecordsPath: String,
                                eventLogsDF: DataFrame,
                                eventLogsTarget: PipelineTable): Array[String] = {
    if (spark.catalog.tableExists(eventLogsTarget.tableFullName)) {
      val existingFiles = eventLogsTarget.asDF.select('filename)
        .distinct
      val badFiles = spark.read.format("json").load(s"${badRecordsPath}/*/*/")
        .select('path.alias("filename"))
        .distinct
      eventLogsDF.except(existingFiles.unionByName(badFiles)).as[String].collect()
    } else {
      eventLogsDF.select('filename)
        .distinct.as[String].collect()
    }
  }


  def generateEventLogsDF(badRecordsPath: String, eventLogsTarget: PipelineTable)(eventLogsDF: DataFrame): DataFrame = {

    val pathsGlob = getUniqueSparkEventsFiles(badRecordsPath, eventLogsDF, eventLogsTarget)
    val dropCols = Array("Classpath Entries", "HadoopProperties", "SparkProperties", "SystemProperties", "sparkPlanInfo")

    val rawEventsDF = spark.read.option("badRecordsPath", badRecordsPath)
      .json(pathsGlob: _*).drop(dropCols: _*)

    SchemaTools.scrubSchema(rawEventsDF)
      .withColumn("filename", input_file_name)
      .withColumn("pathSize", size(split('filename, "/")) - lit(2))
      .withColumn("SparkContextID", split('filename, "/")('pathSize))
      .drop("pathSize")
  }

  protected def collectEventLogPaths(fromTimeCol: Column,
                                     isFirstRun: Boolean,
                                     scope: Seq[OverwatchScope.OverwatchScope])(df: DataFrame): DataFrame = {
    if (scope.contains(OverwatchScope.audit)) {
      val cluster_id_gen_w = Window.partitionBy('cluster_name)
        .orderBy('timestamp)
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)

      // Lookup null cluster_ids
      val cluster_id_gen = first('cluster_id, ignoreNulls = true).over(cluster_id_gen_w)

      // get edited and created clusters
      var baseClusterLookupDF = df
        .selectExpr("*", "requestParams.*")
        .filter('serviceName === "clusters")
        .filter('date > fromTimeCol.cast("date")) //Partition filter
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)

      // get cluster ids with events since last run to get new cluster events from api
      setClusterIDs(baseClusterLookupDF.select('cluster_id).distinct().as[String].collect())

      // Removing this for now -- I don't think it's necessary to do the look back
      // Combine edited and created clusters with historical clusters to get full list of event log paths
      //      if (!isFirstRun) {
      //        baseClusterLookupDF = baseClusterLookupDF
      //          .filter('actionName.isin("create", "createResult", "edit"))
      //          .unionByName(
      //            df
      //              .selectExpr("*", "requestParams.*")
      //              .filter('serviceName === "clusters")
      //              .filter('actionName.isin("create", "createResult", "edit"))
      //              .filter('date <= fromTimeCol.cast("date")) //Partition filter
      //              .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
      //          ).distinct
      //      }

      // get all relevant cluster log paths
      baseClusterLookupDF
        .filter('cluster_log_conf.isNotNull)
        .withColumn("s3", get_json_object('cluster_log_conf, "$.s3"))
        .withColumn("dbfs", get_json_object('cluster_log_conf, "$.dbfs"))
        .withColumn("destination",
          when('s3.isNotNull, regexp_replace(get_json_object('s3, "$.destination"), "\\/$", ""))
            when('dbfs.isNotNull, regexp_replace(get_json_object('dbfs, "$.destination"), "\\/$", ""))
        )
        .withColumn("cluster_id", cluster_id_gen)
        .select(
          array(col("destination"), col("cluster_id"),
            lit("eventlog"), lit("*"), lit("*"), lit("eventlo*"))
        ).rdd
        .map(r => r.getSeq[String](0).mkString("/")).collect()
        .flatMap(Helpers.globPath)
        .toSeq.toDF("filename")
    } else { // TODO - TEST -- might have broken when not using audit
      val colsDF = df.select($"cluster_log_conf.*")
      val cols = colsDF.columns.map(c => s"${c}.destination")
      val logsDF = df.select($"cluster_log_conf.*", $"cluster_id".alias("cluster_id"))
      cols.flatMap(
        c => {
          logsDF.select(col("cluster_id"), col(c)).filter(col("destination").isNotNull).distinct
            .select(
              array(regexp_replace(col("destination"), "\\/$", ""), col("cluster_id"),
                lit("eventlog"), lit("*"), lit("*"), lit("eventlo*"))
            ).rdd
            .map(r => r.getSeq[String](0).mkString("/")).collect()
        }).distinct
        .flatMap(Helpers.globPath)
        .toSeq.toDF("filename")
    }
  }

  //  protected def collectEventLogPaths()(df: DataFrame): DataFrame = {
  //
  //    // Best effort to get currently existing cluster ids
  //    val colsDF = df.select($"cluster_log_conf.*")
  //    val cols = colsDF.columns.map(c => s"${c}.destination")
  //    val logsDF = df.select($"cluster_log_conf.*", $"cluster_id".alias("cluster_id"))
  //    cols.flatMap(
  //      c => {
  //        logsDF.select(col("cluster_id"), col(c)).filter(col("destination").isNotNull).distinct
  //          .select(
  //            array(regexp_replace(col("destination"), "\\/$", ""), col("cluster_id"),
  //              lit("eventlog"), lit("*"), lit("*"), lit("eventlo*"))
  //          ).rdd
  //          .map(r => r.getSeq[String](0).mkString("/")).collect()
  //      }).distinct
  //      .flatMap(Helpers.globPath)
  //      .toSeq.toDF("filename")
  //    setEventLogGlob(eventLogsPathGlobDF)
  //    df
  //  }

}
