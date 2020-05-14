package com.databricks.labs.overwatch.pipeline

import java.io.{PrintWriter, StringWriter}

import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{ApiCallFailure, Config, Helpers, ModuleStatusReport, NoNewDataException, OverwatchScope, SchemaTools, SparkSessionWrapper}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import org.apache.spark.sql.functions.{array, col, explode, lit}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import scala.collection.mutable.ArrayBuffer


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
  with SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sw = new StringWriter

  private def apiByID[T](endpoint: String, apiType: String,
                         ids: Array[T], idsKey: String,
                         extraQuery: Option[Map[String, Any]] = None): Array[String] = {
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(6))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport

    try {
    ids.flatMap(id => {
      val rawQuery = Map(
        idsKey -> id
      )

      val query = if (extraQuery.nonEmpty) {
        extraQuery.get ++ rawQuery
      } else rawQuery

        val apiCall = ApiCall(endpoint, Some(query))
        if (apiType == "post") apiCall.executePost().asStrings
        else apiCall.executeGet().asStrings
    })
    } catch {
      case e: NoNewDataException =>
        setNewDataRetrievedFlag(false)
        val failMsg = s"Failed: No New Data Retrieved ${endpoint} with query: $extraQuery " +
          s"and ids ${ids.mkString(", ")}! Skipping ${endpoint} load"
        println(failMsg)
        logger.log(Level.WARN, failMsg, e)
        Array("FAILURE")
      case e: Throwable =>
        val failMsg = s"Failed: ${endpoint} with query: $extraQuery " +
          s"and ids ${ids.mkString(", ")}! Skipping ${endpoint} load"
        println(failMsg)
        println(e)
        logger.log(Level.WARN, failMsg, e)
        Array("FAILURE")
    }
  }

  private def collectJobsIDs()(df: DataFrame): DataFrame = {
    if (config.overwatchScope.contains(OverwatchScope.jobRuns)) {
      //TODO -- Add the .distinct back -- bug -- distinct is resuling in no return values????
      //DEBUG
      //        val debugX = df.select('job_id).as[Long].collect()
      //        val debugY = df.select('job_id).distinct.as[Long].collect()
      // setJobIDs(df.select('job_id).distinct().as[Long].collect())
      setJobIDs(df.select('job_id).distinct().as[Long].collect())
    }
    df
  }

  lazy private val appendJobsProcess = EtlDefinition(
    workspace.getJobsDF.cache(),
    Some(Seq(collectJobsIDs())),
    append(jobsTarget),
    Module(1001, "Bronze_Jobs")
  )

  // TODO -- add assertion that df count == total count from API CALL
  private def prepJobRunsDF: DataFrame = {
    val extraQuery = Map("completed_only" -> true)
    val jobRuns = apiByID("jobs/runs/list",
      "get", jobIDs, "job_id", Some(extraQuery))

    if (newDataRetrieved) {
      // TODO -- add filter to remove runs before fromTS
      spark.read.json(Seq(jobRuns: _*).toDS()).select(explode('runs).alias("runs"))
        .select(col("runs.*"))
    } else jobRuns.toSeq.toDF("FAILURE")
  }

  lazy private val appendJobRunsProcess = EtlDefinition(
    prepJobRunsDF,
    None,
    append(jobRunsTarget, newDataOnly = true),
    Module(1007, "Bronze_JobRuns")
  )

  private def collectClusterIDs()(df: DataFrame): DataFrame = {
    if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
      // TODO -- DEBUG -- DISTINCT
      // TODO -- Add the .distinct back -- bug -- distinct is resuling in no return values????
      // TODO -- SAME AS jobRuns
      //        setClusterIDs(df.select('cluster_id).distinct().as[String].collect())
      setClusterIDs(df.select('cluster_id).distinct().as[String].collect())
    }
    df
  }

  private def collectEventLogPaths()(df: DataFrame): DataFrame = {
    if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
      val colsDF = df.select($"cluster_log_conf.*")
      val cols = colsDF.columns.map(c => s"${c}.destination")
      val logsDF = df.select($"cluster_log_conf.*", $"cluster_id".alias("cluster_id"))
      val eventLogsPathGlobDF = cols.flatMap(
        c => {
          logsDF.select(col("cluster_id"), col(c)).filter(col("destination").isNotNull).distinct
            .select(
              array(col("destination"), col("cluster_id"),
                lit("eventlog"), lit("*"), lit("*"), lit("eventlo*"))
            ).rdd
            .map(r => r.getSeq[String](0).mkString("/")).collect()
        }).distinct
        .flatMap(Helpers.globPath)
        .toSeq.toDF("filename")
      setEventLogGlob(eventLogsPathGlobDF)
    }
    df
  }

  lazy private val appendClustersProcess = EtlDefinition(
    workspace.getClustersDF.cache(),
    Some(Seq(collectClusterIDs(), collectEventLogPaths())),
    append(clustersTarget),
    Module(1002, "Bronze_Clusters")
  )

  lazy private val appendPoolsProcess = EtlDefinition(
    workspace.getPoolsDF,
    None,
    append(poolsTarget),
    Module(1003, "Bronze_Pools")
  )

  lazy private val appendAuditLogsProcess = EtlDefinition(
    workspace.getAuditLogsDF,
    None,
    append(auditLogsTarget, newDataOnly = true),
    Module(1004, "Bronze_AuditLogs")
  )

  private def prepClusterEventLogs(moduleID: Int): DataFrame = {
    val extraQuery = Map(
      "start_time" -> config.fromTime(moduleID).asUnixTimeMilli,
      "end_time" -> config.pipelineSnapTime.asUnixTimeMilli
    )

      // TODO -- add assertion that df count == total count from API CALL
      val clusterEvents = apiByID("clusters/events", "post",
        clusterIDs, "cluster_id", Some(extraQuery))

    if (newDataRetrieved) {
      spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
        .select(col("events.*"))
    } else clusterEvents.toSeq.toDF("FAILURE")

  }

  private val appendClusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs")
  lazy private val appendClusterEventLogsProcess = EtlDefinition(
    prepClusterEventLogs(appendClusterEventLogsModule.moduleID),
    None,
    append(clusterEventsTarget, newDataOnly = true),
    appendClusterEventLogsModule
  )

  lazy private val appendSparkEventLogsProcess = EtlDefinition(
    workspace.getEventLogsDF(sparkEventsLogGlob),
    None,
    append(sparkEventLogsTarget),
    Module(1006, "Bronze_EventLogs")
  )

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Unit = {
    val reports = ArrayBuffer[ModuleStatusReport]()
    if (config.overwatchScope.contains(OverwatchScope.jobs)) {
      reports.append(appendJobsProcess.process())
    }
    if (config.overwatchScope.contains(OverwatchScope.jobRuns)) {
      reports.append(appendJobRunsProcess.process())
    }
    if (config.overwatchScope.contains(OverwatchScope.clusters)) {
      reports.append(appendClustersProcess.process())
    }
    if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
      reports.append(appendClusterEventLogsProcess.process())
    }
    if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
      reports.append(appendSparkEventLogsProcess.process())
    }
    if (config.overwatchScope.contains(OverwatchScope.pools)) {
      reports.append(appendPoolsProcess.process())
    }
    if (config.overwatchScope.contains(OverwatchScope.audit)) {
      reports.append(appendAuditLogsProcess.process())
    }
    //    DOES NOT PRESERVER NECESSARY ORDERING
    //    val reports = Config.overwatchScope.map {
    //      case OverwatchScope.jobs => appendJobs
    //      case OverwatchScope.jobRuns => appendJobRuns(jobIDs)
    //      case OverwatchScope.clusters => appendClusters
    //      case OverwatchScope.clusterEvents => appendClusterEventLogs(clusterIDs)
    //      case OverwatchScope.pools => appendPools
    //      case OverwatchScope.audit => appendAuditLogs
    //      case OverwatchScope.sparkEvents => appendEventLogs
    //    }

    // TODO -- update reports to note if post_porcessing / optimize was run
    val pipelineReportTarget = PipelineTable("pipeline_report", Array("Overwatch_RunID"), "Pipeline_SnapTS")
    database.write(reports.toDF, pipelineReportTarget)

    postProcessor.analyze()
    postProcessor.optimize()


  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = new Bronze(workspace, workspace.database, workspace.getConfig)
//    .setWorkspace(workspace).setDatabase(workspace.database)

}