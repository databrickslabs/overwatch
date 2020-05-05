package com.databricks.labs.overwatch.pipeline

import java.io.File

import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, ModuleStatusReport, OverwatchScope, SparkSessionWrapper, Helpers}
import org.apache.hadoop.fs.Path

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import org.apache.spark.sql.functions.{array, col, explode, lit}
import org.apache.log4j.Level


class Bronze extends Pipeline with SparkSessionWrapper {

  import spark.implicits._

  private def apiByID[T](moduleID: Int, endpoint: String, apiType: String,
                         ids: Array[T], idsKey: String,
                         extraQuery: Option[Map[String, Any]] = None): Array[String] = {
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(6))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport

    ids.flatMap(id => {
      val rawQuery = Map(
        idsKey -> id,
        "start_time" -> Config.fromTime(moduleID).asUnixTime
      )

      val query = if (extraQuery.nonEmpty) {
        extraQuery.get ++ rawQuery
      } else rawQuery

      val apiCall = ApiCall(endpoint, Some(query))
      if (apiType == "post") apiCall.executePost().asStrings
      else apiCall.executeGet().asStrings
    })
  }

  def appendJobs: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1001
    val moduleName = "Bronze_Jobs"
    val success: Boolean = try {
      val df = workspace.getJobsDF
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))
        .cache()
      append("jobs_master", df)
      if (Config.overwatchScope.contains(OverwatchScope.jobRuns)) {
        setJobIDs(df.select('job_id).distinct().as[Long].collect())
      }
      df.unpersist(blocking = true)
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendJobRuns(_jobIDs: Array[Long]): ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1007
    val moduleName = "Bronze_JobRuns"
    val success: Boolean = try {

      val extraQuery = Map("completed_only" -> true)
      val jobRuns = apiByID(moduleID, "jobs/runs/list",
        "get", _jobIDs, "job_id", Some(extraQuery))

      // TODO -- add filter to remove runs before fromTS
      val df = spark.read.json(Seq(jobRuns: _*).toDS()).select(explode('runs).alias("runs"))
        .select(col("runs.*"))
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))
        .cache()
      append("runs_by_job_master", df)
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendClusters: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1002
    val moduleName = "Bronze_Clusters"
    val success: Boolean = try {
      val df = workspace.getClustersDF
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))
        .cache()
      append("cluster_master", df)
      if (Config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
        setClusterIDs(df.select('cluster_id).distinct().as[String].collect())
      }

      if (Config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
        val colsDF = df.select($"data.cluster_log_conf.*")
        val cols = colsDF.columns.map(c => s"${c}.destination")
        val logsDF = df.select($"data.cluster_log_conf.*", $"data.cluster_id".alias("cluster_id"))
        val eventLogsPathGlob = cols.flatMap(
          c => {
            logsDF.select(col("cluster_id"), col(c)).filter(col("destination").isNotNull).distinct
              .select(
                array(col("destination"), col("cluster_id"),
                  lit("eventlog"), lit("*"), lit("*"), lit("eventlog"))
              ).rdd
              .map(r => r.getSeq[String](0).mkString("/")).collect()
          }).distinct
          .flatMap(Helpers.globPath)
          .toSeq.toDF("filenames")
        setEventLogGlob(eventLogsPathGlob)
      }

      df.unpersist(blocking = true)
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )

  }

  def appendPools: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1003
    val moduleName = "Bronze_Pools"
    val success: Boolean = try {
      val df = workspace.getPoolsDF
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))
      append("pools_master", df)
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendAuditLogs: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1004
    val moduleName = "Bronze_AuditLogs"
    // Audit logs are Daily so get date from TS

    //    val fromDate =
    val success: Boolean = try {
      val df = workspace.getAuditLogsDF
        .filter('Date.between(
          Config.fromTime(moduleID).asColumnTS,
          Config.pipelineSnapTime.asColumnTS
        ))
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))
      append("audit_log_master", df)
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = Config.fromTime(moduleID).asMidnightEpochMilli,
      untilTS = Config.pipelineSnapTime.asMidnightEpochMilli,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendClusterEventLogs(_clusterIDs: Array[String]): ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1005
    val moduleName = "Bronze_ClusterEventLogs"
    val success: Boolean = try {

      val extraQuery = Map(
        "start_time" -> Config.fromTime(moduleID).asUnixTime,
        "end_time" -> Config.pipelineSnapTime.asUnixTime
      )
      val clusterEvents = apiByID(moduleID, "clusters/events", "post",
        _clusterIDs, "cluster_id", Some(extraQuery))

      val df = spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
        .select(col("events.*"))
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))

      append("events_by_cluster_master", df)
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = Config.fromTime(moduleID).asUnixTime,
      untilTS = Config.pipelineSnapTime.asUnixTime,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendEventLogs: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1006
    val moduleName = "Bronze_EventLogs"
    val success: Boolean = try {
      val df = workspace.getEventLogsDF(sparkEventsLogGlob)
        .withColumn("Overwatch_RunID", lit(Config.runID))
        .withColumn("Pipeline_SnapTS", lit(Config.pipelineSnapTime.asUnixTime))
      append("spark_events_master", df)
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed: ${moduleName}", e); false
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      success = success,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  // TODO -- Add try/catch around each scope
  def run(): Boolean = {
    val reports = Config.overwatchScope.map {
      case OverwatchScope.jobs => appendJobs
      case OverwatchScope.jobRuns => appendJobRuns(jobIDs)
      case OverwatchScope.clusters => appendClusters
      case OverwatchScope.clusterEvents => appendClusterEventLogs(clusterIDs)
      case OverwatchScope.pools => appendPools
      case OverwatchScope.audit => appendAuditLogs
      case OverwatchScope.sparkEvents => appendEventLogs
    }

    append("pipeline_report", reports.toSeq.toDF)


  }

}

object Bronze {
  def apply(workspace: Workspace, database: Database): Bronze = new Bronze()
    .setWorkspace(workspace).setDatabase(database)

}