package com.databricks.labs.overwatch.pipeline

import java.io.{PrintWriter, StringWriter}

import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, ModuleStatusReport, OverwatchScope, SchemaTools, SparkSessionWrapper}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import org.apache.spark.sql.functions.{array, col, explode, lit}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer


class Bronze extends Pipeline with SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sw = new StringWriter

  private def apiByID[T](endpoint: String, apiType: String,
                         ids: Array[T], idsKey: String,
                         extraQuery: Option[Map[String, Any]] = None): Array[String] = {
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(6))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport

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
  }

  def appendJobs: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1001
    val moduleName = "Bronze_Jobs"
    val status: String = try {
      val df = workspace.getJobsDF
        .cache()
      append("jobs_bronze", df)
      if (Config.overwatchScope.contains(OverwatchScope.jobRuns)) {
        //TODO -- Add the .distinct back -- bug -- distinct is resuling in no return values????
        //DEBUG
//        val debugX = df.select('job_id).as[Long].collect()
//        val debugY = df.select('job_id).distinct.as[Long].collect()
        // setJobIDs(df.select('job_id).distinct().as[Long].collect())
        setJobIDs(df.select('job_id).as[Long].collect())
      }
      df.unpersist(blocking = true)
      "SUCCESS"
    } catch {
          // TODO -- Figure out how to bubble up the exceptions
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      status = status,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendJobRuns(_jobIDs: Array[Long]): ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1007
    val moduleName = "Bronze_JobRuns"
    val status: String = try {

      val extraQuery = Map("completed_only" -> true)
      val jobRuns = apiByID("jobs/runs/list",
        "get", _jobIDs, "job_id", Some(extraQuery))

      // TODO -- add filter to remove runs before fromTS
      val df = spark.read.json(Seq(jobRuns: _*).toDS()).select(explode('runs).alias("runs"))
        .select(col("runs.*"))
      append("jobruns_bronze", df)
      "SUCCESS"
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      status = status,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  // TODO - sparkConf -- convert struct to array
  def appendClusters: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1002
    val moduleName = "Bronze_Clusters"
    val status = try {
      val df = workspace.getClustersDF
        .cache()
      append("clusters_bronze", df)
      if (Config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
        // TODO -- DEBUG -- DISTINCT
        // TODO -- Add the .distinct back -- bug -- distinct is resuling in no return values????
        // TODO -- SAME AS jobRuns
//        setClusterIDs(df.select('cluster_id).distinct().as[String].collect())
        setClusterIDs(df.select('cluster_id).as[String].collect())
      }

      if (Config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
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

      df.unpersist(blocking = true)
      "SUCCESS"
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      status = status,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )

  }

  def appendPools: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1003
    val moduleName = "Bronze_Pools"
    val status: String = try {
      val df = workspace.getPoolsDF
      append("pools_bronze", df)
      "SUCCESS"
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      status = status,
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
    val status: String = try {
      val df = workspace.getAuditLogsDF
        .filter('Date.between(
          Config.fromTime(moduleID).asColumnTS,
          Config.pipelineSnapTime.asColumnTS
        ))
      append("audit_log_bronze", df, fromTime = Some(Config.fromTime(moduleID).asTSString))
      "SUCCESS"
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = Config.fromTime(moduleID).asMidnightEpochMilli,
      untilTS = Config.pipelineSnapTime.asMidnightEpochMilli,
      status = status,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendClusterEventLogs(_clusterIDs: Array[String]): ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1005
    val moduleName = "Bronze_ClusterEventLogs"
    val status: String = try {

      val extraQuery = Map(
        "start_time" -> Config.fromTime(moduleID).asUnixTime,
        "end_time" -> Config.pipelineSnapTime.asUnixTime
      )
      val clusterEvents = apiByID("clusters/events", "post",
        _clusterIDs, "cluster_id", Some(extraQuery))

      val df = spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
        .select(col("events.*"))

      // TODO -- When cols are arranged in abstract Table class, pull the zorder cols from there
      // Rearranging for zorder
      val orderedDF = SchemaTools.moveColumnsToFront(df, "cluster_id, timestamp".split(", "))

      append("cluster_events_bronze", orderedDF)
      "SUCCESS"
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = Config.fromTime(moduleID).asUnixTime,
      untilTS = Config.pipelineSnapTime.asUnixTime,
      status = status,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  def appendEventLogs: ModuleStatusReport = {
    val startTime = System.currentTimeMillis()
    val moduleID = 1006
    val moduleName = "Bronze_EventLogs"
    val status: String = try {
      val df = workspace.getEventLogsDF(sparkEventsLogGlob)
      append("spark_events_bronze", df, partitionBy = Array("event"))
      "SUCCESS"
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"Failed: ${moduleName}", e)
        e.printStackTrace(new PrintWriter(sw)).toString
      }
    }
    val endTime = System.currentTimeMillis()
    ModuleStatusReport(
      moduleID = moduleID,
      moduleName = moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = 0L,
      untilTS = 0L,
      status = status,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )
  }

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Boolean = {
    val reports = ArrayBuffer[ModuleStatusReport]()
    if (Config.overwatchScope.contains(OverwatchScope.jobs)) {
      reports.append(appendJobs)
    }
    if (Config.overwatchScope.contains(OverwatchScope.jobRuns)) {
      reports.append(appendJobRuns(jobIDs))
    }
    if (Config.overwatchScope.contains(OverwatchScope.clusters)) {
      reports.append(appendClusters)
    }
    if (Config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
      reports.append(appendClusterEventLogs(clusterIDs))
    }
    if (Config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
      reports.append(appendEventLogs)
    }
    if (Config.overwatchScope.contains(OverwatchScope.pools)) {
      reports.append(appendPools)
    }
    if (Config.overwatchScope.contains(OverwatchScope.audit)) {
      reports.append(appendAuditLogs)
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

    append("pipeline_report", reports.toDF)


  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = new Bronze()
    .setWorkspace(workspace).setDatabase(workspace.database)

}