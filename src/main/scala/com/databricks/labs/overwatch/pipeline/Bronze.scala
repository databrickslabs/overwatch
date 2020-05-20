package com.databricks.labs.overwatch.pipeline

import java.io.{PrintWriter, StringWriter}

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, ModuleStatusReport, OverwatchScope, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SparkSessionWrapper with BronzeTransforms {

  envInit()
  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private val appendJobsProcess = EtlDefinition(
    workspace.getJobsDF.cache(),
    Some(Seq(collectJobsIDs())),
    append(BronzeTargets.jobsTarget),
    Module(1001, "Bronze_Jobs")
  )

  private val jobRunsModule = Module(1007, "Bronze_JobRuns")
  lazy private val appendJobRunsProcess = EtlDefinition(
    prepJobRunsDF(config.apiEnv),
    None,
    append(BronzeTargets.jobRunsTarget, newDataOnly = true),
    jobRunsModule
  )

  lazy private val appendJobRunsAuditFilteredProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asDF,
    Some(Seq(getNewJobRuns(config.apiEnv, config.fromTime(jobRunsModule.moduleID).asColumnTS))),
    append(BronzeTargets.jobRunsTarget, newDataOnly = true),
    jobRunsModule
  )

  private val appendClustersModule = Module(1002, "Bronze_Clusters")
  lazy private val appendClustersAPIProcess = EtlDefinition(
    workspace.getClustersDF.cache(),
    Some(Seq(collectClusterIDs())),
    append(BronzeTargets.clustersTarget),
    appendClustersModule
  )

  lazy private val appendPoolsProcess = EtlDefinition(
    workspace.getPoolsDF,
    None,
    append(BronzeTargets.poolsTarget),
    Module(1003, "Bronze_Pools")
  )

  lazy private val appendAuditLogsProcess = EtlDefinition(
    getAuditLogsDF(config.auditLogPath.get),
    Some(Seq(
      collectClusterIDs(
        config.fromTime(sparkEventLogsModule.moduleID).asColumnTS
      ))),
    append(BronzeTargets.auditLogsTarget, newDataOnly = true),
    Module(1004, "Bronze_AuditLogs")
  )

  private val appendClusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs")
  lazy private val appendClusterEventLogsProcess = EtlDefinition(
    prepClusterEventLogs(
      config.fromTime(appendClusterEventLogsModule.moduleID).asUnixTimeMilli,
      config.pipelineSnapTime.asUnixTimeMilli,
      config.apiEnv
    ),
    None,
    append(BronzeTargets.clusterEventsTarget, newDataOnly = true),
    appendClusterEventLogsModule
  )

  private def getEventLogPathsSourceDF: DataFrame = {
    if (config.overwatchScope.contains(OverwatchScope.audit)) BronzeTargets.auditLogsTarget.asDF
    else BronzeTargets.clustersTarget.asDF.filter('Pipeline_SnapTS === config.pipelineSnapTime.asColumnTS)
  }

  private val sparkEventLogsModule = Module(1006, "Bronze_EventLogs")
  lazy private val appendSparkEventLogsProcess = EtlDefinition(
    getEventLogPathsSourceDF,
    Some(Seq(
      collectEventLogPaths(
        config.fromTime(sparkEventLogsModule.moduleID).asColumnTS,
        config.isFirstRun,
        config.overwatchScope
      ),
      generateEventLogsDF(config.badRecordsPath, BronzeTargets.sparkEventLogsTarget)
    )),
    append(BronzeTargets.sparkEventLogsTarget),
    sparkEventLogsModule
  )

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Unit = {
    val reports = ArrayBuffer[ModuleStatusReport]()

    if (config.overwatchScope.contains(OverwatchScope.audit)) {
      reports.append(appendAuditLogsProcess.process())
      // TODO -- keeping these two api events with audit since there appears to be more granular data available
      //  from the api than from audit -- VERIFY
      if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) reports.append(appendClusterEventLogsProcess.process())
      if (config.overwatchScope.contains(OverwatchScope.jobRuns)) reports.append(appendJobRunsAuditFilteredProcess.process())
      //      if (config.overwatchScope.contains(OverwatchScope.jobs)) reports.append(appendJobsProcess.process())
      //      The following are disabled when audit is in scope
      //      if (config.overwatchScope.contains(OverwatchScope.clusters)) reports.append(appendClustersAPIProcess.process())
      //      if (config.overwatchScope.contains(OverwatchScope.pools)) reports.append(appendPoolsProcess.process())
    } else {
      if (config.overwatchScope.contains(OverwatchScope.jobs)) {
        reports.append(appendJobsProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.jobRuns)) {
        reports.append(appendJobRunsProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.clusters)) {
        reports.append(appendClustersAPIProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
        reports.append(appendClusterEventLogsProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
        reports.append(appendSparkEventLogsProcess.process())
      }
//      if (config.overwatchScope.contains(OverwatchScope.pools)) {
//        reports.append(appendPoolsProcess.process())
//      }
    }

    if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) reports.append(appendSparkEventLogsProcess.process())

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

    finalizeRun(reports.toArray)


  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = new Bronze(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}