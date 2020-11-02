package com.databricks.labs.overwatch.pipeline

import java.io.{File, PrintWriter, StringWriter}

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, Module, ModuleStatusReport, OverwatchScope, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with BronzeTransforms {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private val appendJobsProcess = EtlDefinition(
    workspace.getJobsDF,
    None,
    append(BronzeTargets.jobsSnapshotTarget),
    Module(1001, "Bronze_Jobs_Snapshot")
  )

  private val appendClustersModule = Module(1002, "Bronze_Clusters_Snapshot")
  lazy private val appendClustersAPIProcess = EtlDefinition(
    workspace.getClustersDF,
    Some(Seq(cleanseRawClusterSnapDF(config.cloudProvider))),
    append(BronzeTargets.clustersSnapshotTarget),
    appendClustersModule
  )

  // TODO -- Not implemented
  lazy private val appendPoolsProcess = EtlDefinition(
    workspace.getPoolsDF,
    None,
    append(BronzeTargets.poolsTarget),
    Module(1003, "Bronze_Pools")
  )

  private val appendAuditLogsModule = Module(1004, "Bronze_AuditLogs")
  lazy private val appendAuditLogsProcess = EtlDefinition(
    getAuditLogsDF(
      config.auditLogConfig,
      config.isFirstRun,
      config.untilTime(appendAuditLogsModule.moduleID).asLocalDateTime,
      config.fromTime(appendAuditLogsModule.moduleID).asLocalDateTime,
      BronzeTargets.auditLogAzureLandRaw,
      config.runID
    ),
    None,
    append(BronzeTargets.auditLogsTarget),
    appendAuditLogsModule
  )

  private val appendClusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs")
  lazy private val appendClusterEventLogsProcess = EtlDefinition(
    prepClusterEventLogs(
      BronzeTargets.auditLogsTarget,
      config.fromTime(appendClusterEventLogsModule.moduleID),
      config.untilTime(appendClusterEventLogsModule.moduleID),
      config.apiEnv
    ),
    None,
    append(BronzeTargets.clusterEventsTarget),
    appendClusterEventLogsModule
  )

  private val sparkEventLogsModule = Module(1006, "Bronze_EventLogs")
  private def getEventLogPathsSourceDF: DataFrame = {
    if (config.overwatchScope.contains(OverwatchScope.audit)) BronzeTargets.auditLogsTarget.asDF
    else BronzeTargets.clustersSnapshotTarget.asDF
      .filter('Pipeline_SnapTS === config.pipelineSnapTime.asColumnTS)
  }
  lazy private val appendSparkEventLogsProcess = EtlDefinition(
    getEventLogPathsSourceDF,
    Some(Seq(
      collectEventLogPaths(
        config.fromTime(sparkEventLogsModule.moduleID).asColumnTS,
        config.untilTime(sparkEventLogsModule.moduleID).asColumnTS,
        config.fromTime(sparkEventLogsModule.moduleID).asUnixTimeMilli,
        config.untilTime(sparkEventLogsModule.moduleID).asUnixTimeMilli,
        SilverTargets.clustersSpecTarget,
        config.isFirstRun
      ),
      generateEventLogsDF(database, config.badRecordsPath, BronzeTargets.processedEventLogs) //,
//      saveAndLoadTempEvents(database, BronzeTargets.sparkEventLogsTempTarget) // TODO -- Perf testing without
    )),
    append(BronzeTargets.sparkEventLogsTarget), // Not new data only -- date filters handled in function logic
    sparkEventLogsModule
  )

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Unit = {

    restoreSparkConf()

    if (config.debugFlag) println(s"DEBUG: CLOUD PROVIDER = ${config.cloudProvider}")
    setCloudProvider(config.cloudProvider)

    if (config.cloudProvider == "azure") {
      val rawAzureAuditEvents = landAzureAuditLogDF(
        config.auditLogConfig.azureAuditLogEventhubConfig.get,
        config.isFirstRun
      )
      database.write(rawAzureAuditEvents, BronzeTargets.auditLogAzureLandRaw)

//      Helpers.fastDrop(BronzeTargets.auditLogAzureLandRaw.tableFullName, "azure")

      val rawProcessCompleteMsg = "Azure audit ingest process complete"
      if (config.debugFlag) println(rawProcessCompleteMsg)
      logger.log(Level.INFO, rawProcessCompleteMsg)
    }

    appendAuditLogsProcess.process()

      /** Current cluster snapshot is important because cluster spec details are only available from audit logs
       * during create/edit events. Thus all existing clusters created/edited last before the audit logs were
       * enabled will be missing all info. This is especially important for overwatch early stages
       */
      if (config.overwatchScope.contains(OverwatchScope.clusters))
        appendClustersAPIProcess.process()
      if (config.overwatchScope.contains(OverwatchScope.clusterEvents))
        appendClusterEventLogsProcess.process()
      if (config.overwatchScope.contains(OverwatchScope.jobs))
        appendJobsProcess.process()
      if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
        appendSparkEventLogsProcess.process()
//        // TODO -- Temporary until refactor
//        Helpers.fastDrop(
//          BronzeTargets.sparkEventLogsTempTarget.tableFullName,
//          config.cloudProvider
//        )
      }

    initiatePostProcessing()

  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = new Bronze(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}