package com.databricks.labs.overwatch.pipeline

import java.io.{File, PrintWriter, StringWriter}

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with BronzeTransforms {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private val appendJobsProcess = ETLDefinition(
    Module(1001, "Bronze_Jobs_Snapshot", this),
    workspace.getJobsDF,
    None,
    append(BronzeTargets.jobsSnapshotTarget)
  )

  lazy private val appendClustersAPIProcess = ETLDefinition(
    Module(1002, "Bronze_Clusters_Snapshot", this),
    workspace.getClustersDF,
    Some(Seq(cleanseRawClusterSnapDF(config.cloudProvider))),
    append(BronzeTargets.clustersSnapshotTarget)
  )

  lazy private val appendPoolsProcess = ETLDefinition(
    Module(1003, "Bronze_Pools", this),
    workspace.getPoolsDF,
    Some(Seq(cleanseRawPoolsDF())),
    append(BronzeTargets.poolsTarget)
  )

  private val auditLogModule = Module(1004, "Bronze_AuditLogs", this)
  lazy private val appendAuditLogsProcess = ETLDefinition(
    auditLogModule,
    getAuditLogsDF(
      config.auditLogConfig,
      config.isFirstRun,
      config.cloudProvider,
      config.fromTime(appendAuditLogsModule.moduleId).asLocalDateTime,
      config.untilTime(appendAuditLogsModule.moduleId).asLocalDateTime,
      BronzeTargets.auditLogAzureLandRaw,
      config.runID,
      config.organizationId
    ),
    None,
    append(BronzeTargets.auditLogsTarget)
  )

  private val appendClusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs")
  lazy private val appendClusterEventLogsProcess = ETLDefinition(
    prepClusterEventLogs(
      BronzeTargets.auditLogsTarget,
      config.fromTime(appendClusterEventLogsModule.moduleId),
      config.untilTime(appendClusterEventLogsModule.moduleId),
      config.apiEnv,
      config.organizationId
    ),
    None,
    append(BronzeTargets.clusterEventsTarget),
    appendClusterEventLogsModule
  )

  private val sparkEventLogsModule = Module(1006, "Bronze_EventLogs")

  // TODO -- Error if auditLogsTarget does not exist -- determine how to handle
  //  decided -- audit logs are required for sparkEvents anyway -- just remove the
  //  if statement
//  private def getEventLogPathsSourceDF: DataFrame = {
//    if (BronzeTargets.auditLogsTarget.exists) BronzeTargets.auditLogsTarget.asDF
//    else BronzeTargets.clustersSnapshotTarget.asDF
//      .filter('Pipeline_SnapTS === config.pipelineSnapTime.asColumnTS)
//  }

  lazy private val appendSparkEventLogsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, auditLogsIncrementalCols: _*),
    Some(Seq(
      collectEventLogPaths(
        config.fromTime(sparkEventLogsModule.moduleId).asColumnTS,
        config.untilTime(sparkEventLogsModule.moduleId).asColumnTS,
        config.fromTime(sparkEventLogsModule.moduleId).asUnixTimeMilli,
        config.untilTime(sparkEventLogsModule.moduleId).asUnixTimeMilli,
        SilverTargets.clustersSpecTarget,
        BronzeTargets.clustersSnapshotTarget,
        config.isFirstRun
      ),
      generateEventLogsDF(
        database,
        config.badRecordsPath,
//        config.daysToProcess(sparkEventLogsModule.moduleID),
        BronzeTargets.processedEventLogs,
        config.organizationId) //,
      //      saveAndLoadTempEvents(database, BronzeTargets.sparkEventLogsTempTarget) // TODO -- Perf testing without
    )),
    append(BronzeTargets.sparkEventLogsTarget), // Not new data only -- date filters handled in function logic
    sparkEventLogsModule
  )

  private def declareModules = {
    config.overwatchScope.map {
      case OverwatchScope.audit => registerModule(1004, "Bronze_AuditLogs", appendAuditLogsProcess)
      case OverwatchScope.clusters => registerModule(1002, "Bronze_Clusters_Snapshot", appendClustersAPIProcess)
      case OverwatchScope.clusterEvents => registerModule(1005, "Bronze_ClusterEventLogs", appendClusterEventLogsProcess)
      case OverwatchScope.jobs => registerModule(1001, "Bronze_Jobs_Snapshot", appendJobsProcess)
      case OverwatchScope.pools => registerModule(1003, "Bronze_Pools", appendPoolsProcess)
      case OverwatchScope.sparkEvents => registerModule(1006, "Bronze_SparkEventLogs", appendSparkEventLogsProcess)
    }
  }

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Pipeline = {

    restoreSparkConf()

    if (config.debugFlag) println(s"DEBUG: CLOUD PROVIDER = ${config.cloudProvider}")

    if (config.cloudProvider == "azure") {
      val rawAzureAuditEvents = landAzureAuditLogDF(
        config.auditLogConfig.azureAuditLogEventhubConfig.get,
        config.isFirstRun,
        config.organizationId,
        config.runID
      )

      val optimizedAzureAuditEvents = PipelineFunctions.optimizeWritePartitions(
        rawAzureAuditEvents,
        400,
        BronzeTargets.auditLogAzureLandRaw,
        spark, config, None
      )

      database.write(optimizedAzureAuditEvents, BronzeTargets.auditLogAzureLandRaw)

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
    if (config.overwatchScope.contains(OverwatchScope.clusters)) {
      appendClustersAPIProcess.process()
    }

    if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
      appendClusterEventLogsProcess.process()
    }

    if (config.overwatchScope.contains(OverwatchScope.jobs)) {
      appendJobsProcess.process()
    }

    if (config.overwatchScope.contains(OverwatchScope.pools)) {
      appendPoolsProcess.process()
    }

    if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
      appendSparkEventLogsProcess.process()
    }

//    initiatePostProcessing()
    this

  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = {
    new Bronze(workspace, workspace.database, workspace.getConfig)
      .initializePipelineState // Initialize all existing module states

  }
  //    .setWorkspace(workspace).setDatabase(workspace.database)

}