package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.{Level, Logger}


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with BronzeTransforms {

  /**
   * Enable access to Bronze pipeline tables externally.
   * @return
   */
  def getAllTargets: Map[PipelineTable,Option[Int]] = {
    Map(
      BronzeTargets.jobsSnapshotTarget -> Some(1001),
      BronzeTargets.clustersSnapshotTarget -> Some(1002),
      BronzeTargets.poolsTarget -> Some(1003),
      BronzeTargets.auditLogsTarget -> Some(1004),
      BronzeTargets.auditLogAzureLandRaw -> None,
      BronzeTargets.clusterEventsTarget -> Some(1005),
      BronzeTargets.sparkEventLogsTarget -> Some(1006),
      BronzeTargets.processedEventLogs -> None,
      BronzeTargets.cloudMachineDetail -> None
    )
  }

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private val jobsSnapshotModule = Module(1001, "Bronze_Jobs_Snapshot", this)
  lazy private val appendJobsProcess = ETLDefinition(
    workspace.getJobsDF,
    append(BronzeTargets.jobsSnapshotTarget)
  )

  lazy private val clustersSnapshotModule = Module(1002, "Bronze_Clusters_Snapshot", this)
  lazy private val appendClustersAPIProcess = ETLDefinition(
    workspace.getClustersDF,
    Seq(cleanseRawClusterSnapDF(config.cloudProvider)),
    append(BronzeTargets.clustersSnapshotTarget)
  )

  lazy private val poolsSnapshotModule = Module(1003, "Bronze_Pools", this)
  lazy private val appendPoolsProcess = ETLDefinition(
    workspace.getPoolsDF,
    Seq(cleanseRawPoolsDF()),
    append(BronzeTargets.poolsTarget)
  )

  lazy private val auditLogsModule = Module(1004, "Bronze_AuditLogs", this)
  lazy private val appendAuditLogsProcess = ETLDefinition(
    getAuditLogsDF(
      config.auditLogConfig,
      config.cloudProvider,
      auditLogsModule.fromTime.asLocalDateTime,
      auditLogsModule.untilTime.asLocalDateTime,
      BronzeTargets.auditLogAzureLandRaw,
      config.runID,
      config.organizationId
    ),
    append(BronzeTargets.auditLogsTarget)
  )

  lazy private val clusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs", this, Array(1004))
  lazy private val appendClusterEventLogsProcess = ETLDefinition(
    prepClusterEventLogs(
      BronzeTargets.auditLogsTarget.asIncrementalDF(clusterEventLogsModule, auditLogsIncrementalCols),
      BronzeTargets.clustersSnapshotTarget,
      clusterEventLogsModule.fromTime,
      clusterEventLogsModule.untilTime,
      config.apiEnv,
      config.organizationId
    ),
    append(BronzeTargets.clusterEventsTarget)
  )

  lazy private val sparkEventLogsModule = Module(1006, "Bronze_SparkEventLogs", this, Array(1004))
  lazy private val appendSparkEventLogsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, auditLogsIncrementalCols),
    Seq(
      collectEventLogPaths(
        sparkEventLogsModule.fromTime,
        sparkEventLogsModule.untilTime,
        BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, auditLogsIncrementalCols, 30),
        BronzeTargets.clustersSnapshotTarget
      ),
      generateEventLogsDF(
        database,
        config.badRecordsPath,
        BronzeTargets.processedEventLogs,
        config.organizationId,
        config.runID,
        pipelineSnapTime.asColumnTS
      ) //,
    ),
    append(BronzeTargets.sparkEventLogsTarget) // Not new data only -- date filters handled in function logic
  )

  // TODO -- convert and merge this into audit's ETLDefinition
  private def landAzureAuditEvents(): Unit = {
    val rawAzureAuditEvents = landAzureAuditLogDF(
      config.auditLogConfig.azureAuditLogEventhubConfig.get,
      config.isFirstRun,
      config.organizationId,
      config.runID
    )

    val optimizedAzureAuditEvents = PipelineFunctions.optimizeWritePartitions(
      rawAzureAuditEvents,
      BronzeTargets.auditLogAzureLandRaw,
      spark, config, "azure_audit_log_preProcessing", getTotalCores
    )

    database.write(optimizedAzureAuditEvents, BronzeTargets.auditLogAzureLandRaw,pipelineSnapTime.asColumnTS)

    val rawProcessCompleteMsg = "Azure audit ingest process complete"
    if (config.debugFlag) println(rawProcessCompleteMsg)
    logger.log(Level.INFO, rawProcessCompleteMsg)
  }

  private def executeModules(): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.audit => auditLogsModule.execute(appendAuditLogsProcess)
      case OverwatchScope.clusters => clustersSnapshotModule.execute(appendClustersAPIProcess)
      case OverwatchScope.clusterEvents => clusterEventLogsModule.execute(appendClusterEventLogsProcess)
      case OverwatchScope.jobs => jobsSnapshotModule.execute(appendJobsProcess)
      case OverwatchScope.pools => poolsSnapshotModule.execute(appendPoolsProcess)
      case OverwatchScope.sparkEvents => sparkEventLogsModule.execute(appendSparkEventLogsProcess)
      case _ =>
    }
  }

  def run(): Pipeline = {

    restoreSparkConf()

    if (config.debugFlag) println(s"DEBUG: CLOUD PROVIDER = ${config.cloudProvider}")

    if (config.cloudProvider == "azure") {
      landAzureAuditEvents()
    }

    executeModules()

    initiatePostProcessing()
    this

  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = {
    new Bronze(workspace, workspace.database, workspace.getConfig)
      .initPipelineRun()
      .loadStaticDatasets()
  }

}