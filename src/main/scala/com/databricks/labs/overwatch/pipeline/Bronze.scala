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

  def getAllTargets: Array[PipelineTable] = {
    Array(
      BronzeTargets.jobsSnapshotTarget,
      BronzeTargets.clustersSnapshotTarget,
      BronzeTargets.poolsTarget,
      BronzeTargets.auditLogsTarget,
      BronzeTargets.auditLogAzureLandRaw,
      BronzeTargets.clusterEventsTarget,
      BronzeTargets.sparkEventLogsTarget,
      BronzeTargets.processedEventLogs,
      BronzeTargets.cloudMachineDetail
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
  lazy private val  timeTest = s"DEBUG: AUDIT LOG MODULE bronze.scala = From -> To | ${auditLogsModule.fromTime.asTSString} --> ${auditLogsModule.untilTime.asTSString}"
  lazy private val appendAuditLogsProcess = ETLDefinition(
    getAuditLogsDF(
      config.auditLogConfig,
      config.isFirstRun,
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
      BronzeTargets.auditLogsTarget,
      clusterEventLogsModule.fromTime,
      clusterEventLogsModule.untilTime,
      config.apiEnv,
      config.organizationId
    ),
    append(BronzeTargets.clusterEventsTarget)
  )

  lazy private val sparkEventLogsModule = Module(1006, "Bronze_SparkEventLogs", this, Array(1004))
  lazy private val appendSparkEventLogsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, auditLogsIncrementalCols: _*),
    Seq(
      collectEventLogPaths(
        sparkEventLogsModule.fromTime,
        sparkEventLogsModule.untilTime,
        BronzeTargets.processedEventLogs,
        BronzeTargets.clustersSnapshotTarget,
        config.isFirstRun
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
      spark, config, "azure_audit_log_preProcessing"
    )

    database.write(optimizedAzureAuditEvents, BronzeTargets.auditLogAzureLandRaw,pipelineSnapTime.asColumnTS)

    //      Helpers.fastDrop(BronzeTargets.auditLogAzureLandRaw.tableFullName, "azure")

    val rawProcessCompleteMsg = "Azure audit ingest process complete"
    if (config.debugFlag) println(rawProcessCompleteMsg)
    logger.log(Level.INFO, rawProcessCompleteMsg)
  }

  private def executeModules(): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.audit => {
//        val x = auditLogsModule
//        println(timeTest)
//        logger.log(Level.INFO, timeTest)
        auditLogsModule.execute(appendAuditLogsProcess)
      }
      case OverwatchScope.clusters => clustersSnapshotModule.execute(appendClustersAPIProcess)
      case OverwatchScope.clusterEvents => clusterEventLogsModule.execute(appendClusterEventLogsProcess)
      case OverwatchScope.jobs => jobsSnapshotModule.execute(appendJobsProcess)
      case OverwatchScope.pools => poolsSnapshotModule.execute(appendPoolsProcess)
      case OverwatchScope.sparkEvents => sparkEventLogsModule.execute(appendSparkEventLogsProcess)
      case _ =>
    }
  }

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Pipeline = {

    restoreSparkConf()

    if (config.debugFlag) println(s"DEBUG: CLOUD PROVIDER = ${config.cloudProvider}")

    if (config.cloudProvider == "azure") {
      landAzureAuditEvents()
    }

    executeModules()


//    appendAuditLogsProcess.process()
//
//    /** Current cluster snapshot is important because cluster spec details are only available from audit logs
//     * during create/edit events. Thus all existing clusters created/edited last before the audit logs were
//     * enabled will be missing all info. This is especially important for overwatch early stages
//     */
//    if (config.overwatchScope.contains(OverwatchScope.clusters)) {
//      appendClustersAPIProcess.process()
//    }
//
//    if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
//      appendClusterEventLogsProcess.process()
//    }
//
//    if (config.overwatchScope.contains(OverwatchScope.jobs)) {
//      appendJobsProcess.process()
//    }
//
//    if (config.overwatchScope.contains(OverwatchScope.pools)) {
//      appendPoolsProcess.process()
//    }
//
//    if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
//      appendSparkEventLogsProcess.process()
//    }

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
  //    .setWorkspace(workspace).setDatabase(workspace.database)

}