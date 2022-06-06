package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{CloneDetail, Config, Helpers, OverwatchScope}
import org.apache.log4j.{Level, Logger}


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with BronzeTransforms {

  /**
   * Enable access to Bronze pipeline tables externally.
   *
   * @return
   */
  def getAllTargets: Array[PipelineTable] = {
    Array(
      BronzeTargets.jobsSnapshotTarget,
      BronzeTargets.clustersSnapshotTarget,
      BronzeTargets.poolsSnapshotTarget,
      BronzeTargets.auditLogsTarget,
      BronzeTargets.auditLogAzureLandRaw,
      BronzeTargets.clusterEventsTarget,
      BronzeTargets.sparkEventLogsTarget,
      BronzeTargets.processedEventLogs,
      BronzeTargets.cloudMachineDetail,
      BronzeTargets.dbuCostDetail,
      BronzeTargets.clusterEventsErrorsTarget
    )
  }

  def getAllModules: Seq[Module] = {
    config.overwatchScope.flatMap {
      case OverwatchScope.audit => Array(auditLogsModule)
      case OverwatchScope.clusters => Array(clustersSnapshotModule)
      case OverwatchScope.clusterEvents => Array(clusterEventLogsModule)
      case OverwatchScope.jobs => Array(jobsSnapshotModule)
      case OverwatchScope.pools => Array(poolsSnapshotModule)
      case OverwatchScope.sparkEvents => Array(sparkEventLogsModule)
      case _ => Array[Module]()
    }
  }

  /**
   * Simplified method for the common task of deep cloning bronze targets.
   * This function will perform a deep clone on all existing bronze targets
   * If not overwriting, it's important for customer to set up lifecycle management to ensure
   * max age of backups as this data can grow quite large
   * @param targetPrefix where to store the backups -- subdir handling is done automatically
   * @param overwrite whether or not to overwrite the backups
   *                  if choosing to overwrite, only one backup will be maintained
   * @param excludes which bronze targets to exclude from the snapshot
   */
  def snapshot(
                targetPrefix: String,
                overwrite: Boolean,
                excludes: Array[String] = Array()
              ): Unit = {
    val bronzeTargets = getAllTargets :+ pipelineStateTarget
    val currTime = Pipeline.createTimeDetail(System.currentTimeMillis())
    val timestampedTargetPrefix = s"$targetPrefix/${currTime.asDTString}/${currTime.asUnixTimeMilli.toString}"

    // if user provides dot path to table -- remove dot path and lower case the name
    val cleanExcludes = excludes.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })

    // remove excludes
    // remove non-existing bronze targets
    val targetsToSnap = bronzeTargets
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))

    val finalTargetPathPrefix = if (overwrite) { // Overwrite - clean paths and reuse prefix
      val dirsToClean = targetsToSnap.map(t => s"${targetPrefix}/${t.name.toLowerCase}")
      Helpers.fastrm(dirsToClean)
      targetPrefix
    } else timestampedTargetPrefix // !Overwrite - targetPrefix/currDateString/timestampMillisString

    // build clone details
    val cloneSpecs = targetsToSnap.map(t => {
      val targetPath = s"${finalTargetPathPrefix}/${t.name.toLowerCase}"
      CloneDetail(t.tableLocation, targetPath)
    })

    // par clone
    Helpers.parClone(cloneSpecs)

  }

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private[overwatch] val jobsSnapshotModule = Module(1001, "Bronze_Jobs_Snapshot", this)
  lazy private val appendJobsProcess = ETLDefinition(
    workspace.getJobsDF,
    Seq(cleanseRawJobsSnapDF(config.cloudProvider)),
    append(BronzeTargets.jobsSnapshotTarget)
  )

  lazy private[overwatch] val clustersSnapshotModule = Module(1002, "Bronze_Clusters_Snapshot", this)
  lazy private val appendClustersAPIProcess = ETLDefinition(
    workspace.getClustersDF,
    Seq(cleanseRawClusterSnapDF(config.cloudProvider)),
    append(BronzeTargets.clustersSnapshotTarget)
  )

  lazy private[overwatch] val poolsSnapshotModule = Module(1003, "Bronze_Pools_Snapshot", this)
  lazy private val appendPoolsProcess = ETLDefinition(
    workspace.getPoolsDF,
    Seq(cleanseRawPoolsDF()),
    append(BronzeTargets.poolsSnapshotTarget)
  )

  lazy private[overwatch] val auditLogsModule = Module(1004, "Bronze_AuditLogs", this)
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

  lazy private[overwatch] val clusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs", this, Array(1004), 0.0, Some(30))
  lazy private val appendClusterEventLogsProcess = ETLDefinition(
    BronzeTargets.clustersSnapshotTarget.asDF,
    Seq(
      prepClusterEventLogs(
        BronzeTargets.auditLogsTarget.asIncrementalDF(clusterEventLogsModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        clusterEventLogsModule.fromTime,
        clusterEventLogsModule.untilTime,
        pipelineSnapTime,
        config.apiEnv,
        config.organizationId,
        database,
        BronzeTargets.clusterEventsErrorsTarget,
        config.tempWorkingDir
      )
    ),
    append(BronzeTargets.clusterEventsTarget)
  )

  private val sparkEventLogsSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
    "spark.sql.files.maxPartitionBytes" -> (1024 * 1024 * 64).toString,
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> (1024 * 1024 * 8).toString
    // very large schema to imply, too much parallelism and schema result size is too large to
    // serialize, 64m seems to be a good middle ground.
  )
  lazy private val sparkLogClusterScaleCoefficient = 2.4
  lazy private[overwatch] val sparkEventLogsModule = Module(1006, "Bronze_SparkEventLogs", this, Array(1004), sparkLogClusterScaleCoefficient)
    .withSparkOverrides(sparkEventLogsSparkOverrides)
  lazy private val appendSparkEventLogsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, BronzeTargets.auditLogsTarget.incrementalColumns),
    Seq(
      collectEventLogPaths(
        sparkEventLogsModule.fromTime,
        sparkEventLogsModule.untilTime,
        sparkEventLogsModule.daysToProcess,
        BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, BronzeTargets.auditLogsTarget.incrementalColumns, 30),
        BronzeTargets.clustersSnapshotTarget,
        sparkLogClusterScaleCoefficient
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
    val isFirstAuditRun = !BronzeTargets.auditLogsTarget.exists(dataValidation = true)
    val rawAzureAuditEvents = landAzureAuditLogDF(
      BronzeTargets.auditLogAzureLandRaw,
      config.auditLogConfig.azureAuditLogEventhubConfig.get,
      config.etlDataPathPrefix, config.databaseLocation, config.consumerDatabaseLocation,
      isFirstAuditRun,
      config.organizationId,
      config.runID
    )

    database.write(rawAzureAuditEvents, BronzeTargets.auditLogAzureLandRaw, pipelineSnapTime.asColumnTS)

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

  def refreshViews(): Unit = {
    postProcessor.refreshPipReportView(pipelineStateViewTarget)
    BronzeTargets.dbuCostDetailViewTarget.publish("*")
    BronzeTargets.cloudMachineDetailViewTarget.publish("*")
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
    apply(
      workspace,
      readOnly = false,
      suppressReport = false,
      suppressStaticDatasets = false
    )
  }

  private[overwatch] def apply(
                                workspace: Workspace,
                                readOnly: Boolean = false,
                                suppressReport: Boolean = false,
                                suppressStaticDatasets: Boolean = false
                              ): Bronze = {

    val bronzePipeline = new Bronze(workspace, workspace.database, workspace.getConfig)
      .setReadOnly(if (workspace.isValidated) readOnly else true) // if workspace is not validated set it read only
      .suppressRangeReport(suppressReport)
      .initPipelineRun()

    if (suppressStaticDatasets) {
      bronzePipeline
    } else {
      bronzePipeline.loadStaticDatasets()
    }
  }

}