package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.Helpers.deriveApiTempDir
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
      BronzeTargets.clusterEventsErrorsTarget,
      BronzeTargets.libsSnapshotTarget,
      BronzeTargets.policiesSnapshotTarget,
      BronzeTargets.instanceProfilesSnapshotTarget,
      BronzeTargets.tokensSnapshotTarget,
      BronzeTargets.globalInitScSnapshotTarget,
      BronzeTargets.jobRunsSnapshotTarget,
      BronzeTargets.warehousesSnapshotTarget
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
      case OverwatchScope.dbsql => Array(warehousesSnapshotModule)
      case _ => Array[Module]()
    }
  }

  private val logger: Logger = Logger.getLogger(this.getClass)
  /**
   * Simplified method for the common task of deep cloning bronze targets.
   * This function will perform a deep clone on all existing bronze targets
   * If not overwriting, it's important for customer to set up lifecycle management to ensure
   * max age of backups as this data can grow quite large
   *
   * @param targetPrefix where to store the backups -- subdir handling is done automatically
   * @param overwrite    whether or not to overwrite the backups
   *                     if choosing to overwrite, only one backup will be maintained
   * @param excludes     which bronze targets to exclude from the snapshot
   */
 //TODO: Add link for new Snapshot Class functionality
  @deprecated("This Method is deprecated, Use new Snapshot Class instead. Please check the link for more details")
  def snapshot(
                targetPrefix: String,
                overwrite: Boolean,
                excludes: Array[String] = Array()
              ): Unit = {
    val bronzeTargets = getAllTargets :+ pipelineStateTarget
    val currTime = Pipeline.createTimeDetail(System.currentTimeMillis())
    val timestampedTargetPrefix = s"$targetPrefix/${currTime.asDTString}/${currTime.asUnixTimeMilli.toString}"
   import spark.implicits._

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

   val cloneSpecs = new Snapshot(workspace.getConfig.databaseName, finalTargetPathPrefix, workspace, workspace.database, workspace.getConfig, "snapShot").buildCloneSpecs("Deep", targetsToSnap)


    // par clone
   val cloneReport = Helpers.parClone(cloneSpecs)
   val cloneReportPath = s"${finalTargetPathPrefix}/clone_report/"
   val ProcessCompleteMsg = s"Cloning is one and clone report is being written to ${cloneReportPath}"
   logger.log(Level.INFO, ProcessCompleteMsg)
   cloneReport.toDS.write.format("delta").mode("append").save(cloneReportPath)

  }

  lazy private[overwatch] val jobsSnapshotModule = Module(1001, "Bronze_Jobs_Snapshot", this)
  lazy private val appendJobsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getJobsDF(deriveApiTempDir(config.tempWorkingDir,jobsSnapshotModule.moduleName,pipelineSnapTime)),
        Seq(cleanseRawJobsSnapDF(BronzeTargets.jobsSnapshotTarget.keys, config.runID)),
        append(BronzeTargets.jobsSnapshotTarget)
      )
  }

  lazy private[overwatch] val clustersSnapshotModule = Module(1002, "Bronze_Clusters_Snapshot", this)
  lazy private val appendClustersAPIProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(clustersSnapshotModule, BronzeTargets.auditLogsTarget.incrementalColumns, additionalLagDays = 1), // 1 lag day to get laggard records,
        Seq(prepClusterSnapshot(
          workspace,
          pipelineSnapTime,
          config.apiEnv,
          database,
          BronzeTargets.clusterSnapshotErrorsTarget,
          config,
          clustersSnapshotModule.moduleName
        )
        ),
        append(BronzeTargets.clustersSnapshotTarget)
      )
  }

  lazy private[overwatch] val poolsSnapshotModule = Module(1003, "Bronze_Pools_Snapshot", this)
  lazy private val appendPoolsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getPoolsDF(deriveApiTempDir(config.tempWorkingDir,poolsSnapshotModule.moduleName,pipelineSnapTime)),
        Seq(cleanseRawPoolsDF()),
        append(BronzeTargets.poolsSnapshotTarget)
      )
  }

  lazy private[overwatch] val auditLogsModule = Module(1004, "Bronze_AuditLogs", this)
  lazy private val appendAuditLogsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        getAuditLogsDF(
          config.auditLogConfig,
          config.cloudProvider,
          auditLogsModule.fromTime.asLocalDateTime,
          auditLogsModule.untilTime.asLocalDateTime,
          BronzeTargets.auditLogAzureLandRaw,
          config.runID,
          config.organizationId,
          config.sqlEndpoint,
          config.apiEnv
        ),
        append(BronzeTargets.auditLogsTarget)
      )
  }

  lazy private[overwatch] val clusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs", this, Array(1004), 0.0, Some(60))
  lazy private val appendClusterEventLogsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.clustersSnapshotTarget.asDF,
        Seq(
          prepClusterEventLogs(
            BronzeTargets.auditLogsTarget.asIncrementalDF(clusterEventLogsModule, BronzeTargets.auditLogsTarget.incrementalColumns, additionalLagDays = 1), // 1 lag day to get laggard records
            clusterEventLogsModule.fromTime,
            clusterEventLogsModule.untilTime,
            pipelineSnapTime,
            config.apiEnv,
            config.organizationId,
            database,
            BronzeTargets.clusterEventsErrorsTarget,
            config,
            clusterEventLogsModule.moduleName
          )
        ),
        append(BronzeTargets.clusterEventsTarget)
      )
  }

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
  lazy private val appendSparkEventLogsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(
          collectEventLogPaths(
            sparkEventLogsModule.fromTime,
            sparkEventLogsModule.untilTime,
            sparkEventLogsModule.daysToProcess,
            BronzeTargets.auditLogsTarget.asIncrementalDF(sparkEventLogsModule, BronzeTargets.auditLogsTarget.incrementalColumns, 30),
            BronzeTargets.clustersSnapshotTarget,
            sparkLogClusterScaleCoefficient,
            config.apiEnv,
            config.isMultiworkspaceDeployment,
            config.organizationId,
            config.cloudProvider
          ),
          generateEventLogsDF(
            database,
            config.badRecordsPath,
            BronzeTargets.processedEventLogs,
            config.organizationId,
            config.runID,
            pipelineSnapTime,
            sparkEventLogsModule.daysToProcess
          ) //,
        ),
        append(BronzeTargets.sparkEventLogsTarget) // Not new data only -- date filters handled in function logic
      )
  }

  lazy private[overwatch] val libsSnapshotModule = Module(1007, "Bronze_Libraries_Snapshot", this)
  lazy private val appendLibsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getClusterLibraries(deriveApiTempDir(config.tempWorkingDir,libsSnapshotModule.moduleName,pipelineSnapTime)),
        append(BronzeTargets.libsSnapshotTarget)
      )
  }

  lazy private[overwatch] val policiesSnapshotModule = Module(1008, "Bronze_Policies_Snapshot", this)
  lazy private val appendPoliciesProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getClusterPolicies(deriveApiTempDir(config.tempWorkingDir,policiesSnapshotModule.moduleName,pipelineSnapTime)),
        append(BronzeTargets.policiesSnapshotTarget)
      )
  }

  lazy private[overwatch] val instanceProfileSnapshotModule = Module(1009, "Bronze_Instance_Profile_Snapshot", this)
  lazy private val appendInstanceProfileProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getProfilesDF(deriveApiTempDir(config.tempWorkingDir,instanceProfileSnapshotModule.moduleName,pipelineSnapTime)),
        append(BronzeTargets.instanceProfilesSnapshotTarget)
      )
  }

  lazy private[overwatch] val tokenSnapshotModule = Module(1010, "Bronze_Token_Snapshot", this)
  lazy private val appendTokenProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getTokens(deriveApiTempDir(config.tempWorkingDir,tokenSnapshotModule.moduleName,pipelineSnapTime)),
        append(BronzeTargets.tokensSnapshotTarget)
      )
  }

  lazy private[overwatch] val globalInitScSnapshotModule = Module(1011, "Bronze_Global_Init_Scripts_Snapshot", this)
  lazy private val appendGlobalInitScProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getGlobalInitScripts(deriveApiTempDir(config.tempWorkingDir,globalInitScSnapshotModule.moduleName,pipelineSnapTime)),
        append(BronzeTargets.globalInitScSnapshotTarget)
      )
  }

  lazy private[overwatch] val jobRunsSnapshotModule = Module(1012, "Bronze_Job_Runs_Snapshot", this) // check module number
  lazy private val appendJobRunsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getJobRunsDF(jobRunsSnapshotModule.fromTime, jobRunsSnapshotModule.untilTime,deriveApiTempDir(config.tempWorkingDir,jobRunsSnapshotModule.moduleName,pipelineSnapTime)),
        Seq(cleanseRawJobRunsSnapDF(BronzeTargets.jobRunsSnapshotTarget.keys, config.runID)),
        append(BronzeTargets.jobRunsSnapshotTarget)
      )
  }


  lazy private[overwatch] val warehousesSnapshotModule = Module(1013, "Bronze_Warehouses_Snapshot", this)
  lazy private val appendWarehousesAPIProcess: () => ETLDefinition = {
    () =>
    ETLDefinition (
    workspace.getWarehousesDF(deriveApiTempDir(config.tempWorkingDir,warehousesSnapshotModule.moduleName,pipelineSnapTime)),
    Seq(cleanseRawWarehouseSnapDF),
    append(BronzeTargets.warehousesSnapshotTarget)
    )
  }


  // TODO -- convert and merge this into audit's ETLDefinition
  private def landAzureAuditEvents(): Unit = {

    println(s"Audit Logs Bronze: Land Stream Beginning for WorkspaceID: ${config.organizationId}")
    val rawAzureAuditEvents = landAzureAuditLogDF(
      BronzeTargets.auditLogAzureLandRaw,
      config.auditLogConfig.azureAuditLogEventhubConfig.get,
      config.etlDataPathPrefix, config.databaseLocation, config.consumerDatabaseLocation,
      auditLogsModule.isFirstRun,
      config.organizationId,
      config.runID
    )

    database.writeWithRetry(rawAzureAuditEvents, BronzeTargets.auditLogAzureLandRaw, pipelineSnapTime.asColumnTS)

    val rawProcessCompleteMsg = "Azure audit ingest process complete"
    if (config.debugFlag) println(rawProcessCompleteMsg)
    logger.log(Level.INFO, rawProcessCompleteMsg)
  }

  private def executeModules(): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.audit =>
        if (config.cloudProvider == "azure" && !config.auditLogConfig.systemTableName.isDefined) {
          landAzureAuditEvents()
        }
        auditLogsModule.execute(appendAuditLogsProcess)
      case OverwatchScope.clusters =>
        clustersSnapshotModule.execute(appendClustersAPIProcess)
        libsSnapshotModule.execute(appendLibsProcess)
        policiesSnapshotModule.execute(appendPoliciesProcess)
        if (config.cloudProvider == "aws") {
          instanceProfileSnapshotModule.execute(appendInstanceProfileProcess)
        }
      case OverwatchScope.clusterEvents => clusterEventLogsModule.execute(appendClusterEventLogsProcess)
      case OverwatchScope.jobs =>
        jobsSnapshotModule.execute(appendJobsProcess)
        // setting this to experimental -- runtimes can be EXTREMELY long for customers with MANY job runs
        if (spark(globalSession = true).conf.getOption("overwatch.experimental.enablejobrunsnapshot").getOrElse("false").toBoolean) {
          jobRunsSnapshotModule.execute(appendJobRunsProcess)
        }
      case OverwatchScope.pools => poolsSnapshotModule.execute(appendPoolsProcess)
      case OverwatchScope.sparkEvents => sparkEventLogsModule.execute(appendSparkEventLogsProcess)
      case OverwatchScope.accounts =>
        tokenSnapshotModule.execute(appendTokenProcess)
        globalInitScSnapshotModule.execute(appendGlobalInitScProcess)
      case OverwatchScope.dbsql => warehousesSnapshotModule.execute(appendWarehousesAPIProcess)
      case _ =>
    }
  }

  def refreshViews(workspacesAllowed: Array[String] = Array()): Unit = {
    postProcessor.refreshPipReportView(pipelineStateViewTarget)
    BronzeTargets.dbuCostDetailViewTarget.publish("*", workspacesAllowed = workspacesAllowed)
    BronzeTargets.cloudMachineDetailViewTarget.publish("*", workspacesAllowed = workspacesAllowed)
  }

  def run(): Pipeline = {

    restoreSparkConf()

    if (config.debugFlag) println(s"DEBUG: CLOUD PROVIDER = ${config.cloudProvider}")

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