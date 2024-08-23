package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.Logger


class Gold(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with GoldTransforms {

  /**
   * Enable access to Gold pipeline tables externally.
   *
   * @return
   */
  def getAllTargets: Array[PipelineTable] = {
    Array(
      GoldTargets.poolsTarget,
      GoldTargets.clusterTarget,
      GoldTargets.jobTarget,
      GoldTargets.jobRunTarget,
      GoldTargets.jobRunCostPotentialFactTarget,
      GoldTargets.notebookTarget,
      GoldTargets.accountModsTarget,
      GoldTargets.accountLoginTarget,
      GoldTargets.clusterStateFactTarget,
      GoldTargets.sparkJobTarget,
      GoldTargets.sparkStageTarget,
      GoldTargets.sparkTaskTarget,
      GoldTargets.sparkExecutionTarget,
      GoldTargets.sparkStreamTarget,
      GoldTargets.sparkExecutorTarget,
      GoldTargets.sqlQueryHistoryTarget,
      GoldTargets.warehouseTarget,
      GoldTargets.notebookCommandsTarget
    )
  }

  def getAllModules: Seq[Module] = {
    config.overwatchScope.flatMap {
      case OverwatchScope.accounts => {
        Array(accountModModule, accountLoginModule)
      }
      case OverwatchScope.notebooks => {
        Array(notebookModule)
      }
      case OverwatchScope.pools => {
        Array(poolsModule)
      }
      case OverwatchScope.clusters => {
        Array(clusterModule)
      }
      case OverwatchScope.clusterEvents => {
        Array(clusterStateFactModule)
      }
      case OverwatchScope.jobs => {
        Array(jobsModule, jobRunsModule, jobRunCostPotentialFactModule)
      }
      case OverwatchScope.sparkEvents => {
        Array(
          sparkJobModule,
          sparkStageModule,
          sparkTaskModule,
          sparkExecutorModule,
          sparkExecutionModule,
          sparkStreamModule
        )
      }
      case OverwatchScope.dbsql => {
        Array(
          sqlQueryHistoryModule,
          warehouseModule
        )
      }
      case OverwatchScope.notebookCommands => {
        Array(
          notebookCommandsFactModule
        )
      }
      case _ => Array[Module]()
    }
  }

  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private[overwatch] val clusterModule = Module(3001, "Gold_Cluster", this, Array(2014))
  lazy private val appendClusterProccess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.clustersSpecTarget.asIncrementalDF(clusterModule, "timestamp"),
        Seq(buildCluster()),
        append(GoldTargets.clusterTarget)
      )
  }

  private val clsfSparkOverrides = Map(
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "67108864" // lower to 64MB due to high skew potential
  )
  lazy private[overwatch] val clusterStateFactModule = Module(3005, "Gold_ClusterStateFact", this, Array(2019, 2014), 3.0)
    .withSparkOverrides(clsfSparkOverrides)
  lazy private val appendClusterStateFactProccess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.clusterStateDetailTarget.asIncrementalDF(
          clusterStateFactModule,
          SilverTargets.clusterStateDetailTarget.incrementalColumns,
          GoldTargets.clusterStateFactTarget.maxMergeScanDates
        ),
        Seq(buildClusterStateFact(
          BronzeTargets.cloudMachineDetail,
          BronzeTargets.dbuCostDetail,
          BronzeTargets.clustersSnapshotTarget,
          SilverTargets.clustersSpecTarget,
          pipelineSnapTime
        )),
        append(GoldTargets.clusterStateFactTarget)
      )
  }

  lazy private[overwatch] val poolsModule = Module(3009, "Gold_Pools", this, Array(2009))
  lazy private val appendPoolsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.poolsSpecTarget.asDF,
        Seq(buildPools()),
        append(GoldTargets.poolsTarget)
      )
  }

  lazy private[overwatch] val jobsModule = Module(3002, "Gold_Job", this, Array(2010))
  lazy private val appendJobsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.dbJobsStatusTarget.asIncrementalDF(jobsModule, "timestamp"),
        Seq(buildJobs()),
        append(GoldTargets.jobTarget)
      )
  }

  lazy private[overwatch] val jobRunsModule = Module(3003, "Gold_JobRun", this, Array(2011))
  lazy private val appendJobRunsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.dbJobRunsTarget.asIncrementalDF(jobRunsModule, SilverTargets.dbJobRunsTarget.incrementalColumns, 30),
        Seq(buildJobRuns()),
        append(GoldTargets.jobRunTarget)
      )
  }

  val jrcpSparkOverrides = Map(
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "67108864" // lower to 64MB due to high skew potential
  )
  lazy private[overwatch] val jobRunCostPotentialFactModule = Module(3015, "Gold_jobRunCostPotentialFact", this, Array(3001, 3003, 3005), 3.0)
    .withSparkOverrides(jrcpSparkOverrides)
  //Incremental current spark job and tasks DFs plus 2 days for lag coverage
  lazy private val appendJobRunCostPotentialFactProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        // new jobRuns to be considered are job runs completed since the last overwatch import for this module
        GoldTargets.jobRunTarget.asIncrementalDF(jobRunCostPotentialFactModule, GoldTargets.jobRunTarget.incrementalColumns, 30),
        Seq(
          // Retrieve cluster states for current time period plus 2 days for lagging states
          buildJobRunCostPotentialFact(
            GoldTargets.jobRunCostPotentialFactTarget.asIncrementalDF(
              jobRunCostPotentialFactModule,
              GoldTargets.jobRunCostPotentialFactTarget.incrementalColumns,
              30
            ),
            GoldTargets.clusterStateFactTarget
              .asIncrementalDF(jobRunCostPotentialFactModule, GoldTargets.clusterStateFactTarget.incrementalColumns, 90),
            GoldTargets.sparkJobTarget.asIncrementalDF(jobRunCostPotentialFactModule, GoldTargets.sparkJobTarget.incrementalColumns, 2),
            GoldTargets.sparkTaskTarget.asIncrementalDF(jobRunCostPotentialFactModule, GoldTargets.sparkTaskTarget.incrementalColumns, 2),
            jobRunCostPotentialFactModule.fromTime,
            jobRunCostPotentialFactModule.untilTime
          )),
        append(GoldTargets.jobRunCostPotentialFactTarget)
      )
  }

  lazy private[overwatch] val notebookModule = Module(3004, "Gold_Notebook", this, Array(2018))
  lazy private val appendNotebookProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.notebookStatusTarget.asIncrementalDF(notebookModule, "timestamp"),
        Seq(buildNotebook()),
        append(GoldTargets.notebookTarget)
      )
  }

  lazy private[overwatch] val accountModModule = Module(3007, "Gold_AccountMod", this, Array(2017))
  lazy private val appendAccountModProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.accountModTarget.asIncrementalDF(accountModModule, "timestamp"),
        Seq(buildAccountMod()),
        append(GoldTargets.accountModsTarget)
      )
  }

  lazy private[overwatch] val accountLoginModule = Module(3008, "Gold_AccountLogin", this, Array(2016))
  lazy private val appendAccountLoginProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.accountLoginTarget.asIncrementalDF(accountLoginModule, "timestamp"),
        Seq(buildLogin(SilverTargets.accountModTarget.asDF)),
        append(GoldTargets.accountLoginTarget)
      )
  }

  private val sparkBaseSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
  )

  lazy private[overwatch] val sparkJobModule = Module(3010, "Gold_SparkJob", this, Array(2006), 6.0, shuffleFactor = 2.0)
    .withSparkOverrides(sparkBaseSparkOverrides)
  lazy private val appendSparkJobProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.jobsTarget.asIncrementalDF(sparkJobModule, SilverTargets.jobsTarget.incrementalColumns),
        Seq(buildSparkJob(config.cloudProvider)),
        append(GoldTargets.sparkJobTarget)
      )
  }

  lazy private[overwatch] val sparkStageModule = Module(3011, "Gold_SparkStage", this, Array(2007), 6.0, shuffleFactor = 4.0)
    .withSparkOverrides(sparkBaseSparkOverrides)
  lazy private val appendSparkStageProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.stagesTarget.asIncrementalDF(sparkStageModule, SilverTargets.stagesTarget.incrementalColumns),
        Seq(buildSparkStage()),
        append(GoldTargets.sparkStageTarget)
      )
  }

  lazy private[overwatch] val sparkTaskModule = Module(3012, "Gold_SparkTask", this, Array(2008), 6.0, shuffleFactor = 8.0)
    .withSparkOverrides(sparkBaseSparkOverrides)
  lazy private val appendSparkTaskProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.tasksTarget.asIncrementalDF(sparkTaskModule, SilverTargets.tasksTarget.incrementalColumns),
        Seq(buildSparkTask()),
        append(GoldTargets.sparkTaskTarget)
      )
  }

  lazy private[overwatch] val sparkExecutionModule = Module(3013, "Gold_SparkExecution", this, Array(2005), 6.0, shuffleFactor = 2.0)
    .withSparkOverrides(sparkBaseSparkOverrides)
  lazy private val appendSparkExecutionProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.executionsTarget.asIncrementalDF(sparkExecutionModule, SilverTargets.executionsTarget.incrementalColumns),
        Seq(buildSparkExecution()),
        append(GoldTargets.sparkExecutionTarget)
      )
  }

  lazy private[overwatch] val sparkStreamModule = Module(3016, "Gold_SparkStream", this, Array(1006, 2005), 6.0, shuffleFactor = 4.0)
    .withSparkOverrides(sparkBaseSparkOverrides ++ Map("spark.sql.caseSensitive" -> "true"))
  lazy private val appendSparkStreamProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.sparkEventLogsTarget.asIncrementalDF(sparkStreamModule, BronzeTargets.sparkEventLogsTarget.incrementalColumns),
        Seq(
          buildSparkStream(
            GoldTargets.sparkStreamTarget,
            SilverTargets.executionsTarget.asIncrementalDF(sparkStreamModule, SilverTargets.executionsTarget.incrementalColumns, 30)
          )),
        append(GoldTargets.sparkStreamTarget)
      )
  }

  lazy private[overwatch] val sparkExecutorModule = Module(3014, "Gold_SparkExecutor", this, Array(2003), 6.0)
    .withSparkOverrides(sparkBaseSparkOverrides)
  lazy private val appendSparkExecutorProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.executorsTarget.asIncrementalDF(sparkExecutorModule, SilverTargets.executorsTarget.incrementalColumns),
        Seq(buildSparkExecutor()),
        append(GoldTargets.sparkExecutorTarget)
      )
  }

  lazy private[overwatch] val sqlQueryHistoryModule = Module(3017, "Gold_Sql_QueryHistory", this, Array(2020), 6.0)
    .withSparkOverrides(sparkBaseSparkOverrides)
  lazy private val appendSqlQueryHistoryProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.sqlQueryHistoryTarget.asIncrementalDF(sqlQueryHistoryModule, SilverTargets.sqlQueryHistoryTarget.incrementalColumns, 2),
        append(GoldTargets.sqlQueryHistoryTarget)
      )
  }

  lazy private[overwatch] val warehouseModule = Module(3018, "Gold_Warehouse", this, Array(2021))
  lazy private val appendWarehouseProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.warehousesSpecTarget.asIncrementalDF(warehouseModule, SilverTargets.warehousesSpecTarget.incrementalColumns),
        Seq(buildWarehouse()),
        append(GoldTargets.warehouseTarget)
      )
  }

  lazy private[overwatch] val notebookCommandsFactModule = Module(3019, "Gold_NotebookCommands", this, Array(1004,3004,3005),6.0, shuffleFactor = 4.0)
  lazy private val appendNotebookCommandsFactProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(notebookCommandsFactModule, BronzeTargets.auditLogsTarget.incrementalColumns,1),
        Seq(buildNotebookCommandsFact(
          GoldTargets.notebookTarget,
          GoldTargets.clusterStateFactTarget
            .asIncrementalDF(notebookCommandsFactModule, GoldTargets.clusterStateFactTarget.incrementalColumns, 90)
        )),
        append(GoldTargets.notebookCommandsTarget)
      )
  }

  lazy private[overwatch] val warehouseStateFactModule = Module(3020, "Gold_WarehouseStateFact", this, Array(2022, 2021), 3.0)
  lazy private val appendWarehouseStateFactProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        SilverTargets.warehousesStateDetailTarget.asIncrementalDF(
          warehouseStateFactModule,
          SilverTargets.warehousesStateDetailTarget.incrementalColumns,
          GoldTargets.warehouseStateFactTarget.maxMergeScanDates
        ),
        Seq(buildWarehouseStateFact(
          BronzeTargets.cloudMachineDetail,
          BronzeTargets.warehouseDbuDetail,
          SilverTargets.warehousesSpecTarget
        )),
        append(GoldTargets.warehouseStateFactTarget)
      )
  }

  private def processSparkEvents(): Unit = {

    sparkExecutorModule.execute(appendSparkExecutorProcess)
    sparkExecutionModule.execute(appendSparkExecutionProcess)
    sparkStreamModule.execute(appendSparkStreamProcess)
    sparkJobModule.execute(appendSparkJobProcess)
    sparkStageModule.execute(appendSparkStageProcess)
    sparkTaskModule.execute(appendSparkTaskProcess)

    GoldTargets.sparkJobViewTarget.publish(sparkJobViewColumnMapping)
    GoldTargets.sparkStageViewTarget.publish(sparkStageViewColumnMapping)
    GoldTargets.sparkTaskViewTarget.publish(sparkTaskViewColumnMapping)
    GoldTargets.sparkExecutionViewTarget.publish(sparkExecutionViewColumnMapping)
    GoldTargets.sparkStreamViewTarget.publish(sparkStreamViewColumnMapping)
    GoldTargets.sparkExecutorViewTarget.publish(sparkExecutorViewColumnMapping)
    GoldTargets.sqlQueryHistoryViewTarget.publish(sqlQueryHistoryViewColumnMapping)
    GoldTargets.warehouseViewTarget.publish(warehouseViewColumnMapping)

  }

  private def validateCostSources(): Unit = {
    PipelineFunctions.validateType2Input(BronzeTargets.dbuCostDetail, "activeFrom", "activeUntil", "isActive", pipelineSnapTime.asDTString)
    PipelineFunctions.validateType2Input(BronzeTargets.cloudMachineDetail, "activeFrom", "activeUntil", "isActive", pipelineSnapTime.asDTString)
  }

  private def executeModules(): Unit = {
    validateCostSources()
    config.overwatchScope.foreach {
      case OverwatchScope.accounts => {
        accountModModule.execute(appendAccountModProcess)
        accountLoginModule.execute(appendAccountLoginProcess)

        GoldTargets.accountLoginViewTarget.publish(accountLoginViewColumnMappings)
        GoldTargets.accountModsViewTarget.publish(accountModViewColumnMappings)
      }
      case OverwatchScope.notebooks => {
        notebookModule.execute(appendNotebookProcess)
        GoldTargets.notebookViewTarget.publish(notebookViewColumnMappings)
      }
      case OverwatchScope.pools => {
        poolsModule.execute(appendPoolsProcess)
        GoldTargets.poolsViewTarget.publish(poolsViewColumnMapping)
      }
      case OverwatchScope.clusters => {
        clusterModule.execute(appendClusterProccess)
        GoldTargets.clusterViewTarget.publish(clusterViewColumnMapping)
      }
      case OverwatchScope.jobs => {
        jobsModule.execute(appendJobsProcess)
        jobRunsModule.execute(appendJobRunsProcess)
        GoldTargets.jobViewTarget.publish(jobViewColumnMapping)
        GoldTargets.jobRunsViewTarget.publish(jobRunViewColumnMapping)
      }
      case OverwatchScope.dbsql => {
        sqlQueryHistoryModule.execute(appendSqlQueryHistoryProcess)
        warehouseModule.execute(appendWarehouseProcess)
        GoldTargets.sqlQueryHistoryViewTarget.publish(sqlQueryHistoryViewColumnMapping)
        GoldTargets.warehouseViewTarget.publish(warehouseViewColumnMapping)
      }
      case OverwatchScope.sparkEvents => {
        processSparkEvents()
      }
      case _ =>
    }
  }

  private def buildFacts(): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.clusterEvents => {
        clusterStateFactModule.execute(appendClusterStateFactProccess)
        GoldTargets.clusterStateFactViewTarget.publish(clusterStateFactViewColumnMappings)
      }
      case OverwatchScope.jobs => {
        jobRunCostPotentialFactModule.execute(appendJobRunCostPotentialFactProcess)
        GoldTargets.jobRunCostPotentialFactViewTarget.publish(jobRunCostPotentialFactViewColumnMapping)
      }
      case OverwatchScope.notebookCommands => {
        notebookCommandsFactModule.execute(appendNotebookCommandsFactProcess)
        GoldTargets.notebookCommandsFactViewTarget.publish(notebookCommandsFactViewColumnMapping)
      }
      case OverwatchScope.warehouseEvents => {
        warehouseStateFactModule.execute(appendWarehouseStateFactProcess)
        GoldTargets.warehouseStateFactViewTarget.publish(warehouseStateFactViewColumnMappings)
      }
      case _ =>
    }
  }

  def refreshViews(workspacesAllowed: Array[String] = Array()): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.accounts => {
        GoldTargets.accountLoginViewTarget.publish(accountLoginViewColumnMappings,workspacesAllowed = workspacesAllowed)
        GoldTargets.accountModsViewTarget.publish(accountModViewColumnMappings,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.notebooks => {
        GoldTargets.notebookViewTarget.publish(notebookViewColumnMappings,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.pools => {
        GoldTargets.poolsViewTarget.publish(poolsViewColumnMapping,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.clusters => {
        GoldTargets.clusterViewTarget.publish(clusterViewColumnMapping,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.clusterEvents => {
        GoldTargets.clusterStateFactViewTarget.publish(clusterStateFactViewColumnMappings,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.jobs => {
        GoldTargets.jobViewTarget.publish(jobViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.jobRunsViewTarget.publish(jobRunViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.jobRunCostPotentialFactViewTarget.publish(jobRunCostPotentialFactViewColumnMapping,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.sparkEvents => {
        GoldTargets.sparkJobViewTarget.publish(sparkJobViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.sparkStageViewTarget.publish(sparkStageViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.sparkTaskViewTarget.publish(sparkTaskViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.sparkExecutionViewTarget.publish(sparkExecutionViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.sparkStreamViewTarget.publish(sparkStreamViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.sparkExecutorViewTarget.publish(sparkExecutorViewColumnMapping,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.dbsql => {
        GoldTargets.sqlQueryHistoryViewTarget.publish(sqlQueryHistoryViewColumnMapping,workspacesAllowed = workspacesAllowed)
        GoldTargets.warehouseViewTarget.publish(warehouseViewColumnMapping,workspacesAllowed = workspacesAllowed)
      }
      case OverwatchScope.notebookCommands => {
        GoldTargets.notebookCommandsFactViewTarget.publish(notebookCommandsFactViewColumnMapping,workspacesAllowed = workspacesAllowed)
      }
      case _ =>
    }
  }


  def run(): Pipeline = {

    restoreSparkConf()
    executeModules()
    buildFacts()

    initiatePostProcessing()
    this // to be used as fail switch later if necessary
  }

}

object Gold {
  def apply(workspace: Workspace): Gold = {
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

                              ): Gold = {
    val goldPipeline = new Gold(workspace, workspace.database, workspace.getConfig)
      .setReadOnly(if (workspace.isValidated) readOnly else true) // if workspace is not validated set it read only
      .suppressRangeReport(suppressReport)
      .initPipelineRun()

    if (suppressStaticDatasets) {
      goldPipeline
    } else {
      goldPipeline.loadStaticDatasets()
    }
  }

}