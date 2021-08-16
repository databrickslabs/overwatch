package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.Logger

class Gold(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with GoldTransforms {

  /**
   * Enable access to Gold pipeline tables externally.
   * @return
   */
  def getAllTargets: Array[PipelineTable] = {
    Array(
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
      GoldTargets.sparkExecutorTarget
    )
  }

  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private[overwatch] val clusterModule = Module(3001, "Gold_Cluster", this, Array(2014))
  lazy private val appendClusterProccess = ETLDefinition(
    SilverTargets.clustersSpecTarget.asIncrementalDF(clusterModule, "timestamp"),
    Seq(buildCluster()),
    append(GoldTargets.clusterTarget)
  )

  lazy private[overwatch] val clusterStateFactModule = Module(3005, "Gold_ClusterStateFact", this, Array(1005, 2014), 3.0)
  lazy private val appendClusterStateFactProccess = ETLDefinition(
    BronzeTargets.clusterEventsTarget.asIncrementalDF(clusterStateFactModule, 7, "timestamp"),
    Seq(buildClusterStateFact(
      BronzeTargets.cloudMachineDetail.asDF,
      BronzeTargets.clustersSnapshotTarget,
      SilverTargets.clustersSpecTarget,
      clusterStateFactModule.untilTime,
      clusterStateFactModule.fromTime
    )),
    append(GoldTargets.clusterStateFactTarget)
  )

  // Issue_37
  //  private val poolsModule = Module(3006, "Gold_Pools")
  //  lazy private val appendPoolsProcess = EtlDefinition(
  //    BronzeTargets.poolsTarget.asIncrementalDF(
  //      buildIncrementalFilter("")
  //    )
  //  )

  lazy private[overwatch] val jobsModule = Module(3002, "Gold_Job", this, Array(2010))
  lazy private val appendJobsProcess = ETLDefinition(
    SilverTargets.dbJobsStatusTarget.asIncrementalDF(jobsModule, "timestamp"),
    Seq(buildJobs()),
    append(GoldTargets.jobTarget)
  )

  lazy private[overwatch] val jobRunsModule = Module(3003, "Gold_JobRun", this, Array(2011))
  lazy private val appendJobRunsProcess = ETLDefinition(
    SilverTargets.dbJobRunsTarget.asIncrementalDF(jobRunsModule, "endEpochMS"),
    Seq(buildJobRuns()),
    append(GoldTargets.jobRunTarget)
  )

  lazy private[overwatch] val jobRunCostPotentialFactModule = Module(3015, "Gold_jobRunCostPotentialFact", this, Array(3001, 3003, 3005, 3010, 3012), 3.0)
  //Incremental current spark job and tasks DFs plus 2 days for lag coverage
  lazy private val appendJobRunCostPotentialFactProcess = ETLDefinition(
    // new jobRuns to be considered are job runs completed since the last overwatch import for this module
    GoldTargets.jobRunTarget.asIncrementalDF(jobRunCostPotentialFactModule, "endEpochMS"),
    Seq(
      // Retrieve cluster states for current time period plus 2 days for lagging states
      buildJobRunCostPotentialFact(
        GoldTargets.clusterStateFactTarget.asIncrementalDF(jobRunCostPotentialFactModule, 90, "unixTimeMS_state_start"),
        GoldTargets.sparkJobTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date"),
        GoldTargets.sparkTaskTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date")
      )),
    append(GoldTargets.jobRunCostPotentialFactTarget)
  )

  lazy private[overwatch] val notebookModule = Module(3004, "Gold_Notebook", this, Array(2018))
  lazy private val appendNotebookProcess = ETLDefinition(
    SilverTargets.notebookStatusTarget.asIncrementalDF(notebookModule, "timestamp"),
    Seq(buildNotebook()),
    append(GoldTargets.notebookTarget)
  )

  lazy private[overwatch] val accountModModule = Module(3007, "Gold_AccountMod", this, Array(2017))
  lazy private val appendAccountModProcess = ETLDefinition(
    SilverTargets.accountModTarget.asIncrementalDF(accountModModule, "timestamp"),
    Seq(buildAccountMod()),
    append(GoldTargets.accountModsTarget)
  )

  lazy private[overwatch] val accountLoginModule = Module(3008, "Gold_AccountLogin", this, Array(2016))
  lazy private val appendAccountLoginProcess = ETLDefinition(
    SilverTargets.accountLoginTarget.asIncrementalDF(accountLoginModule, "timestamp"),
    Seq(buildLogin(SilverTargets.accountModTarget.asDF)),
    append(GoldTargets.accountLoginTarget)
  )

  lazy private[overwatch] val sparkJobModule = Module(3010, "Gold_SparkJob", this, Array(2006), 6.0)
  lazy private val appendSparkJobProcess = ETLDefinition(
    SilverTargets.jobsTarget.asIncrementalDF(sparkJobModule, "startTimestamp"),
    Seq(buildSparkJob(config.cloudProvider)),
    append(GoldTargets.sparkJobTarget)
  )

  lazy private[overwatch] val sparkStageModule = Module(3011, "Gold_SparkStage", this, Array(2007), 6.0)
  lazy private val appendSparkStageProcess = ETLDefinition(
    SilverTargets.stagesTarget.asIncrementalDF(sparkStageModule, "startTimestamp"),
    Seq(buildSparkStage()),
    append(GoldTargets.sparkStageTarget)
  )

  lazy private[overwatch] val sparkTaskModule = Module(3012, "Gold_SparkTask", this, Array(2008), 6.0)
  lazy private val appendSparkTaskProcess = ETLDefinition(
    SilverTargets.tasksTarget.asIncrementalDF(sparkTaskModule, "startTimestamp"),
    Seq(buildSparkTask()),
    append(GoldTargets.sparkTaskTarget)
  )

  private[overwatch] val sparkExecutionModule = Module(3013, "Gold_SparkExecution", this, Array(2005), 6.0)
  lazy private val appendSparkExecutionProcess = ETLDefinition(
    SilverTargets.executionsTarget.asIncrementalDF(sparkExecutionModule, "startTimestamp"),
    Seq(buildSparkExecution()),
    append(GoldTargets.sparkExecutionTarget)
  )

  lazy private[overwatch] val sparkExecutorModule = Module(3014, "Gold_SparkExecutor", this, Array(2003), 6.0)
  lazy private val appendSparkExecutorProcess = ETLDefinition(
    SilverTargets.executorsTarget.asIncrementalDF(sparkExecutorModule, "addedTimestamp"),
    Seq(buildSparkExecutor()),
    append(GoldTargets.sparkExecutorTarget)
  )


  private def processSparkEvents(): Unit = {

    sparkExecutorModule.execute(appendSparkExecutorProcess)
    sparkExecutionModule.execute(appendSparkExecutionProcess)
    sparkJobModule.execute(appendSparkJobProcess)
    sparkStageModule.execute(appendSparkStageProcess)
    sparkTaskModule.execute(appendSparkTaskProcess)

    GoldTargets.sparkJobViewTarget.publish(sparkJobViewColumnMapping)
    GoldTargets.sparkStageViewTarget.publish(sparkStageViewColumnMapping)
    GoldTargets.sparkTaskViewTarget.publish(sparkTaskViewColumnMapping)
    GoldTargets.sparkExecutionViewTarget.publish(sparkExecutionViewColumnMapping)
    GoldTargets.sparkExecutorViewTarget.publish(sparkExecutorViewColumnMapping)

  }

  private def executeModules(): Unit = {
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
    new Gold(workspace, workspace.database, workspace.getConfig)
      .initPipelineRun()
      .loadStaticDatasets()
  }

  private[overwatch] def apply(
                                workspace: Workspace,
                                readOnly: Boolean = false,
                                suppressReport: Boolean = false,
                                suppressStaticDatasets: Boolean = false
                              ): Gold = {
    val goldPipeline = new Gold(workspace, workspace.database, workspace.getConfig)
      .setReadOnly(readOnly)
      .suppressRangeReport(suppressReport)
      .initPipelineRun()

    if (suppressStaticDatasets) {
      goldPipeline
    } else {
      goldPipeline.loadStaticDatasets()
    }
  }

}