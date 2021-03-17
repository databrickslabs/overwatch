package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.{Level, Logger}

class Gold(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with GoldTransforms {

  import spark.implicits._

  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val clusterModule = Module(3001, "Gold_Cluster", this, Array(2014))
  lazy private val appendClusterProccess = ETLDefinition(
    SilverTargets.clustersSpecTarget.asIncrementalDF(clusterModule, "timestamp"),
    Seq(buildCluster()),
    append(GoldTargets.clusterTarget)
  )

  private val clusterStateFactModule = Module(3005, "Gold_ClusterStateFact", this, Array(1005, 2014))
  lazy private val appendClusterStateFactProccess = ETLDefinition(
    BronzeTargets.clusterEventsTarget.asIncrementalDF(clusterStateFactModule, "timestamp"),
    Seq(buildClusterStateFact(
      BronzeTargets.cloudMachineDetail,
      BronzeTargets.clustersSnapshotTarget,
      SilverTargets.clustersSpecTarget
    )),
    append(GoldTargets.clusterStateFactTarget)
  )

  //  private val poolsModule = Module(3006, "Gold_Pools")
  //  lazy private val appendPoolsProcess = EtlDefinition(
  //    BronzeTargets.poolsTarget.asIncrementalDF(
  //      buildIncrementalFilter("")
  //    )
  //  )

  private val jobsModule = Module(3002, "Gold_Job", this, Array(2010))
  lazy private val appendJobsProcess = ETLDefinition(
    SilverTargets.dbJobsStatusTarget.asIncrementalDF(jobsModule, "timestamp"),
    Seq(buildJobs()),
    append(GoldTargets.jobTarget)
  )

  private val jobRunsModule = Module(3003, "Gold_JobRun", this, Array(2011))
  lazy private val appendJobRunsProcess = ETLDefinition(
    SilverTargets.dbJobRunsTarget.asIncrementalDF(jobRunsModule, "endEpochMS"),
    Seq(buildJobRuns()),
    append(GoldTargets.jobRunTarget)
  )

  private val jobRunCostPotentialFactModule = Module(3015, "Gold_jobRunCostPotentialFact", this, Array(3001, 3003, 3005, 3010, 3012))
  //Incremental current spark job and tasks DFs plus 2 days for lag coverage
  lazy private val appendJobRunCostPotentialFactProcess = ETLDefinition(
    // new jobRuns to be considered are job runs completed since the last overwatch import for this module
    GoldTargets.jobRunTarget.asIncrementalDF(jobRunCostPotentialFactModule, "endEpochMS"),
    Seq(
      // Retrieve cluster states for current time period plus 2 days for lagging states
      buildJobRunCostPotentialFact(
        GoldTargets.clusterStateFactTarget.asIncrementalDF(jobRunCostPotentialFactModule, 90, "date_state_start"),
        // Lookups
        GoldTargets.clusterTarget.asDF,
        BronzeTargets.cloudMachineDetail.asDF,
        GoldTargets.sparkJobTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date"),
        GoldTargets.sparkTaskTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date"),
        config.contractInteractiveDBUPrice, config.contractAutomatedDBUPrice
      )),
    append(GoldTargets.jobRunCostPotentialFactTarget)
  )

  private val notebookModule = Module(3004, "Gold_Notebook", this, Array(2018))
  lazy private val appendNotebookProcess = ETLDefinition(
    SilverTargets.notebookStatusTarget.asIncrementalDF(notebookModule, "timestamp"),
    Seq(buildNotebook()),
    append(GoldTargets.notebookTarget)
  )

  private val accountModModule = Module(3007, "Gold_AccountMod", this, Array(2017))
  lazy private val appendAccountModProcess = ETLDefinition(
    SilverTargets.accountModTarget.asIncrementalDF(accountModModule, "timestamp"),
    Seq(buildAccountMod()),
    append(GoldTargets.accountModsTarget)
  )

  private val accountLoginModule = Module(3008, "Gold_AccountLogin", this, Array(2016))
  lazy private val appendAccountLoginProcess = ETLDefinition(
    SilverTargets.accountLoginTarget.asIncrementalDF(accountLoginModule, "timestamp"),
    Seq(buildLogin(SilverTargets.accountModTarget.asDF)),
    append(GoldTargets.accountLoginTarget)
  )

  private val sparkJobModule = Module(3010, "Gold_SparkJob", this, Array(2006))
  lazy private val appendSparkJobProcess = ETLDefinition(
    SilverTargets.jobsTarget.asIncrementalDF(sparkJobModule, "startTimestamp"),
    Seq(buildSparkJob(config.cloudProvider)),
    append(GoldTargets.sparkJobTarget)
  )

  private val sparkStageModule = Module(3011, "Gold_SparkStage", this, Array(2007))
  lazy private val appendSparkStageProcess = ETLDefinition(
    SilverTargets.stagesTarget.asIncrementalDF(sparkStageModule, "startTimestamp"),
    Seq(buildSparkStage()),
    append(GoldTargets.sparkStageTarget)
  )

  private val sparkTaskModule = Module(3012, "Gold_SparkTask", this, Array(2008))
  lazy private val appendSparkTaskProcess = ETLDefinition(
    SilverTargets.tasksTarget.asIncrementalDF(sparkTaskModule, "startTimestamp"),
    Seq(buildSparkTask()),
    append(GoldTargets.sparkTaskTarget)
  )

  private val sparkExecutionModule = Module(3013, "Gold_SparkExecution", this, Array(2005))
  lazy private val appendSparkExecutionProcess = ETLDefinition(
    SilverTargets.executionsTarget.asIncrementalDF(sparkExecutionModule, "startTimestamp"),
    Seq(buildSparkExecution()),
    append(GoldTargets.sparkExecutionTarget)
  )

  private val sparkExecutorModule = Module(3014, "Gold_SparkExecutor", this, Array(2003))
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
      case OverwatchScope.sparkEvents => {
        processSparkEvents()
      }
      case OverwatchScope.jobs => {
        jobsModule.execute(appendJobsProcess)
        jobRunsModule.execute(appendJobRunsProcess)
        GoldTargets.jobViewTarget.publish(jobViewColumnMapping)
        GoldTargets.jobRunsViewTarget.publish(jobRunViewColumnMapping)
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

    //    val scope = config.overwatchScope
    //
    //    if (scope.contains(OverwatchScope.accounts)) {
    //      appendAccountModProcess.process()
    //      appendAccountLoginProcess.process()
    //
    //      GoldTargets.accountLoginViewTarget.publish(accountLoginViewColumnMappings)
    //      GoldTargets.accountModsViewTarget.publish(accountModViewColumnMappings)
    //    }
    //
    //    if (scope.contains(OverwatchScope.clusters)) {
    //      appendClusterProccess.process()
    //      GoldTargets.clusterViewTarget.publish(clusterViewColumnMapping)
    //    }
    //
    ////    if (scope.contains(OverwatchScope.pools)) {
    ////
    ////    }
    //
    //    if (scope.contains(OverwatchScope.jobs)) {
    //      appendJobsProcess.process()
    //      appendJobRunsProcess.process()
    //      GoldTargets.jobViewTarget.publish(jobViewColumnMapping)
    //      GoldTargets.jobRunsViewTarget.publish(jobRunViewColumnMapping)
    //    }
    //
    //    if (scope.contains(OverwatchScope.notebooks)) {
    //      appendNotebookProcess.process()
    //      GoldTargets.notebookViewTarget.publish(notebookViewColumnMappings)
    //    }
    //
    //    if (scope.contains(OverwatchScope.sparkEvents)) {
    //      processSparkEvents()
    //    }
    //
    //    //Build facts in scope -- this is done last as facts can consume from the gold model itself
    //    if (scope.contains(OverwatchScope.clusterEvents)) {
    //      appendClusterStateFactProccess.process()
    //      GoldTargets.clusterStateFactViewTarget.publish(clusterStateFactViewColumnMappings)
    //    }
    //
    //    if (scope.contains(OverwatchScope.jobs)) {
    //      appendJobRunCostPotentialFactProcess.process()
    //      GoldTargets.jobRunCostPotentialFactViewTarget.publish(jobRunCostPotentialFactViewColumnMapping)
    //    }

        initiatePostProcessing()
    this // to be used as fail switch later if necessary
  }

}

object Gold {
  def apply(workspace: Workspace): Gold = new Gold(workspace, workspace.database, workspace.getConfig)
    .initPipelineRun()
    .loadStaticDatasets()

}