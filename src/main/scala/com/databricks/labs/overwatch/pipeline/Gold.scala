package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, IncrementalFilter, Module, OverwatchScope}
import org.apache.ivy.core.module.id.ModuleId
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

class Gold(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with GoldTransforms {

  import spark.implicits._

  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val clusterModule = Module(3001, "Gold_Cluster")
  lazy private val appendClusterProccess = EtlDefinition(
    SilverTargets.clustersSpecTarget.asIncrementalDF(clusterModule, "timestamp"),
    Some(Seq(buildCluster())),
    append(GoldTargets.clusterTarget),
    clusterModule
  )

  private val clusterStateFactModule = Module(3005, "Gold_ClusterStateFact")
  lazy private val appendClusterStateFactProccess = EtlDefinition(
    BronzeTargets.clusterEventsTarget.asIncrementalDF(clusterStateFactModule, "timestamp"),
    Some(Seq(buildClusterStateFact(
      BronzeTargets.cloudMachineDetail,
      BronzeTargets.clustersSnapshotTarget,
      SilverTargets.clustersSpecTarget
    ))),
    append(GoldTargets.clusterStateFactTarget),
    clusterStateFactModule
  )

//  private val poolsModule = Module(3006, "Gold_Pools")
//  lazy private val appendPoolsProcess = EtlDefinition(
//    BronzeTargets.poolsTarget.asIncrementalDF(
//      buildIncrementalFilter("")
//    )
//  )

  private val jobsModule = Module(3002, "Gold_Job")
  lazy private val appendJobsProcess = EtlDefinition(
    SilverTargets.dbJobsStatusTarget.asIncrementalDF(jobsModule, "timestamp"),
    Some(Seq(buildJobs())),
    append(GoldTargets.jobTarget),
    jobsModule
  )

  private val jobRunsModule = Module(3003, "Gold_JobRun")
  lazy private val appendJobRunsProcess = EtlDefinition(
    SilverTargets.dbJobRunsTarget.asIncrementalDF(jobRunsModule, "endEpochMS"),
    Some(Seq(buildJobRuns())),
    append(GoldTargets.jobRunTarget),
    jobRunsModule
  )

  private val jobRunCostPotentialFactModule = Module(3015, "Gold_jobRunCostPotentialFact")
  //Incremental current spark job and tasks DFs plus 2 days for lag coverage
  lazy private val appendJobRunCostPotentialFactProcess = EtlDefinition(
    // new jobRuns to be considered are job runs completed since the last overwatch import for this module
    GoldTargets.jobRunTarget.asIncrementalDF(jobRunCostPotentialFactModule, "endEpochMS"),
    Some(Seq(
      // Retrieve cluster states for current time period plus 2 days for lagging states
      buildJobRunCostPotentialFact(
        GoldTargets.clusterStateFactTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date_state_start"),
        // Lookups
        GoldTargets.clusterTarget.asDF,
        BronzeTargets.cloudMachineDetail.asDF,
        GoldTargets.sparkJobTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date"),
        GoldTargets.sparkTaskTarget.asIncrementalDF(jobRunCostPotentialFactModule, 2, "date"),
        config.contractInteractiveDBUPrice, config.contractAutomatedDBUPrice
    ))),
    append(GoldTargets.jobRunCostPotentialFactTarget),
    jobRunCostPotentialFactModule
  )

  private val notebookModule = Module(3004, "Gold_Notebook")
  lazy private val appendNotebookProcess = EtlDefinition(
    SilverTargets.notebookStatusTarget.asIncrementalDF(notebookModule, "timestamp"),
    Some(Seq(buildNotebook())),
    append(GoldTargets.notebookTarget),
    notebookModule
  )

  private val accountModModule = Module(3007, "Gold_AccountMod")
  lazy private val appendAccountModProcess = EtlDefinition(
    SilverTargets.accountModTarget.asIncrementalDF(accountModModule, "timestamp"),
    Some(Seq(buildAccountMod())),
    append(GoldTargets.accountModsTarget),
    accountModModule
  )

  private val accountLoginModule = Module(3008, "Gold_AccountLogin")
  lazy private val appendAccountLoginProcess = EtlDefinition(
    SilverTargets.accountLoginTarget.asIncrementalDF(accountLoginModule, "timestamp"),
    Some(Seq(buildLogin(SilverTargets.accountModTarget.asDF))),
    append(GoldTargets.accountLoginTarget),
    accountLoginModule
  )

  private val sparkJobModule = Module(3010, "Gold_SparkJob")
  lazy private val appendSparkJobProcess = EtlDefinition(
    SilverTargets.jobsTarget.asIncrementalDF(sparkJobModule, "startTimestamp"),
    Some(Seq(buildSparkJob(config.cloudProvider))),
    append(GoldTargets.sparkJobTarget),
    sparkJobModule
  )

  private val sparkStageModule = Module(3011, "Gold_SparkStage")
  lazy private val appendSparkStageProcess = EtlDefinition(
    SilverTargets.stagesTarget.asIncrementalDF(sparkStageModule, "startTimestamp"),
    Some(Seq(buildSparkStage())),
    append(GoldTargets.sparkStageTarget),
    sparkStageModule
  )

  private val sparkTaskModule = Module(3012, "Gold_SparkTask")
  lazy private val appendSparkTaskProcess = EtlDefinition(
    SilverTargets.tasksTarget.asIncrementalDF(sparkTaskModule, "startTimestamp"),
    Some(Seq(buildSparkTask())),
    append(GoldTargets.sparkTaskTarget),
    sparkTaskModule
  )

  private val sparkExecutionModule = Module(3013, "Gold_SparkExecution")
  lazy private val appendSparkExecutionProcess = EtlDefinition(
    SilverTargets.executionsTarget.asIncrementalDF(sparkExecutionModule, "startTimestamp"),
    Some(Seq(buildSparkExecution())),
    append(GoldTargets.sparkExecutionTarget),
    sparkExecutionModule
  )

  private val sparkExecutorModule = Module(3014, "Gold_SparkExecutor")
  lazy private val appendSparkExecutorProcess = EtlDefinition(
    SilverTargets.executorsTarget.asIncrementalDF(sparkExecutorModule, "addedTimestamp"),
    Some(Seq(buildSparkExecutor())),
    append(GoldTargets.sparkExecutorTarget),
    sparkExecutorModule
  )


  private def processSparkEvents(): Unit = {

    appendSparkJobProcess.process()
    appendSparkStageProcess.process()
    appendSparkTaskProcess.process()
    appendSparkExecutionProcess.process()
    appendSparkExecutorProcess.process()

    GoldTargets.sparkJobViewTarget.publish(sparkJobViewColumnMapping)
    GoldTargets.sparkStageViewTarget.publish(sparkStageViewColumnMapping)
    GoldTargets.sparkTaskViewTarget.publish(sparkTaskViewColumnMapping)
    GoldTargets.sparkExecutionViewTarget.publish(sparkExecutionViewColumnMapping)
    GoldTargets.sparkExecutorViewTarget.publish(sparkExecutorViewColumnMapping)

  }



  def run(): Boolean = {

    restoreSparkConf()

    val scope = config.overwatchScope

    if (scope.contains(OverwatchScope.accounts)) {
      appendAccountLoginProcess.process()
      appendAccountModProcess.process()

      GoldTargets.accountLoginViewTarget.publish(accountLoginViewColumnMappings)
      GoldTargets.accountModsViewTarget.publish(accountModViewColumnMappings)
    }

    if (scope.contains(OverwatchScope.clusters)) {
      appendClusterProccess.process()
      GoldTargets.clusterViewTarget.publish(clusterViewColumnMapping)
    }

//    if (scope.contains(OverwatchScope.pools)) {
//
//    }

    if (scope.contains(OverwatchScope.jobs)) {
      appendJobsProcess.process()
      appendJobRunsProcess.process()
      GoldTargets.jobViewTarget.publish(jobViewColumnMapping)
      GoldTargets.jobRunsViewTarget.publish(jobRunViewColumnMapping)
    }

    if (scope.contains(OverwatchScope.notebooks)) {
      appendNotebookProcess.process()
      GoldTargets.notebookViewTarget.publish(notebookViewColumnMappings)
    }

    if (scope.contains(OverwatchScope.sparkEvents)) {
      try {
        processSparkEvents()
      } catch {
        case e: FailedModuleException =>
          logger.log(Level.ERROR, "FAILED: SparkEvents Gold Module", e)
      }
    }

    //Build facts in scope -- this is done last as facts can consume from the gold model itself
    if (scope.contains(OverwatchScope.clusterEvents)) {
      appendClusterStateFactProccess.process()
      GoldTargets.clusterStateFactViewTarget.publish(clusterStateFactViewColumnMappings)
    }

    if (scope.contains(OverwatchScope.jobs)) {
      appendJobRunCostPotentialFactProcess.process()
      GoldTargets.jobRunCostPotentialFactViewTarget.publish(jobRunCostPotentialFactViewColumnMapping)
    }

    initiatePostProcessing()
    true // to be used as fail switch later if necessary
  }


}

object Gold {
  def apply(workspace: Workspace): Gold = new Gold(workspace, workspace.database, workspace.getConfig)

}