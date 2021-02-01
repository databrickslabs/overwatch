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

  private def buildIncrementalFilter(incrementalColumn: String, moduleId: Int): Seq[IncrementalFilter] = {
    val logStatement =
      s"""
         |ModuleID: ${moduleId}
         |IncrementalColumn: ${incrementalColumn}
         |FromTime: ${config.fromTime(moduleId).asTSString} --> ${config.fromTime(moduleId).asUnixTimeMilli}
         |UntilTime: ${config.untilTime(moduleId).asTSString} --> ${config.untilTime(moduleId).asUnixTimeMilli}
         |""".stripMargin
    logger.log(Level.INFO, logStatement)

    Seq(IncrementalFilter(incrementalColumn,
      lit(config.fromTime(moduleId).asUnixTimeMilli),
      lit(config.untilTime(moduleId).asUnixTimeMilli)
    ))
  }

  private val clusterModule = Module(3001, "Gold_Cluster")
  lazy private val appendClusterProccess = EtlDefinition(
    SilverTargets.clustersSpecTarget.asIncrementalDF(
      buildIncrementalFilter("timestamp", clusterModule.moduleID)
    ),
    Some(Seq(buildCluster())),
    append(GoldTargets.clusterTarget),
    clusterModule
  )

  private val clusterStateFactModule = Module(3005, "Gold_ClusterStateFact")
  lazy private val appendClusterStateFactProccess = EtlDefinition(
    BronzeTargets.clusterEventsTarget.asIncrementalDF(
      buildIncrementalFilter("timestamp", clusterStateFactModule.moduleID)
    ),
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
    SilverTargets.dbJobsStatusTarget.asIncrementalDF(
      buildIncrementalFilter("timestamp", jobsModule.moduleID)
    ),
    Some(Seq(buildJobs())),
    append(GoldTargets.jobTarget),
    jobsModule
  )

  private val jobRunsModule = Module(3003, "Gold_JobRun")
  lazy private val appendJobRunsProcess = EtlDefinition(
    SilverTargets.dbJobRunsTarget.asIncrementalDF(
      buildIncrementalFilter("endEpochMS", jobRunsModule.moduleID)
    ),
    Some(Seq(buildJobRuns())),
    append(GoldTargets.jobRunTarget),
    jobRunsModule
  )

  private val jobRunCostPotentialFactModule = Module(3015, "Gold_jobRunCostPotentialFact")
  lazy private val appendJobRunCostPotentialFactProcess = EtlDefinition(
    // new jobRuns to be considered are job runs completed since the last overwatch import for this module
    GoldTargets.jobRunTarget.asIncrementalDF(
      buildIncrementalFilter("endEpochMS", jobRunCostPotentialFactModule.moduleID)
    ),
    Some(Seq(
      // 3 most recent clusterStateFact imports to obtain previous cluster events for job cost mapping
      buildJobRunCostPotentialFact(
        TransformFunctions.previousNOverwatchRuns(
          GoldTargets.clusterStateFactTarget,
          config.databaseName,
          3,
          jobRunCostPotentialFactModule
      ),
        // Lookups
        GoldTargets.clusterTarget.asDF,
        BronzeTargets.cloudMachineDetail.asDF
    ))),
    append(GoldTargets.jobRunCostPotentialFactTarget),
    jobRunCostPotentialFactModule
  )

  private val notebookModule = Module(3004, "Gold_Notebook")
  lazy private val appendNotebookProcess = EtlDefinition(
    SilverTargets.notebookStatusTarget.asIncrementalDF(
      buildIncrementalFilter("timestamp", notebookModule.moduleID)
    ),
    Some(Seq(buildNotebook())),
    append(GoldTargets.notebookTarget),
    notebookModule
  )

  private val sparkJobModule = Module(3010, "Gold_SparkJob")
  lazy private val appendSparkJobProcess = EtlDefinition(
    SilverTargets.jobsTarget.asIncrementalDF(
      buildIncrementalFilter("startTimestamp", sparkJobModule.moduleID)
    ),
    Some(Seq(buildSparkJob(config.cloudProvider))),
    append(GoldTargets.sparkJobTarget),
    sparkJobModule
  )

  private val sparkStageModule = Module(3011, "Gold_SparkStage")
  lazy private val appendSparkStageProcess = EtlDefinition(
    SilverTargets.stagesTarget.asIncrementalDF(
      buildIncrementalFilter("startTimestamp", sparkStageModule.moduleID)
    ),
    Some(Seq(buildSparkStage())),
    append(GoldTargets.sparkStageTarget),
    sparkStageModule
  )

  private val sparkTaskModule = Module(3012, "Gold_SparkTask")
  lazy private val appendSparkTaskProcess = EtlDefinition(
    SilverTargets.tasksTarget.asIncrementalDF(
      buildIncrementalFilter("startTimestamp", sparkTaskModule.moduleID)
    ),
    Some(Seq(buildSparkTask())),
    append(GoldTargets.sparkTaskTarget),
    sparkTaskModule
  )

  private val sparkExecutionModule = Module(3013, "Gold_SparkExecution")
  lazy private val appendSparkExecutionProcess = EtlDefinition(
    SilverTargets.executionsTarget.asIncrementalDF(
      buildIncrementalFilter("startTimestamp", sparkExecutionModule.moduleID)
    ),
    Some(Seq(buildSparkExecution())),
    append(GoldTargets.sparkExecutionTarget),
    sparkExecutionModule
  )

  private val sparkExecutorModule = Module(3014, "Gold_SparkExecutor")
  lazy private val appendSparkExecutorProcess = EtlDefinition(
    SilverTargets.executorsTarget.asIncrementalDF(
      buildIncrementalFilter("addedTimestamp", sparkExecutorModule.moduleID)
    ),
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

    }

    if (scope.contains(OverwatchScope.clusters)) {
      appendClusterProccess.process()
      appendClusterStateFactProccess.process()
      GoldTargets.clusterViewTarget.publish(clusterViewColumnMapping)
      GoldTargets.clusterStateFactViewTarget.publish(clusterStateFactViewColumnMappings)
    }

//    if (scope.contains(OverwatchScope.pools)) {
//
//    }

    if (scope.contains(OverwatchScope.jobs)) {
      appendJobsProcess.process()
      appendJobRunsProcess.process()
      appendJobRunCostPotentialFactProcess.process()
      GoldTargets.jobViewTarget.publish(jobViewColumnMapping)
      GoldTargets.jobRunsViewTarget.publish(jobRunViewColumnMapping)
      GoldTargets.jobRunCostPotentialFactViewTarget.publish(jobRunCostPotentialFactViewColumnMapping)
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

    initiatePostProcessing()
    true // to be used as fail switch later if necessary
  }


}

object Gold {
  def apply(workspace: Workspace): Gold = new Gold(workspace, workspace.database, workspace.getConfig)

}