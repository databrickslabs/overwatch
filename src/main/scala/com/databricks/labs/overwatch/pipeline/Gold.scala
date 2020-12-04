package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, IncrementalFilter, Module, OverwatchScope}
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
      buildIncrementalFilter("timestamp", jobRunsModule.moduleID)
    ),
    Some(Seq(buildJobRuns())),
    append(GoldTargets.jobRunTarget),
    jobRunsModule
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

  def run(): Boolean = {

    restoreSparkConf()
    setCloudProvider(config.cloudProvider)

    val scope = config.overwatchScope

    if (scope.contains(OverwatchScope.accounts)) {

    }

    if (scope.contains(OverwatchScope.clusters)) {
      appendClusterProccess.process()
      GoldTargets.clusterViewTarget.publish(clusterViewColumnMapping)
    }

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

    }

    true // to be used as fail switch later if necessary
  }


}

object Gold {
  def apply(workspace: Workspace): Gold = new Gold(workspace, workspace.database, workspace.getConfig)

}