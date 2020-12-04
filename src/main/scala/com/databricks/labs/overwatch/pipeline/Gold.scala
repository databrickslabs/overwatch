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
    buildCluster(),
    append(GoldTargets.clusterTarget),
    clusterModule
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

    }

    if (scope.contains(OverwatchScope.notebooks)) {

    }

    if (scope.contains(OverwatchScope.sparkEvents)) {

    }

   true // to be used as fail switch later if necessary
  }


}

object Gold {
  def apply(workspace: Workspace): Gold = new Gold(workspace, workspace.database, workspace.getConfig)

}