package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.Logger


class Platinum(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with PlatinumTransforms {

  /**
   * Enable access to Gold pipeline tables externally.
   *
   * @return
   */
  def getAllTargets: Array[PipelineTable] = {
    Array(
      PlatinumTargets.clusterPlatinumTarget
    )
  }

  def getAllModules: Seq[Module] = {
    config.overwatchScope.flatMap {
      case OverwatchScope.clusters => {
        Array(clusterModule)
      }
      case _ => Array[Module]()
    }
  }
  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val clsfSparkOverrides = Map(
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "67108864" // lower to 64MB due to high skew potential
  )

  lazy private[overwatch] val clusterModule = Module(4001, "Platinum_Cluster", this, Array(3005))
  lazy private val appendClusterProccess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        GoldTargets.clusterStateFactTarget.asDF,
        Seq(buildClusterPlatinum(config,GoldTargets.clusterTarget.asDF)),
        append(PlatinumTargets.clusterPlatinumTarget)
      )
  }



  private def executeModules(): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.clusters =>  clusterModule.execute(appendClusterProccess)
      case _ =>
    }
  }

  def run(): Pipeline = {

    restoreSparkConf()
    executeModules()

    initiatePostProcessing()
    this // to be used as fail switch later if necessary
  }

}

object Platinum {
  def apply(workspace: Workspace): Platinum = {
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

                              ): Platinum = {
    val platinumPipeline = new Platinum(workspace, workspace.database, workspace.getConfig)
      .setReadOnly(if (workspace.isValidated) readOnly else true) // if workspace is not validated set it read only
      .suppressRangeReport(suppressReport)
      .initPipelineRun()

    if (suppressStaticDatasets) {
      platinumPipeline
    } else {
      platinumPipeline.loadStaticDatasets()
    }
  }

}