package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, NoNewDataException, SimplifiedModuleStatusReport}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}
//import PipelineFunctions._
//import TransformFunctions._

private[overwatch] sealed class ETLDefinition(
                                               _moduleId: Int,
                                               _moduleName: String,
                                               _moduleDependencies: Option[Array[Int]],
                                               _workspace: Workspace,
                                               _database: Database,
                                               _config: Config,
                                               sourceDF: DataFrame,
                                               transforms: Option[Seq[DataFrame => DataFrame]],
                                               write: (DataFrame, Int, String) => Unit
                                             ) extends Module(
  _moduleId, _moduleName, _moduleDependencies, _workspace, _database, _config
) {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def execute(): Unit = {
    println(s"Beginning: $moduleName")

    try {
      validatePipelineState()
      val verifiedSourceDF = validateSourceDF(sourceDF)
      val sourceDFParts = sourceDFParts(verifiedSourceDF)

      if (transforms.nonEmpty) {
        val transformedDF = transforms.get.foldLeft(verifiedSourceDF) {
          case (df, transform) =>
            df.transform(transform)
        }
        write(transformedDF, moduleId, moduleName)
      } else {
        write(verifiedSourceDF, moduleId, moduleName)
      }
    } catch {
      case e: FailedModuleException =>
        val errMessage = s"FAILED: $moduleId-$moduleName Module"
        logger.log(Level.ERROR, errMessage, e)
      case e: NoNewDataException =>
        val errMessage = s"EMPTY: $moduleId-$moduleName Module: SKIPPING"
        logger.log(Level.ERROR, errMessage, e)
        noNewDataHandler()
    }
  }

}

object ETLDefinition {

  def apply(module: Module,
            sourceDF: DataFrame,
            transforms: Option[Seq[DataFrame => DataFrame]],
            write: (DataFrame, Int, String) => Unit): ETLDefinition = {

    new ETLDefinition(
      module.moduleId,
      module.moduleName,
      module.moduleDependencies,
      module.pipeline.workspace,
      module.pipeline.database,
      module.pipeline.config,
      sourceDF,
      transforms,
      write
    )

  }

}
