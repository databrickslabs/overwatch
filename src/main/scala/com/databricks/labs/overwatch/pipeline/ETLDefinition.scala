package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.ModuleStatusReport
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

class ETLDefinition(
                     _sourceDF: DataFrame,
                     transforms: Seq[DataFrame => DataFrame],
                     write: (DataFrame, Module) => ModuleStatusReport
                   ) {

  private val logger: Logger = Logger.getLogger(this.getClass)
  val sourceDF: DataFrame = _sourceDF

  def copy(
            _sourceDF: DataFrame = sourceDF,
            transforms: Seq[DataFrame => DataFrame] = transforms,
            write: (DataFrame, Module) => ModuleStatusReport = write): ETLDefinition = {
    new ETLDefinition(_sourceDF, transforms, write)
  }

  def executeETL(
                  module: Module,
                  verifiedSourceDF: DataFrame = sourceDF
                ): ModuleStatusReport = {

    val transformedDF = transforms.foldLeft(verifiedSourceDF) {
      case (df, transform) =>
        /*
         * reverting Spark UI Job Group labels for now
         *
         * TODO: enumerate the regressions this would introduce
         *       when the labels set by then platform are replaced
         *       this way.
         * df.sparkSession.sparkContext.setJobGroup(
         *    s"${module.pipeline.config.workspaceName}:${module.moduleName}",
         *    transform.toString)
         */

        df.transform( transform)
    }
    write(transformedDF, module)
  }

}

object ETLDefinition {

  def apply(sourceDF: DataFrame,
            transforms: Seq[DataFrame => DataFrame],
            write: (DataFrame, Module) => ModuleStatusReport): ETLDefinition = {

    new ETLDefinition(
      sourceDF,
      transforms,
      write
    )

  }

  def apply(sourceDF: DataFrame,
            write: (DataFrame, Module) => ModuleStatusReport): ETLDefinition = {

    new ETLDefinition(
      sourceDF,
      Seq(),
      write
    )

  }

}
