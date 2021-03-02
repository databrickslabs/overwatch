package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.ModuleStatusReport
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class ETLDefinition(
                     _sourceDF: DataFrame,
                     transforms: Option[Seq[DataFrame => DataFrame]],
                     write: (DataFrame, Module) => ModuleStatusReport
                   ) {

  private val logger: Logger = Logger.getLogger(this.getClass)
  val sourceDF: DataFrame = _sourceDF

  def copy(
            _sourceDF: DataFrame = sourceDF,
            transforms: Option[Seq[DataFrame => DataFrame]] = transforms,
            write: (DataFrame, Module) => ModuleStatusReport = write): ETLDefinition = {
    new ETLDefinition(_sourceDF, transforms, write)
  }

  def executeETL(
                  module: Module,
                  verifiedSourceDF: DataFrame = sourceDF
                ): ModuleStatusReport = {

    if (transforms.nonEmpty) {
      val transformedDF = transforms.get.foldLeft(verifiedSourceDF) {
        case (df, transform) =>
          df.transform(transform)
      }
      write(transformedDF, module)
    } else {
      write(verifiedSourceDF, module)
    }
  }

}

object ETLDefinition {

  def apply(sourceDF: DataFrame,
            transforms: Option[Seq[DataFrame => DataFrame]],
            write: (DataFrame, Module) => ModuleStatusReport): ETLDefinition = {

    new ETLDefinition(
      sourceDF,
      transforms,
      write
    )

  }

}
