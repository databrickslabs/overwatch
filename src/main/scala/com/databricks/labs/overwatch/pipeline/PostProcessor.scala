package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Helpers
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer

class PostProcessor {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val tablesToOptimize: ArrayBuffer[PipelineTable] = ArrayBuffer[PipelineTable]()

  private[overwatch] def markOptimize(table: PipelineTable): Unit = {
    tablesToOptimize.append(table)
  }

  def optimize(pipeline: Pipeline, optimizeScaleCoefficient: Int): Unit = {
    logger.log(Level.INFO, s"BEGINNING OPTIMIZE & ZORDER. " +
      s"Tables in scope are: ${tablesToOptimize.map(_.tableFullName).mkString(",")}")

    PipelineFunctions.scaleCluster(pipeline, optimizeScaleCoefficient)
    // TODO -- spark_events_bronze -- put in proper rules -- hot fix due to optimization issues
    Helpers.parOptimize(tablesToOptimize.toArray.filterNot(_.name == "spark_events_bronze"), maxFileSizeMB = 128)
    Helpers.parOptimize(tablesToOptimize.toArray.filter(_.name == "spark_events_bronze"), maxFileSizeMB = 32)
  }


}
