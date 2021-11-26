package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.Optimizer
import com.databricks.labs.overwatch.utils.{Config, Helpers}
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

class PostProcessor(config: Config) extends PipelineTargets(config) {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val tablesToOptimize: ArrayBuffer[PipelineTable] = ArrayBuffer[PipelineTable]()

  /**
   * Specify pipeline target to be optimized
   * Optimize is not done immediately but later so the cluster can be sized correctly and optimize and zorder ops can
   * be completed in parallel
   * @param table Pipeline Target to be marked for optimized
   */
  private[overwatch] def markOptimize(table: PipelineTable): Unit = {
    tablesToOptimize.append(table)
  }

  /**
   * Several module output targets have a few other, intermediate "targets" that aren't the output of an
   * ETLDefinition thus they will never be optimized. This keeps those tables tied to their leader tables
   * and doesn't force an optimize on each pipeline run.
   */
  private def appendIntermediateTargets: Unit = {
    tablesToOptimize.map(_.name).foreach {
      case "audit_log_bronze" => {
        tablesToOptimize.append(BronzeTargets.processedEventLogs)
        tablesToOptimize.append(BronzeTargets.clusterEventsErrorsTarget)
        tablesToOptimize.append(pipelineStateTarget)
        tablesToOptimize.append(BronzeTargets.cloudMachineDetail)
        tablesToOptimize.append(BronzeTargets.dbuCostDetail)
      }
      case "spark_events_bronze" => tablesToOptimize.append(BronzeTargets.processedEventLogs)
      case _ =>
    }
  }

  /**
   * Output optimize start log
   */
  private def emitStartLog: Unit = logger.log(Level.INFO, s"OPTIMIZE: BEGINNING OPTIMIZE & ZORDER. " +
    s"Tables in scope are: ${tablesToOptimize.map(_.tableFullName).mkString(",")}")

  /**
   * Output optimize completion log
   */
  private def emitCompletionLog: Unit = logger.log(Level.INFO, s"OPTIMIZE: OPTIMIZE & ZORDER COMPLETE. ")

  /**
   * Depending on the scopes several of the targets may not exist. The scopes could be disabled and / or the module
   * may have failed since first run resulting in no table / etc.
   * @return All pipeline targets marked for optimize less those that do not exist.
   */
  private def validateExists: Array[PipelineTable] = {
    val targetsPar = tablesToOptimize.toArray.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(16))
    targetsPar.tasksupport = taskSupport
    targetsPar.filter(_.exists).toArray
  }

  /**
   * launch the optimize jobs in parallel
   */
  private def executeOptimize: Unit = {
    appendIntermediateTargets
    val validatedTargets = validateExists
    val invalidTargets = tablesToOptimize.map(_.tableLocation).diff(validatedTargets.map(_.tableLocation))
    val invalidTargetsMsg = s"OPTIMIZE: Missing Targets: \n${invalidTargets.mkString("\n")}"
    val msg = s"OPTIMIZE: Targets To Optimize: \n${validatedTargets.map(_.tableLocation).mkString("\n")}"

    if (config.debugFlag) {
      println(invalidTargetsMsg)
      println(msg)
    }
    logger.log(Level.INFO, invalidTargetsMsg)
    logger.log(Level.INFO, msg)
    // TODO -- spark_events_bronze -- put in proper rules -- hot fix due to optimization issues
    Helpers.parOptimize(validatedTargets.filterNot(_.name == "spark_events_bronze"), maxFileSizeMB = 128)
    Helpers.parOptimize(validatedTargets.filter(_.name == "spark_events_bronze"), maxFileSizeMB = 32)
  }

  /**
   * When the optimize is not executed from within the pipeline run structure it's necessary to go back and
   * update the latest "lastOptimizedTS" state of each module to reflect this optimize as the latest optimization complete
   */
  private def updateLastOptimizedDate: Unit = {
    val lastOptimizedUpdateDF = Optimizer.getLatestSuccessState(config.databaseName)
      .withColumn("lastOptimizedTS", lit(System.currentTimeMillis()))
      .alias("updates")

    val immutableCols = (pipelineStateTarget.keys ++ pipelineStateTarget.incrementalColumns).distinct
    val mergeCondition = immutableCols.map(k => s"updates.$k = target.$k").mkString(" AND ")
    val updateExpr = Map("target.lastOptimizedTS" -> "updates.lastOptimizedTS")

    val msg =
      s"""
         |Pipeline State Update -- Updating Last Optimization Timestamp
         |MERGE CONDITION: $mergeCondition
         |UPDATE EXPRESSION: $updateExpr
         |""".stripMargin

    if (config.debugFlag) println(msg)
    logger.log(Level.INFO, msg)
    DeltaTable.forPath(pipelineStateTarget.tableLocation).alias("target")
      .merge(lastOptimizedUpdateDF, mergeCondition)
      .whenMatched
      .updateExpr(updateExpr)
      .execute()
  }

  /**
   * Start the optimize process. this function should only be called from within the Overwatch package
   * from the Pipeline
   * @param pipeline pipeline to optimize
   * @param optimizeScaleCoefficient scaling coefficient for intelligent scaling
   */
  private[overwatch] def optimize(pipeline: Pipeline, optimizeScaleCoefficient: Int): Unit = {
    emitStartLog
    PipelineFunctions.scaleCluster(pipeline, optimizeScaleCoefficient)
    executeOptimize
    emitCompletionLog
  }

  /**
   * Start the optimize process. This should only be called from the Optimizer Main Class.
   * @param targetsToOptimize Array of PipelineTables considered as optimize candidates.
   */
  private[overwatch] def optimizeOverwatch(targetsToOptimize: Array[PipelineTable]): Unit = {
    emitStartLog
    targetsToOptimize.foreach(t => tablesToOptimize.append(t))
    executeOptimize
    updateLastOptimizedDate
    emitCompletionLog
  }

  private[overwatch] def refreshPipReportView(pipReportViewTarget: PipelineView): Unit = {
    val pipReportViewColumnMappings =
      s"""
         |organization_id, workspace_name, moduleID as module_id, moduleName as module_name,
         |cast(primordialDateString as date) as primordialDate,
         |to_timestamp(fromTS / 1000.0) as fromTS,
         |to_timestamp(untilTS / 1000.0) as untilTS,
         |status, writeOpsMetrics as write_metrics,
         |Pipeline_SnapTS, Overwatch_RunID
         |""".stripMargin
    pipReportViewTarget.publish(pipReportViewColumnMappings, sorted = true, reverse = true)
  }

}
