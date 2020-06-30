package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, ModuleStatusReport, NoNewDataException, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row}
import java.io.{PrintWriter, StringWriter}

class Pipeline(_workspace: Workspace, _database: Database,
               _config: Config) extends PipelineTargets(_config) with SparkSessionWrapper {

  // TODO -- Validate Targets (unique table names, ModuleIDs and names, etc)
  private val logger: Logger = Logger.getLogger(this.getClass)
  protected final val workspace: Workspace = _workspace
  protected final val database: Database = _database
  protected final val config: Config = _config
  lazy protected final val postProcessor = new PostProcessor()
  private val sw = new StringWriter
  private var sourceDFparts: Int = 200

  envInit()

  // Todo Add Description
  case class Module(moduleID: Int, moduleName: String)

  // TODO -- Add Rules engine
  //  additional field under transforms rules: Option[Seq[Rule => Boolean]]
  case class EtlDefinition(
                            sourceDF: DataFrame,
                            transforms: Option[Seq[DataFrame => DataFrame]],
                            write: (DataFrame, Module) => Unit,
                            module: Module
                          ) {

    def process(): Unit = {
      println(s"Beginning: ${module.moduleName}")
      sourceDFparts = sourceDF.rdd.partitions.length

      if (transforms.nonEmpty) {
        val transformedDF = transforms.get.foldLeft(sourceDF) {
          case (df, transform) =>
            df.transform(transform)
        }
        write(transformedDF, module)
      } else {
        write(sourceDF, module)
      }
    }
  }

  import spark.implicits._

  protected def finalizeModule(report: ModuleStatusReport): Unit = {
    val pipelineReportTarget = PipelineTable("pipeline_report", Array("Overwatch_RunID"), config, Array("Pipeline_SnapTS"))
    database.write(Seq(report).toDF, pipelineReportTarget)
  }

  protected def initiatePostProcessing(): Unit = {
//    postProcessor.analyze()
    postProcessor.optimize()
  }

  private def restoreSparkConf(value: Map[String, String]) : Unit= {
    value foreach { case (k, v) =>
      try{
        spark.conf.set(k, v)
      } catch {
        case e: org.apache.spark.sql.AnalysisException => logger.log(Level.WARN, s"Not Settable: $k", e)
        case e: Throwable => println(s"ERROR: $k, --> $e")
      }
    }
  }

  private def getLastOptimized(moduleID: Int): Long = {
    val lastRunOptimizeTS = config.lastRunDetail.filter(_.moduleID == moduleID)
    if (!config.isFirstRun && lastRunOptimizeTS.nonEmpty) lastRunOptimizeTS.head.lastOptimizedTS
    else 0L
  }

  // TODO - make this timeframe configurable by module
  private def needsOptimize(moduleID: Int): Boolean = {
    // TODO -- Don't use 7 days -- use optimizeFrequency in PipelineTable
    val WEEK = 1000L * 60L * 60L * 24L * 7L // week of milliseconds
    val tsLessSevenD = System.currentTimeMillis() - WEEK.toLong
    if ((getLastOptimized(moduleID) < tsLessSevenD || config.isFirstRun) && !config.isLocalTesting) true
    else false
  }

  private def failModule(module: Module, outcome: String, msg: String): ModuleStatusReport = {
    ModuleStatusReport(
      moduleID = module.moduleID,
      moduleName = module.moduleName,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = 0L,
      untilTS = 0L,
      dataFrequency = "",
      status = s"${outcome}: $msg",
      recordsAppended = 0L,
      lastOptimizedTS = 0L,
      vacuumRetentionHours = 0,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )

  }

  private[overwatch] def append(target: PipelineTable)(df: DataFrame, module: Module): Unit = {
    try {
      if (df.schema.fieldNames.contains("FAILURE")) throw new NoNewDataException(s"FAILED to append: ${target.tableFullName}")
      var finalDF = df
      var lastOptimizedTS: Long = getLastOptimized(module.moduleID)

      finalDF = if (target.zOrderBy.nonEmpty) {
        SchemaTools.moveColumnsToFront(finalDF, target.zOrderBy)
      } else finalDF

      val finalDFPartCount = sourceDFparts * target.shuffleFactor
      val estimatedFinalDFSizeMB = finalDFPartCount * spark.conf.get("spark.sql.files.maxPartitionBytes").toInt / 1024 / 1024
      val targetShuffleSize = math.max(100, finalDFPartCount).toInt

      if (config.debugFlag) {
        println(s"DEBUG: Target Shuffle Partitions: ${targetShuffleSize}")
        println(s"DEBUG: Max PartitionBytes (MB): ${spark.conf.get("spark.sql.files.maxPartitionBytes").toInt / 1024 / 1024}")
      }

      logger.log(Level.INFO, s"${module.moduleName}: Final DF estimated at ${estimatedFinalDFSizeMB} MBs." +
        s"\nShufflePartitions: ${targetShuffleSize}")

      spark.conf.set("spark.sql.shuffle.partitions", targetShuffleSize)

      val startLogMsg = s"Beginning append to ${target.tableFullName}"
      logger.log(Level.INFO, startLogMsg)


      val startTime = System.currentTimeMillis()
      _database.write(finalDF, target)
      val debugCount = if (!config.isFirstRun || config.debugFlag) {
        val dfCount = finalDF.count()
        val msg = s"${module.moduleName} SUCCESS! ${dfCount} records appended."
        println(msg)
        logger.log(Level.INFO, msg)
        dfCount
      } else {
        logger.log(Level.INFO, "Counts not calculated on first run.")
        0
      }

      spark.conf.set("spark.sql.shuffle.partitions", getTotalCores * 2)

      if (needsOptimize(module.moduleID)) {
        postProcessor.add(target)
        lastOptimizedTS = config.pipelineSnapTime.asUnixTimeMilli
      }

      restoreSparkConf(config.initialSparkConf())

      val endTime = System.currentTimeMillis()

      val moduleStatusReport = ModuleStatusReport(
        moduleID = module.moduleID,
        moduleName = module.moduleName,
        runStartTS = startTime,
        runEndTS = endTime,
        fromTS = config.fromTime(module.moduleID).asUnixTimeMilli,
        untilTS = config.pipelineSnapTime.asUnixTimeMilli,
        dataFrequency = target.dataFrequency.toString,
        status = "SUCCESS",
        recordsAppended = debugCount,
        lastOptimizedTS = lastOptimizedTS,
        vacuumRetentionHours = 24 * 7,
        inputConfig = config.inputConfig,
        parsedConfig = config.parsedConfig
      )
      finalizeModule(moduleStatusReport)
    } catch {
      case e: NoNewDataException =>
        val msg = s"Failed: No New Data Retrieved for Module ${module.moduleID}! Skipping"
        println(msg)
        logger.log(Level.WARN, msg, e)
        failModule(module, "SKIPPED", msg)
      case e: Throwable =>
        val msg = s"${module.moduleName} FAILED, Unhandled Error ${e.printStackTrace(new PrintWriter(sw)).toString}"
        logger.log(Level.ERROR, msg, e)
        val rollbackMsg = s"ROLLBACK: Rolling back ${module.moduleName}."
        println(msg)
        println(rollbackMsg)
        logger.log(Level.WARN, rollbackMsg)
        val failedModuleReport = failModule(module, "FAILED", msg)
        finalizeModule(failedModuleReport)
    }

  }

}

