package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, ModuleStatusReport,
  NoNewDataException, SchemaTools, SparkSessionWrapper, Module}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row}
import Schema.verifyDF
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

      println("Validating Input Schemas")
      val verifiedSourceDF = verifyDF(sourceDF, module)

      try {
        sourceDFparts = verifiedSourceDF.rdd.partitions.length
      } catch {
        case _: AnalysisException =>
          println(s"Delaying source shuffle Partition Set since input is stream")
      }

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

  import spark.implicits._

  protected def finalizeModule(report: ModuleStatusReport): Unit = {
    val pipelineReportTarget = PipelineTable("pipeline_report", Array("Overwatch_RunID"), config, Array("Pipeline_SnapTS"))
    database.write(Seq(report).toDF, pipelineReportTarget)
  }

  protected def initiatePostProcessing(): Unit = {
//    postProcessor.analyze()
    postProcessor.optimize()
  }

  protected def restoreSparkConf(value: Map[String, String] = config.initialSparkConf) : Unit= {
    value foreach { case (k, v) =>
      try{
        if (config.debugFlag && spark.conf.get(k) != v)
          println(s"Resetting $k from ${spark.conf.get(k)} --> $v")
        spark.conf.set(k, v)
      } catch {
        case e: org.apache.spark.sql.AnalysisException => logger.log(Level.WARN, s"Not Settable: $k", e)
        case e: Throwable => logger.log(Level.DEBUG, s"Spark Setting, $k could not be set.")
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

      // TODO -- handle streaming until Module refactor with source -> target mappings
      val finalDFPartCount = if (target.checkpointPath.nonEmpty && config.cloudProvider == "azure") {
        target.name match {
          case "audit_log_bronze" => spark.table(s"${config.databaseName}.audit_log_raw_events")
            .rdd.partitions.length * target.shuffleFactor
        }
      } else {
        sourceDFparts * target.shuffleFactor
      }
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
      if(!_database.write(finalDF, target)) throw new Exception("PIPELINE FAILURE")
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

      if (needsOptimize(module.moduleID)) {
        postProcessor.add(target)
        lastOptimizedTS = config.pipelineSnapTime.asUnixTimeMilli
      }

      restoreSparkConf()

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
        val msg = s"ALERT: No New Data Retrieved for Module ${module.moduleID}! Skipping"
        println(msg)
        logger.log(Level.WARN, msg, e)
        failModule(module, "SKIPPED", msg)
      case e: Throwable =>
        val msg = s"${module.moduleName} FAILED, Unhandled Error"
        logger.log(Level.ERROR, msg, e)
        // TODO -- handle rollback
        val rollbackMsg = s"ROLLBACK: Rolling back ${module.moduleName}."
        println(msg, e)
        println(rollbackMsg)
        logger.log(Level.WARN, rollbackMsg)
        val failedModuleReport = failModule(module, "FAILED", msg)
        finalizeModule(failedModuleReport)
    }

  }

}

