package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, Helpers, Module, ModuleStatusReport, NoNewDataException, SchemaTools, SparkSessionWrapper, UnhandledException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row}
import Schema.verifyDF
import org.apache.spark.sql.functions.col

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

      if (sourceDF.rdd.take(1).nonEmpty) { // if source DF is nonEmpty, continue with transforms
        if (transforms.nonEmpty) {
          val transformedDF = transforms.get.foldLeft(verifiedSourceDF) {
            case (df, transform) =>
              df.transform(transform)
          }
          write(transformedDF, module)
        } else {
          write(verifiedSourceDF, module)
        }
      } else { // if Source DF is empty don't attempt transforms and send EMPTY to writer
        write(spark.emptyDataFrame, module)
      }
    }
  }

  import spark.implicits._

  protected def finalizeModule(report: ModuleStatusReport): Unit = {
    val pipelineReportTarget = PipelineTable(
      name =  "pipeline_report",
      keys = Array("organization_id", "Overwatch_RunID"),
      config = config,
      incrementalColumns = Array("Pipeline_SnapTS")
    )
    database.write(Seq(report).toDF, pipelineReportTarget)
  }

  protected def initiatePostProcessing(): Unit = {
    //    postProcessor.analyze()
    postProcessor.optimize()
    Helpers.fastrm(Array(
      "/tmp/overwatch/bronze/clusterEventsBatches",
      "/tmp/overwatch/bronze/sparkEventLogPaths"
    ))

  }

  protected def restoreSparkConf(): Unit = {
    restoreSparkConf(config.initialSparkConf)
  }

  protected def restoreSparkConf(value: Map[String, String]): Unit = {
    PipelineFunctions.setSparkOverrides(spark, value, config.debugFlag)
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


  /**
   * Some modules should never progress through time while being empty
   * For example, cluster events may get ahead of audit logs and be empty but that doesn't mean there are
   * no cluster events for that time period
   *
   * @param moduleID
   * @return
   */
  private def getVerifiedUntilTS(moduleID: Int): Long = {
    val nonEmptyModules = Array(1005)
    if (nonEmptyModules.contains(moduleID)) {
      config.fromTime(moduleID).asUnixTimeMilli
    } else {
      config.untilTime(moduleID).asUnixTimeMilli
    }
  }

  private def failModule(module: Module, target: PipelineTable, outcome: String, msg: String): ModuleStatusReport = {

    ModuleStatusReport(
      organization_id = config.organizationId,
      moduleID = module.moduleID,
      moduleName = module.moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = config.fromTime(module.moduleID).asUnixTimeMilli,
      untilTS = getVerifiedUntilTS(module.moduleID),
      dataFrequency = target.dataFrequency.toString,
      status = s"${outcome}: $msg",
      recordsAppended = 0L,
      lastOptimizedTS = getLastOptimized(module.moduleID),
      vacuumRetentionHours = 0,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )

  }

  // TODO -- refactor the module status report process
  private[overwatch] def append(target: PipelineTable)(df: DataFrame, module: Module): Unit = {
    val startTime = System.currentTimeMillis()

    try {
      if (df.schema.fieldNames.contains("__OVERWATCHEMPTY") || df.rdd.take(1).isEmpty) {
        val emptyStatusReport = ModuleStatusReport(
          organization_id = config.organizationId,
          moduleID = module.moduleID,
          moduleName = module.moduleName,
          primordialDateString = config.primordialDateString,
          runStartTS = startTime,
          runEndTS = startTime,
          fromTS = config.fromTime(module.moduleID).asUnixTimeMilli,
          untilTS = getVerifiedUntilTS(module.moduleID),
          dataFrequency = target.dataFrequency.toString,
          status = "EMPTY",
          recordsAppended = 0L,
          lastOptimizedTS = getLastOptimized(module.moduleID),
          vacuumRetentionHours = 24 * 7,
          inputConfig = config.inputConfig,
          parsedConfig = config.parsedConfig
        )
        finalizeModule(emptyStatusReport)
        throw new NoNewDataException(s"FAILED to append: ${target.tableFullName}")
      }
      if (df.schema.fieldNames.contains("__OVERWATCHFAILURE")) throw new UnhandledException(s"FAILED to append: ${
        target.tableFullName
      }")
      var finalDF = df
      var lastOptimizedTS: Long = getLastOptimized(module.moduleID)

      finalDF = if (target.zOrderBy.nonEmpty) {
        TransformFunctions.moveColumnsToFront(finalDF, target.zOrderBy ++ target.statsColumns)
      } else finalDF

      // TODO -- handle streaming until Module refactor with source -> target mappings
      val finalDFPartCount = if (target.checkpointPath.nonEmpty && config.cloudProvider == "azure") {
        target.name match {
          case "audit_log_bronze" => spark.table(s"${
            config.databaseName
          }.audit_log_raw_events")
            .rdd.partitions.length * target.shuffleFactor
        }
      } else {
        sourceDFparts * target.shuffleFactor
      }
      val estimatedFinalDFSizeMB = finalDFPartCount * spark.conf.get("spark.sql.files.maxPartitionBytes").toInt / 1024 / 1024
      val targetShuffleSize = math.max(100, finalDFPartCount).toInt

      if (config.debugFlag) {
        println(s"DEBUG: Target Shuffle Partitions: ${
          targetShuffleSize
        }")
        println(s"DEBUG: Max PartitionBytes (MB): ${
          spark.conf.get("spark.sql.files.maxPartitionBytes").toInt / 1024 / 1024
        }")
      }

      logger.log(Level.INFO, s"${
        module.moduleName
      }: Final DF estimated at ${
        estimatedFinalDFSizeMB
      } MBs." +
        s"\nShufflePartitions: ${
          targetShuffleSize
        }")

      spark.conf.set("spark.sql.shuffle.partitions", targetShuffleSize)

      // repartition partitioned tables that are not auto-optimized into the range partitions for writing
      // without this the file counts of partitioned tables will be extremely high
      finalDF = if (target.partitionBy.nonEmpty && !target.autoOptimize) {
        finalDF.repartition(targetShuffleSize, target.partitionBy map col: _*)
      } else finalDF

      val startLogMsg = s"Beginning append to ${
        target.tableFullName
      }"
      logger.log(Level.INFO, startLogMsg)


      // Append the output
      if (!_database.write(finalDF, target)) throw new Exception("PIPELINE FAILURE")
      val debugCount = if (!config.isFirstRun || config.debugFlag) {
        val dfCount = finalDF.count()
        val msg = s"${
          module.moduleName
        } SUCCESS! ${
          dfCount
        } records appended."
        println(msg)
        logger.log(Level.INFO, msg)
        dfCount
      } else {
        logger.log(Level.INFO, "Counts not calculated on first run.")
        0
      }

      if (needsOptimize(module.moduleID)) {
        postProcessor.markOptimize(target)
        lastOptimizedTS = config.untilTime(module.moduleID).asUnixTimeMilli
      }

      restoreSparkConf()

      val endTime = System.currentTimeMillis()

      val moduleStatusReport = ModuleStatusReport(
        organization_id = config.organizationId,
        moduleID = module.moduleID,
        moduleName = module.moduleName,
        primordialDateString = config.primordialDateString,
        runStartTS = startTime,
        runEndTS = endTime,
        fromTS = config.fromTime(module.moduleID).asUnixTimeMilli,
        untilTS = config.untilTime(module.moduleID).asUnixTimeMilli,
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
        val msg = s"ALERT: No New Data Retrieved for Module ${
          module.moduleID
        }! Skipping"
        println(msg)
        logger.log(Level.WARN, msg, e)
      case e: Throwable =>
        val msg = s"${
          module.moduleName
        } FAILED -->\nMessage: ${
          e.getMessage
        }\nCause:${
          e.getCause
        }"
        logger.log(Level.ERROR, msg, e)
        // TODO -- handle rollback
        // TODO -- Capture e message in failReport
        val rollbackMsg = s"ROLLBACK: Attempting Roll back ${
          module.moduleName
        }."
        println(rollbackMsg)
        println(msg, e)
        logger.log(Level.WARN, rollbackMsg)
        try {
          database.rollbackTarget(target)
        } catch {
          case eSub: Throwable => {
            val rollbackFailedMsg = s"ROLLBACK FAILED: ${
              module.moduleName
            } -->\nMessage: ${
              eSub.getMessage
            }\nCause:" +
              s" ${
                eSub.getCause
              }"
            println(rollbackFailedMsg, eSub)
            logger.log(Level.ERROR, rollbackFailedMsg, eSub)
          }
        }
        val failedModuleReport = failModule(module, target, "FAILED", msg)
        finalizeModule(failedModuleReport)
        throw new FailedModuleException(s"MODULE FAILED: ${
          module.moduleID
        } --> ${
          module.moduleName
        }\n ${
          e.getMessage
        }")
    }

  }

}

