package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, Helpers, ModuleStatusReport, NoNewDataException, OverwatchScope, SchemaTools, SimplifiedModuleStatusReport, SparkSessionWrapper, TimeTypes, TimeTypesConstants, UnhandledException}
import TransformFunctions._
import com.databricks.labs.overwatch.pipeline.Pipeline.createTimeDetail
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{from_unixtime, lit}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row}

import java.io.{PrintWriter, StringWriter}
import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

class Pipeline(_workspace: Workspace, _database: Database,
               _config: Config) extends PipelineTargets(_config) with SparkSessionWrapper {

  // TODO -- Validate Targets (unique table names, ModuleIDs and names, etc)
  private val logger: Logger = Logger.getLogger(this.getClass)
  protected final val workspace: Workspace = _workspace
  protected final val database: Database = _database
  protected final val config: Config = _config
  private var _pipelineSnapTime: Long = _
  lazy protected final val postProcessor = new PostProcessor()
  private val pipelineState = scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport]()
  private var sourceDFparts: Int = 200

  envInit()

  def run(): Pipeline
  protected def pipeline: Pipeline = this

  protected def getModuleState(moduleID: Int): Option[SimplifiedModuleStatusReport] = {
    pipelineState.get(moduleID)
  }

  protected def getPipelineState: scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport] = {
    pipelineState
  }

  protected def updateModuleState(moduleState: SimplifiedModuleStatusReport): this.type = {
    pipelineState.put(moduleState.moduleID, moduleState)
    this
  }

  protected def initializePipelineState: this.type = {
    config.lastRunDetail.foreach(updateModuleState)
    this
  }

  /**
   * Snapshot time of the time the snapshot was started. This is used throughout the process as the until timestamp
   * such that every data point to be loaded during the current run must be < this pipeline SnapTime.
   * NOTE: PipelineSnapTime is EXCLUSIVE meaning < ONLY NOT <=
   *
   * @return
   */
  private[overwatch] def setPipelineSnapTime(): this.type = {
    _pipelineSnapTime = LocalDateTime.now(Pipeline.systemZoneId).toInstant(Pipeline.systemZoneOffset).toEpochMilli
    this
  }

  protected def pipelineSnapTime: TimeTypes = {
    createTimeDetail(_pipelineSnapTime)
  }

  /**
   * Getter for Pipeline Snap Time
   * NOTE: PipelineSnapTime is EXCLUSIVE meaning < ONLY NOT <=
   *
   * @return
   */
  def pipelineSnapTime: TimeTypes = {
    Pipeline.createTimeDetail(_pipelineSnapTime)
  }

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
      module.validatePipelineState(getPipelineState)


      println("Validating Input Schemas")

      try {

        if (!sourceDF.isEmpty) { // if source DF is nonEmpty, continue with transforms
          @transient
          lazy val verifiedSourceDF = sourceDF
            .verifyMinimumSchema(Schema.get(module), enforceNonNullCols = true, config.debugFlag)

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
        } else { // if Source DF is empty don't attempt transforms and send EMPTY to writer
//          write(spark.emptyDataFrame, module)
          val msg = s"ALERT: No New Data Retrieved for Module ${module.moduleId}-${module.moduleName}! Skipping"
          println(msg)
          throw new NoNewDataException(msg)
        }
      } catch {
        case e: FailedModuleException =>
          val errMessage = s"FAILED: ${module.moduleId}-${module.moduleName} Module"
          logger.log(Level.ERROR, errMessage, e)
        case e: NoNewDataException =>
          val errMessage = s"EMPTY: ${module.moduleId}-${module.moduleName} Module: SKIPPING"
          logger.log(Level.ERROR, errMessage, e)
          noNewDataHandler(module)
      }
    }
  }

  protected def registerModule(
                              moduleId: Int,
                              moduleName: String,
                              moduleDependencies: Option[Array[Int]] = None
                              ): Unit = {
    Module(
      moduleId,
      moduleName,
      moduleDependencies,
      this
    )

  }

  import spark.implicits._

  /**
    * Azure retrieves audit logs from EH which is to the millisecond whereas aws audit logs are delivered daily.
    * Accepting data with higher precision than delivery causes bad data
    */
  protected val auditLogsIncrementalCols: Seq[String] = if (config.cloudProvider == "azure") Seq("timestamp", "date") else Seq("date")

//  protected def finalizeModule(report: ModuleStatusReport): Unit = {
//    updateModuleState(report.moduleID, report.simple)
//    val pipelineReportTarget = PipelineTable(
//      name = "pipeline_report",
//      keys = Array("organization_id", "Overwatch_RunID"),
//      config = config,
//      incrementalColumns = Array("Pipeline_SnapTS")
//    )
//    database.write(Seq(report).toDF, pipelineReportTarget)
//  }

  private[overwatch] def initiatePostProcessing(): Unit = {
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

//  private def failModule(moduleId: Int, moduleName: String, target: PipelineTable, msg: String): Unit = {
//
//    val rollbackMsg = s"ROLLBACK: Attempting Roll back $moduleName."
//    println(rollbackMsg)
//    logger.log(Level.WARN, rollbackMsg)
//
//    val rollbackStatus = try {
//      database.rollbackTarget(target)
//      "ROLLBACK SUCCESSFUL"
//    } catch {
//      case e: Throwable => {
//        val rollbackFailedMsg = s"ROLLBACK FAILED: $moduleName -->\nMessage: ${e.getMessage}\nCause:" +
//          s"${e.getCause}"
//        println(rollbackFailedMsg, e)
//        logger.log(Level.ERROR, rollbackFailedMsg, e)
//        "ROLLBACK FAILED"
//      }
//    }
//
//    val failedStatusReport = ModuleStatusReport(
//      organization_id = config.organizationId,
//      moduleID = moduleId,
//      moduleName = moduleName,
//      primordialDateString = config.primordialDateString,
//      runStartTS = 0L,
//      runEndTS = 0L,
//      fromTS = config.fromTime(module.moduleId).asUnixTimeMilli,
//      untilTS = getVerifiedUntilTS(module.moduleId),
//      dataFrequency = target.dataFrequency.toString,
//      status = s"FAILED --> $rollbackStatus: ERROR:\n$msg",
//      recordsAppended = 0L,
//      lastOptimizedTS = getLastOptimized(module.moduleId),
//      vacuumRetentionHours = 0,
//      inputConfig = config.inputConfig,
//      parsedConfig = config.parsedConfig
//    )
//    finalizeModule(failedStatusReport)
//    throw new FailedModuleException(msg)
//
//  }

//  protected def noNewDataHandler(): Unit = {
//    val startTime = System.currentTimeMillis()
//    val emptyStatusReport = ModuleStatusReport(
//      organization_id = config.organizationId,
//      moduleID = module.moduleId,
//      moduleName = module.moduleName,
//      primordialDateString = config.primordialDateString,
//      runStartTS = startTime,
//      runEndTS = startTime,
//      fromTS = config.fromTime(module.moduleId).asUnixTimeMilli,
//      untilTS = getVerifiedUntilTS(module.moduleId),
//      dataFrequency = "",
//      status = "EMPTY",
//      recordsAppended = 0L,
//      lastOptimizedTS = getLastOptimized(module.moduleId),
//      vacuumRetentionHours = 24 * 7,
//      inputConfig = config.inputConfig,
//      parsedConfig = config.parsedConfig
//    )
//    finalizeModule(emptyStatusReport)
//  }

  private[overwatch] def append(target: PipelineTable)(df: DataFrame, moduleId: Int, moduleName: String): Unit = {
    val startTime = System.currentTimeMillis()

    try {

      val finalDF = PipelineFunctions.optimizeWritePartitions(df, sourceDFparts, target, spark, config, moduleName: String)


      val startLogMsg = s"Beginning append to ${target.tableFullName}"
      logger.log(Level.INFO, startLogMsg)


      // Append the output
      if (!_database.write(finalDF, target)) throw new Exception("PIPELINE FAILURE")

      // Source files for spark event logs are extremely inefficient. Get count from bronze table instead
      // of attempting to re-read the very inefficient json.gz files.
      val dfCount = if (target.name == "spark_events_bronze") {
        target.asIncrementalDF(module, 2, "fileCreateDate", "fileCreateEpochMS").count()
      } else finalDF.count()

      val msg = s"SUCCESS! ${module.moduleName}: $dfCount records appended."
      println(msg)
      logger.log(Level.INFO, msg)

      var lastOptimizedTS: Long = getLastOptimized(module.moduleId)
      if (needsOptimize(module.moduleId)) {
        postProcessor.markOptimize(target)
        lastOptimizedTS = config.untilTime(module.moduleId).asUnixTimeMilli
      }

      restoreSparkConf()

      val endTime = System.currentTimeMillis()

      // Generate Success Report
      val moduleStatusReport = ModuleStatusReport(
        organization_id = config.organizationId,
        moduleID = module.moduleId,
        moduleName = module.moduleName,
        primordialDateString = config.primordialDateString,
        runStartTS = startTime,
        runEndTS = endTime,
        fromTS = config.fromTime(module.moduleId).asUnixTimeMilli,
        untilTS = config.untilTime(module.moduleId).asUnixTimeMilli,
        dataFrequency = target.dataFrequency.toString,
        status = "SUCCESS",
        recordsAppended = dfCount,
        lastOptimizedTS = lastOptimizedTS,
        vacuumRetentionHours = 24 * 7,
        inputConfig = config.inputConfig,
        parsedConfig = config.parsedConfig
      )
      finalizeModule(moduleStatusReport)
    } catch {
      case e: Throwable =>
        val msg = s"${module.moduleName} FAILED -->\nMessage: ${e.getMessage}\nCause:${e.getCause}"
        logger.log(Level.ERROR, msg, e)
        failModule(module, target, msg)
    }

  }

}

object Pipeline {

  val systemZoneId: ZoneId = ZoneId.systemDefault()
  val systemZoneOffset: ZoneOffset = systemZoneId.getRules.getOffset(LocalDateTime.now(systemZoneId))

  /**
   * Most of Overwatch uses a custom time type, "TimeTypes" which simply pre-builds the most common forms / formats
   * of time. The sheer number of sources and heterogeneous time rules makes time management very challenging,
   * the idea here is to get it right once and just get the time type necessary.
   *
   * @param tsMilli Unix epoch as a Long in milliseconds
   * @return
   */
  def createTimeDetail(tsMilli: Long): TimeTypes = {
    val localDT = new Date(tsMilli).toInstant.atZone(systemZoneId).toLocalDateTime
    val instant = Instant.ofEpochMilli(tsMilli)
    TimeTypes(
      tsMilli, // asUnixTimeMilli
      lit(from_unixtime(lit(tsMilli).cast("double") / 1000).cast("timestamp")), // asColumnTS in local time,
      Date.from(instant), // asLocalDateTime
      instant.atZone(systemZoneId), // asSystemZonedDateTime
      localDT, // asLocalDateTime
      localDT.toLocalDate.atStartOfDay(systemZoneId).toInstant.toEpochMilli // asMidnightEpochMilli
    )
  }

  def apply(workspace: Workspace, database: Database, config: Config): Pipeline = {

    new Pipeline(workspace, database, config)

  }
}

