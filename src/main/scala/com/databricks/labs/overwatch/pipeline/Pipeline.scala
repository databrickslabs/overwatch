package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.pipeline.Pipeline.{systemZoneId, systemZoneOffset}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{from_unixtime, lit, rank, row_number}
import org.apache.spark.sql.DataFrame
import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

class Pipeline(_workspace: Workspace, _database: Database,
               _config: Config) extends PipelineTargets(_config) with SparkSessionWrapper {

  // TODO -- Validate Targets (unique table names, ModuleIDs and names, etc)
  private val logger: Logger = Logger.getLogger(this.getClass)
  final val workspace: Workspace = _workspace
  final val database: Database = _database
  final val config: Config = _config
  private var _pipelineSnapTime: Long = _
  lazy protected final val postProcessor = new PostProcessor()
  private val pipelineState = scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport]()
  import spark.implicits._

  envInit()

  protected def pipeline: Pipeline = this

  protected def getModuleState(moduleID: Int): Option[SimplifiedModuleStatusReport] = {
    pipelineState.get(moduleID)
  }

  protected def getPipelineState: scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport] = {
    pipelineState
  }

  protected def updateModuleState(moduleState: SimplifiedModuleStatusReport): Unit = {
    pipelineState.put(moduleState.moduleID, moduleState)
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

  private def showRangeReport(): Unit = {
    val rangeReport = pipelineState.values.map(lr => (
      lr.moduleID,
      lr.moduleName,
      lr.primordialDateString,
      lr.fromTS,
      lr.untilTS,
      pipelineSnapTime
    ))

    rangeReport.toSeq.toDF("moduleID", "moduleName", "primordialDateString", "fromTS", "untilTS", "snapTS")
      .orderBy('snapTS.desc, 'moduleId)
      .show(50, false)
  }

  /**
   * Ensure all static datasets exist in the newly initialized Database. This function must be called after
   * the database has been initialized.
   *
   * @return
   */
  private def loadStaticDatasets(): this.type = {
    if (!spark.catalog.tableExists(config.consumerDatabaseName, "instanceDetails")) {
      val instanceDetailsDF = config.cloudProvider match {
        case "aws" =>
          InitializerFunctions.loadLocalCSVResource(spark, "/AWS_Instance_Details.csv")
        case "azure" =>
          InitializerFunctions.loadLocalCSVResource(spark, "/Azure_Instance_Details.csv")
        case _ =>
          throw new IllegalArgumentException("Overwatch only supports cloud providers, AWS and Azure.")
      }

      instanceDetailsDF
        .withColumn("organization_id", lit(config.organizationId))
        .withColumn("interactiveDBUPrice", lit(config.contractInteractiveDBUPrice))
        .withColumn("automatedDBUPrice", lit(config.contractAutomatedDBUPrice))
        .withColumn("Pipeline_SnapTS", pipelineSnapTime.asColumnTS)
        .withColumn("Overwatch_RunID", lit(config.runID))
        .coalesce(1)
        .write.format("delta")
        .partitionBy("organization_id")
        .saveAsTable(s"${config.consumerDatabaseName}.instanceDetails")
    }
    this
  }

  /**
   * initialize the pipeline run
   * Identify the timestamps to use by module and set them
   *
   * @return
   */
  private def initPipelineRun(): this.type = {
    if (spark.catalog.databaseExists(config.databaseName) &&
      spark.catalog.tableExists(config.databaseName, "pipeline_report")) {
      val w = Window.partitionBy('organization_id, 'moduleID).orderBy('Pipeline_SnapTS.desc)
      spark.table(s"${config.databaseName}.pipeline_report")
        .filter('Status === "SUCCESS" || 'Status.startsWith("EMPTY"))
        .filter('organization_id === config.organizationId)
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter('rnk === 1 && 'rn === 1)
        .drop("inputConfig", "parsedConfig")
        .as[SimplifiedModuleStatusReport]
        .collect()
        .foreach(updateModuleState)
    } else {
      config.setIsFirstRun(true)
      Array[SimplifiedModuleStatusReport]()
    }
    if (pipelineState.nonEmpty) showRangeReport()
    setPipelineSnapTime()
    this
  }

  /**
   * Absolute oldest date for which to pull data. This is to help limit the stress on a cold start / gap start.
   * If trying to pull more than 60 days of data before https://databricks.atlassian.net/browse/SC-38627 is complete
   * The primary concern is that the historical data from the cluster events API generally expires on/before 60 days
   * and the event logs are not stored in an optimal way at all. SC-38627 should help with this but for now, max
   * age == 60 days.
   *
   * @return
   */
  protected def primordialEpoch: Long = {
    LocalDateTime.now(systemZoneId).minusDays(derivePrimordialDaysDiff)
      .toLocalDate.atStartOfDay
      .toInstant(systemZoneOffset)
      .toEpochMilli
  }

  /**
   *
   * Pipeline Snap Date minus primordial date or 60
   *
   * @return
   */
  @throws(classOf[IllegalArgumentException])
  private def derivePrimordialDaysDiff: Int = {

    if (config.primordialDateString.nonEmpty) {
      try {
        val primordialLocalDate = TimeTypesConstants.dtFormat.parse(config.primordialDateString.get)
          .toInstant.atZone(systemZoneId)
          .toLocalDate

        Duration.between(
          primordialLocalDate.atStartOfDay(),
          pipelineSnapTime.asLocalDateTime.toLocalDate.atStartOfDay())
          .toDays.toInt
      } catch {
        case e: IllegalArgumentException => {
          val errorMessage = s"ERROR: Primordial Date String has Incorrect Date Format: Must be ${TimeTypesConstants.dtStringFormat}"
          println(errorMessage, e)
          logger.log(Level.ERROR, errorMessage, e)
          // Throw new error to avoid hidden, unexpected/incorrect primordial date -- Force Fail and bubble up
          throw new IllegalArgumentException(e)
        }
      }
    } else {
      60
    }

  }

  /**
    * Azure retrieves audit logs from EH which is to the millisecond whereas aws audit logs are delivered daily.
    * Accepting data with higher precision than delivery causes bad data
    */
  protected val auditLogsIncrementalCols: Seq[String] = if (config.cloudProvider == "azure") Seq("timestamp", "date") else Seq("date")

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

  private[overwatch] def append(target: PipelineTable)(df: DataFrame, module: Module): ModuleStatusReport = {
    val startTime = System.currentTimeMillis()

    try {

      val finalDF = PipelineFunctions.optimizeWritePartitions(df, target, spark, config, module.moduleName)

      val startLogMsg = s"Beginning append to ${target.tableFullName}"
      logger.log(Level.INFO, startLogMsg)

      // Append the output
      if (!_database.write(finalDF, target, pipelineSnapTime.asColumnTS)) throw new Exception("PIPELINE FAILURE")

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
        lastOptimizedTS = module.untilTime.asUnixTimeMilli
      }

      restoreSparkConf()

      val endTime = System.currentTimeMillis()

      // Generate Success Report
      ModuleStatusReport(
        organization_id = config.organizationId,
        moduleID = module.moduleId,
        moduleName = module.moduleName,
        primordialDateString = config.primordialDateString,
        runStartTS = startTime,
        runEndTS = endTime,
        fromTS = module.fromTime.asUnixTimeMilli,
        untilTS = module.untilTime.asUnixTimeMilli,
        dataFrequency = target.dataFrequency.toString,
        status = "SUCCESS",
        recordsAppended = dfCount,
        lastOptimizedTS = lastOptimizedTS,
        vacuumRetentionHours = 24 * 7,
        inputConfig = config.inputConfig,
        parsedConfig = config.parsedConfig
      )
    } catch {
      case e: Throwable =>
        val msg = s"${module.moduleName} FAILED -->\nMessage: ${e.getMessage}\nCause:${e.getCause}"
        logger.log(Level.ERROR, msg, e)
        throw new FailedModuleException(msg, target, Level.ERROR)
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
      .initPipelineRun()
      .loadStaticDatasets()

  }
}

