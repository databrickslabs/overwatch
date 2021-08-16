package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.pipeline.Pipeline.{deriveLocalDate, systemZoneId, systemZoneOffset}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//import io.delta.tables._

import java.text.SimpleDateFormat
import java.time._
import java.util.Date

class Pipeline(
                _workspace: Workspace,
                final val database: Database,
                _config: Config
              ) extends PipelineTargets(_config) with SparkSessionWrapper {

  // TODO -- Validate Targets (unique table names, ModuleIDs and names, etc)
  //  developer validation to guard against multiple Modules with same target and/or ID/Name
  private val logger: Logger = Logger.getLogger(this.getClass)
  final val workspace: Workspace = _workspace
  final val config: Config = _config
  private var _pipelineSnapTime: Long = _
  private var _readOnly: Boolean = false
  private var _supressRangeReport: Boolean = false
  lazy protected final val postProcessor = new PostProcessor()
  private val pipelineState = scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport]()

  import spark.implicits._

  envInit()

  protected def pipeline: Pipeline = this

  def getConfig: Config = this.config

  def getModuleState(moduleID: Int): Option[SimplifiedModuleStatusReport] = {
    pipelineState.get(moduleID)
  }

  def getPipelineState: scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport] = {
    pipelineState
  }

  def getVerbosePipelineState: Array[ModuleStatusReport] = {
    pipelineStateTarget.asDF.as[ModuleStatusReport].collect()
  }

  def updateModuleState(moduleState: SimplifiedModuleStatusReport): Unit = {
    pipelineState.put(moduleState.moduleID, moduleState)
  }

  def clearPipelineState(): this.type = {
    pipelineState.clear()
    this
  }

  def setReadOnly(value: Boolean): this.type = {
    _readOnly = value
    this
  }

  def readOnly: Boolean = _readOnly

  def suppressRangeReport: this.type = {
    suppressRangeReport(true)
    this
  }
  def suppressRangeReport(value: Boolean): this.type = {
    _supressRangeReport = value
    this
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
    logger.log(Level.INFO, s"INIT: Pipeline Snap TS: ${pipelineSnapTime.asUnixTimeMilli}-${pipelineSnapTime.asTSString}")
    this
  }

  def showRangeReport(): Unit = {
    if (!_supressRangeReport) {
      val rangeReport = pipelineState.values.map(lr => (
        lr.moduleID,
        lr.moduleName,
        lr.primordialDateString,
        lr.fromTS,
        lr.untilTS,
        pipelineSnapTime.asTSString
      ))

      println(s"Current Pipeline State: BEFORE this run.")
      rangeReport.toSeq.toDF("moduleID", "moduleName", "primordialDateString", "fromTS", "untilTS", "snapTS")
        .orderBy('snapTS.desc, 'moduleID)
        .show(50, false)
    }
  }

  /**
   * If latest, active records contain a DBU contract price that differs from those found in the config, return
   * true, otherwise return false
   * @param lastRunPricing instanceDetails dataframe with rnk column
   * @return
   */
  private def dbuContractPriceChange(lastRunPricing: DataFrame): Boolean = {

    lastRunPricing
      .filter('rnk === 1 && 'activeUntil.isNull) // most recent, active records
      .filter(
        'interactiveDBUPrice === config.contractInteractiveDBUPrice &&
          'automatedDBUPrice === config.contractAutomatedDBUPrice &&
          'sqlComputeDBUPrice === config.contractSQLComputeDBUPrice &&
          'jobsLightDBUPrice === config.contractJobsLightDBUPrice
      )
      .isEmpty

  }

  /**
   * Check for changes between last instance details contract DBU Prices and configured prices from Overwatch
   * config. If changes are detected, rebuild the slow-changing dim appropriately.
   * @param cloudDetailTarget PipelineTable for instanceDetails
   */
  private def updateDBUContractPricing(cloudDetailTarget: PipelineTable): Unit = {
    val lastRunDBUPriceW = Window.partitionBy('API_Name).orderBy('Pipeline_SnapTS.desc)
    val lastRunPricing = cloudDetailTarget.asDF()
      .withColumn("rnk", rank().over(lastRunDBUPriceW))

    if (dbuContractPriceChange(lastRunPricing)) {
      val msg = "DBU Pricing differences detected in config. Updating prices."
      logger.log(Level.INFO, msg)
      if (config.debugFlag) println(msg)

      // All active records with new DBU pricing
      val newDBUPriceRecords = lastRunPricing
        .filter('rnk === 1 && 'activeUntil.isNull)
        .withColumn("interactiveDBUPrice", lit(config.contractInteractiveDBUPrice))
        .withColumn("automatedDBUPrice", lit(config.contractAutomatedDBUPrice))
        .withColumn("sqlComputeDBUPrice", lit(config.contractSQLComputeDBUPrice))
        .withColumn("jobsLightDBUPrice", lit(config.contractJobsLightDBUPrice))
        .withColumn("activeFrom", lit(pipelineSnapTime.asDTString).cast("date"))
        .withColumn("activeUntil", lit(null).cast("date"))
        .withColumn("Pipeline_SnapTS", lit(pipelineSnapTime.asColumnTS))
        .withColumn("Overwatch_RunID", lit(config.runID))

      // All originally active records with expiration date of yesterday
      val expiredRecords = lastRunPricing
        .filter('rnk === 1)
        .withColumn("activeUntil",
          when('activeUntil.isNull, lit(pipelineSnapTime.asDTString).cast("date"))
            .otherwise('activeUntil)
        )

      // New pricing records ++ expired pricing records ++ historical records
      val newCloudDetailsDF = newDBUPriceRecords
        .unionByName(expiredRecords)
        .unionByName(lastRunPricing.filter('rnk > 1))
        .drop("rnk")
        .repartition(4)

      // overwrite the target and preserve original Overwatch metadata
      val overwriteTarget = cloudDetailTarget.copy(
        withOverwatchRunID = false,
        withCreateDate = false,
        mode = "overwrite"
      )
      database.write(newCloudDetailsDF, overwriteTarget, lit(null))
    }

  }

  /**
   * Ensure all static datasets exist in the newly initialized Database. This function must be called after
   * the database has been initialized.
   *
   * @return
   */
  protected def loadStaticDatasets(): this.type = {
    if (
      !BronzeTargets.cloudMachineDetail.exists ||
        BronzeTargets.cloudMachineDetail.asDF.isEmpty
    ) { // if not exists OR if empty for current orgID (.asDF applies global filters)
      val logMsg = if (!BronzeTargets.cloudMachineDetail.exists) {
        "instanceDetails table not found. Building"
      } else if (BronzeTargets.cloudMachineDetail.asDF.isEmpty) {
        "instanceDetails table found but is empty for this workspace. Appending compute costs for this workspace"
      } else ""
      logger.log(Level.INFO, logMsg)
      if (getConfig.debugFlag) println(logMsg)
      val instanceDetailsDF = config.cloudProvider match {
        case "aws" =>
          InitializerFunctions.loadLocalCSVResource(spark, "/AWS_Instance_Details.csv")
        case "azure" =>
          InitializerFunctions.loadLocalCSVResource(spark, "/Azure_Instance_Details.csv")
        case _ =>
          throw new IllegalArgumentException("Overwatch only supports cloud providers, AWS and Azure.")
      }

      val finalInstanceDetailsDF = instanceDetailsDF
        .withColumn("organization_id", lit(config.organizationId))
        .withColumn("interactiveDBUPrice", lit(config.contractInteractiveDBUPrice))
        .withColumn("automatedDBUPrice", lit(config.contractAutomatedDBUPrice))
        .withColumn("sqlComputeDBUPrice", lit(config.contractSQLComputeDBUPrice))
        .withColumn("jobsLightDBUPrice", lit(config.contractJobsLightDBUPrice))
        .withColumn("activeFrom", lit(primordialTime.asDTString).cast("date"))
        .withColumn("activeUntil", lit(null).cast("date"))
        .coalesce(1)

      database.write(finalInstanceDetailsDF, BronzeTargets.cloudMachineDetail, pipelineSnapTime.asColumnTS)
      if (config.databaseName != config.consumerDatabaseName) BronzeTargets.cloudMachineDetailViewTarget.publish("*")
    } else {
      updateDBUContractPricing(BronzeTargets.cloudMachineDetail)
    }
    this
  }

  /**
   * initialize the pipeline run
   * Identify the timestamps to use by module and set them
   *
   * @return
   */
  protected def initPipelineRun(): this.type = {
    logger.log(Level.INFO, "INIT: Pipeline Run")
    val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(config.databaseName)
    val dbProperties = dbMeta.properties
    val overwatchSchemaVersion = dbProperties.getOrElse("SCHEMA", "BAD_SCHEMA")
    if (overwatchSchemaVersion != config.overwatchSchemaVersion && !readOnly) { // If schemas don't match and the pipeline is being written to
      throw new BadConfigException(s"The overwatch DB Schema version is: $overwatchSchemaVersion but this" +
        s" version of Overwatch requires ${config.overwatchSchemaVersion}. Upgrade Overwatch Schema to proceed " +
        s"or drop existing database and allow Overwatch to recreate.")
    }

    if (spark.catalog.databaseExists(config.databaseName) &&
      spark.catalog.tableExists(config.databaseName, "pipeline_report")) {
      val w = Window.partitionBy('organization_id, 'moduleID).orderBy('Pipeline_SnapTS.desc)
      spark.table(s"${config.databaseName}.pipeline_report")
        .filter('status === "SUCCESS" || 'status.startsWith("EMPTY"))
        .filter('organization_id === config.organizationId)
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter('rnk === 1 && 'rn === 1)
        .drop("inputConfig", "parsedConfig")
        .as[SimplifiedModuleStatusReport]
        .collect()
        .foreach(updateModuleState)
    } else {
      Array[SimplifiedModuleStatusReport]()
    }
    setPipelineSnapTime()
    if (pipelineState.nonEmpty && !_supressRangeReport) showRangeReport()
    this
  }

  def primordialTime: TimeTypes = primordialTime(None)

  /**
   * Absolute oldest date for which to pull data. This is to help limit the stress on a cold start / gap start.
   * If primordial date is not provided in the config use now() - maxDays to derive a primordial date otherwise
   * use provided primordial date.
   *
   * If trying to pull more than 60 days of data before https://databricks.atlassian.net/browse/SC-38627 is complete
   * The primary concern is that the historical data from the cluster events API generally expires on/before 60 days
   * and the event logs are not stored in an optimal way at all. SC-38627 should help with this but for now, max
   * age == 60 days.
   *
   * @return
   */
  def primordialTime(hardLimitMaxHistory: Option[Int]): TimeTypes = {

    val pipelineSnapDate = pipelineSnapTime.asLocalDateTime.toLocalDate

    val primordialEpoch = if (config.primordialDateString.nonEmpty) { // primordialDateString is provided
      val configuredPrimordial = deriveLocalDate(config.primordialDateString.get, TimeTypesConstants.dtFormat)
        .atStartOfDay(Pipeline.systemZoneId)

      if (hardLimitMaxHistory.nonEmpty) {
        val minEpochDay = Math.max( // if module has max history allowed get latest date configured primordial or snap - limit
          configuredPrimordial.toLocalDate.toEpochDay,
          pipelineSnapDate.minusDays(hardLimitMaxHistory.get.toLong).toEpochDay
        )
        LocalDate.ofEpochDay(minEpochDay).atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli
      } else configuredPrimordial.toInstant.toEpochMilli // no hard limit, use configured primordial

    } else { // if no limit and no configured primordial, calc by max days

      LocalDateTime.now(systemZoneId).minusDays(derivePrimordialDaysDiff)
        .toLocalDate.atStartOfDay
        .toInstant(systemZoneOffset)
        .toEpochMilli
    }

    Pipeline.createTimeDetail(primordialEpoch)

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
        val primordialLocalDate = deriveLocalDate(config.primordialDateString.get, TimeTypesConstants.dtFormat)

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
      config.maxDays
    }

  }

  /**
   * Azure retrieves audit logs from EH which is to the millisecond whereas aws audit logs are delivered daily.
   * Accepting data with higher precision than delivery causes bad data
   */
  protected val auditLogsIncrementalCols: Seq[String] = if (config.cloudProvider == "azure") Seq("timestamp", "date") else Seq("date")

  private[overwatch] def initiatePostProcessing(): Unit = {

    postProcessor.optimize(this, 12)
    Helpers.fastrm(Array(
      "/tmp/overwatch/bronze/clusterEventsBatches"
    ))

    spark.catalog.clearCache()
  }

  protected def restoreSparkConf(): Unit = {
    restoreSparkConf(config.initialSparkConf)
  }

  protected def restoreSparkConf(value: Map[String, String]): Unit = {
    PipelineFunctions.setSparkOverrides(spark, value, config.debugFlag)
  }

  private def getLastOptimized(moduleID: Int): Long = {
    val state = pipelineState.get(moduleID)
    if (state.nonEmpty) state.get.lastOptimizedTS else 0L
  }

  private def needsOptimize(moduleID: Int, optimizeFreq_H: Int): Boolean = {
    val optFreq_Millis = 1000L * 60L * 60L * optimizeFreq_H.toLong
    val tsLessSevenD = System.currentTimeMillis() - optFreq_Millis
    if (getLastOptimized(moduleID) < tsLessSevenD && !config.isLocalTesting) true
    else false
  }

  private[overwatch] def append(target: PipelineTable)(df: DataFrame, module: Module): ModuleStatusReport = {
    target.applySparkOverrides()
    val startTime = System.currentTimeMillis()

    //      if (!target.exists && !module.isFirstRun) throw new PipelineStateException("MODULE STATE EXCEPTION: " +
    //        s"Module ${module.moduleName} has a defined state but the target to which it writes is missing.", Some(target))

    val localSafeTotalCores = if (config.isLocalTesting) spark.conf.getOption("spark.sql.shuffle.partitions").getOrElse("200").toInt
    else getTotalCores
    val finalDF = PipelineFunctions.optimizeWritePartitions(df, target, spark, config, module.moduleName, localSafeTotalCores)

    val startLogMsg = s"Beginning append to ${target.tableFullName}"
    logger.log(Level.INFO, startLogMsg)

    // Append the output -- don't apply spark overrides, applied at top of function
    if (!readOnly) database.write(finalDF, target, pipelineSnapTime.asColumnTS, applySparkOverrides = false)
    else {
      val readOnlyMsg = "PIPELINE IS READ ONLY: Writes cannot be performed on read only pipelines."
      println(readOnlyMsg)
      logger.log(Level.WARN, readOnlyMsg)
    }

    // Source files for spark event logs are extremely inefficient. Get count from bronze table instead
    // of attempting to re-read the very inefficient json.gz files.
    val dfCount = if (target.name == "spark_events_bronze") {
      target.asIncrementalDF(module, 2, "fileCreateDate", "fileCreateEpochMS").count()
    } else finalDF.count()

    val msg = s"SUCCESS! ${module.moduleName}: $dfCount records appended."
    println(msg)
    logger.log(Level.INFO, msg)

    var lastOptimizedTS: Long = getLastOptimized(module.moduleId)
    if (needsOptimize(module.moduleId, target.optimizeFrequency_H)) {
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
  }

}

object Pipeline {

  val systemZoneId: ZoneId = ZoneId.systemDefault()
  val systemZoneOffset: ZoneOffset = systemZoneId.getRules.getOffset(LocalDateTime.now(systemZoneId))

  def deriveLocalDate(dtString: String, dtFormat: SimpleDateFormat): LocalDate = {
    dtFormat.parse(dtString).toInstant.atZone((systemZoneId)).toLocalDate
  }

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
      Date.from(instant), // asJavadate
      instant.atZone(systemZoneId), // asSystemZonedDateTime
      localDT, // asLocalDateTime
      localDT.toLocalDate.atStartOfDay(systemZoneId).toInstant.toEpochMilli // asMidnightEpochMilli
    )
  }

  def apply(workspace: Workspace, database: Database, config: Config, suppressStaticDatasets: Boolean = false): Pipeline = {

    new Pipeline(workspace, database, config)

  }
}

