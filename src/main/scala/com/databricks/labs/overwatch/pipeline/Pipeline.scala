package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
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
  lazy protected final val postProcessor = new PostProcessor(config)
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

  def overridePipelineState(newPipelineState: scala.collection.mutable.Map[Int, SimplifiedModuleStatusReport]): Unit = {
    clearPipelineState()
    newPipelineState.values.foreach(state => updateModuleState(state))
  }

  def getVerbosePipelineState: Array[ModuleStatusReport] = {
    pipelineStateTarget.asDF.as[ModuleStatusReport].collect()
  }

  def updateModuleState(moduleState: SimplifiedModuleStatusReport): Unit = {
    pipelineState.put(moduleState.moduleID, moduleState)
  }

  def dropModuleState(moduleId: Int): Unit = {
    pipelineState.remove(moduleId)
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
   * TRUE if changes detected
   * FALSE if no changes
   * changes mean that the active dbu price for a sku is different in the config than it is in the dbuCosting table
   * @param lastRunDBUCosts instanceDetails dataframe with rnk column
   * @return
   */
  private def dbuContractPriceChange(lastRunDBUCosts: DataFrame): Boolean = {

    !lastRunDBUCosts
      .filter('rnk === 1 && 'activeUntil.isNull && 'isActive) // most recent, active records
      .filter(
        when('sku === "interactive", 'contract_price =!= config.contractInteractiveDBUPrice)
        .when('sku === "automated", 'contract_price =!= config.contractAutomatedDBUPrice)
        .when('sku === "sqlCompute", 'contract_price =!= config.contractSQLComputeDBUPrice)
        .when('sku === "jobsLight", 'contract_price =!= config.contractJobsLightDBUPrice)
      )
      .isEmpty

  }

  /**
   * Check for changes in configured contract DBU Prices.
   * If changes are detected, rebuild the slow-changing dim appropriately.
   * @param dbuCostDetail PipelineTable for dbuContractCosts
   */
  private def updateDBUCosts(dbuCostDetail: PipelineTable): Unit = {
    val keyCols = dbuCostDetail.keys.map(col)
    val lastRunDBUPriceW = Window.partitionBy(keyCols: _*).orderBy('Pipeline_SnapTS.desc)
    val dbuCostDetailDF = dbuCostDetail.asDF()

    val lastRunDBUCosts = dbuCostDetailDF
      .withColumn("rnk", rank().over(lastRunDBUPriceW))

    val activeRecord = 'rnk === 1 && 'activeUntil.isNull && 'isActive
    if (dbuContractPriceChange(lastRunDBUCosts)) {
      val msg = "DBU Pricing differences detected in config. Updating prices."
      logger.log(Level.INFO, msg)
      if (config.debugFlag) println(msg)

      // configured costs by sku as a column
      val activeCost = when('sku === "interactive", config.contractInteractiveDBUPrice)
        .when('sku === "automated", config.contractAutomatedDBUPrice)
        .when('sku === "sqlCompute", config.contractSQLComputeDBUPrice)
        .when('sku === "jobsLight", config.contractJobsLightDBUPrice)
        .otherwise(lit(0.0))

      // records that were already expired
      val originalExpiredRecords = lastRunDBUCosts.filter(!activeRecord)

      // active costs with no changes
      val unchangedActiveCosts = lastRunDBUCosts.filter(activeRecord && 'contract_price === activeCost)

      // newly active records due to costing change
      val newDBUPriceRecords = lastRunDBUCosts
        .filter(activeRecord && 'contract_price =!= activeCost)
        .withColumn("contract_price", activeCost)
        .withColumn("activeFrom", lit(pipelineSnapTime.asDTString).cast("date"))
        .withColumn("activeUntil", lit(null).cast("date"))
        .withColumn("isActive", lit(true))
        .withColumn("Pipeline_SnapTS", lit(pipelineSnapTime.asColumnTS))
        .withColumn("Overwatch_RunID", lit(config.runID))

      // active costs to be expired due to replacement
      val expiringRecords = lastRunDBUCosts
        .filter(activeRecord&& 'contract_price =!= activeCost)
        .withColumn("activeUntil", lit(pipelineSnapTime.asDTString).cast("date"))
        .withColumn("isActive", lit(false))

      // historical records ++ new pricing records ++ newly expired pricing records ++ unchanged active records
      val updatedDBUCostDetail = newDBUPriceRecords
        .unionByName(expiringRecords)
        .unionByName(unchangedActiveCosts)
        .unionByName(originalExpiredRecords)
        .drop("rnk")
        .coalesce(1)

      // overwrite the target and preserve original Overwatch metadata
      val overwriteTarget = dbuCostDetail.copy(
        withOverwatchRunID = false,
        withCreateDate = false,
        _mode = WriteMode.overwrite
      )
      database.writeWithRetry(updatedDBUCostDetail, overwriteTarget, lit(null).cast("timestamp"))
    }

  }

  private def publishDBUCostDetails(): Unit = {
    val costDetailDF = Seq(
      DBUCostDetail(config.organizationId, "interactive", config.contractInteractiveDBUPrice, primordialTime.asLocalDateTime.toLocalDate, None, true),
      DBUCostDetail(config.organizationId, "automated", config.contractAutomatedDBUPrice, primordialTime.asLocalDateTime.toLocalDate, None, true),
      DBUCostDetail(config.organizationId, "sqlCompute", config.contractSQLComputeDBUPrice, primordialTime.asLocalDateTime.toLocalDate, None, true),
      DBUCostDetail(config.organizationId, "jobsLight", config.contractJobsLightDBUPrice, primordialTime.asLocalDateTime.toLocalDate, None, true),
    ).toDF().coalesce(1)

    database.writeWithRetry(costDetailDF, BronzeTargets.dbuCostDetail, pipelineSnapTime.asColumnTS)//
    if (config.databaseName != config.consumerDatabaseName) BronzeTargets.dbuCostDetailViewTarget.publish("*")
  }

  private def publishInstanceDetails(): Unit = {
    val logMsg = "instanceDetails does not exist and/or does not contain data for this workspace. BUILDING/APPENDING"
    logger.log(Level.INFO, logMsg)

    if (getConfig.debugFlag) println(logMsg)
    val instanceDetailsDF = config.cloudProvider match {
      case "aws" =>
        Initializer.loadLocalCSVResource(spark, "/AWS_Instance_Details.csv")
      case "azure" =>
        Initializer.loadLocalCSVResource(spark, "/Azure_Instance_Details.csv")
      case "gcp" =>
        Initializer.loadLocalCSVResource(spark, "/Gcp_Instance_Details.csv")
      case _ =>
        throw new IllegalArgumentException("Overwatch only supports cloud providers, AWS and Azure.")
    }

    val finalInstanceDetailsDF = instanceDetailsDF
      .withColumn("Memory_GB", 'Memory_GB.cast("double")) // ensure static load is in double format
      .withColumn("organization_id", lit(config.organizationId))
      .withColumn("activeFrom", lit(primordialTime.asDTString).cast("date"))
      .withColumn("activeUntil", lit(null).cast("date"))
      .withColumn("isActive", lit(true))
      .coalesce(1)

    database.writeWithRetry(finalInstanceDetailsDF, BronzeTargets.cloudMachineDetail, pipelineSnapTime.asColumnTS)
    if (config.databaseName != config.consumerDatabaseName) BronzeTargets.cloudMachineDetailViewTarget.publish("*")
  }

  /**
   * Ensure all static datasets exist in the newly initialized Database. This function must be called after
   * the database has been initialized.
   *
   * @return
   */
  protected def loadStaticDatasets(): this.type = {
    if (!BronzeTargets.cloudMachineDetail.exists(dataValidation = true)) publishInstanceDetails()
    if (!BronzeTargets.dbuCostDetail.exists(dataValidation = true)) publishDBUCostDetails()
    if (!BronzeTargets.warehouseDbuDetail.exists(dataValidation = true)) publishWarehouseDbuDetails()
    else updateDBUCosts(BronzeTargets.dbuCostDetail)
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
    setCurrentCatalog(spark, config.etlCatalogName)
    val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(config.databaseName)
    val dbProperties = dbMeta.properties
    val overwatchSchemaVersion = dbProperties.getOrElse("SCHEMA", "BAD_SCHEMA")
    if (overwatchSchemaVersion != config.overwatchSchemaVersion && !readOnly) { // If schemas don't match and the pipeline is being written to
      throw new BadConfigException(s"The overwatch DB Schema version is: $overwatchSchemaVersion but this" +
        s" version of Overwatch requires ${config.overwatchSchemaVersion}. Upgrade Overwatch Schema to proceed " +
        s"or drop existing database and allow Overwatch to recreate.")
    }

    if (pipelineStateTarget.exists(dataValidation = true)) { // data must exist for this workspace or this is fresh start
      val w = Window.partitionBy('organization_id, 'moduleID).orderBy('Pipeline_SnapTS.desc)
      pipelineStateTarget.asDF
        .filter('status === "SUCCESS" || 'status.startsWith("EMPTY"))
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

  private[overwatch] def initiatePostProcessing(): Unit = {

    // cleanse the temp dir
    // if failure doesn't allow pipeline to get here, temp dir will be cleansed on workspace init
    if (!config.externalizeOptimize) postProcessor.optimize(this, Pipeline.OPTIMIZESCALINGCOEF)
    Helpers.fastrm(Array(config.tempWorkingDir))
    dbutils.fs.rm(config.tempWorkingDir)

    postProcessor.refreshPipReportView(pipelineStateViewTarget)
    //TODO clearcache will clear global cache multithread performance issue
   // spark.catalog.clearCache()
    clearThreadFromSessionsMap()
  }

  /**
   * restore the spark config to the way it was when config / workspace were first instantiated
   */
  private[overwatch] def restoreSparkConf(): Unit = {
    restoreSparkConf(config.initialSparkConf)
  }

  /**
   * restore spark configs that are passed into this function
   * @param value map of configs to be overridden
   */
  protected def restoreSparkConf(value: Map[String, String]): Unit = {
    PipelineFunctions.setSparkOverrides(spark, value, config.debugFlag)
  }

  private def getLastOptimized(moduleID: Int): Long = {
    val state = pipelineState.get(moduleID)
    if (state.nonEmpty) state.get.lastOptimizedTS else 0L
  }

  private def needsOptimize(lastOptimizedTS: Long, optimizeFreq_H: Int): Boolean = {
    val optFreq_Millis = 1000L * 60L * 60L * optimizeFreq_H.toLong
    val tsLessSevenD = System.currentTimeMillis() - optFreq_Millis
    if (lastOptimizedTS < tsLessSevenD && !config.isLocalTesting) true
    else false
  }

  private def getMaxMergeScanDates(fromTime: TimeTypes, untilTime: TimeTypes, maxMergeScanDays: Int): Array[String] = {
    val startDate = fromTime.asLocalDateTime.minusDays(maxMergeScanDays)
    Helpers.getDatesGlob(startDate.toLocalDate, untilTime.asLocalDateTime.plusDays(1).toLocalDate)
  }

  private[overwatch] def append(target: PipelineTable)(df: DataFrame, module: Module): ModuleStatusReport = {
//    val startTime = System.currentTimeMillis()

    val finalDF = PipelineFunctions.optimizeDFForWrite(df, target)

    val maxMergeScanDates = if (target.writeMode == WriteMode.merge) {
      getMaxMergeScanDates(module.fromTime, module.untilTime, target.maxMergeScanDates)
    } else Array[String]()

    val startLogMsg = s"Beginning append to ${target.tableFullName}"
    logger.log(Level.INFO, startLogMsg)

    // Append the output -- don't apply spark overrides, applied at top of function
    //
    if (!readOnly) database.writeWithRetry(finalDF, target, pipelineSnapTime.asColumnTS, maxMergeScanDates,Some(module.daysToProcess))
    else {
      val readOnlyMsg = "PIPELINE IS READ ONLY: Writes cannot be performed on read only pipelines."
      println(readOnlyMsg)
      logger.log(Level.WARN, readOnlyMsg)
    }

    val writeOpsMetrics = PipelineFunctions.getTargetWriteMetrics(spark, target, pipelineSnapTime, config.runID)

    val lastOptimizedTS: Long = PipelineFunctions.getLastOptimized(spark, target)
    if (!config.externalizeOptimize && needsOptimize(lastOptimizedTS, target.optimizeFrequency_H)) {
      postProcessor.markOptimize(target)
    }

    restoreSparkConf()

    val endTime = System.currentTimeMillis()

    val rowsWritten = writeOpsMetrics.getOrElse("numOutputRows", "0")
    val execMins: Double = (endTime - module.startTime) / 1000.0 / 60.0
    val simplifiedExecMins: Double = execMins - (execMins % 0.01)
    val successMessage = s"SUCCESS! ${module.moduleName}\nOUTPUT ROWS: $rowsWritten\nRUNTIME MINS: " +
      s"$simplifiedExecMins --> Workspace ID: ${config.organizationId}"
    println(s"COMPLETED: ${module.moduleStartMessage} $successMessage")
    logger.log(Level.INFO, module.moduleStartMessage ++ successMessage)

    // Generate Success Report
    ModuleStatusReport(
      organization_id = config.organizationId,
      workspace_name = config.workspaceName,
      moduleID = module.moduleId,
      moduleName = module.moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = module.startTime,
      runEndTS = endTime,
      fromTS = module.fromTime.asUnixTimeMilli,
      untilTS = module.untilTime.asUnixTimeMilli,
      status = "SUCCESS",
      writeOpsMetrics = writeOpsMetrics,
      lastOptimizedTS = lastOptimizedTS,
      vacuumRetentionHours = 24 * 7,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig,
      externalizeOptimize = config.externalizeOptimize
    )
  }

  private def publishWarehouseDbuDetails(): Unit = {
    val logMsg = "warehouseDbuDetails does not exist and/or does not contain data for this workspace. BUILDING/APPENDING"
    logger.log(Level.INFO, logMsg)

    if (getConfig.debugFlag) println(logMsg)
    val warehouseDbuDetailsDF = Initializer.loadLocalCSVResource(spark, "/Warehouse_DBU_Details.csv")
      .filter(lower('cloud) === s"${config.cloudProvider}".toLowerCase)

    val finalWarehouseDbuDetailsDF = warehouseDbuDetailsDF
      .withColumn("organization_id", lit(config.organizationId))
      .withColumn("activeFrom", lit(primordialTime.asDTString).cast("date"))
      .withColumn("activeUntil", lit(null).cast("date"))
      .withColumn("activeFromEpochMillis", unix_timestamp('activeFrom) * 1000)
      .withColumn("activeUntilEpochMillis",
        coalesce(unix_timestamp('activeUntil) * 1000, unix_timestamp(pipelineSnapTime.asColumnTS) * 1000))
      .withColumn("isActive", lit(true))
      .coalesce(1)

    database.writeWithRetry(finalWarehouseDbuDetailsDF, BronzeTargets.warehouseDbuDetail, pipelineSnapTime.asColumnTS)
    if (config.databaseName != config.consumerDatabaseName) BronzeTargets.warehouseDbuDetailViewTarget.publish("*")
  }
}

object Pipeline {

  val systemZoneId: ZoneId = ZoneId.systemDefault()
  val systemZoneOffset: ZoneOffset = systemZoneId.getRules.getOffset(LocalDateTime.now(systemZoneId))
  private val OPTIMIZESCALINGCOEF = 12

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

  def apply(workspace: Workspace, database: Database, config: Config): Pipeline = {

    new Pipeline(workspace, database, config)

  }
}

