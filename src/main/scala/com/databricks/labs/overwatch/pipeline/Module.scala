package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, ModuleStatusReport, NoNewDataException, SimplifiedModuleStatusReport, TimeTypes, TimeTypesConstants}
import org.apache.spark.sql.DataFrame
import TransformFunctions._
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.pipeline.Pipeline.{createTimeDetail, systemZoneId, systemZoneOffset}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{from_unixtime, lit}

import java.time.{Duration, Instant, LocalDateTime}
import java.util.Date

class Module(
              _moduleID: Int,
              _moduleName: String,
              _moduleDependencies: Option[Array[Int]],
              _workspace: Workspace,
              _database: Database,
              _config: Config
            ) extends Pipeline(_workspace, _database, _config) {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  protected val moduleId: Int = _moduleID
  protected val moduleName: String = _moduleName
  protected val moduleDependencies: Option[Array[Int]] = _moduleDependencies


  protected def moduleState: Option[SimplifiedModuleStatusReport] = getModuleState(moduleId)

  protected def pipelineState: Map[Int, SimplifiedModuleStatusReport] = getPipelineState.toMap

  /**
   * Absolute oldest date for which to pull data. This is to help limit the stress on a cold start / gap start.
   * If trying to pull more than 60 days of data before https://databricks.atlassian.net/browse/SC-38627 is complete
   * The primary concern is that the historical data from the cluster events API generally expires on/before 60 days
   * and the event logs are not stored in an optimal way at all. SC-38627 should help with this but for now, max
   * age == 60 days.
   *
   * @return
   */
  private def primordialEpoch: Long = {
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
   * Defines the latest timestamp to be used for a give module as a TimeType
   *
   * @param moduleID moduleID for which to get the until Time
   * @return
   */
  def untilTime: TimeTypes = {
    val startSecondPlusMaxDays = fromTime.asLocalDateTime.plusDays(config.maxDays)
      .atZone(systemZoneId).toInstant.toEpochMilli
    val defaultUntilSecond = pipelineSnapTime.asUnixTimeMilli
    if (startSecondPlusMaxDays < defaultUntilSecond) {
      Pipeline.createTimeDetail(startSecondPlusMaxDays)
    } else {
      Pipeline.createTimeDetail(defaultUntilSecond)
    }
  }

  /**
   * This is also used for simulation of start/end times during testing. This also should not be a public function
   * when completed. Note that this controlled by module. Not every module is executed on every run or a module could
   * fail and this allows for missed data since the last successful run to be acquired without having to pull all the
   * data for all modules each time.
   *
   * @param moduleID moduleID for modules in scope for the run
   * @throws java.util.NoSuchElementException
   * @return
   */
  def fromTime: TimeTypes = {
    if (moduleState.isEmpty)
      createTimeDetail(primordialEpoch)
    else createTimeDetail(moduleState.get.untilTS)
  }

  private def overrideMaxUntilTime: Unit = {

  }

  protected def finalize(report: ModuleStatusReport): Unit = {
    updateModuleState(report.simple)
    val pipelineReportTarget = PipelineTable(
      name = "pipeline_report",
      keys = Array("organization_id", "Overwatch_RunID"),
      config = config,
      incrementalColumns = Array("Pipeline_SnapTS")
    )
    database.write(Seq(report).toDF, pipelineReportTarget)
  }

  protected def fail(target: PipelineTable, msg: String): Unit = {
    val rollbackMsg = s"ROLLBACK: Attempting Roll back $moduleName."
    println(rollbackMsg)
    logger.log(Level.WARN, rollbackMsg)

    val rollbackStatus = try {
      database.rollbackTarget(target)
      "ROLLBACK SUCCESSFUL"
    } catch {
      case e: Throwable => {
        val rollbackFailedMsg = s"ROLLBACK FAILED: $moduleName -->\nMessage: ${e.getMessage}\nCause:" +
          s"${e.getCause}"
        println(rollbackFailedMsg, e)
        logger.log(Level.ERROR, rollbackFailedMsg, e)
        "ROLLBACK FAILED"
      }
    }

    val failedStatusReport = ModuleStatusReport(
      organization_id = config.organizationId,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = untilTime.asUnixTimeMilli,
      dataFrequency = target.dataFrequency.toString,
      status = s"FAILED --> $rollbackStatus: ERROR:\n$msg",
      recordsAppended = 0L,
      lastOptimizedTS = moduleState.orNull.lastOptimizedTS,
      vacuumRetentionHours = moduleState.orNull.vacuumRetentionHours,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )
    finalize(failedStatusReport)
    throw new FailedModuleException(msg)
  }

  protected def noNewDataHandler(msg: String): Unit = {
    val startTime = System.currentTimeMillis()
    val emptyStatusReport = ModuleStatusReport(
      organization_id = config.organizationId,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = startTime,
      runEndTS = startTime,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = untilTime.asUnixTimeMilli,
      dataFrequency = moduleState.orNull.dataFrequency,
      status = "EMPTY",
      recordsAppended = 0L,
      lastOptimizedTS = moduleState.orNull.lastOptimizedTS,
      vacuumRetentionHours = moduleState.orNull.vacuumRetentionHours,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )
    finalize(emptyStatusReport)
    throw new NoNewDataException(msg)
  }

  protected def validateSourceDF(df: DataFrame): DataFrame = {
    if (df.isEmpty) {
      val msg = s"ALERT: No New Data Retrieved for Module ${moduleId}-${moduleName}! Skipping"
      println(msg)
      throw new NoNewDataException(msg)
    } else {
      println(s"$moduleName: Validating Input Schemas")
      df.verifyMinimumSchema(Schema.get(moduleId), enforceNonNullCols = true, isDebug = _debugFlag)
    }
  }

  protected def sourceDFParts(df: DataFrame): Int = if (!df.isStreaming) df.rdd.partitions.length else 200

  def validatePipelineState(): Boolean = {
    var requirementsPassed: Boolean = true

    if (moduleDependencies.nonEmpty) { // if dependencies present
      moduleDependencies.get.foreach(dep => {
        val depStateOp = pipelineState.get(dep)
        if (depStateOp.isEmpty) { // No existing state for pre-requisite
          noNewDataHandler()
        } else { // Dependency State Exists
          val depState = depStateOp.get
          if (depState.status != "SUCCESS" || depState.status != "EMPTY") { // required pre-requisite failed
            noNewDataHandler()
          }
          if (depState.recordsAppended == 0L) { // required pre-requisite has no new data
            noNewDataHandler()
          }
        }
      })
      true // no exception thrown -- continue
    } else { // no dependencies -- continue
      true
    }
    //    requirementsPassed
  }

}

object Module {

  def apply(moduleId: Int,
            moduleName: String,
            pipeline: Pipeline,
            moduleDependencies: Option[Array[Int]] = None
           ): Module = {

    new Module(
      moduleId,
      moduleName,
      moduleDependencies,
      pipeline.workspace,
      pipeline.database,
      pipeline.config
    )
  }

}
