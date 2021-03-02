package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, ModuleStatusReport, NoNewDataException, SimplifiedModuleStatusReport, TimeTypes, TimeTypesConstants, UnhandledException}
import org.apache.spark.sql.DataFrame
import TransformFunctions._
import com.databricks.labs.overwatch.env.{Database, Workspace}
import org.apache.log4j.{Level, Logger}

class Module(
              _moduleID: Int,
              _moduleName: String,
              _moduleDependencies: Array[Int],
              _workspace: Workspace,
              _database: Database,
              _config: Config
            ) extends Pipeline(_workspace, _database, _config) {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  val moduleId: Int = _moduleID
  val moduleName: String = _moduleName
  private var _isFirstRun: Boolean = false
  protected val moduleDependencies: Array[Int] = _moduleDependencies

  protected def moduleState: SimplifiedModuleStatusReport = {
    if (getModuleState(moduleId).isEmpty) {
      _isFirstRun = true
      val initialModuleState = initModuleState
      updateModuleState(initialModuleState)
      initialModuleState
    }
    //      throw new UnhandledException(s"ERROR: Could not build or retrieve state for Module: $moduleId-$moduleName")
    else getModuleState(moduleId).get
  }

  protected def pipelineState: Map[Int, SimplifiedModuleStatusReport] = getPipelineState.toMap

  /**
   * This is also used for simulation of start/end times during testing. This also should not be a public function
   * when completed. Note that this controlled by module. Not every module is executed on every run or a module could
   * fail and this allows for missed data since the last successful run to be acquired without having to pull all the
   * data for all modules each time.
   *
   * @param moduleID moduleID for modules in scope for the run
   * @return
   */
  def fromTime: TimeTypes = if (getModuleState(moduleId).isEmpty){
    Pipeline.createTimeDetail(primordialEpoch)
  } else Pipeline.createTimeDetail(moduleState.fromTS)

  /**
   * Defines the latest timestamp to be used for a give module as a TimeType
   *
   * @param moduleID moduleID for which to get the until Time
   * @return
   */
  def untilTime: TimeTypes = {
    val startSecondPlusMaxDays = fromTime.asLocalDateTime.plusDays(config.maxDays)
      .atZone(Pipeline.systemZoneId).toInstant.toEpochMilli

    val defaultUntilSecond = pipelineSnapTime.asUnixTimeMilli

    if (startSecondPlusMaxDays < defaultUntilSecond) {
      Pipeline.createTimeDetail(startSecondPlusMaxDays)
    } else {
      Pipeline.createTimeDetail(defaultUntilSecond)
    }
  }

  private def mostLaggingDependency: SimplifiedModuleStatusReport = {
    moduleDependencies.map(depState => pipelineState(depState))
      .sortBy(_.untilTS).reverse.head
  }

  /**
   * Override the initial untilTS with a new value equal to the specified value
   *
   * @param newUntilTime
   */
  private def overrideUntilTS(newUntilTime: Long): Unit = {
    if (moduleDependencies.nonEmpty) {
      // get earliest date of all required modules
      val msg = s"WARNING: ENDING TIMESTAMP CHANGED:\nInitial UntilTS of ${untilTime.asUnixTimeMilli} " +
        s"exceeds that of an upstream requisite module: ${mostLaggingDependency.moduleID}-${mostLaggingDependency.moduleName} " +
        s"with untilTS of: ${mostLaggingDependency.untilTS}. Setting current module until TS to == min requisite module."
      logger.log(Level.WARN, msg)
      println(msg)
      overrideUntilTS(mostLaggingDependency.untilTS)
    }
    val newState = moduleState.copy(untilTS = newUntilTime)
    updateModuleState(newState)
  }

  private def initModuleState: SimplifiedModuleStatusReport = {
    SimplifiedModuleStatusReport(
      organization_id = config.organizationId,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = untilTime.asUnixTimeMilli,
      dataFrequency = "",
      status = s"Initialized",
      recordsAppended = 0L,
      lastOptimizedTS = 0L,
      vacuumRetentionHours = 24 * 7
    )
  }

  private def finalizeModule(report: ModuleStatusReport): Unit = {
    updateModuleState(report.simple)
    val pipelineReportTarget = PipelineTable(
      name = "pipeline_report",
      keys = Array("organization_id", "Overwatch_RunID"),
      config = config,
      incrementalColumns = Array("Pipeline_SnapTS")
    )
    database.write(Seq(report).toDF, pipelineReportTarget, pipelineSnapTime.asColumnTS)
  }

  private def fail(msg: String, rollbackStatus: String = ""): Unit = {
    val failedStatusReport = ModuleStatusReport(
      organization_id = config.organizationId,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = untilTime.asUnixTimeMilli,
      dataFrequency = moduleState.dataFrequency,
      status = s"FAILED --> $rollbackStatus: ERROR:\n$msg",
      recordsAppended = 0L,
      lastOptimizedTS = moduleState.lastOptimizedTS,
      vacuumRetentionHours = moduleState.vacuumRetentionHours,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )
    finalizeModule(failedStatusReport)
  }

  private def failWithRollback(target: PipelineTable, msg: String): Unit = {
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
    fail(msg, rollbackStatus)
  }

  private def noNewDataHandler(msg: String, errorLevel: Level): Unit = {
    logger.log(errorLevel, msg)
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
      dataFrequency = moduleState.dataFrequency,
      status = s"EMPTY: $msg",
      recordsAppended = 0L,
      lastOptimizedTS = moduleState.lastOptimizedTS,
      vacuumRetentionHours = moduleState.vacuumRetentionHours,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )
    finalizeModule(emptyStatusReport)
  }

  private def validateSourceDF(df: DataFrame): DataFrame = {
    if (df.isEmpty) {
      val msg = s"ALERT: No New Data Retrieved for Module ${moduleId}-${moduleName}! Skipping"
      println(msg)
      throw new NoNewDataException(msg, Level.WARN)
    } else {
      println(s"$moduleName: Validating Input Schemas")
      df.verifyMinimumSchema(Schema.get(moduleId), enforceNonNullCols = true, isDebug = config.debugFlag)
    }
  }

  private def validatePipelineState(): Unit = {

    if (moduleDependencies.nonEmpty) { // if dependencies present
      // If earliest untilTS of dependencies < current untilTS edit current untilTS to match
      if (mostLaggingDependency.untilTS < untilTime.asUnixTimeMilli) overrideUntilTS(mostLaggingDependency.untilTS)
      moduleDependencies.foreach(dependentModuleId => {
        val depStateOp = pipelineState.get(dependentModuleId)
        if (depStateOp.isEmpty) { // No existing state for pre-requisite
          val msg = s"A pipeline state cannot be deterined for Module ID $dependentModuleId. Setting module to EMPTY and " +
            s"attempting to proceed. Note: Any other modules depending on Module ID $dependentModuleId will also be set to empty."
          throw new NoNewDataException(msg, Level.WARN)
        } else { // Dependency State Exists
          val depState = depStateOp.get
          if (depState.status != "SUCCESS" || !depState.status.startsWith("EMPTY")) { // required pre-requisite failed
            val msg = s"Requires $dependentModuleId is in a failed state. This Module will not progress until its " +
              s"upstream load is successful"
            throw new NoNewDataException(msg, Level.WARN)
          }
        }
      })
    }// requirementsPassed
  }

  @throws(classOf[IllegalArgumentException])
  def execute(_etlDefinition: ETLDefinition): Unit = {
    println(s"Beginning: $moduleName")

    try {
      validatePipelineState()
      // validation may alter state, especially time states, reInstantiate etlDefinition to ensure current state
      val etlDefinition = _etlDefinition.copy()
      val verifiedSourceDF = validateSourceDF(etlDefinition.sourceDF)
      val sourceDFParts = PipelineFunctions.getSourceDFParts(verifiedSourceDF)

      val newState = etlDefinition.executeETL(this, verifiedSourceDF)
      updateModuleState(newState.simple)
      finalizeModule(newState)

    } catch {
      case e: FailedModuleException =>
        val errMessage = s"FAILED: $moduleId-$moduleName Module"
        logger.log(Level.ERROR, errMessage, e)
        failWithRollback(e.target, s"$errMessage\n${e.getMessage}")
      case e: NoNewDataException =>
        // EMPTY prefix gets prepended in the errorHandler
        val errMessage = s"$moduleId-$moduleName Module: SKIPPING\nDownstream modules that depend on this " +
          s"module will not progress until new data is received by this module.\n " +
          s"Module Dependencies: ${moduleDependencies.mkString(", ")}\n" + e.getMessage
        logger.log(Level.ERROR, errMessage, e)
        noNewDataHandler(errMessage, e.level)
      case e: Throwable =>
        val msg = s"$moduleName FAILED -->\nMessage: ${e.getMessage}\nCause:${e.getCause}"
        logger.log(Level.ERROR, msg, e)
        fail(msg)
    }

  }


}

object Module {

  def apply(moduleId: Int,
            moduleName: String,
            pipeline: Pipeline,
            moduleDependencies: Array[Int] = Array()
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
