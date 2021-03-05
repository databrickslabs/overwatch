package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.DataFrame
import TransformFunctions._
import org.apache.log4j.{Level, Logger}

class Module(
              _moduleID: Int,
              _moduleName: String,
              pipeline: Pipeline,
              _moduleDependencies: Array[Int]
            ) {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import pipeline.spark.implicits._
  private val config = pipeline.config


  val moduleId: Int = _moduleID
  val moduleName: String = _moduleName
  private var _isFirstRun: Boolean = false
  private val moduleDependencies: Array[Int] = _moduleDependencies

  private def moduleState: SimplifiedModuleStatusReport = {
    if (pipeline.getModuleState(moduleId).isEmpty) {
      _isFirstRun = true
      val initialModuleState = initModuleState
      pipeline.updateModuleState(initialModuleState)
      initialModuleState
    }
    else pipeline.getModuleState(moduleId).get
  }

  private def pipelineState: Map[Int, SimplifiedModuleStatusReport] = pipeline.getPipelineState.toMap

  /**
   * This is also used for simulation of start/end times during testing. This also should not be a public function
   * when completed. Note that this controlled by module. Not every module is executed on every run or a module could
   * fail and this allows for missed data since the last successful run to be acquired without having to pull all the
   * data for all modules each time.
   *
   * @param moduleID moduleID for modules in scope for the run
   * @return
   */
  def fromTime: TimeTypes = if (pipeline.getModuleState(moduleId).isEmpty){
    Pipeline.createTimeDetail(pipeline.primordialEpoch)
  } else Pipeline.createTimeDetail(moduleState.untilTS)

  /**
   * Defines the latest timestamp to be used for a give module as a TimeType
   *
   * @param moduleID moduleID for which to get the until Time
   * @return
   */
  def untilTime: TimeTypes = {
    val startSecondPlusMaxDays = fromTime.asLocalDateTime.plusDays(pipeline.config.maxDays)
      .atZone(Pipeline.systemZoneId).toInstant.toEpochMilli

    val defaultUntilSecond = pipeline.pipelineSnapTime.asUnixTimeMilli

    // Reduce UntilTS IF fromTime + MAX Days < pipeline Snap Time
    val maxIndependentUntilTime = if (startSecondPlusMaxDays < defaultUntilSecond) {
      Pipeline.createTimeDetail(startSecondPlusMaxDays)
    } else {
      Pipeline.createTimeDetail(defaultUntilSecond)
    }

    if (moduleDependencies.nonEmpty && mostLaggingDependency.untilTS < maxIndependentUntilTime.asUnixTimeMilli) {
      val msg = s"WARNING: ENDING TIMESTAMP CHANGED:\nInitial UntilTS of ${maxIndependentUntilTime.asUnixTimeMilli} " +
        s"exceeds that of an upstream requisite module: ${mostLaggingDependency.moduleID}-${mostLaggingDependency.moduleName} " +
        s"with untilTS of: ${mostLaggingDependency.untilTS}. Setting current module untilTS == min requisite module: " +
        s"${mostLaggingDependency.untilTS}."
      logger.log(Level.WARN, msg)
      if (pipeline.config.debugFlag) println(msg)
      Pipeline.createTimeDetail(mostLaggingDependency.untilTS)
    } else maxIndependentUntilTime
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
        s"with untilTS of: ${mostLaggingDependency.untilTS}. Setting current module untilTS == min requisite module: " +
        s"${mostLaggingDependency.untilTS}."
      logger.log(Level.WARN, msg)
      println(msg)
    }
    val newState = moduleState.copy(untilTS = newUntilTime)
    pipeline.updateModuleState(newState)
  }

  private def initModuleState: SimplifiedModuleStatusReport = {
    val initState = SimplifiedModuleStatusReport(
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
    pipeline.updateModuleState(initState)
    initState
  }

  private def finalizeModule(report: ModuleStatusReport): Unit = {
    pipeline.updateModuleState(report.simple)
    val pipelineReportTarget = PipelineTable(
      name = "pipeline_report",
      keys = Array("organization_id", "Overwatch_RunID"),
      config = config,
      incrementalColumns = Array("Pipeline_SnapTS")
    )
    pipeline.database.write(Seq(report).toDF, pipelineReportTarget, pipeline.pipelineSnapTime.asColumnTS)
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
      pipeline.database.rollbackTarget(target)
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

  /**
   *
   * @param msg
   * @param errorLevel
   * @param allowModuleProgression
   */
  private def noNewDataHandler(msg: String, errorLevel: Level, allowModuleProgression: Boolean): Unit = {
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
      untilTS = if (allowModuleProgression) untilTime.asUnixTimeMilli else fromTime.asUnixTimeMilli,
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
      throw new NoNewDataException(msg, Level.WARN, allowModuleProgression = true)
    } else {
      println(s"$moduleName: Validating Input Schemas")
      df.verifyMinimumSchema(Schema.get(moduleId), enforceNonNullCols = true, isDebug = config.debugFlag)
    }
  }

  private def validatePipelineState(): Unit = {

    if (moduleDependencies.nonEmpty) { // if dependencies present
      // If earliest untilTS of dependencies < current untilTS edit current untilTS to match
//      if (mostLaggingDependency.untilTS < untilTime.asUnixTimeMilli) overrideUntilTS(mostLaggingDependency.untilTS)
      moduleDependencies.foreach(dependentModuleId => {
        val depStateOp = pipelineState.get(dependentModuleId)
        if (depStateOp.isEmpty) { // No existing state for pre-requisite
          val msg = s"A pipeline state cannot be deterined for Module ID $dependentModuleId. Setting module to EMPTY and " +
            s"attempting to proceed. Note: Any other modules depending on Module ID $dependentModuleId will also be set to empty."
          throw new NoNewDataException(msg, Level.WARN)
        } else { // Dependency State Exists
          val depState = depStateOp.get
          if (depState.status != "SUCCESS" && !depState.status.startsWith("EMPTY")) { // required pre-requisite failed
            val msg = s"Requires $dependentModuleId is in a failed state: ${depState.status}. This Module will not progress until its " +
              s"upstream requirement[s] is loaded successfully"
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
      val debugMsg = s"MODULE: $moduleId-$moduleName\nTIME RANGE: " +
        s"    From -> To == ${fromTime.asTSString} -> ${untilTime.asTSString}"
      println(debugMsg)
      logger.log(Level.INFO, debugMsg)
      val newState = etlDefinition.executeETL(this, verifiedSourceDF)
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
        noNewDataHandler(errMessage, e.level, e.allowModuleProgression)
      case e: Throwable =>
        val msg = s"$moduleName FAILED -->\nMessage: ${e.getMessage}"
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
      pipeline,
      moduleDependencies
    )
  }

}
