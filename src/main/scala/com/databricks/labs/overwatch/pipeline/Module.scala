package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Shuffle

import java.time.Duration

class Module(
              val moduleId: Int,
              val moduleName: String,
              private[overwatch] val pipeline: Pipeline,
              val moduleDependencies: Array[Int],
              val moduleScaleCoefficient: Double,
              hardLimitMaxHistory: Option[Int],
              private var _shuffleFactor: Double
            ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import pipeline.spark.implicits._

  private val config = pipeline.config

  private var _isFirstRun: Boolean = false

  private[overwatch] val moduleState: SimplifiedModuleStatusReport = {
    if (pipeline.getModuleState(moduleId).isEmpty) {
      initModuleState
    }
    else pipeline.getModuleState(moduleId).get
  }

  private var sparkOverrides: Map[String, String] = Map[String, String]()

  def isFirstRun: Boolean = _isFirstRun

  def daysToProcess: Int = {
    Duration.between(
      fromTime.asLocalDateTime.toLocalDate.atStartOfDay(),
      untilTime.asLocalDateTime.toLocalDate.plusDays(1L).atStartOfDay()
    ).toDays.toInt
  }

  /**
   * use shuffle factor as a starting point and for every 5 days of history add an additional shuffle factor
   * coefficient to scale shuffle partitions with history
   * @return
   */
  def shuffleFactor: Double = {
    val daysBucket = 5
    val derivedShuffleFactor = _shuffleFactor + Math.floor(daysToProcess / daysBucket).toInt
    logger.info(s"SHUFFLE FACTOR: Set to $derivedShuffleFactor")

    derivedShuffleFactor
  }

  private def optimizeShufflePartitions(): Unit = {
    val defaultShuffleParts = spark.conf.get("spark.sql.shuffle.partitions").toInt
    val coreTargetOptimization = getTotalCores * 2

    // At least 2 * cluster core count
    val derivedShuffleParts = Math.max(
      // Max out at 40,000 shuffle parts
      Math.min(
        Math.floor(defaultShuffleParts * shuffleFactor).toInt,
        40000
      ),
      coreTargetOptimization
    )
    logger.info(s"SHUFFLE PARTITIONS SET: $derivedShuffleParts")
    withSparkOverrides(Map("spark.sql.shuffle.partitions" -> derivedShuffleParts.toString))
  }

  def copy(
            _moduleID: Int = moduleId,
            _moduleName: String = moduleName,
            _pipeline: Pipeline = pipeline,
            _moduleDependencies: Array[Int] = moduleDependencies,
            _hardLimitMaxHistory: Option[Int] = hardLimitMaxHistory,
            _shuffleFactor: Double = _shuffleFactor): Module = {
    new Module(_moduleID, _moduleName, _pipeline, _moduleDependencies, moduleScaleCoefficient, _hardLimitMaxHistory, _shuffleFactor)
  }

  def withSparkOverrides(overrides: Map[String, String]): this.type = {
    sparkOverrides = sparkOverrides ++ overrides
    this
  }

  private def pipelineState: Map[Int, SimplifiedModuleStatusReport] = pipeline.getPipelineState.toMap

  /**
   * This is also used for simulation of start/end times during testing.
   * Note that this controlled by module. Not every module is executed on every run or a module could
   * fail and this allows for missed data since the last successful run to be acquired without having to pull all the
   * data for all modules each time.
   * fromTime is always INCLUSIVE >= when used for calculating incrementals
   *
   * @return
   */
  def fromTime: TimeTypes = if (pipeline.getModuleState(moduleId).isEmpty || isFirstRun) {
    Pipeline.createTimeDetail(pipeline.primordialTime(hardLimitMaxHistory).asUnixTimeMilli)
  } else Pipeline.createTimeDetail(moduleState.untilTS)

  /**
   * Disallow pipeline start time state + max days to exceed snapshot time. Keeps pipelines from running into
   * the future.
   *
   * @return
   */
  private def limitUntilTimeToSnapTime: TimeTypes = {
    val startSecondPlusMaxDays = fromTime.asLocalDateTime.plusDays(pipeline.config.maxDays)
      .atZone(Pipeline.systemZoneId).toInstant.toEpochMilli

    val defaultUntilSecond = pipeline.pipelineSnapTime.asUnixTimeMilli

    val sourceAvailableFrom = if (hardLimitMaxHistory.nonEmpty) {
      // snapTS - hardLimitMaxDaysHistory
      pipeline.primordialTime(hardLimitMaxHistory).asUnixTimeMilli
    } else -1L

    val maxAllowedUntilDate = Math.max( // untilTime cannot be < sourceAvailableFrom
      Math.min( // untilTime cannot be > the least of snapTime, start+maxDays
        startSecondPlusMaxDays, defaultUntilSecond
      ), sourceAvailableFrom
    )
    Pipeline.createTimeDetail(maxAllowedUntilDate)
  }

  private def limitUntilTimeToMostLaggingDependency(originalUntilTime: TimeTypes): TimeTypes = {
    if (moduleDependencies.nonEmpty) {
      val mostLaggingDependencyUntilTS = if (deriveMostLaggingDependency.isEmpty) {
        // Return primordial time if upstream dependency is completely missing. This will act as place holder and keep
        // usages of untilTime from failing until pipeline is ready to execute and is validated. The module will fail
        // gracefully and place error in pipeline_report
        fromTime.asUnixTimeMilli
      } else { // all dependencies have state
        deriveMostLaggingDependency.get.untilTS
      }

      // Check if any dependency states latest data content (untilTS state) is < this module's untilTS
      // This check keeps a child module from getting ahead of it's parent
      if (mostLaggingDependencyUntilTS < originalUntilTime.asUnixTimeMilli) {
        val msg = s"WARNING: ENDING TIMESTAMP CHANGED:\nInitial UntilTS of ${originalUntilTime.asUnixTimeMilli} " +
          s"exceeds that of an upstream requisite module. " +
          s"with untilTS of: ${mostLaggingDependencyUntilTS}. Setting current module untilTS == min requisite module: " +
          s"${mostLaggingDependencyUntilTS}."
        logger.log(Level.WARN, msg)
        if (pipeline.config.debugFlag) println(msg)
        Pipeline.createTimeDetail(mostLaggingDependencyUntilTS)
      } else originalUntilTime

    } else originalUntilTime
  }

  private def deriveMostLaggingDependency: Option[SimplifiedModuleStatusReport] = {

    val dependencyStates = moduleDependencies.map(depModID => pipelineState.get(depModID))
    val missingModuleIds = moduleDependencies.filterNot(depModID => pipelineState.contains(depModID))
    val modulesMissingStates = dependencyStates.filter(_.isEmpty)
    if (modulesMissingStates.nonEmpty) {
      val errMsg = s"Missing upstream requisite Modules (${missingModuleIds.mkString(", ")}) to execute this Module " +
        s"$moduleId - $moduleName.\n\nThis is likely due to lacking " +
        s"scenarios for this scope within this workspace. For example, if running the jobs scope but no jobs have " +
        s"executed since your primordial date (i.e. ${pipeline.config.primordialDateString}), the downstream modules " +
        s"that depend on jobs such as jobRunCostPotentialFact cannot execute as there's no data present."
      if (pipeline.config.debugFlag) println(errMsg)
      logger.log(Level.ERROR, errMsg)
      None
    } else {
      Some(dependencyStates.map(_.get).minBy(_.untilTS))
    }
  }

  /**
   * Defines the latest timestamp to be used for a give module as a TimeType
   * When pipeline is read only, module state can be read independent of upstream dependencies.
   * untilTime is EXCLUSIVE in other words when it is used as a calculation in "asIncrementalDF" it is < untilTime
   * not <= untilTime
   *
   * @return
   */
  def untilTime: TimeTypes = {
    // Reduce UntilTS IF fromTime + MAX Days < pipeline Snap Time
    // Increase UntilTS IF snap - hardLimitMaxDays > pipelineSnapTime
    val maxIndependentUntilTime = limitUntilTimeToSnapTime

    if (pipeline.readOnly) { // don't validate dependency progress when not writing data to pipeline
      maxIndependentUntilTime
    } else limitUntilTimeToMostLaggingDependency(maxIndependentUntilTime)
  }

  private def initModuleState: SimplifiedModuleStatusReport = {
    _isFirstRun = true
    val initState = SimplifiedModuleStatusReport(
      organization_id = config.organizationId,
      workspace_name = config.workspaceName,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = Some(pipeline.primordialTime(hardLimitMaxHistory).asDTString),
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = untilTime.asUnixTimeMilli,
      status = s"Initialized",
      writeOpsMetrics = Map[String, String](),
      lastOptimizedTS = 0L,
      vacuumRetentionHours = 24 * 7,
      externalizeOptimize = config.externalizeOptimize
    )
    pipeline.updateModuleState(initState)
    initState
  }

  private def finalizeModule(report: ModuleStatusReport): Unit = {
    pipeline.updateModuleState(report.simple)
    if (!pipeline.readOnly) {
      pipeline.database.write(Seq(report).toDF, pipeline.pipelineStateTarget, pipeline.pipelineSnapTime.asColumnTS)
    }
  }

  private def fail(msg: String, rollbackStatus: String = ""): ModuleStatusReport = {
    val failedStatusReport = ModuleStatusReport(
      organization_id = config.organizationId,
      workspace_name = config.workspaceName,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = untilTime.asUnixTimeMilli,
      status = s"FAILED --> $rollbackStatus\nERROR:\n$msg",
      writeOpsMetrics = Map[String, String](),
      lastOptimizedTS = moduleState.lastOptimizedTS,
      vacuumRetentionHours = moduleState.vacuumRetentionHours,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig,
      externalizeOptimize = config.externalizeOptimize
    )

    finalizeModule(failedStatusReport)
    failedStatusReport
  }

  private def failWithRollback(target: PipelineTable, msg: String): ModuleStatusReport = {
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
  private def noNewDataHandler(msg: String, errorLevel: Level, allowModuleProgression: Boolean): ModuleStatusReport = {
    logger.log(errorLevel, msg)
    val startTime = System.currentTimeMillis()
    val emptyStatusReport = ModuleStatusReport(
      organization_id = config.organizationId,
      workspace_name = config.workspaceName,
      moduleID = moduleId,
      moduleName = moduleName,
      primordialDateString = config.primordialDateString,
      runStartTS = startTime,
      runEndTS = startTime,
      fromTS = fromTime.asUnixTimeMilli,
      untilTS = if (allowModuleProgression) untilTime.asUnixTimeMilli else fromTime.asUnixTimeMilli,
      status = s"EMPTY: $msg",
      writeOpsMetrics = Map[String, String](),
      lastOptimizedTS = moduleState.lastOptimizedTS,
      vacuumRetentionHours = moduleState.vacuumRetentionHours,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig,
      externalizeOptimize = config.externalizeOptimize
    )
    finalizeModule(emptyStatusReport)
    emptyStatusReport
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

  private[overwatch] def validatePipelineState(): Unit = {

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
    } // requirementsPassed
  }

  @throws(classOf[IllegalArgumentException])
  def execute(_etlDefinition: ETLDefinition): ModuleStatusReport = {
    optimizeShufflePartitions()
    logger.log(Level.INFO, s"Spark Overrides Initialized for target: $moduleName to\n${sparkOverrides.mkString(", ")}")
    PipelineFunctions.setSparkOverrides(spark, sparkOverrides, config.debugFlag)

    val startMsg = s"\nBeginning: $moduleId-$moduleName\nTIME RANGE: ${fromTime.asTSString} -> ${untilTime.asTSString}"
    println(startMsg)

    if (config.debugFlag) println(startMsg)
    logger.log(Level.INFO, startMsg)
    try {
      if (fromTime.asUnixTimeMilli == untilTime.asUnixTimeMilli)
        throw new NoNewDataException("FROM and UNTIL times are identical. Likely due to upstream dependencies " +
          "being at or ahead of current module.", Level.WARN)
      validatePipelineState()
      PipelineFunctions.scaleCluster(pipeline, moduleScaleCoefficient)
      // validation may alter state, especially time states, reInstantiate etlDefinition to ensure current state
      val etlDefinition = _etlDefinition.copy()
      val verifiedSourceDF = validateSourceDF(etlDefinition.sourceDF)
      val newState = etlDefinition.executeETL(this, verifiedSourceDF)
      finalizeModule(newState)
      newState
    } catch {
      case e: ApiCallEmptyResponse =>
        noNewDataHandler(PipelineFunctions.appendStackStrace(e, e.apiCallDetail), Level.ERROR, allowModuleProgression = e.allowModuleProgression)
      case e: ApiCallFailure if e.failPipeline =>
        fail(PipelineFunctions.appendStackStrace(e, e.msg))
      case e: FailedModuleException =>
        val errMessage = s"FAILED: $moduleId-$moduleName Module"
        logger.log(Level.ERROR, errMessage, e)
        failWithRollback(e.target, PipelineFunctions.appendStackStrace(e, errMessage))
      case e: NoNewDataException =>
        // EMPTY prefix gets prepended in the errorHandler
        val customMsg = if (!e.allowModuleProgression) {
          s"$moduleId-$moduleName Module: SKIPPING\nDownstream modules that depend on this " +
            s"module will not progress until new data is received by this module.\n " +
            s"Module Dependencies: ${moduleDependencies.mkString(", ")}"
        } else {
          s"$moduleId-$moduleName Module: No new data found. This does not appear to be an error but simply a " +
            s"lack of data present for the module. Progressing module state.\n " +
            s"Module Dependencies: ${moduleDependencies.mkString(", ")}"
        }
        val errMessage = PipelineFunctions.appendStackStrace(e, customMsg)
        logger.log(Level.ERROR, errMessage, e)
        noNewDataHandler(errMessage, e.level, e.allowModuleProgression)
      case e: Throwable =>
        val msg = PipelineFunctions.appendStackStrace(e, s"$moduleName FAILED -->\n")
        logger.log(Level.ERROR, msg, e)
        fail(msg)
    }

  }


}

object Module {

  /**
   *
   * @param moduleId
   * @param moduleName
   * @param pipeline
   * @param moduleDependencies
   * @param clusterScaleUpPercent
   * @param hardLimitMaxHistory
   * @param shuffleFactor starting point for shuffle factor coefficient
   * @return
   */
  def apply(moduleId: Int,
            moduleName: String,
            pipeline: Pipeline,
            moduleDependencies: Array[Int] = Array(),
            clusterScaleUpPercent: Double = 1.0,
            hardLimitMaxHistory: Option[Int] = None,
            shuffleFactor: Double = 1.0
           ): Module = {

    // reset spark configs to default for each module
    pipeline.restoreSparkConf()

    new Module(
      moduleId,
      moduleName,
      pipeline,
      moduleDependencies,
      clusterScaleUpPercent,
      hardLimitMaxHistory,
      shuffleFactor
    )

  }

}
