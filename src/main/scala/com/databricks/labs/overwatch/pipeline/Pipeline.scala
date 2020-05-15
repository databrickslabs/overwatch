package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, ModuleStatusReport, SchemaTools, SparkSessionWrapper}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Pipeline(_workspace: Workspace, _database: Database,
               _config: Config) extends PipelineTargets(_config) with SparkSessionWrapper {

  // TODO -- Validate Targets (unique table names, ModuleIDs and names, etc)
  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _clusterIDs: Array[String] = _
  private var _jobIDs: Array[Long] = _
  private var _eventLogGlob: DataFrame = _
  private var _newDataRetrieved: Boolean = true
  private var _pipelineStatus: String = "SUCCESS"
  protected final val workspace: Workspace = _workspace
  protected final val database: Database = _database
  protected final val config: Config = _config
  lazy protected final val postProcessor = new PostProcessor()

  // Todo Add Description
  case class Module(moduleID: Int, moduleName: String)

  // TODO -- Add Rules engine
  //  additional field under transforms rules: Option[Seq[Rule => Boolean]]
  case class EtlDefinition(
                            sourceDF: DataFrame,
                            transforms: Option[Seq[DataFrame => DataFrame]],
                            write: (DataFrame, Module) => ModuleStatusReport,
                            module: Module
                          ) {

    def process(): ModuleStatusReport = {
      if (transforms.nonEmpty) {
        val transformedDF = transforms.get.foldLeft(sourceDF) {
          case (df, transform) =>
            df.transform(transform)
        }
        write(transformedDF, module)
      } else {
        write(sourceDF, module)
      }
    }
  }

  import spark.implicits._

  protected def setClusterIDs(value: Array[String]): this.type = {
    _clusterIDs = value
    this
  }

  protected def setPipelineStatus(value: String): this.type = {
    _pipelineStatus = value
    this
  }

  protected def resetDefaults(): this.type = {
    _newDataRetrieved = true
    _pipelineStatus = "SUCCESS"
    this
  }

  protected def setJobIDs(value: Array[Long]): this.type = {
    _jobIDs = value
    this
  }

  protected def setEventLogGlob(value: DataFrame): this.type = {
    _eventLogGlob = value
    this
  }

  protected def setNewDataRetrievedFlag(value: Boolean): this.type = {
    _newDataRetrieved = value
    this
  }

  protected def clusterIDs: Array[String] = _clusterIDs

  protected def jobIDs: Array[Long] = _jobIDs

  protected def newDataRetrieved: Boolean = _newDataRetrieved

  protected def sparkEventsLogGlob: DataFrame = _eventLogGlob

  private def pipelineStatus: String = _pipelineStatus

  private def getLastOptimized(moduleID: Int): Long = {
    val lastRunOptimizeTS = config.lastRunDetail.filter(_.moduleID == moduleID)
    if (!config.isFirstRun && lastRunOptimizeTS.nonEmpty) lastRunOptimizeTS.head.lastOptimizedTS
    else 0L
  }

  // TODO - make this timeframe configurable by module
  private def needsOptimize(moduleID: Int): Boolean = {
    val WEEK = 1000L * 60L * 60L * 24L * 7L // week of milliseconds
    val tsLessSevenD = System.currentTimeMillis() - WEEK.toLong
    if ((getLastOptimized(moduleID) < tsLessSevenD ||
      config.isFirstRun) && !config.isLocalTesting) true
    else false
  }

  // TODO -- Enable parallelized write
  // TODO -- Add assertion that max(count) groupBy keys == 1
  // TODO -- Add support for every-incrasing IDs that are not TimeStamps
  private[overwatch] def append(target: PipelineTable,
                                newDataOnly: Boolean = false,
                                cdc: Boolean = false)(df: DataFrame, module: Module): ModuleStatusReport = {

    var finalDF = df
    var fromTS: Long = 0L
    var untilTS: Long = 0L
    var lastOptimizedTS: Long = getLastOptimized(module.moduleID)
    val fromTime = config.fromTime(module.moduleID)


    finalDF = if (target.zOrderBy.nonEmpty) {
      SchemaTools.moveColumnsToFront(finalDF, target.zOrderBy)
    } else finalDF

    if (newDataOnly) {

      fromTS = fromTime.asUnixTimeMilli
      untilTS = config.pipelineSnapTime.asUnixTimeMilli

      val timeFilter = finalDF.schema.fields.filter(_.name == target.incrementalFromColumn).head.dataType match {
        case dt: TimestampType =>
          col(target.incrementalFromColumn).cast(LongType).between(fromTS, untilTS)
        case dt: DateType =>
          // Date filters -- The unixTS must be at the epoch Second level but the storage must be at the
          // epoch MilliSecond level
          untilTS = config.pipelineSnapTime.asMidnightEpochMilli
          fromTS = fromTime.asMidnightEpochMilli
          to_timestamp(col(target.incrementalFromColumn)).cast(LongType).between(fromTS / 1000, untilTS / 1000)
        case LongType => col(target.incrementalFromColumn).between(fromTS, untilTS)
        case e: _ => throw new IllegalArgumentException(s"IncreasingID Type: ${e.typeName} is Not supported")
      }

      finalDF = finalDF
        .filter(timeFilter)
    }

    val startLogMsg = if (newDataOnly) {
      s"Beginning append to ${target.tableFullName}. " +
        s"\n From Time: ${config.createTimeDetail(fromTS).asTSString} \n"+
        s"Until Time: ${config.createTimeDetail(untilTS).asTSString}"
    } else s"Beginning append to ${target.tableFullName}"
    logger.log(Level.INFO, startLogMsg)

    val startTime = System.currentTimeMillis()
    val dfCount = try {
      _database.write(finalDF, target)
      val debugCount = finalDF.count()
      logger.log(Level.INFO, s"${module.moduleName} SUCCESS! ${debugCount} records appended.")
      debugCount
    } catch {
      // TODO -- Figure out how to bubble up the exceptions
      case e: Throwable => {
        val errorMsg = s"FAILED: ${target.tableFullName} Could not append!"
        logger.log(Level.ERROR, errorMsg, e)
        setPipelineStatus(errorMsg)
        println(errorMsg)
        0
      }
    } finally df.unpersist(blocking = true)

    if (needsOptimize(module.moduleID)) {
      postProcessor.add(target)
      lastOptimizedTS = config.pipelineSnapTime.asUnixTimeMilli
    }

    val endTime = System.currentTimeMillis()

    // TODO - satus is not reflecting exception from upstream
    ModuleStatusReport(
      moduleID = module.moduleID,
      moduleName = module.moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = fromTS,
      untilTS = untilTS,
      dataFrequency = target.dataFrequency.toString,
      status = pipelineStatus,
      recordsAppended = dfCount,
      lastOptimizedTS = lastOptimizedTS,
      vacuumRetentionHours = 24 * 7,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )

  }

}

