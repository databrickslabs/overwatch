package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, ModuleStatusReport, SchemaTools, SparkSessionWrapper}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Pipeline(_workspace: Workspace, _database: Database) extends SparkSessionWrapper {

  // TODO - cleanse column names (no special chars)
  // TODO - enable merge schema on write -- includes checks for number of new columns
  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _clusterIDs: Array[String] = _
  private var _jobIDs: Array[Long] = _
  private var _eventLogGlob: DataFrame = _
  protected final val workspace: Workspace = _workspace
  protected final val database: Database = _database
  lazy protected final val postProcessor = new PostProcessor()
//  private var _database: Database = _

  case class Module(moduleID: Int,moduleName: String)

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

//  def setWorkspace(value: Workspace): this.type = {
//    _workspace = value
//    this
//  }
//
//  def setDatabase(value: Database): this.type = {
//    _database = value
//    this
//  }

  protected def setClusterIDs(value: Array[String]): this.type = {
    _clusterIDs = value
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

  protected def clusterIDs: Array[String] = _clusterIDs

  protected def jobIDs: Array[Long] = _jobIDs

  protected def sparkEventsLogGlob: DataFrame = _eventLogGlob

  // TODO -- Enable parallelized write
  private[overwatch] def append(target: PipelineTable,
                                newDataOnly: Boolean = false,
                                cdc: Boolean = false)(df: DataFrame, module: Module): ModuleStatusReport = {

    var finalDF = df
    var fromTS: Long = 0L
    var untilTS: Long = 0L
    val fromTime = Config.fromTime(module.moduleID)
    val startLogMsg = if (newDataOnly) {
      s"Beginning append to ${target.tableFullName}. " +
        s"\n From Time: ${fromTime.asTSString} \n Until Time: ${Config.pipelineSnapTime.asTSString}"
    } else s"Beginning append to ${target.tableFullName}"

    finalDF = if (target.zOrderBy.nonEmpty) {
      SchemaTools.moveColumnsToFront(finalDF, target.zOrderBy)
    } else finalDF

    if (newDataOnly) {

//      val x = finalDF.schema.fields.collect{case x if x.name == target.tsCol => x.dataType}

      val typedTSCol = finalDF.schema.fields.filter(_.name == target.tsCol).head.dataType match {
        case dt: TimestampType =>
          fromTS = fromTime.asUnixTime
          untilTS = Config.pipelineSnapTime.asUnixTime
          col(target.tsCol).cast(LongType)
        case dt: DateType =>
          untilTS = Config.pipelineSnapTime.asMidnightEpochMilli
          fromTS = fromTime.asMidnightEpochMilli
          col(target.tsCol)
        case _ => col(target.tsCol)
      }

      finalDF = finalDF
        .filter(typedTSCol.between(fromTS, untilTS))
    }

    logger.log(Level.INFO, startLogMsg)
    val startTime = System.currentTimeMillis()
    val status: String = try {
      _database.write(finalDF, target)
      if (Config.debugFlag) {
        val debugCount = finalDF.count()
        logger.log(Level.INFO, s"${module.moduleName} SUCCESS: appended ${debugCount}.")
        "SUCCESS"
      } else {
        logger.log(Level.INFO, s"${module.moduleName} SUCCESS!")
        "SUCCESS"
      }
    } catch {
      // TODO -- Figure out how to bubble up the exceptions
      case e: Throwable => {
        val errorMsg = s"FAILED: ${target.tableFullName} Could not append!"
        logger.log(Level.ERROR, errorMsg, e)
        println(errorMsg)
        errorMsg
      }
    } finally df.unpersist(blocking = true)

    // TODO -- this should be calculated by module/target
    if (Config.postProcessingFlag) postProcessor.add(target)

    val endTime = System.currentTimeMillis()

    ModuleStatusReport(
      moduleID = module.moduleID,
      moduleName = module.moduleName,
      runStartTS = startTime,
      runEndTS = endTime,
      fromTS = fromTS,
      untilTS = untilTS,
      status = status,
      lastOptimizedTS = untilTS,
      inputConfig = Config.inputConfig,
      parsedConfig = Config.parsedConfig
    )



  }

  //  def appendBronze(): Boolean = {
  //
  //    try {
  //      val reports = Bronze().run()
  //    }
  //
  //  }

}

