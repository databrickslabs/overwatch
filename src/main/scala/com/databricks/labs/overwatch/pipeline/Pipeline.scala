package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, Helpers, ModuleStatusReport, NoNewDataException, SchemaTools, SparkSessionWrapper}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{PrintWriter, StringWriter}

class Pipeline(_workspace: Workspace, _database: Database,
               _config: Config) extends PipelineTargets(_config) with SparkSessionWrapper {

  // TODO -- Validate Targets (unique table names, ModuleIDs and names, etc)
  private val logger: Logger = Logger.getLogger(this.getClass)
  protected final val workspace: Workspace = _workspace
  protected final val database: Database = _database
  protected final val config: Config = _config
  lazy protected final val postProcessor = new PostProcessor()
  private val sw = new StringWriter

  envInit()

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
      println(s"Beginning: ${module.moduleName}")
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

  protected def finalizeRun(reports: Array[ModuleStatusReport]): Unit = {
    println(s"Writing Pipeline Report for modules: ${reports.map(_.moduleID).sorted.mkString(",")}")
    val pipelineReportTarget = PipelineTable("pipeline_report", Array("Overwatch_RunID"), "Pipeline_SnapTS", config)
    database.write(reports.toSeq.toDF, pipelineReportTarget)
    initiatePostProcessing()
  }

  protected def initiatePostProcessing(): Unit = {
//    postProcessor.analyze()
    postProcessor.optimize()
  }

  private def restoreSparkConf(value: Map[String, String]) : Unit= {
    value foreach { case (k, v) =>
      try{
        spark.conf.set(k, v)
      } catch {
        case e: org.apache.spark.sql.AnalysisException => logger.log(Level.WARN, s"Not Settable: $k", e)
        case e: Throwable => println(s"ERROR: $k, --> $e")
      }
    }
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

  //  todo -- engineer test data to TEST the case
  //  The "between" function is inclusive on both sides
  //  Subtracting a millisecond to ensure no duplicates
  private def addOneTick(ts: Column, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 + 1) / 1000).cast("timestamp")
      case _: DateType =>
        date_add(ts, 1)
      case _: DoubleType =>
        ts + lit(0.001)
      case _: LongType =>
        ts + 1
      case _: IntegerType =>
        ts + 1
      case _ => throw new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }

  // TODO -- Build out automated partition filter -- handle other types besides date

  private def buildIncrementalPartitionFilter(dfSchema: StructType,
                                              target: PipelineTable,
                                              fromTime: Column): Column = {
    val partColNames = target.partitionBy
    val partFields = partColNames.flatMap(colName => {
      dfSchema.fields.filter(_.name == colName)
    })
    partFields.map(partField => {
      partField.dataType match {
        case _: DateType =>
          col(partField.name) > fromTime.cast(DateType)
      }
    }).head // TODO -- handle more than one partition filter

  }

  private def failModule(module: Module, outcome: String, msg: String): ModuleStatusReport = {
    ModuleStatusReport(
      moduleID = module.moduleID,
      moduleName = module.moduleName,
      runStartTS = 0L,
      runEndTS = 0L,
      fromTS = 0L,
      untilTS = 0L,
      dataFrequency = "",
      status = s"${outcome}: $msg",
      recordsAppended = 0L,
      lastOptimizedTS = 0L,
      vacuumRetentionHours = 0,
      inputConfig = config.inputConfig,
      parsedConfig = config.parsedConfig
    )

  }

  // TODO -- Enable parallelized write
  // TODO -- Add assertion that max(count) groupBy keys == 1
  // TODO -- Add support for every-incrasing IDs that are not TimeStamps
  private[overwatch] def append(target: PipelineTable,
                                newDataOnly: Boolean = false,
                                cdc: Boolean = false)(df: DataFrame, module: Module): ModuleStatusReport = {
    try {
      if (df.schema.fieldNames.contains("FAILURE")) throw new NoNewDataException(s"FAILED to append: ${target.tableFullName}")
      var finalDF = df
      var fromTS: Long = 0L
      var untilTS: Long = 0L
      var lastOptimizedTS: Long = getLastOptimized(module.moduleID)
      val fromTime = config.fromTime(module.moduleID)
      var updateProcessedFlag: Boolean = false

      finalDF = if (target.zOrderBy.nonEmpty) {
        SchemaTools.moveColumnsToFront(finalDF, target.zOrderBy)
      } else finalDF

      //    DEBUG
      //    println("DEBUG: DF BEFORE TS Filter")
      //    finalDF.show(20, false)

      if (newDataOnly) {

        fromTS = fromTime.asUnixTimeMilli
        untilTS = config.pipelineSnapTime.asUnixTimeMilli

        val timeFilter = finalDF.schema.fields.filter(_.name == target.incrementalColumn).head.dataType match {
          case dt: TimestampType =>
            col(target.incrementalColumn)
              .between(addOneTick(fromTime.asColumnTS), config.pipelineSnapTime.asColumnTS)
          case dt: DateType =>
            col(target.incrementalColumn)
              .between(addOneTick(fromTime.asColumnTS), config.pipelineSnapTime.asColumnTS)
          case LongType => col(target.incrementalColumn)
            .between(fromTS + 1, untilTS)
          case DoubleType => col(target.incrementalColumn).between(fromTS + .001, untilTS)
          case StringType => {
            updateProcessedFlag = true
            col(target.incrementalColumn) =!= "Processed"
          }
          case e: DataType => // TODO -- add this back and handle other incremental types
            //          col(target.incrementalFromColumn)
            throw new IllegalArgumentException(s"IncreasingID Type: ${e.typeName} is Not supported")
        }

        finalDF = finalDF
          .filter(timeFilter)

        if (target.partitionBy.nonEmpty) {
          if (target.partitionBy.contains(target.incrementalColumn)) {
            val partitionFilter = buildIncrementalPartitionFilter(finalDF.schema, target, fromTime.asColumnTS)
            finalDF = finalDF.filter(partitionFilter)
          }
        }

      }

//          DEBUG
//      println("DEBUG: DF AFTER TS Filter")
//      println(s"OUTPUT for ${target.tableFullName}")
//      finalDF.show(10, false)

      val startLogMsg = if (newDataOnly) {
        s"Beginning append to ${target.tableFullName}. " +
          s"\n From Time: ${config.createTimeDetail(fromTS).asTSString} \n" +
          s"Until Time: ${config.createTimeDetail(untilTS).asTSString}"
      } else s"Beginning append to ${target.tableFullName}"
      logger.log(Level.INFO, startLogMsg)

      val shufflePartsPrior = spark.conf.get("spark.sql.shuffle.partitions")
      val finalDFPartCount = finalDF.rdd.partitions.length
      val estimatedFinalDFSizeMB = finalDFPartCount * spark.conf.get("spark.sql.files.maxPartitionBytes").toInt / 1024 / 1024
      val targetShuffleSize = math.max(200, finalDFPartCount)
      logger.log(Level.INFO, s"${module.moduleName}: Final DF estimated at ${estimatedFinalDFSizeMB} MBs." +
        s"\nShufflePartitions: ${targetShuffleSize}")
      spark.conf.set("spark.sql.shuffle.partitions", targetShuffleSize)
      val startTime = System.currentTimeMillis()
      _database.write(finalDF, target)
      val debugCount = if (!config.isFirstRun || config.debugFlag) {
        val dfCount = finalDF.count()
        val msg = s"${module.moduleName} SUCCESS! ${dfCount} records appended."
        println(msg)
        logger.log(Level.INFO, msg)
        dfCount
      } else {
        logger.log(Level.INFO, "Counts not calculated on first run.")
        0
      }
      spark.conf.set("spark.sql.shuffle.partitions", shufflePartsPrior)

      if (needsOptimize(module.moduleID)) {
        postProcessor.add(target)
        lastOptimizedTS = config.pipelineSnapTime.asUnixTimeMilli
      }

      restoreSparkConf(config.initialSparkConf())

      val endTime = System.currentTimeMillis()

      ModuleStatusReport(
        moduleID = module.moduleID,
        moduleName = module.moduleName,
        runStartTS = startTime,
        runEndTS = endTime,
        fromTS = fromTS,
        untilTS = untilTS,
        dataFrequency = target.dataFrequency.toString,
        status = "SUCCESS",
        recordsAppended = debugCount,
        lastOptimizedTS = lastOptimizedTS,
        vacuumRetentionHours = 24 * 7,
        inputConfig = config.inputConfig,
        parsedConfig = config.parsedConfig
      )
    } catch {
      case e: NoNewDataException =>
        val msg = s"Failed: No New Data Retrieved for Module ${module.moduleID}! Skipping"
        println(msg)
        logger.log(Level.WARN, msg, e)
        failModule(module, "SKIPPED", msg)
      case e: Throwable =>
        val msg = s"${module.moduleName} FAILED, Unhandled Error ${e.printStackTrace(new PrintWriter(sw)).toString}"
        logger.log(Level.ERROR, msg, e)
        println(msg)
        failModule(module, "FAILED", msg)
    }

  }

}

