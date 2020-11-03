package com.databricks.labs.overwatch.env

import java.util

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Dataset, Row}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, struct}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener._

class Database(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _

  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  def getDatabaseName: String = _databaseName

  def rollbackTarget(target: PipelineTable): Unit = {
    val rollbackSql =
      s"""
         |delete from ${target.tableFullName}
         |where Overwatch_RunID = '${config.runID}'
         |""".stripMargin
    val rollBackMsg = s"Executing Rollback: STMT: ${rollbackSql}"
    if (config.debugFlag) println(rollBackMsg)
    logger.log(Level.WARN, rollBackMsg)
    spark.sql(rollbackSql)

    // Specific Rollback logic
    if (target.name == "spark_events_bronze") {
      val eventsFileTrackerTable = s"${config.databaseName}.spark_events_processedfiles"
      val updateStmt =
        s"""
           |update ${eventsFileTrackerTable}
           |set failed = true
           |where Overwatch_RunID = '${config.runID}'
           |""".stripMargin
      val fileFailMsg = s"Failing Files for ${config.runID}.\nSTMT: $updateStmt"
      logger.log(Level.WARN, fileFailMsg)
      if (config.debugFlag) {
        println(updateStmt)
        println(fileFailMsg)
      }
      spark.sql(updateStmt)
    }
  }

  private def initializeStreamTarget(df: DataFrame, target: PipelineTable): Unit = {
    val dfWSchema = spark.createDataFrame(new util.ArrayList[Row](), df.schema)
    val staticDFWriter = target.copy(checkpointPath = None).writer(dfWSchema)
    staticDFWriter
      .asInstanceOf[DataFrameWriter[Row]]
      .saveAsTable(target.tableFullName)
  }

  private def getQueryListener(query: StreamingQuery): StreamingQueryListener = {
    val streamManager = new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
        if (config.debugFlag) {
          println(query.status.prettyJson)
        }
        if (queryProgress.progress.numInputRows == 0) {
          query.stop()
        }
      }
    }
    streamManager
  }

  def write(df: DataFrame, target: PipelineTable): Boolean = {

    var finalDF: DataFrame = df
    finalDF = if (target.withCreateDate) finalDF.withColumn("Pipeline_SnapTS", config.pipelineSnapTime.asColumnTS) else finalDF
    finalDF = if (target.withOverwatchRunID) finalDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalDF

    // TODO -- Enhance schema scrubber. If target has an array of nested structs which is populated but the new
    //  incoming data (finalDF) has nulls inside one of the nested structs the result is a schema failure.
//    try {
      logger.log(Level.INFO, s"Beginning write to ${target.tableFullName}")
      if (target.checkpointPath.nonEmpty) {

        val msg = s"Checkpoint Path Set: ${target.checkpointPath.get} - proceeding with streaming write"
        logger.log(Level.INFO, msg)
        if (config.debugFlag) println(msg)

        val beginMsg = s"Stream to ${target.tableFullName} beginning."
        if (config.debugFlag) println(beginMsg)
        logger.log(Level.INFO, beginMsg)
//        finalDF = SchemaTools.scrubSchema(finalDF)
        if (config.isFirstRun || !spark.catalog.tableExists(config.databaseName, target.name)) {
          initializeStreamTarget(finalDF, target)
        }
        val targetTablePath = s"${config.databaseLocation}/${target.name}"
        val streamWriter = target.writer(finalDF)
            .asInstanceOf[DataStreamWriter[Row]]
            .option("path", targetTablePath)
            .start()
        val streamManager = getQueryListener(streamWriter)
        spark.streams.addListener(streamManager)
        val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter.name}\nID: ${streamWriter.id}"
        if (config.debugFlag) println(listenerAddedMsg)
        logger.log(Level.INFO, listenerAddedMsg)

        streamWriter.awaitTermination()
        spark.streams.removeListener(streamManager)

      } else {
        finalDF = SchemaTools.scrubSchema(finalDF)
        target.writer(finalDF).asInstanceOf[DataFrameWriter[Row]].saveAsTable(target.tableFullName)
      }
      logger.log(Level.INFO, s"Completed write to ${target.tableFullName}")
      true
//    } catch {
//      case e: Throwable => {
//        finalDF = df
//        finalDF = if (target.withCreateDate) finalDF.withColumn("Pipeline_SnapTS", config.pipelineSnapTime.asColumnTS) else finalDF
//        finalDF = if (target.withOverwatchRunID) finalDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalDF
//        val newDataSchemaBeforeScrub = s"DEBUG: SCHEMA: Source Schema for: ${target.tableFullName} BEFORE SCRUBBER was: \n${finalDF.printSchema()}"
//        // Only do this for when not writing to a streaming target
//        if (target.checkpointPath.isEmpty) finalDF = SchemaTools.scrubSchema(finalDF)
//        val newDataSchemaMsg = s"DEBUG: SCHEMA: Source Schema for to be written to target: ${target.tableFullName} is \n${finalDF.printSchema()}"
//        val targetSchemaMsg = if (!config.isFirstRun) {
//          s"DEBUG: SCHEMA: Target Schema for target ${target.tableFullName}: \n${target.asDF.printSchema()}"
//        } else { "NO Target Schema -- First run or target doesn't yet exist" }
//        logger.log(Level.DEBUG, newDataSchemaBeforeScrub)
//        logger.log(Level.DEBUG, newDataSchemaMsg)
//        logger.log(Level.DEBUG, targetSchemaMsg)
//        if (config.debugFlag) {
//          println(newDataSchemaBeforeScrub)
//          println(newDataSchemaMsg)
//          println(targetSchemaMsg)
//        }
//        val failMsg = s"Failed to write to ${_databaseName}. Attempting to rollback"
//        logger.log(Level.ERROR, failMsg, e)
//        println(s"ERROR --> ${target.tableFullName}", e)
//        rollback(target)
//        false
//      }
//    }
  }

  def readTable(tableName: String): DataFrame = {
    spark.table(s"${_databaseName}.${tableName}")
  }

  def readPath(path: String, format: String = "delta"): DataFrame = {
    spark.read.format(format).load(path)
  }

  def tableExists(tableName: String): Boolean = {
    spark.catalog.tableExists(_databaseName, tableName)
  }

  //TODO -- Add dbexists func

}

object Database {

  def apply(config: Config): Database = {
    new Database(config).setDatabaseName(config.databaseName)
  }

}
