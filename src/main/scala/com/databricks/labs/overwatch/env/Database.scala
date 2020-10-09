package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Dataset, Row}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, struct}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class Database(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _

  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  def getDatabaseName: String = _databaseName

  def rollback(target: PipelineTable): Unit = {
    val rollbackSql =
      s"""
         |delete from ${target.tableFullName}
         |where Overwatch_RunID = ${config.runID}
         |""".stripMargin
    spark.sql(rollbackSql)
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



    try {
      logger.log(Level.INFO, s"Beginning write to ${target.tableFullName}")
      if (target.checkpointPath.nonEmpty) {

        val msg = s"Checkpoint Path Set: ${target.checkpointPath.get} - proceeding with streaming write"
        logger.log(Level.INFO, msg)
        if (config.debugFlag) println(msg)

        val beginMsg = s"Stream to ${target.tableFullName} beginning."
        if (config.debugFlag) println(beginMsg)
        logger.log(Level.INFO, beginMsg)
        val streamWriter = target.writer(finalDF).asInstanceOf[DataStreamWriter[Row]].table(target.tableFullName)
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
    } catch {
      case e: Throwable => {
        val failMsg = s"Failed to write to ${_databaseName}. Attempting to rollback"
        logger.log(Level.ERROR, failMsg, e)
        println(s"ERROR --> ${target.tableFullName}", e)
        rollback(target)
        false
      }
    }
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
