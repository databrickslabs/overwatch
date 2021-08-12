package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row}

import java.util

class Database(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _


  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  private def registerTarget(table: PipelineTable): Unit = {
    if (!table.exists) {
      val createStatement = s"create table ${table.tableFullName} " +
        s"USING DELTA location '${table.tableLocation}'"
      val logMessage = s"CREATING TABLE: ${table.tableFullName} at ${table.tableLocation}\n\n$createStatement"
      logger.log(Level.INFO, logMessage)
      if (config.debugFlag) println(logMessage)
      spark.sql(createStatement)
    }
  }

  def getDatabaseName: String = _databaseName

  // TODO -- Move this to post processing and improve
  //  all rollbacks should be completed during post processing from full inventory in par
  def rollbackTarget(target: PipelineTable): Unit = {

    // TODO -- on failure this causes multiple deletes unnecessarily -- improve this
    if (target.tableFullName matches ".*spark_.*_silver") {
      val sparkSilverTables = Array(
        "spark_executors_silver", "spark_Executions_silver", "spark_jobs_silver",
        "spark_stages_silver", "spark_tasks_silver"
      )
      val spark_silver_failMsg = s"Spark Silver FAILED: Rolling back all Spark Silver tables."
      if (config.debugFlag) println(spark_silver_failMsg)
      logger.log(Level.WARN, spark_silver_failMsg)

      sparkSilverTables.foreach(tbl => {
        val tableFullName = s"${config.databaseName}.${tbl}"
        val rollbackSql =
          s"""
             |delete from ${tableFullName}
             |where Overwatch_RunID = '${config.runID}'
             |""".stripMargin
        logger.log(Level.INFO, s"Rollback Statement to Execute: ${rollbackSql}")
        spark.sql(rollbackSql)
      })
    } else {
      val rollbackSql =
        s"""
           |delete from ${target.tableFullName}
           |where Overwatch_RunID = '${config.runID}'
           |""".stripMargin
      val rollBackMsg = s"Executing Rollback: STMT: ${rollbackSql}"
      if (config.debugFlag) println(rollBackMsg)
      logger.log(Level.WARN, rollBackMsg)
      spark.sql(rollbackSql)
    }

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
      .save(target.tableLocation)

    registerTarget(target)
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

  def write(df: DataFrame, target: PipelineTable, pipelineSnapTime: Column, applySparkOverrides: Boolean = true): Boolean = {
    if (applySparkOverrides) target.applySparkOverrides()
    var finalDF: DataFrame = df
    finalDF = if (target.withCreateDate) finalDF.withColumn("Pipeline_SnapTS", pipelineSnapTime) else finalDF
    finalDF = if (target.withOverwatchRunID) finalDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalDF

    logger.log(Level.INFO, s"Beginning write to ${target.tableFullName}")
    if (target.checkpointPath.nonEmpty) {

      val msg = s"Checkpoint Path Set: ${target.checkpointPath.get} - proceeding with streaming write"
      logger.log(Level.INFO, msg)
      if (config.debugFlag) println(msg)

      val beginMsg = s"Stream to ${target.tableFullName} beginning."
      if (config.debugFlag) println(beginMsg)
      logger.log(Level.INFO, beginMsg)
      if (!spark.catalog.tableExists(config.databaseName, target.name)) {
        initializeStreamTarget(finalDF, target)
      }
      val streamWriter = target.writer(finalDF)
        .asInstanceOf[DataStreamWriter[Row]]
        .option("path", target.tableLocation)
        .start()
      val streamManager = getQueryListener(streamWriter)
      spark.streams.addListener(streamManager)
      val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter.name}\nID: ${streamWriter.id}"
      if (config.debugFlag) println(listenerAddedMsg)
      logger.log(Level.INFO, listenerAddedMsg)

      streamWriter.awaitTermination()
      spark.streams.removeListener(streamManager)

    } else {
      target.writer(finalDF).asInstanceOf[DataFrameWriter[Row]].save(target.tableLocation)
      registerTarget(target)
    }
    logger.log(Level.INFO, s"Completed write to ${target.tableFullName}")
    true
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

  def exists: Boolean = {
    spark.catalog.databaseExists(_databaseName)
  }

}

object Database {

  def apply(config: Config): Database = {
    new Database(config).setDatabaseName(config.databaseName)
  }

}
