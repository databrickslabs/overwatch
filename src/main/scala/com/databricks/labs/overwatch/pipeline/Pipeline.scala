package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.struct

class Pipeline extends SparkSessionWrapper {

  // TODO - cleanse column names (no special chars)
  // TODO - enable merge schema on write -- includes checks for number of new columns
  protected val logger: Logger = Logger.getLogger(this.getClass)
  protected var _clusterIDs: Array[String] = _
  protected var _jobIDs: Array[Long] = _
  protected var _eventLogGlob: DataFrame = _
  private var _workspace: Workspace = _
  private var _database: Database = _

  import spark.implicits._

  def setWorkspace(value: Workspace): this.type = {
    _workspace = value
    this
  }

  def setDatabase(value: Database): this.type = {
    _database = value
    this
  }

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

  def workspace: Workspace = _workspace

  def database: Database = _database

  protected def clusterIDs: Array[String] = _clusterIDs

  protected def jobIDs: Array[Long] = _jobIDs

  protected def sparkEventsLogGlob: DataFrame = _eventLogGlob

  // TODO -- Enable parallelized write
  private[overwatch] def append(table: String, df: DataFrame, fromTime: Option[Long] = None): Boolean = {
    val startLogMsg = if (fromTime.nonEmpty) {
      s"Beginning append to " +
        s"${_database.getDatabaseName}.${table}. " +
        s"\n From Time: ${fromTime.toString} \n Until Time: ${Config.pipelineSnapTime.asTSString}"
    } else {
      s"Beginning append to " +
        s"${_database.getDatabaseName}.${table}."
    }
    logger.log(Level.INFO, startLogMsg)
    try {
      val f = if (Config.isLocalTesting) "parquet" else "delta"
      _database.write(df, table, withCreateDate = true, format = f)
      logger.log(Level.INFO, s"Append to $table success.")
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Could not append to $table", e)
        false
    }

  }

  //  def appendBronze(): Boolean = {
  //
  //    try {
  //      val reports = Bronze().run()
  //    }
  //
  //  }

}

