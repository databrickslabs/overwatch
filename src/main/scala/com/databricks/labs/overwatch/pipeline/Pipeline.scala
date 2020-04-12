package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Global, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.struct

class Pipeline extends SparkSessionWrapper {

  // TODO - cleanse column names (no special chars)
  // TODO - enable merge schema on write -- includes checks for number of new columns
  private val logger: Logger = Logger.getLogger(this.getClass)
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

  def workspace: Workspace = _workspace
  def database: Database = _database

  private def append(table: String, df: DataFrame): Boolean = {
    logger.log(Level.INFO, s"Beginning append to " +
      s"${_database.getDatabaseName}.${table}.")

    try {
      _database.write(df, table, withCreateDate = true)
      logger.log(Level.INFO, s"Append to $table success." +
        s"Start Time: ${Global.fromTime} \n End Time: ${Global.pipelineSnapTime}")
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Could not append to $table", e)
        false
    }

  }

  // TODO -- Get start date from persisted snapshot time in previous run
  def appendJobs(): Boolean = {
    val jobsTable = "jobs_master"
    val jobsDF = workspace.getJobsDF

    logger.log(Level.INFO, s"Beginning jobs append to " +
      s"${_database.getDatabaseName}.${jobsTable}.")

    try {
      _database.write(jobsDF, jobsTable, withCreateDate = true)
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could not append to jobs_master", e)
        false
    }
  }

  def appendClusters(): Unit = {

  }

}

object Pipeline {

  def apply(workspace: Workspace, database: Database): Pipeline = {
    new Pipeline().setWorkspace(workspace).setDatabase(database)

  }

}
