package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}

class Appender extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _workspace: Workspace = _
  private var _database: Database = _

  def setWorkspace(value: Workspace): this.type = {
    _workspace = value
    this
  }

  def setDatabase(value: Database): this.type = {
    _database = value
    this
  }

  def getWorkspace: Workspace = _workspace
  def getDatabase: Database = _database

  def appendJobs(fromDate: String): Unit = {
    val jobsTable = "jobs_master"
    val jobsDF = getWorkspace.getJobsDF

    logger.log(Level.INFO, s"Beginning jobs append to " +
      s"${_database.getDatabaseName}.${jobsTable}.")

    _database.write(jobsDF, jobsTable)

  }

}

object Appender {

  def apply(workspace: Workspace, database: Database): Appender = {
    new Appender().setWorkspace(workspace).setDatabase(database)

  }

}
