package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
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

  // TODO -- Enable parallelized write
  private def append(table: String, df: DataFrame): Boolean = {
    logger.log(Level.INFO, s"Beginning append to " +
      s"${_database.getDatabaseName}.${table}. " +
      s"\n Start Time: ${Config.fromTime.asString} \n End Time: ${Config.pipelineSnapTime.asString}")
    try {
      val f = if (Config.isLocalTesting) "parquet" else "delta"
      _database.write(df, table, withCreateDate = true, format = f)
      logger.log(Level.INFO, s"Append to $table success." +
        s"Start Time: ${Config.fromTime.asString} \n End Time: ${Config.pipelineSnapTime.asString}")
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Could not append to $table", e)
        false
    }

  }

  def buildBronze(): Boolean = {

    append("jobs_master", workspace.getJobsDF)
    append("cluster_master", workspace.getClustersDF)
    append("pools_master", workspace.getPoolsDF)
    append("profiles_master", workspace.getProfilesDF)
    append("users_master", workspace.getWorkspaceUsersDF)
//    append("audit_log_master", workspace.getAuditLogsDF)
  }

}

object Pipeline {

  def apply(workspace: Workspace, database: Database): Pipeline = {
    new Pipeline().setWorkspace(workspace).setDatabase(database)

  }

}
