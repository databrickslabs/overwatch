package com.databricks.labs.overwatch.env

import com.databricks.backend.common.rpc.CommandContext
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.pipeline.Pipeline
import com.databricks.labs.overwatch.utils.{Config, JsonUtils, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.io.Source

/**
 * The Workspace class gets instantiated once per run per Databricks workspace. THe need for this class evolved
 * over time and is being restructured as part of an outstanding refactor branch which is likely not to be completed
 * until after v1.0 release.
 * @param config
 */
class Workspace(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _database: Database = _

  private[overwatch] def database: Database = _database

  private def setDatabase(value: Database): this.type = {
    _database = value
    this
  }

  /**
   * Most of the jobs data comes from the audit logs but there are several edge cases that result in incomplete
   * jobs log data in the audit logs (same true for cluster specs). As such, each time the jobs module executes
   * a snapshot of actively defined jobs is captured and used to fill in the blanks in the silver+ layers.
   * @return
   */
  def getJobsDF: DataFrame = {

    val jobsEndpoint = "jobs/list"

    try {
      ApiCall(jobsEndpoint, config.apiEnv)
        .executeGet()
        .asDF
        .withColumn("organization_id", lit(config.organizationId))
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, "ERROR: Failed to execute jobs/list API call.", e)
        spark.sql("select ERROR")
      }
    }

  }

  // TODO -- switch back to private overwatch after dev complete
  /**
   * Exposed config as a public getter to enable access to config for testing. This should not be public facing
   * public function.
   * @return
   */
  def getConfig: Config = config

  def getClustersDF: DataFrame = {
    val clustersEndpoint = "clusters/list"
    ApiCall(clustersEndpoint, config.apiEnv)
      .executeGet()
      .asDF
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * For future development
   * @param dbfsPath
   * @return
   */
  def getDBFSPaths(dbfsPath: String): DataFrame = {
    val dbfsEndpoint = "dbfs/list"
    val queryMap = Map[String, Any](
      "path" -> dbfsPath
    )
    ApiCall(dbfsEndpoint, config.apiEnv, Some(queryMap))
      .executeGet()
      .asDF
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Placeholder for future dev
   * @return
   */
  def getPoolsDF: DataFrame = {
    val poolsEndpoint = "instance-pools/list"
    ApiCall(poolsEndpoint, config.apiEnv)
      .executeGet()
      .asDF
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Placeholder for future dev
   * @return
   */
  def getProfilesDF: DataFrame = {
    val profilesEndpoint = "instance-profiles/list"
    ApiCall(profilesEndpoint, config.apiEnv)
      .executeGet()
      .asDF
      .withColumn("organization_id", lit(config.organizationId))
  }

  /**
   * Placeholder for future dev
   * @return
   */
  def getWorkspaceUsersDF: DataFrame = {
    val workspaceEndpoint = "workspace/list"
    ApiCall(workspaceEndpoint, config.apiEnv, Some(Map("path" -> "/Users")))
      .executeGet()
      .asDF
      .withColumn("organization_id", lit(config.organizationId))
  }

}


object Workspace {
  /**
   * Workspace companion object initializer.
   */
  def apply(database: Database, config: Config): Workspace = {
    new Workspace(config).setDatabase(database)
  }

}
