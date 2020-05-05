package com.databricks.labs.overwatch.env

import com.databricks.backend.common.rpc.CommandContext
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.utils.{Config, JsonUtils, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.io.Source

class Workspace extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  // TODO -- Change queries appropriately based on Cloud Type

  // TODO -- Load up spark logs via a path glob
  def getLogGlobs = ???

  def getJobsDF: DataFrame = {

    val jobsEndpoint = "jobs/list"

    // TODO -- Add pagination support
    // TODO -- ADD derived metadata
    // TODO -- Break out new_cluster
    // TODO -- Add Cluster Events - review pagination -- may belong in StreamRunner
    // TODO -- Only add new data, identify method for identifying new data
    try {
      ApiCall(jobsEndpoint)
        .executeGet()
        .asDF
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could", e); spark.sql("select ERROR")
    }

  }

  def getClustersDF: DataFrame = {
    val clustersEndpoint = "clusters/list"
    ApiCall(clustersEndpoint)
      .executeGet()
      .asDF
  }

  def getEventsByCluster(clusterId: String): DataFrame = {
    val eventsEndpoint = "clusters/events"
    val queryMap = Map[String, Any](
      "cluster_id" -> clusterId
    )
    ApiCall(eventsEndpoint, Some(queryMap))
      .executePost()
      .asDF
  }

  def getDBFSPaths(dbfsPath: String): DataFrame = {
    val dbfsEndpoint = "dbfs/list"
    val queryMap = Map[String, Any](
      "path" -> dbfsPath
    )
    ApiCall(dbfsEndpoint, Some(queryMap))
      .executeGet()
      .asDF
  }

  def getPoolsDF: DataFrame = {
    val poolsEndpoint = "instance-pools/list"
    ApiCall(poolsEndpoint)
      .executeGet()
      .asDF
  }

  def getProfilesDF: DataFrame = {
    val profilesEndpoint = "instance-profiles/list"
    ApiCall(profilesEndpoint)
      .executeGet()
      .asDF
  }

  def getWorkspaceUsersDF: DataFrame = {
    val workspaceEndpoint = "workspace/list"
//    val queryMap = Map[String, Any](
//      "path" -> "/Users"
//    )

    ApiCall(workspaceEndpoint, Some(Map("path" -> "/Users")))
      .executeGet()
      .asDF
  }

  def getAuditLogsDF: DataFrame = {
    spark.read.json(Config.auditLogPath.get)
  }

  private def getUniqueSparkEventsFiles(filesToBeConsidered: DataFrame): Array[String] = {
    spark.table("spark_events_master").select('filename)
      .unionByName(filesToBeConsidered)
      .distinct.as[String].collect()
  }


  def getEventLogsDF(pathsGlobDF: DataFrame): DataFrame = {

    val pathsGlob = getUniqueSparkEventsFiles(pathsGlobDF)

    val eventsDFRaw = spark.read.option("badRecordsPath", Config.badRecordsPath)
      .json(pathsGlob: _*).drop("Classpath Entries")
    spark.createDataFrame(eventsDFRaw.rdd, SchemaTools.sanitizeSchema(eventsDFRaw.schema).asInstanceOf[StructType])
      .withColumn("filename", input_file_name)
      .repartition(getTotalCores * 4)
  }

}

object Workspace {
  def apply(): Workspace = {
    new Workspace
  }

}
