package com.databricks.labs.overwatch.env

import com.databricks.backend.common.rpc.CommandContext
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.DBWrapper
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.GlobalStructures._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Workspace(
                 url: String,
                 token: String
               ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._
//  lazy private val ctx: CommandContext = dbutils.notebook.getContext()
  val dbWrapper: DBWrapper = new DBWrapper

  // TODO -- Change queries appropriately based on Cloud Type

  def getJobsDF: DataFrame = {

    val jobsEndpoint = "jobs/list"

    // TODO -- Add pagination support
    // TODO -- ADD derived metadata
    // TODO -- Break out new_cluster
    // TODO -- Add Cluster Events - review pagination -- may belong in StreamRunner
    // TODO -- Only add new data, identify method for identifying new data
    try {
      spark.read.json(Seq(dbWrapper.executeGet(jobsEndpoint)).toDS).toDF
        .withColumn("data", explode('jobs))
        .drop("jobs")
        .select(
          from_unixtime($"data.created_time" / 1000).alias("create_time"),
          $"data.creator_user_name",
          $"data.job_id",
          $"data.settings",
          $"data.settings.email_notifications",
          $"data.settings.email_notifications.alert_on_last_attempt".alias("alerts_on_last_attempt"),
          $"data.settings.email_notifications.no_alert_for_skipped_runs".alias("no_alerts_on_skipped_runs"),
          $"data.settings.email_notifications.on_failure".alias("alerts_on_failure"),
          $"data.settings.email_notifications.on_start".alias("alerts_on_start"),
          $"data.settings.email_notifications.on_success".alias("alerts_on_success"),
          $"data.settings.existing_cluster_id",
          $"data.settings.libraries",
          $"data.settings.max_concurrent_runs",
          $"data.settings.max_retries",
          $"data.settings.min_retry_interval_millis",
          $"data.settings.name",
          $"data.settings.new_cluster",
//          $"data.settings.notebook_task.base_parameters".alias("notebook_base_params"), //Multiple fields same name
          $"data.settings.notebook_task.notebook_path".alias("notebook_path"),
          $"data.settings.notebook_task.revision_timestamp".alias("notebook_revision_timestamp"),
          $"data.settings.retry_on_timeout",
          $"data.settings.schedule",
          $"data.settings.spark_jar_task",
          $"data.settings.spark_python_task",
          $"data.settings.spark_submit_task",
          $"data.settings.timeout_seconds"
        )
        .drop("settings")
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could"); System.exit(1); spark.sql("select ERROR")
    }

  }
  def getClustersDF: DataFrame = {
    val clustersEndpoint = "clusters/list"

    spark.read.json(Seq(dbWrapper.executeGet(clustersEndpoint)).toDS).toDF
      .withColumn("data", explode('clusters))
      .drop("clusters")
      .select(
        $"data.autoscale",
        $"data.autoscale.max_workers",
        $"data.autoscale.min_workers",
        $"data.autotermination_minutes",
        $"data.aws_attributes",
        $"data.cluster_cores",
        $"data.cluster_id",
        $"data.cluster_log_conf",
        $"data.aws_attributes",
        $"data.cluster_log_status",
        $"data.cluster_memory_mb",
        $"data.cluster_name",
        $"data.cluster_source",
        $"data.creator_user_name",
        $"data.custom_tags",
        $"data.default_tags",
        $"data.docker_image",
        $"data.driver",
        $"data.driver_node_type_id",
        $"data.enable_elastic_disk",
        $"data.enable_local_disk_encryption",
        $"data.executors",
        $"data.init_scripts",
        $"data.init_scripts_safe_mode",
        $"data.instance_pool_id",
        $"data.jdbc_port",
        $"data.last_activity_time",
        $"data.last_state_loss_time",
        $"data.node_type_id",
        $"data.num_workers",
        $"data.pinned_by_user_name",
        $"data.spark_conf",
        $"data.spark_context_id",
        $"data.spark_env_vars",
        $"data.spark_version",
        $"data.ssh_public_keys",
        $"data.start_time",
        $"data.state",
        $"data.state_message",
        $"data.terminated_time",
        $"data.termination_reason"
      )
  }

  def getEventsByCluster(clusterId: String): DataFrame = {
    val eventsEndpoint = "clusters/events"
    val jquery = s"""{"cluster_id":"${clusterId}"}"""
    spark.read.json(Seq(dbWrapper.executePost(eventsEndpoint, jquery)).toDS).toDF
      .withColumn("data", explode('events))
      .drop("events")
    // TODO - Fill in Structure select
  }

}

object Workspace {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private def initUrl: String = {

    try {
      if (System.getenv("OVERWATCH_ENV").nonEmpty) {
        System.getenv("OVERWATCH_ENV")
      } else { dbutils.notebook.getContext().apiUrl.get }
    } catch {
      case e: Throwable => {
        val e = new Exception("Cannot Acquire Databricks URL")
        logger.log(Level.FATAL, e.getStackTrace.toString)
        throw e
      }
    }
  }

  private def initToken(tokenSecret: Option[TokenSecret]): String = {
    if (tokenSecret.nonEmpty) {
      try {
        val scope = tokenSecret.get.scope
        val key = tokenSecret.get.key
        val token = if (scope == "LOCALTESTING" && key == "TOKEN") {
          System.getenv("OVERWATCH_TOKEN")
        } else { dbutils.secrets.get(scope, key) }
        val authMsg = s"Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMsg)
        token
      } catch {
        case e: Throwable => logger.log(Level.FATAL, e); "NULL"
      }
    } else {
      try {
        val token = dbutils.notebook.getContext().apiToken.get
        println("Token scope and key not provided, running with default credentials.")
        token
      } catch {
        case e: Throwable => logger.log(Level.FATAL, e); "NULL"
      }
    }
  }

  def apply(params: OverwatchParams): Workspace = {
    new Workspace(initUrl, initToken(params.tokenSecret))
  }

  def apply(): Workspace = { new Workspace(initUrl, initToken(None))}

}
