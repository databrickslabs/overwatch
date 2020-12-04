package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait GoldTransforms extends SparkSessionWrapper{

  import spark.implicits._

  protected def buildCluster()(df: DataFrame): DataFrame = {
    val clusterCols: Array[Column] = Array(
      'cluster_id,
      'actionName.alias("action"),
      'timestamp.alias("unixTimeMS"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'cluster_name,
      'driver_node_type_id.alias("driver_node_type"),
      'node_type_id.alias("node_type"),
      'num_workers,
      'autoscale,
      'autoTermination_minutes.alias("auto_termination_minutes"),
      'enable_elastic_disk,
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'cluster_source,
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'instance_pool_id,
      'spark_version,
      'idempotency_token,
      'organization_id,
      'deleted_by,
      'createdBy.alias("created_by"),
      'lastEditedBy.alias("last_edited_by"),
      'Pipeline_SnapTS,
      'Overwatch_RunID
    )
    df.select(clusterCols: _*)
  }

  protected def buildJobs()(df:DataFrame): DataFrame = {
    val jobCols: Array[Column] = Array(
      'jobId.alias("job_id"),
      'actionName.alias("action"),
      'timestamp.alias("unixTimeMS"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'jobName.alias("job_name"),
      'job_type,
      'timeout_seconds,
      'schedule,
      'notebook_path,
      'new_settings,
      struct(
        'existing_cluster_id,
        'new_cluster
      ).alias("cluster"),
      'sessionId.alias("session_id"),
      'requestId.alias("request_id"),
      'userAgent.alias("user_agent"),
      'response,
      'sourceIPAddress.alias("source_ip_address"),
      'created_by,
      'created_ts,
      'deleted_by,
      'deleted_ts,
      'last_edited_by,
      'last_edited_ts,
      'Pipeline_SnapTS,
      'Overwatch_RunID
    )
    df.select(jobCols: _*)
  }

  protected def buildJobRuns()(df:DataFrame): DataFrame = {
    val jobRunCols: Array[Column] = Array(
      'runId.alias("run_id"),
      'run_name,
      $"JobRunTime.startEpochMS".alias("unixTimeMS"),
      'jobRunTime.alias("job_runtime"),
      'jobId.alias("job_id"),
      'idInJob.alias("id_in_job"),
      'jobClusterType.alias("job_cluster_type"),
      'jobTaskType.alias("job_task_type"),
      'jobTerminalState.alias("job_terminal_state"),
      'jobTriggerType.alias("job_trigger_type"),
      'clusterId.alias("cluster_id"),
      'orgId.alias("organization_id"),
      'notebook_params,
      'libraries,
      'workflow_context,
      'taskDetail.alias("task_detail"),
      'cancellationDetails.alias("cancellation_detail"),
      'timeDetails.alias("time_detail"),
      'startedBy.alias("started_by"),
      'requestDetails.alias("request_detail"),
      'Pipeline_SnapTS,
      'Overwatch_RunID
    )
    df.select(jobRunCols: _*)
  }

  protected def buildNotebook()(df: DataFrame): DataFrame = {
    val notebookCols: Array[Column] = Array(
      'notebookId.alias("notebook_id"),
      'notebookName.alias("notebook_name"),
      'path.alias("notebook_path"),
      'clusterId.alias("cluster_id"),
      'actionName.alias("action"),
      'timestamp.alias("unixTimeMS"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'oldName.alias("old_name"),
      'oldPath.alias("old_path"),
      'newName.alias("new_name"),
      'newPath.alias("new_path"),
      'parentPath.alias("parent_path"),
      'userEmail.alias("user_email"),
      'requestId.alias("request_id"),
      'response,
      'Pipeline_SnapTS,
      'Overwatch_RunID
    )
    df.select(notebookCols: _*)
  }

  protected val clusterViewColumnMapping: String =
    """
      |cluster_id, action, unixTimeMS, timestamp, date, cluster_name, driver_node_type, node_type, num_workers,
      |autoscale, auto_termination_minutes, enable_elastic_disk, cluster_log_conf, init_scripts, custom_tags,
      |cluster_source, spark_env_vars, spark_conf, acl_path_prefix, instance_pool_id, spark_version,
      |idempotency_token, organization_id, deleted_by, created_by, last_edited_by
      |""".stripMargin

  protected val jobViewColumnMapping: String =
    """
      |job_id, action, unixTimeMS, timestamp, date, job_name, job_type, timeout_seconds, schedule, notebook_path,
      | new_settings, cluster, session_id, request_id, user_agent, response, source_ip_address, created_by,
      |created_ts, deleted_by, deleted_ts, last_edited_by, last_edited_ts
      |""".stripMargin

  protected val jobRunViewColumnMapping: String =
    """
      |run_id, run_name, job_runtime, job_id, id_in_job, job_cluster_type, job_task_type, job_terminal_state,
      |job_trigger_type, cluster_id, organization_id, notebook_params, libraries, workflow_context, task_detail,
      |cancellation_detail, time_detail, started_by, request_detail
      |""".stripMargin

  protected val notebookViewColumnMappings: String =
    """
      |notebook_id, notebook_name, notebook_path, cluster_id, action, unixTimeMS, timestamp, date, old_name, old_path,
      |new_name, new_path, parent_path, user_email, request_id, response
      |""".stripMargin

}
