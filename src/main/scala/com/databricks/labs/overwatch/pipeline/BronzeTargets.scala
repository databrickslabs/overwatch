package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency

trait BronzeTargets {

  lazy protected val jobsTarget: PipelineTable = PipelineTable("jobs_bronze", Array("job_id"), "created_time",
    statsColumns = "created_time, creator_user_name, job_id, Pipeline_SnapTS, Overwatch_RunID".split(", "))
  lazy protected val jobRunsTarget: PipelineTable = PipelineTable("jobruns_bronze", Array("run_id"), "start_time",
    statsColumns = "job_id, original_attempt_run_id, run_id, start_time, Pipeline_SnapTS, Overwatch_RunID".split(", "))
  lazy protected val clustersTarget: PipelineTable = PipelineTable("clusters_bronze", Array("cluster_id"), "last_activity_time",
    statsColumns = ("cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
      "start_time, terminated_time, Overwatch_RunID").split(", "))
  lazy protected val poolsTarget: PipelineTable = PipelineTable("pools_bronze", Array("instance_pool_id"), "",
    statsColumns = ("instance_pool_id, node_type_id, " +
      "Pipeline_SnapTS, Overwatch_RunID").split(", "))
  lazy protected val auditLogsTarget: PipelineTable = PipelineTable("audit_log_bronze", Array("requestId", "timestamp"), "date",
    partitionBy = Array("date"), zOrderBy = Array("timestamp"), statsColumns = ("actionName, requestId, serviceName, sessionId, " +
      "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "), dataFrequency = Frequency.daily)
  lazy protected val clusterEventsTarget: PipelineTable = PipelineTable("cluster_events_bronze", Array("cluster_id", "timestamp"), "timestamp",
    partitionBy = Array("cluster_id"), zOrderBy = Array("timestamp"), statsColumns = ("cluster_id, timestamp, type, " +
      "Pipeline_SnapTS, Overwatch_RunID").split(", "))
  lazy protected val sparkEventLogsTarget: PipelineTable = PipelineTable("spark_events_bronze", Array("Event"), "timestamp",
    partitionBy = Array("Event"), zOrderBy = Array("SparkContextID"), statsColumns = "SparkContextID, ClusterID, JobGroupID, ExecutionID".split(", "))

}
