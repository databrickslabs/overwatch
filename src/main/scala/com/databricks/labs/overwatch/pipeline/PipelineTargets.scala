package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, Frequency}

abstract class PipelineTargets(config: Config) {

  // TODO -- Partition all bronze tables by yyyyMM and apply appropriate filters
  /**
   * Bronze Targets
   */
    object Bronze {
    lazy private[overwatch] val jobsTarget: PipelineTable = PipelineTable("jobs_bronze", Array("job_id"), "created_time",
      config,
      statsColumns = "created_time, creator_user_name, job_id, Pipeline_SnapTS, Overwatch_RunID".split(", "))
    lazy private[overwatch] val jobRunsTarget: PipelineTable = PipelineTable("jobruns_bronze", Array("run_id", "job_id"), "start_time",
      config,
      statsColumns = "job_id, original_attempt_run_id, run_id, start_time, Pipeline_SnapTS, Overwatch_RunID".split(", "))
    lazy private[overwatch] val clustersTarget: PipelineTable = PipelineTable("clusters_bronze", Array("cluster_id"), "last_activity_time",
      config,
      statsColumns = ("cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
        "start_time, terminated_time, Overwatch_RunID").split(", "))
    lazy private[overwatch] val poolsTarget: PipelineTable = PipelineTable("pools_bronze", Array("instance_pool_id"), "",
      config,
      statsColumns = ("instance_pool_id, node_type_id, " +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "))
    lazy private[overwatch] val auditLogsTarget: PipelineTable = PipelineTable("audit_log_bronze", Array("requestId", "timestamp"), "date",
      config,
      partitionBy = Array("date"), statsColumns = ("actionName, requestId, serviceName, sessionId, " +
        "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "), dataFrequency = Frequency.daily)
    lazy private[overwatch] val clusterEventsTarget: PipelineTable = PipelineTable("cluster_events_bronze", Array("cluster_id", "timestamp"), "timestamp",
      config,
      statsColumns = ("cluster_id, timestamp, type, Pipeline_SnapTS, Overwatch_RunID").split(", "))
    lazy private[overwatch] val sparkEventLogsTarget: PipelineTable = PipelineTable("spark_events_bronze", Array("Event"), "timestamp",
      config,
      partitionBy = Array("Event"), zOrderBy = Array("SparkContextID"), statsColumns = "SparkContextID, ClusterID, JobGroupID, ExecutionID".split(", "))
  }


  // TODO -- When gold is built, partition silver by yyyyMM and move Zorders to gold
  /**
   * Silver Targets
   */

    object Silver {
    // TODO -- validate -- need some test data
    lazy private[overwatch] val jdbcSessionsTarget: PipelineTable = PipelineTable("jdbc_sessions_silver",
      Array("SparkContextID", "sessionId", "ip"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val jdbcOperationsTarget: PipelineTable = PipelineTable("jdbc_operations_silver",
      Array("SparkContextID", "groupId", "sessionId", "ip"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val executorsTarget: PipelineTable = PipelineTable("spark_executors_silver",
      Array("SparkContextID", "ExecutorID"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val executionsTarget: PipelineTable = PipelineTable("spark_Executions_silver",
      Array("SparkContextID", "ExecutionID"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val jobsTarget: PipelineTable = PipelineTable("spark_jobs_silver",
      Array("SparkContextID", "JobID"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val stagesTarget: PipelineTable = PipelineTable("spark_stages_silver",
      Array("SparkContextID", "StageID", "StageAttemptID"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val tasksTarget: PipelineTable = PipelineTable("spark_tasks_silver",
      Array("SparkContextID", "StageID", "StageAttemptID", "TaskID"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val dbJobsHistoricalTarget: PipelineTable = Bronze.jobsTarget.copy(
      name = "db_jobs_historical_silver",
      incrementalFromColumn = "Pipeline_SnapTS", config = config
    )

    lazy private[overwatch] val dbJobsCurrentTarget: PipelineTable = Bronze.jobsTarget.copy(
      name = "db_jobs_current_silver", mode = "overwrite",
      incrementalFromColumn = "Pipeline_SnapTS", config = config
    )

    lazy private[overwatch] val dbJobRunsTarget: PipelineTable = Bronze.jobRunsTarget.copy(
      name = "db_jobRuns_silver",
      incrementalFromColumn = "Pipeline_SnapTS", config = config
    )

    lazy private[overwatch] val dbClustersHistoricalTarget: PipelineTable = Bronze.clustersTarget.copy(
      name = "db_clusters_historical_silver",
      incrementalFromColumn = "Pipeline_SnapTS", config = config,
      zOrderBy = Array("cluster_id")
    )

    lazy private[overwatch] val dbClustersCurrentTarget: PipelineTable =  Bronze.clustersTarget.copy(
      name = "db_clusters_current_silver", mode = "overwrite",
      incrementalFromColumn = "Pipeline_SnapTS", config = config
    )

    lazy private[overwatch] val dbClustersEventsTarget: PipelineTable = Bronze.clusterEventsTarget.copy(
      name = "db_clustersEvents_silver",
      incrementalFromColumn = "Pipeline_SnapTS", config = config,
      partitionBy = Array("type"),
      zOrderBy = Array("cluster_id")
    )

    lazy private[overwatch] val dbPoolsHistoricalTarget: PipelineTable = Bronze.poolsTarget.copy(
      name = "db_pools_historical_silver", config = config,
      incrementalFromColumn = "Pipeline_SnapTS")

    lazy private[overwatch] val dbPoolsCurrentTarget: PipelineTable = Bronze.poolsTarget.copy(
      name = "db_pools_current_silver", config = config, mode = "overwrite",
      incrementalFromColumn = "Pipeline_SnapTS")

    lazy private[overwatch] val secAuditLogs: PipelineTable = Bronze.auditLogsTarget.copy(
      name = "sec_audit_log_silver", config = config,
      incrementalFromColumn = "Pipeline_SnapTS", zOrderBy = Array("serviceName", "actionName", "requestID"))



  }


}
