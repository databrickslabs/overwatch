package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, Frequency}

abstract class PipelineTargets(config: Config) {

  // TODO -- Partition all bronze tables by yyyyMM and apply appropriate filters
  /**
   * Bronze Targets
   */
    object BronzeTargets {
    lazy private[overwatch] val jobsSnapshotTarget: PipelineTable = PipelineTable("jobs_snapshot_bronze",
      Array("job_id"), "created_time",
      config,
      statsColumns = "created_time, creator_user_name, job_id, Pipeline_SnapTS, Overwatch_RunID".split(", "))
//    lazy private[overwatch] val jobRunsTarget: PipelineTable = PipelineTable("jobruns_bronze", Array("run_id", "job_id"), "start_time",
//      config,
//      statsColumns = "job_id, original_attempt_run_id, run_id, start_time, Pipeline_SnapTS, Overwatch_RunID".split(", "))
//    lazy private[overwatch] val jobRunsSnapshotTarget: PipelineTable = PipelineTable("jobruns_snapshot_bronze",
//      Array("run_id", "job_id"), "start_time",
//      config //,
//      // TODO -- change and put back
////      statsColumns = "job_id, original_attempt_run_id, run_id, start_time, Pipeline_SnapTS, Overwatch_RunID".split(", ")
//    )
    lazy private[overwatch] val clustersSnapshotTarget: PipelineTable = PipelineTable("clusters_snapshot_bronze",
      Array("cluster_id"), "last_activity_time",
      config,
      statsColumns = ("cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
        "start_time, terminated_time, Overwatch_RunID").split(", "))
    lazy private[overwatch] val poolsTarget: PipelineTable = PipelineTable("pools_snapshot_bronze",
      Array("instance_pool_id"), "",
      config,
      statsColumns = ("instance_pool_id, node_type_id, " +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "))
    lazy private[overwatch] val auditLogsTarget: PipelineTable = PipelineTable("audit_log_bronze", Array("requestId", "timestamp"), "date",
      config,
      partitionBy = Array("date"), statsColumns = ("actionName, requestId, serviceName, sessionId, " +
        "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "), dataFrequency = Frequency.daily)
    lazy private[overwatch] val clusterEventsTarget: PipelineTable = PipelineTable("cluster_events_bronze",
      Array("cluster_id", "timestamp"), "timestamp",
      config,
      statsColumns = ("cluster_id, timestamp, type, Pipeline_SnapTS, Overwatch_RunID").split(", "))
    lazy private[overwatch] val sparkEventLogsTempTarget: PipelineTable = PipelineTable("spark_events_temp_raw", Array("Event"), "time",
      config, mode = "overwrite", withCreateDate = false, withOverwatchRunID = false, isTemp = true
    )
    lazy private[overwatch] val sparkEventLogsTarget: PipelineTable = PipelineTable("spark_events_bronze", Array("Event"),
      "Pipeline_SnapTS", // Holder until refactor
      config,
      partitionBy = Array("Event", "Downstream_Processed"), zOrderBy = Array("clusterId", "SparkContextID"),
      statsColumns = "SparkContextID, clusterID, JobGroupID, ExecutionID".split(", ") //,
//      sparkOverrides = Map(
//        "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols" -> "2",
//        "spark.databricks.delta.optimize.maxFileSize" -> (1024 * 1024 * 2).toString
//      )
    )
    lazy private[overwatch] val processedEventLogs: PipelineTable = PipelineTable("spark_events_processedFiles",
      Array("filename"), "Pipeline_SnapTS",
      config
    )

    lazy private[overwatch] val cloudMachineDetail: PipelineTable = if (config.cloudProvider == "azure") {
      PipelineTable("instanceDetails", Array("API_Name"), "", config, mode = "overwrite")
    } else {
      // TODO -- implement for azure
      PipelineTable("instanceDetails", Array("API_Name"), "", config, mode = "overwrite")
    }

  }


  // TODO -- When gold is built, partition silver by yyyyMM and move Zorders to gold
  /**
   * Silver Targets
   */

    object SilverTargets {
    // TODO -- validate -- need some test data
//    lazy private[overwatch] val jdbcSessionsTarget: PipelineTable = PipelineTable("jdbc_sessions_silver",
//      Array("SparkContextID", "sessionId", "ip"), "Pipeline_SnapTS", config)
//
//    lazy private[overwatch] val jdbcOperationsTarget: PipelineTable = PipelineTable("jdbc_operations_silver",
//      Array("SparkContextID", "groupId", "sessionId", "ip"), "Pipeline_SnapTS", config)

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

    lazy private[overwatch] val dbJobsTarget: PipelineTable = BronzeTargets.jobsSnapshotTarget.copy(
      name = "db_jobs_silver",
      incrementalColumn = "Pipeline_SnapTS", config = config
    )

    lazy private[overwatch] val dbJobRunsTarget: PipelineTable = PipelineTable("jobrun_silver",
      Array("startTimestamp", "runId"), "startTimestamp", config, zOrderBy = Array("runId", "jobId"))

//    lazy private[overwatch] val dbClustersTarget: PipelineTable = BronzeTargets.clustersSnapshotTarget.copy(
//      name = "db_clusters_silver",
//      incrementalColumn = "Pipeline_SnapTS", config = config,
//      zOrderBy = Array("cluster_id")
//    )

//    lazy private[overwatch] val dbClustersEventsTarget: PipelineTable = BronzeTargets.clusterEventsTarget.copy(
//      name = "db_clustersEvents_silver",
//      incrementalColumn = "Pipeline_SnapTS", config = config,
//      partitionBy = Array("type"),
//      zOrderBy = Array("cluster_id")
//    )

    lazy private[overwatch] val userLoginsTarget: PipelineTable = PipelineTable("user_login_silver",
      Array("timestamp", "userEmail"), "timestamp", config)

    lazy private[overwatch] val newAccountsTarget: PipelineTable = PipelineTable("user_account_silver",
      Array("timestamp", "targetUserName"), "timestamp", config)

    lazy private[overwatch] val clustersSpecTarget: PipelineTable = PipelineTable("cluster_spec_silver",
      Array("timestamp", "cluster_id"), "timestamp", config, zOrderBy = Array("cluster_id"))

    lazy private[overwatch] val clustersStatusTarget: PipelineTable = PipelineTable("cluster_status_silver",
      Array("timestamp", "cluster_id"), "timestamp", config, zOrderBy = Array("cluster_id"))

    lazy private[overwatch] val dbJobsStatusTarget: PipelineTable = PipelineTable("job_status_silver",
      Array("timestamp", "job_id"), "timestamp", config)

    lazy private[overwatch] val notebookStatusTarget: PipelineTable = PipelineTable("notebook_silver",
      Array("timestamp", "notebook_id"), "timestamp", config)

  }


}
