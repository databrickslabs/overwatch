package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, Frequency}

abstract class PipelineTargets(config: Config) {

  // TODO -- Partition all bronze tables by yyyyMM and apply appropriate filters
  /**
   * Bronze Targets
   */
    object BronzeTargets {
    lazy private[overwatch] val jobsSnapshotTarget: PipelineTable = PipelineTable(
      name = "jobs_snapshot_bronze",
      keys = Array("job_id"),
      config,
      statsColumns = "created_time, creator_user_name, job_id, Pipeline_SnapTS, Overwatch_RunID".split(", "))

    lazy private[overwatch] val clustersSnapshotTarget: PipelineTable = PipelineTable(
      name = "clusters_snapshot_bronze",
      keys = Array("cluster_id"),
      config,
      statsColumns = ("cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
        "start_time, terminated_time, Overwatch_RunID").split(", "))

    lazy private[overwatch] val poolsTarget: PipelineTable = PipelineTable(
      name = "pools_snapshot_bronze",
      keys = Array("instance_pool_id"),
      config,
      statsColumns = ("instance_pool_id, node_type_id, " +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "))

    lazy private[overwatch] val auditLogsTarget: PipelineTable = PipelineTable(
      name = "audit_log_bronze",
      keys = Array("requestId"),
      config,
      incrementalColumns = Array("date", "timestamp"),
      partitionBy = Array("date"),
      statsColumns = ("actionName, requestId, serviceName, sessionId, " +
        "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "),
      dataFrequency = Frequency.daily,
      checkpointPath = if (config.cloudProvider == "azure")
        config.auditLogConfig.azureAuditLogEventhubConfig.get.auditLogChk
      else None
    )

    lazy private[overwatch] val auditLogAzureLandRaw: PipelineTable = PipelineTable(
      name = "audit_log_raw_events",
      keys = Array("sequenceNumber"),
      config,
      checkpointPath = if (config.cloudProvider == "azure")
        config.auditLogConfig.azureAuditLogEventhubConfig.get.auditRawEventsChk
      else None
    )

    lazy private[overwatch] val clusterEventsTarget: PipelineTable = PipelineTable(
      name = "cluster_events_bronze",
      keys = Array("cluster_id", "timestamp"),
      config,
      incrementalColumns = Array("timestamp"),
      statsColumns = ("cluster_id, timestamp, type, Pipeline_SnapTS, Overwatch_RunID").split(", "))

    lazy private[overwatch] val sparkEventLogsTarget: PipelineTable = PipelineTable(
      name = "spark_events_bronze",
      keys = Array("Event"),
      config,
      incrementalColumns = Array("Downstream_Processed"),
      partitionBy = Array("Event", "Downstream_Processed"),
      statsColumns = "SparkContextID, clusterID, JobGroupID, ExecutionID".split(", "),
      sparkOverrides = if (config.isFirstRun) {
        Map(
          "spark.sql.files.maxPartitionBytes" -> (1024 * 1024 * 64).toString,
          "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
          "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
          "spark.hadoop.fs.s3a.multipart.threshold" -> "204857600",
          "spark.hadoop.fs.s3a.multipart.size" -> "104857600"
        )
      } else {
        Map(
          "spark.sql.files.maxPartitionBytes" -> (1024 * 1024 * 32).toString,
          "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
          "spark.hadoop.fs.s3a.multipart.threshold" -> "204857600",
          "spark.hadoop.fs.s3a.multipart.size" -> "104857600"
        )
      },
      autoOptimize = true // TODO -- perftest
    )

    lazy private[overwatch] val processedEventLogs: PipelineTable = PipelineTable(
      name = "spark_events_processedFiles",
      keys = Array("filename"),
      config
    )

    lazy private[overwatch] val cloudMachineDetail: PipelineTable = if (config.cloudProvider == "azure") {
      PipelineTable("instanceDetails", Array("API_Name"), config, mode = "overwrite")
    } else {
      // TODO -- implement for azure
      PipelineTable("instanceDetails", Array("API_Name"), config, mode = "overwrite")
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

    lazy private[overwatch] val executorsTarget: PipelineTable = PipelineTable(
      name = "spark_executors_silver",
      keys = Array("SparkContextID", "ExecutorID"),
      config,
      shuffleFactor = 0.08
    )

    lazy private[overwatch] val executionsTarget: PipelineTable = PipelineTable(
      name = "spark_Executions_silver",
      keys = Array("SparkContextID", "ExecutionID"),
      config,
      shuffleFactor = 0.07
    )

    lazy private[overwatch] val jobsTarget: PipelineTable = PipelineTable(
      name = "spark_jobs_silver",
      keys = Array("SparkContextID", "JobID"),
      config,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Array("startDate"),
      shuffleFactor = 0.06
    )

    lazy private[overwatch] val stagesTarget: PipelineTable = PipelineTable(
      name = "spark_stages_silver",
      keys = Array("SparkContextID", "StageID", "StageAttemptID"),
      config,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Array("startDate"),
      shuffleFactor = 0.07
    )

    lazy private[overwatch] val tasksTarget: PipelineTable = PipelineTable(
      name = "spark_tasks_silver",
      keys = Array("SparkContextID", "StageID", "StageAttemptID", "TaskID"),
      config,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Array("startDate", "clusterId"),
      shuffleFactor = 1.2,
      autoOptimize = true,
      sparkOverrides = if (config.isFirstRun)
        Map(
          "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
          "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
          "spark.hadoop.fs.s3a.multipart.threshold" -> "204857600",
          "spark.hadoop.fs.s3a.multipart.size" -> "104857600",
          "spark.shuffle.io.serverThreads" -> "32"
        )
      else Map[String, String]()
    )

    lazy private[overwatch] val dbJobRunsTarget: PipelineTable = PipelineTable(
      name = "jobrun_silver",
      keys = Array("startTimestamp", "runId"),
      config,
      incrementalColumns = Array("startTimestamp"),
      zOrderBy = Array("runId", "jobId")
    )

    lazy private[overwatch] val userLoginsTarget: PipelineTable = PipelineTable(
      name = "user_login_silver",
      keys = Array("timestamp", "userEmail"),
      config,
      incrementalColumns = Array("timestamp")
    )

    lazy private[overwatch] val newAccountsTarget: PipelineTable = PipelineTable(
      name = "user_account_silver",
      keys = Array("timestamp", "targetUserName"),
      config,
      incrementalColumns = Array("timestamp")
    )

    lazy private[overwatch] val clustersSpecTarget: PipelineTable = PipelineTable(
      name = "cluster_spec_silver",
      keys = Array("timestamp", "cluster_id"),
      config,
      incrementalColumns = Array("timestamp"),
      zOrderBy = Array("cluster_id")
    )

    lazy private[overwatch] val clustersStatusTarget: PipelineTable = PipelineTable(
      name = "cluster_status_silver",
      keys = Array("timestamp", "cluster_id"),
      config,
      incrementalColumns = Array("timestamp"),
      zOrderBy = Array("cluster_id")
    )

    lazy private[overwatch] val dbJobsStatusTarget: PipelineTable = PipelineTable(
      name = "job_status_silver",
      keys = Array("timestamp", "job_id"),
      config,
      incrementalColumns = Array("timestamp")
    )

    lazy private[overwatch] val notebookStatusTarget: PipelineTable = PipelineTable(
      name = "notebook_silver",
      keys = Array("timestamp", "notebook_id"),
      config,
      incrementalColumns = Array("timestamp")
    )

  }


}
