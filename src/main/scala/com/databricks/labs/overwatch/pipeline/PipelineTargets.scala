package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, Frequency}

abstract class PipelineTargets(config: Config) {

  /**
   * Bronze Targets
   */
  object BronzeTargets {
    lazy private[overwatch] val jobsSnapshotTarget: PipelineTable = PipelineTable(
      name = "jobs_snapshot_bronze",
      keys = Array("organization_id", "job_id"),
      config,
      statsColumns = "created_time, creator_user_name, job_id, Pipeline_SnapTS, Overwatch_RunID".split(", "),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val clustersSnapshotTarget: PipelineTable = PipelineTable(
      name = "clusters_snapshot_bronze",
      keys = Array("organization_id", "cluster_id"),
      config,
      statsColumns = ("organization_id, cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
        "start_time, terminated_time, Overwatch_RunID").split(", "),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val poolsTarget: PipelineTable = PipelineTable(
      name = "pools_snapshot_bronze",
      keys = Array("organization_id", "instance_pool_id"),
      config,
      statsColumns = ("instance_pool_id, node_type_id, " +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val auditLogsTarget: PipelineTable = PipelineTable(
      name = "audit_log_bronze",
      keys = Array("organization_id", "requestId"),
      config,
      incrementalColumns = Array("date", "timestamp"),
      partitionBy = Seq("organization_id", "date"),
      statsColumns = ("actionName, requestId, serviceName, sessionId, " +
        "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "),
      dataFrequency = Frequency.daily,
      masterSchema = Some(Schema.auditMasterSchema)
    )

    lazy private[overwatch] val auditLogAzureLandRaw: PipelineTable = PipelineTable(
      name = "audit_log_raw_events",
      keys = Array("sequenceNumber"),
      config,
      partitionBy = Seq("organization_id", "Overwatch_RunID", "__overwatch_ctrl_noise"),
      withOverwatchRunID = if (config.cloudProvider == "azure") false else true,
      checkpointPath = if (config.cloudProvider == "azure")
        config.auditLogConfig.azureAuditLogEventhubConfig.get.auditRawEventsChk
      else None
    )

    lazy private[overwatch] val clusterEventsTarget: PipelineTable = PipelineTable(
      name = "cluster_events_bronze",
      keys = Array("organization_id", "cluster_id", "timestamp"),
      config,
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      incrementalColumns = Array("timestamp"),
      statsColumns = "cluster_id, timestamp, type, Pipeline_SnapTS, Overwatch_RunID".split(", "))

    lazy private[overwatch] val sparkEventLogsTarget: PipelineTable = PipelineTable(
      name = "spark_events_bronze",
      keys = Array("organization_id", "Event"), // really aren't any global valid keys for this table
      config,
      incrementalColumns = Array("fileCreateEpochMS"),
      partitionBy = Seq("organization_id", "Event", "fileCreateDate"),
      statsColumns = ("organization_id, Event, clusterId, SparkContextId, JobID, StageID," +
        "StageAttemptID, TaskType, ExecutorID, fileCreateDate, fileCreateEpochMS, fileCreateTS, filename," +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
        "spark.sql.files.maxPartitionBytes" -> (1024 * 1024 * 64).toString
        // very large schema to imply, too much parallelism and schema result size is too large to
        // serialize, 64m seems to be a good middle ground.
      ),
      autoOptimize = true, // TODO -- perftest
      masterSchema = Some(Schema.sparkEventsRawMasterSchema)
    )

    lazy private[overwatch] val processedEventLogs: PipelineTable = PipelineTable(
      name = "spark_events_processedFiles",
      keys = Array("filename"),
      config,
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val cloudMachineDetail: PipelineTable = if (config.cloudProvider == "azure") {
      PipelineTable(
        "instanceDetails",
        Array("API_Name"),
        config, mode = "overwrite",
        _databaseName = config.consumerDatabaseName,
        partitionBy = Seq("organization_id")
      )
    } else {
      // TODO -- implement for azure
      PipelineTable(
        "instanceDetails",
        Array("API_Name"),
        config, mode = "overwrite",
        _databaseName = config.consumerDatabaseName,
        partitionBy = Seq("organization_id")
      )
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
      partitionBy = Seq("organization_id"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048"
      ),
      shuffleFactor = 0.25
    )

    lazy private[overwatch] val executionsTarget: PipelineTable = PipelineTable(
      name = "spark_Executions_silver",
      keys = Array("SparkContextID", "ExecutionID"),
      config,
      partitionBy = Seq("organization_id"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048"
      ),
      shuffleFactor = 0.25
    )

    lazy private[overwatch] val jobsTarget: PipelineTable = PipelineTable(
      name = "spark_jobs_silver",
      keys = Array("SparkContextID", "JobID"),
      config,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Seq("organization_id", "startDate"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048"
      ),
      shuffleFactor = 0.25
    )

    lazy private[overwatch] val stagesTarget: PipelineTable = PipelineTable(
      name = "spark_stages_silver",
      keys = Array("SparkContextID", "StageID", "StageAttemptID"),
      config,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Seq("organization_id", "startDate"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048"
      ),
      shuffleFactor = 0.25
    )

    lazy private[overwatch] val tasksTarget: PipelineTable = PipelineTable(
      name = "spark_tasks_silver",
      keys = Array("SparkContextID", "StageID", "StageAttemptID", "TaskID"),
      config,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Seq("organization_id", "startDate"),
      shuffleFactor = 5,
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
      )
    )

    lazy private[overwatch] val dbJobRunsTarget: PipelineTable = PipelineTable(
      name = "jobrun_silver",
      keys = Array("timestamp", "runId"),
      config,
      incrementalColumns = Array("endEpochMS"), // don't load into gold until run is terminated
      zOrderBy = Array("runId", "jobId"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val accountLoginTarget: PipelineTable = PipelineTable(
      name = "account_login_silver",
      keys = Array("timestamp", "userEmail"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val accountModTarget: PipelineTable = PipelineTable(
      name = "account_mods_silver",
      keys = Array("requestId"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val clustersSpecTarget: PipelineTable = PipelineTable(
      name = "cluster_spec_silver",
      keys = Array("timestamp", "cluster_id"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val dbJobsStatusTarget: PipelineTable = PipelineTable(
      name = "job_status_silver",
      keys = Array("timestamp", "job_id"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val notebookStatusTarget: PipelineTable = PipelineTable(
      name = "notebook_silver",
      keys = Array("timestamp", "notebook_id"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

  }

  object GoldTargets {

    lazy private[overwatch] val clusterTarget: PipelineTable = PipelineTable(
      name = "cluster_gold",
      keys = Array("cluster_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val clusterViewTarget: PipelineView = PipelineView(
      name = "cluster",
      clusterTarget,
      config
    )

    lazy private[overwatch] val jobTarget: PipelineTable = PipelineTable(
      name = "job_gold",
      keys = Array("job_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val jobViewTarget: PipelineView = PipelineView(
      name = "job",
      jobTarget,
      config
    )

    lazy private[overwatch] val jobRunTarget: PipelineTable = PipelineTable(
      name = "jobRun_gold",
      keys = Array("run_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("endEpochMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val jobRunsViewTarget: PipelineView = PipelineView(
      name = "jobRun",
      jobRunTarget,
      config
    )

    lazy private[overwatch] val jobRunCostPotentialFactTarget: PipelineTable = PipelineTable(
      name = "jobRunCostPotentialFact_gold",
      keys = Array("organization_id", "job_id", "id_in_job"),
      config,
      incrementalColumns = Array("job_start_date"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val jobRunCostPotentialFactViewTarget: PipelineView = PipelineView(
      name = "jobRunCostPotentialFact",
      jobRunCostPotentialFactTarget,
      config
    )

    lazy private[overwatch] val notebookTarget: PipelineTable = PipelineTable(
      name = "notebook_gold",
      keys = Array("notebook_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val notebookViewTarget: PipelineView = PipelineView(
      name = "notebook",
      notebookTarget,
      config
    )

    lazy private[overwatch] val accountModsTarget: PipelineTable = PipelineTable(
      name = "account_mods_gold",
      keys = Array("requestId"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val accountModsViewTarget: PipelineView = PipelineView(
      name = "accountMod",
      accountModsTarget,
      config,
      dbTargetOverride = Some(config.databaseName) // Held in ETL DB due to data sensitivity
    )

    lazy private[overwatch] val accountLoginTarget: PipelineTable = PipelineTable(
      name = "account_login_gold",
      keys = Array("requestId"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val accountLoginViewTarget: PipelineView = PipelineView(
      name = "accountLogin",
      accountLoginTarget,
      config,
      dbTargetOverride = Some(config.databaseName) // Held in ETL DB due to data sensitivity
    )

    lazy private[overwatch] val clusterStateFactTarget: PipelineTable = PipelineTable(
      name = "clusterStateFact_gold",
      keys = Array("cluster_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("unixTimeMS_state_start"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val clusterStateFactViewTarget: PipelineView = PipelineView(
      name = "clusterStateFact",
      clusterStateFactTarget,
      config
    )

    lazy private[overwatch] val sparkJobTarget: PipelineTable = PipelineTable(
      name = "sparkJob_gold",
      keys = Array("spark_context_id", "job_id"),
      config,
      partitionBy = Seq("organization_id", "date"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
      ),
      zOrderBy = Array("cluster_id"),
      incrementalColumns = Array("unixTimeMS")
    )

    lazy private[overwatch] val sparkJobViewTarget: PipelineView = PipelineView(
      name = "sparkJob",
      sparkJobTarget,
      config
    )

    lazy private[overwatch] val sparkStageTarget: PipelineTable = PipelineTable(
      name = "sparkStage_gold",
      keys = Array("spark_context_id", "stage_id", "stage_attempt_id"),
      config,
      partitionBy = Seq("organization_id", "date"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
      ),
      zOrderBy = Array("cluster_id"),
      incrementalColumns = Array("unixTimeMS")
    )

    lazy private[overwatch] val sparkStageViewTarget: PipelineView = PipelineView(
      name = "sparkStage",
      sparkStageTarget,
      config
    )

    lazy private[overwatch] val sparkTaskTarget: PipelineTable = PipelineTable(
      name = "sparkTask_gold",
      keys = Array("spark_context_id", "task_id", "task_attempt_id"),
      config,
      partitionBy = Seq("organization_id", "date"),
      zOrderBy = Array("cluster_id"),
      incrementalColumns = Array("unixTimeMS"),
      shuffleFactor = 5,
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000", // output is very skewed by partition
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
      )
    )

    lazy private[overwatch] val sparkTaskViewTarget: PipelineView = PipelineView(
      name = "sparkTask",
      sparkTaskTarget,
      config
    )

    lazy private[overwatch] val sparkExecutionTarget: PipelineTable = PipelineTable(
      name = "sparkExecution_gold",
      keys = Array("spark_context_id", "execution_id"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
      )
    )

    lazy private[overwatch] val sparkExecutionViewTarget: PipelineView = PipelineView(
      name = "sparkExecution",
      sparkExecutionTarget,
      config
    )

    lazy private[overwatch] val sparkExecutorTarget: PipelineTable = PipelineTable(
      name = "sparkExecutor_gold",
      keys = Array("spark_context_id", "executor_id"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id"),
      autoOptimize = true,
      sparkOverrides = Map(
        "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
        "spark.databricks.delta.optimizeWrite.binSize" -> "2048" // output is very dense, shrink output file size
      )
    )

    lazy private[overwatch] val sparkExecutorViewTarget: PipelineView = PipelineView(
      name = "sparkExecutor",
      sparkExecutorTarget,
      config
    )

  }


}
