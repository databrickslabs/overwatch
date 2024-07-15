package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, MergeScope, WriteMode}

abstract class PipelineTargets(config: Config) {
// TODO -- Refactor -- this class should extend workspace so these are "WorkspaceTargets"

  /**
   * global pipeline state table
   */
  val pipelineStateTarget: PipelineTable = PipelineTable(
    name = "pipeline_report",
    _keys = Array("organization_id", "moduleId", "Overwatch_RunID"),
    config = config,
    partitionBy = Array("organization_id"),
    incrementalColumns = Array("Pipeline_SnapTS"),
    statsColumns = ("organization_id, workspace_name, moduleID, moduleName, runStartTS, runEndTS, " +
      "fromTS, untilTS, status, " +
      "writeOpsMetrics, lastOptimizedTS, Pipeline_SnapTS, primordialDateString").split(", ")
  )

  val apiEventsTarget: PipelineTable = PipelineTable(
    name = "apiEventDetails",
    _keys = Array("organization_id", "Overwatch_RunID", "endPoint"),
    config = config,
    incrementalColumns = Array("Pipeline_SnapTS"),
    partitionBy = Array("organization_id", "endPoint"),
    statsColumns = ("organization_id,endPoint").split(", ")
  )


  lazy private[overwatch] val pipelineStateViewTarget: PipelineView = PipelineView(
    name = "pipReport",
    pipelineStateTarget,
    config,
    dbTargetOverride = Some(config.databaseName)
  )

  /**
   * Bronze Targets
   */
  object BronzeTargets {

    lazy private[overwatch] val jobsSnapshotTarget: PipelineTable = PipelineTable(
      name = "jobs_snapshot_bronze",
      _keys = Array("job_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      statsColumns = "created_time, creator_user_name, job_id, Pipeline_SnapTS, Overwatch_RunID".split(", "),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.jobSnapMinimumSchema)
    )

    lazy private[overwatch] val clustersSnapshotTarget: PipelineTable = PipelineTable(
      name = "clusters_snapshot_bronze",
      _keys = Array("cluster_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      statsColumns = ("organization_id, cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
        "start_time, terminated_time, Overwatch_RunID").split(", "),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.clusterSnapMinimumSchema)
    )

    lazy private[overwatch] val clusterSnapshotErrorsTarget: PipelineTable = PipelineTable(
      name = "clusters_snapshot_error_bronze",
      _keys = Array("cluster_id", "from_epoch", "until_epoch", "Overwatch_RunID"),
      config,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("Pipeline_SnapTS"))

    lazy private[overwatch] val poolsSnapshotTarget: PipelineTable = PipelineTable(
      name = "pools_snapshot_bronze",
      _keys = Array("instance_pool_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      statsColumns = ("instance_pool_id, node_type_id, " +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.poolsSnapMinimumSchema)
    )

    lazy private[overwatch] val auditLogsTarget: PipelineTable = PipelineTable(
      name = "audit_log_bronze",
      _keys = Array("timestamp", "serviceName", "actionName", "requestId", "hashKey"),
      config,
      incrementalColumns = Array("date", "timestamp"),
      partitionBy = Seq("organization_id", "date"),
      statsColumns = ("actionName, requestId, serviceName, sessionId, " +
        "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "),
      _permitDuplicateKeys = false,
      _mode = WriteMode.merge,
      mergeScope = MergeScope.insertOnly,
      masterSchema = Some(Schema.auditMasterSchema),
      excludedReconColumn = Array("hashKey","response","requestParams","userIdentity")// It will be different for system table
    )

    lazy private[overwatch] val auditLogAzureLandRaw: PipelineTable = PipelineTable(
      name = "audit_log_raw_events",
      _keys = Array("sequenceNumber"),
      config,
//      partitionBy = Seq("organization_id", "Overwatch_RunID", "__overwatch_ctrl_noise"),
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("Pipeline_SnapTS"),
      zOrderBy = Array("Overwatch_RunID"),
      withOverwatchRunID = if (config.cloudProvider == "azure") false else true,
      checkpointPath = if (config.cloudProvider == "azure" && !config.auditLogConfig.systemTableName.isDefined)
        config.auditLogConfig.azureAuditLogEventhubConfig.get.auditRawEventsChk
      else None
    )

    lazy private[overwatch] val clusterEventsTarget: PipelineTable = PipelineTable(
      name = "cluster_events_bronze",
      _keys = Array("cluster_id", "type", "timestamp"),
      config,
      _mode = WriteMode.merge,
      mergeScope = MergeScope.insertOnly,
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      incrementalColumns = Array("timestamp"),
      statsColumns = "cluster_id, timestamp, type, Pipeline_SnapTS, Overwatch_RunID".split(", "),
      masterSchema = Some(Schema.clusterEventsMinimumSchema)
    )

    lazy private[overwatch] val clusterEventsErrorsTarget: PipelineTable = PipelineTable(
      name = "cluster_events_errors_bronze",
      _keys = Array("cluster_id", "from_epoch", "until_epoch", "Overwatch_RunID"),
      config,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("Pipeline_SnapTS"))

    lazy private[overwatch] val sparkEventLogsTarget: PipelineTable = PipelineTable(
      name = "spark_events_bronze",
      _keys = Array("Event"), // really aren't any global valid keys for this table
      config,
      incrementalColumns = Array("fileCreateDate", "fileCreateEpochMS"),
      partitionBy = Seq("organization_id", "Event", "fileCreateDate"),
      statsColumns = ("organization_id, Event, clusterId, SparkContextId, JobID, StageID, " +
        "StageAttemptID, TaskType, ExecutorID, fileCreateDate, fileCreateEpochMS, fileCreateTS, filename, " +
        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
      autoOptimize = true, // TODO -- perftest
      masterSchema = Some(Schema.sparkEventsRawMasterSchema)
    )

    lazy private[overwatch] val processedEventLogs: PipelineTable = PipelineTable(
      name = "spark_events_processedFiles",
      _keys = Array("filename"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val cloudMachineDetail: PipelineTable = PipelineTable(
      name = "instanceDetails",
      _keys = Array("API_Name"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.instanceDetailsMinimumSchema)
    )

    lazy private[overwatch] val cloudMachineDetailViewTarget: PipelineView = PipelineView(
      name = "instanceDetails",
      cloudMachineDetail,
      config
    )

    lazy private[overwatch] val dbuCostDetail: PipelineTable = PipelineTable(
      name = "dbuCostDetails",
      _keys = Array("sku"),
      config,
      incrementalColumns = Array("activeFrom"),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.dbuCostDetailsMinimumSchema)
    )

    lazy private[overwatch] val dbuCostDetailViewTarget: PipelineView = PipelineView(
      name = "dbuCostDetails",
      dbuCostDetail,
      config
    )

    lazy private[overwatch] val libsSnapshotTarget: PipelineTable = PipelineTable(
      name = "libs_snapshot_bronze",
      _keys = Array("cluster_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val policiesSnapshotTarget: PipelineTable = PipelineTable(
      name = "policies_snapshot_bronze",
      _keys = Array("policy_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val instanceProfilesSnapshotTarget: PipelineTable = PipelineTable(
      name = "instance_profiles_snapshot_bronze",
      _keys = Array("cluster_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val tokensSnapshotTarget: PipelineTable = PipelineTable(
      name = "tokens_snapshot_bronze",
      _keys = Array("token_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val globalInitScSnapshotTarget: PipelineTable = PipelineTable(
      name = "global_inits_snapshot_bronze",
      _keys = Array("script_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val jobRunsSnapshotTarget: PipelineTable = PipelineTable(
      name = "job_runs_snapshot_bronze",
      _keys = Array("job_id", "run_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val warehousesSnapshotTarget: PipelineTable = PipelineTable(
      name = "warehouses_snapshot_bronze",
      _keys = Array("warehouse_id", "Overwatch_RunID"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.warehouseSnapMinimumSchema)
    )

    lazy private[overwatch] val warehouseDbuDetail: PipelineTable = PipelineTable(
      name = "warehouseDbuDetails",
      _keys = Array("driver_size"),
      config,
      incrementalColumns = Array("Pipeline_SnapTS"),
      partitionBy = Seq("organization_id"),
      masterSchema = Some(Schema.warehouseDbuDetailsMinimumSchema)
    )

    lazy private[overwatch] val warehouseDbuDetailViewTarget: PipelineView = PipelineView(
      name = "warehouseDbuDetails",
      cloudMachineDetail,
      config
    )

  }

  /**
   * Silver Targets
   */

  object SilverTargets {
    // TODO Issue_81
    // TODO -- validate -- need some test data
    //    lazy private[overwatch] val jdbcSessionsTarget: PipelineTable = PipelineTable("jdbc_sessions_silver",
    //      Array("SparkContextID", "sessionId", "ip"), "Pipeline_SnapTS", config)
    //
    //    lazy private[overwatch] val jdbcOperationsTarget: PipelineTable = PipelineTable("jdbc_operations_silver",
    //      Array("SparkContextID", "groupId", "sessionId", "ip"), "Pipeline_SnapTS", config)

    lazy private[overwatch] val executorsTarget: PipelineTable = PipelineTable(
      name = "spark_executors_silver",
      _keys = Array("SparkContextID", "ExecutorID", "addedTimestamp"),
      config,
      _mode = WriteMode.merge,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("addedTimestamp"),
      autoOptimize = true
    )

    lazy private[overwatch] val executionsTarget: PipelineTable = PipelineTable(
      name = "spark_Executions_silver",
      _keys = Array("SparkContextID", "ExecutionID", "startTimestamp"),
      config,
      _mode = WriteMode.merge,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("startTimestamp"),
      autoOptimize = true
    )

    lazy private[overwatch] val jobsTarget: PipelineTable = PipelineTable(
      name = "spark_jobs_silver",
      _keys = Array("SparkContextID", "JobID", "startTimestamp"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Seq("organization_id", "startDate"),
      maxMergeScanDates = 4,
      autoOptimize = true
    )

    lazy private[overwatch] val stagesTarget: PipelineTable = PipelineTable(
      name = "spark_stages_silver",
      _keys = Array("SparkContextID", "StageID", "StageAttemptID", "startTimestamp"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Seq("organization_id", "startDate"),
      maxMergeScanDates = 4,
      autoOptimize = true
    )

    lazy private[overwatch] val tasksTarget: PipelineTable = PipelineTable(
      name = "spark_tasks_silver",
      _keys = Array("SparkContextID", "StageID", "StageAttemptID", "TaskID", "TaskAttempt", "ExecutorID", "Host", "startTimestamp"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("startDate", "startTimestamp"),
      partitionBy = Seq("organization_id", "startDate"),
      maxMergeScanDates = 4,
      autoOptimize = true
    )

    lazy private[overwatch] val dbJobRunsTarget: PipelineTable = PipelineTable(
      name = "jobrun_silver",
      _keys = Array("runId", "startEpochMS"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("startEpochMS"), // don't load into gold until run is terminated
      zOrderBy = Array("runId", "jobId"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      persistBeforeWrite = true,
      excludedReconColumn = Array("requestDetails") //for system tables extra data are coming
    )

    lazy private[overwatch] val accountLoginTarget: PipelineTable = PipelineTable(
      name = "account_login_silver",
      _keys = Array("timestamp", "login_type", "requestId", "sourceIPAddress"),
      config,
      _permitDuplicateKeys = false,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val accountModTarget: PipelineTable = PipelineTable(
      name = "account_mods_silver",
      _keys = Array("requestId"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val clustersSpecTarget: PipelineTable = PipelineTable(
      name = "cluster_spec_silver",
      _keys = Array("timestamp", "cluster_id"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      excludedReconColumn = Array("timestamp")// It will be SnapTS in epoc
    )

    lazy private[overwatch] val clusterStateDetailTarget: PipelineTable = PipelineTable(
      name = "cluster_state_detail_silver",
      _keys = Array("cluster_id", "state", "unixTimeMS_state_start"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("state_start_date", "unixTimeMS_state_start"),
      partitionBy = Seq("organization_id", "state_start_date"),
      maxMergeScanDates = 30, // 1 less than clusterStateFact
    )

    lazy private[overwatch] val poolsSpecTarget: PipelineTable = PipelineTable(
      name = "pools_silver",
      _keys = Array("instance_pool_id", "timestamp"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("timestamp"),
      statsColumns = Array("instance_pool_id", "instance_pool_name", "node_type_id"),
      partitionBy = Seq("organization_id"),
      excludedReconColumn = Array("request_details")
    )

    lazy private[overwatch] val dbJobsStatusTarget: PipelineTable = PipelineTable(
      name = "job_status_silver",
      _keys = Array("timestamp", "jobId", "actionName", "requestId"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      excludedReconColumn = Array("response")
    )

    lazy private[overwatch] val notebookStatusTarget: PipelineTable = PipelineTable(
      name = "notebook_silver",
      _keys = Array("notebookId", "requestId", "actionName", "timestamp"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val sqlQueryHistoryTarget: PipelineTable = PipelineTable(
      name = "sql_query_history_silver",
      _keys = Array("warehouse_id", "query_id", "query_start_time_ms"),
      config,
      _mode = WriteMode.merge,
      _permitDuplicateKeys = false,
      incrementalColumns = Array("query_start_time_ms"),
      partitionBy = Seq("organization_id"),
      excludedReconColumn = Array("Timestamp") //Timestamp is the pipelineSnapTs in epoc
    )

    lazy private[overwatch] val warehousesSpecTarget: PipelineTable = PipelineTable(
      name = "warehouse_spec_silver",
      _keys = Array("timestamp", "warehouse_id"),
      config,
      incrementalColumns = Array("timestamp"),
      partitionBy = Seq("organization_id"),
      excludedReconColumn = Array("Timestamp") //Timestamp is the pipelineSnapTs in epoc
    )

    lazy private[overwatch] val warehousesStateDetailTarget: PipelineTable = PipelineTable(
      name = "warehouse_state_detail_silver",
      _keys = Array("warehouse_id", "state", "unixTimeMS_state_start"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("state_start_date", "unixTimeMS_state_start"),
      partitionBy = Seq("organization_id", "state_start_date"),
      maxMergeScanDates = 30, // 1 less than warehouseStateFact
    )

  }

  object GoldTargets {

    lazy private[overwatch] val clusterTarget: PipelineTable = PipelineTable(
      name = "cluster_gold",
      _keys = Array("cluster_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val clusterViewTarget: PipelineView = PipelineView(
      name = "cluster",
      clusterTarget,
      config
    )

    lazy private[overwatch] val poolsTarget: PipelineTable = PipelineTable(
      name = "instancepool_gold",
      _keys = Array("instance_pool_id", "timestamp"),
      config,
      _mode = WriteMode.merge,
      incrementalColumns = Array("timestamp"),
      statsColumns = Array("instance_pool_id", "instance_pool_name", "node_type_id"),
      partitionBy = Seq("organization_id"),
      excludedReconColumn = Array("request_details")
    )

    lazy private[overwatch] val poolsViewTarget: PipelineView = PipelineView(
      name = "instancepool",
      poolsTarget,
      config
    )

    lazy private[overwatch] val jobTarget: PipelineTable = PipelineTable(
      name = "job_gold",
      _keys = Array("job_id", "unixTimeMS", "action", "request_id"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      excludedReconColumn = Array("response")
    )

    lazy private[overwatch] val jobViewTarget: PipelineView = PipelineView(
      name = "job",
      jobTarget,
      config
    )

    lazy private[overwatch] val jobRunTarget: PipelineTable = PipelineTable(
      name = "jobRun_gold",
      _keys = Array("run_id", "startEpochMS"),
      config,
      _mode = WriteMode.merge,
      zOrderBy = Array("job_id", "run_id"),
      incrementalColumns = Array("startEpochMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise"),
      excludedReconColumn = Array("request_detail")
    )

    lazy private[overwatch] val jobRunsViewTarget: PipelineView = PipelineView(
      name = "jobRun",
      jobRunTarget,
      config
    )

    lazy private[overwatch] val jobRunCostPotentialFactTarget: PipelineTable = PipelineTable(
      name = "jobRunCostPotentialFact_gold",
      _keys = Array("run_id", "startEpochMS"),
      config,
      _mode = WriteMode.merge,
      zOrderBy = Array("job_id", "run_id"),
      incrementalColumns = Array("startEpochMS"),
      partitionBy = Seq("organization_id", "__overwatch_ctrl_noise")
    )

    lazy private[overwatch] val jobRunCostPotentialFactViewTarget: PipelineView = PipelineView(
      name = "jobRunCostPotentialFact",
      jobRunCostPotentialFactTarget,
      config
    )

    lazy private[overwatch] val notebookTarget: PipelineTable = PipelineTable(
      name = "notebook_gold",
      _keys = Array("notebook_id", "request_id", "action", "unixTimeMS"),
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
      _keys = Array("action", "mod_unixTimeMS", "request_id"),
      config,
      incrementalColumns = Array("mod_unixTimeMS"),
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
      _keys = Array("request_id", "login_type", "login_unixTimeMS", "from_ip_address"),
      config,
      _permitDuplicateKeys = false,
      incrementalColumns = Array("login_unixTimeMS"),
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
      _keys = Array("cluster_id", "state", "unixTimeMS_state_start"),
      config,
      _mode = WriteMode.merge,
      partitionBy = Seq("organization_id", "state_start_date", "__overwatch_ctrl_noise"),
      maxMergeScanDates = 31, // 1 greater than clusterStateDetail
      incrementalColumns = Array("state_start_date", "unixTimeMS_state_start"),
      zOrderBy = Array("cluster_id", "unixTimeMS_state_start"),
      excludedReconColumn = Array("driverSpecs","workerSpecs") //driverSpecs and workerSpecs contains PipelineSnapTs and runID
    )

    lazy private[overwatch] val clusterStateFactViewTarget: PipelineView = PipelineView(
      name = "clusterStateFact",
      clusterStateFactTarget,
      config
    )

    lazy private[overwatch] val notebookCommandsTarget: PipelineTable = PipelineTable(
      name = "notebookCommands_gold",
      _keys = Array("notebook_id", "unixTimeMS"),
      config,
      _mode = WriteMode.merge,
      mergeScope = MergeScope.insertOnly,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("unixTimeMS"),
    )

    lazy private[overwatch] val notebookCommandsFactViewTarget: PipelineView = PipelineView(
      name = "notebookCommands",
      notebookCommandsTarget,
      config
    )

    lazy private[overwatch] val sparkJobTarget: PipelineTable = PipelineTable(
      name = "sparkJob_gold",
      _keys = Array("spark_context_id", "job_id", "unixTimeMS"),
      config,
      _mode = WriteMode.merge,
      maxMergeScanDates = 4,
      partitionBy = Seq("organization_id", "date"),
      incrementalColumns = Array("date", "unixTimeMS"),
      autoOptimize = true,
      zOrderBy = Array("cluster_id")
    )

    lazy private[overwatch] val sparkJobViewTarget: PipelineView = PipelineView(
      name = "sparkJob",
      sparkJobTarget,
      config
    )

    lazy private[overwatch] val sparkStageTarget: PipelineTable = PipelineTable(
      name = "sparkStage_gold",
      _keys = Array("spark_context_id", "stage_id", "stage_attempt_id", "unixTimeMS"),
      config,
      _mode = WriteMode.merge,
      partitionBy = Seq("organization_id", "date"),
      maxMergeScanDates = 4,
      incrementalColumns = Array("date", "unixTimeMS"),
      autoOptimize = true,
      zOrderBy = Array("cluster_id")
    )

    lazy private[overwatch] val sparkStageViewTarget: PipelineView = PipelineView(
      name = "sparkStage",
      sparkStageTarget,
      config
    )

    lazy private[overwatch] val sparkTaskTarget: PipelineTable = PipelineTable(
      name = "sparkTask_gold",
      _keys = Array("spark_context_id", "stage_id", "stage_attempt_id", "task_id", "task_attempt_id", "executor_id", "host", "unixTimeMS"),
      config,
      _mode = WriteMode.merge,
      partitionBy = Seq("organization_id", "date"),
      maxMergeScanDates = 4,
      zOrderBy = Array("cluster_id"),
      incrementalColumns = Array("date", "unixTimeMS"),
      autoOptimize = true
    )

    lazy private[overwatch] val sparkTaskViewTarget: PipelineView = PipelineView(
      name = "sparkTask",
      sparkTaskTarget,
      config
    )

    lazy private[overwatch] val sparkExecutionTarget: PipelineTable = PipelineTable(
      name = "sparkExecution_gold",
      _keys = Array("spark_context_id", "execution_id", "date", "unixTimeMS"),
      config,
      _mode = WriteMode.merge,
      maxMergeScanDates = 4,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("date", "unixTimeMS"),
      autoOptimize = true
    )

    lazy private[overwatch] val sparkExecutionViewTarget: PipelineView = PipelineView(
      name = "sparkExecution",
      sparkExecutionTarget,
      config
    )

    lazy private[overwatch] val sparkExecutorTarget: PipelineTable = PipelineTable(
      name = "sparkExecutor_gold",
      _keys = Array("spark_context_id", "executor_id", "date", "unixTimeMS"),
      config,
      _mode = WriteMode.merge,
      maxMergeScanDates = 4,
      partitionBy = Seq("organization_id"),
      incrementalColumns = Array("date", "unixTimeMS")
    )

    lazy private[overwatch] val sparkExecutorViewTarget: PipelineView = PipelineView(
      name = "sparkExecutor",
      sparkExecutorTarget,
      config
    )

    lazy private[overwatch] val sparkStreamTarget: PipelineTable = PipelineTable(
      name = "sparkStream_gold",
      _keys = Array("organization_id", "spark_context_id", "cluster_id", "stream_id", "stream_run_id", "stream_batch_id", "stream_timestamp"),
      config,
      _mode = WriteMode.merge,
      maxMergeScanDates = 30,
      partitionBy = Seq("organization_id", "date"),
      zOrderBy = Array("cluster_id"),
      incrementalColumns = Array("date", "stream_timestamp"),
      autoOptimize = true
    )

    lazy private[overwatch] val sparkStreamViewTarget: PipelineView = PipelineView(
      name = "sparkStream",
      sparkStreamTarget,
      config
    )

    lazy private[overwatch] val sqlQueryHistoryTarget: PipelineTable = PipelineTable(
      name = "sql_query_history_gold",
      _keys = Array("warehouse_id", "query_id", "query_start_time_ms"),
      config,
      _mode = WriteMode.merge,
      _permitDuplicateKeys = false,
      incrementalColumns = Array("query_start_time_ms"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val sqlQueryHistoryViewTarget: PipelineView = PipelineView(
      name = "sqlQueryHistory",
      sqlQueryHistoryTarget,
      config
    )

    lazy private[overwatch] val warehouseTarget: PipelineTable = PipelineTable(
      name = "warehouse_gold",
      _keys = Array("warehouse_id", "unixTimeMS"),
      config,
      incrementalColumns = Array("unixTimeMS"),
      partitionBy = Seq("organization_id")
    )

    lazy private[overwatch] val warehouseViewTarget: PipelineView = PipelineView(
      name = "warehouse",
      warehouseTarget,
      config
    )

    lazy private[overwatch] val warehouseStateFactTarget: PipelineTable = PipelineTable(
      name = "warehouseStateFact_gold",
      _keys = Array("warehouse_id", "state", "unixTimeMS_state_start"),
      config,
      _mode = WriteMode.merge,
      partitionBy = Seq("organization_id", "state_start_date", "__overwatch_ctrl_noise"),
      maxMergeScanDates = 31, // 1 greater than clusterStateDetail
      incrementalColumns = Array("state_start_date", "unixTimeMS_state_start"),
      zOrderBy = Array("warehouse_id", "unixTimeMS_state_start")
    )

    lazy private[overwatch] val warehouseStateFactViewTarget: PipelineView = PipelineView(
      name = "warehouseStateFact",
      warehouseStateFactTarget,
      config
    )

  }


}
