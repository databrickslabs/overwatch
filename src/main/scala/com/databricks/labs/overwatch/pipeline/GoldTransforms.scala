package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.WorkflowsTransforms._
import com.databricks.labs.overwatch.utils.{ModuleDisabled, NoNewDataException, SchemaTools, SparkSessionWrapper, TimeTypes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Column, DataFrame}

trait GoldTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

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
      'is_automated,
      'cluster_type,
      'security_profile,
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'cluster_source,
      'aws_attributes,
      'azure_attributes,
      'gcp_attributes,
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'driver_instance_pool_id,
      'instance_pool_id,
      'driver_instance_pool_name,
      'instance_pool_name,
      'spark_version,
      'runtime_engine,
      'idempotency_token,
      'organization_id,
      'deleted_by,
      'createdBy.alias("created_by"),
      'lastEditedBy.alias("last_edited_by")
    )
    df.select(clusterCols: _*)
  }

  protected def buildJobs()(df: DataFrame): DataFrame = {
    val jobCols: Array[Column] = Array(
      'organization_id,
      'workspace_name,
      'jobId.alias("job_id"),
      'actionName.alias("action"),
      'timestamp.alias("unixTimeMS"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'jobName.alias("job_name"),
      'tags,
      'job_type,
      'format.alias("job_format"),
      'existing_cluster_id,
      'new_cluster,
      'tasks,
      'job_clusters,
      'libraries,
      'git_source,
      'timeout_seconds,
      'max_concurrent_runs,
      'max_retries,
      'retry_on_timeout,
      'min_retry_interval_millis,
      'schedule,
      'task_detail_legacy,
      coalesce('is_from_dlt, lit(false)).alias("is_from_dlt"),
//      'access_control_list, TODO -- add back after 503 is resolved -- cannot verifyMinSchema for array<struct>
      'aclPermissionSet,
//      'grants, TODO -- add back after 503 is resolved -- cannot verifyMinSchema for array<struct>
      'targetUserId,
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
      'last_edited_ts
    )
    df.select(jobCols: _*)
  }

  protected def buildJobRuns()(jobRunsLag30D: DataFrame): DataFrame = {
    val jobRunCols: Array[Column] = Array(
      'organization_id,
      'workspace_name,
      'jobId.alias("job_id"),
      'jobName.alias("job_name"),
      'tags,
      'runId.alias("run_id"),
      'jobRunId.alias("job_run_id"),
      'taskRunId.alias("task_run_id"),
      'taskKey.alias("task_key"),
      'clusterId.alias("cluster_id"),
      'cluster_name,
      'multitaskParentRunId.alias("multitask_parent_run_id"),
      'parentRunId.alias("parent_run_id"),
      'task_detail,
      'taskDependencies.alias("task_dependencies"),
      'TaskRunTime.alias("task_runtime"),
      'TaskExecutionRunTime.alias("task_execution_runtime"),
      'job_cluster_key,
      'clusterType.alias("cluster_type"),
      'job_cluster.alias("job_cluster"),
      'new_cluster,
      'taskType.alias("task_type"),
      'terminalState.alias("terminal_state"),
      'jobTriggerType.alias("job_trigger_type"),
      'schedule,
      'libraries,
      'manual_override_params,
      'repairId.alias("repair_id"),
      'repair_details,
      'run_name,
      'timeout_seconds,
      'retry_on_timeout,
      'max_retries,
      'min_retry_interval_millis,
      'max_concurrent_runs,
      'run_as_user_name,
//      'children,
//      'workflow_children,
      'workflow_context,
      'task_detail_legacy,
      'submitRun_details,
      'created_by,
      'last_edited_by,
      'requestDetails.alias("request_detail"),
      'timeDetails.alias("time_detail"),
      'startEpochMS
    )
    jobRunsLag30D
      .select(jobRunCols: _*)
  }

  protected def buildNotebook()(df: DataFrame): DataFrame = {
    val notebookCols: Array[Column] = Array(
      'organization_id,
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
      'response
    )
    df.select(notebookCols: _*)
  }

  protected def buildAccountMod()(df: DataFrame): DataFrame = {
    df.select(
      'organization_id,
      'timestamp.alias("mod_unixTimeMS"),
      'date.alias("mod_date"),
      'actionName.alias("action"),
      'endpoint,
      'modified_by,
      'user_name,
      'user_id,
      'group_name,
      'group_id,
      'sourceIPAddress.alias("from_ip_address"),
      'userAgent.alias("user_agent"),
      'requestId.alias("request_id"),
      'response
    )
  }

  protected def buildPools()(poolsDF: DataFrame): DataFrame = {
    poolsDF.select(
      'serviceName,
      'actionName,
      'organization_id,
      'timestamp,
      'date,
      'instance_pool_id,
      'instance_pool_name,
      'node_type_id,
      'idle_instance_autotermination_minutes,
      'min_idle_instances,
      'max_capacity,
      'preloaded_spark_versions,
      'aws_attributes,
      'azure_attributes,
      'gcp_attributes,
      'custom_tags,
      'create_details,
      'delete_details,
      'request_details
    )
  }

  protected def buildLogin(accountModSilver: DataFrame)(df: DataFrame): DataFrame = {
    val userIdLookup = if (!accountModSilver.isEmpty) {
      accountModSilver.select('organization_id, 'user_name.alias("login_user"), 'user_id)
    } else Seq(("-99", "-99")).toDF("organization_id", "login_user")
    df.join(userIdLookup, Seq("organization_id", "login_user"), "left")
      .select(
        'organization_id,
        'timestamp.alias("login_unixTimeMS"),
        'login_date,
        'login_type,
        'login_user,
        'user_email,
        'ssh_login_details,
        'sourceIPAddress.alias("from_ip_address"),
        'userAgent.alias("user_agent"),
        'requestId.alias("request_id"),
        'response
      )
  }

  protected def buildClusterStateFact(
                                       instanceDetailsTarget: PipelineTable,
                                       dbuCostDetailsTarget: PipelineTable,
                                       clusterSnapshot: PipelineTable,
                                       clusterSpec: PipelineTable,
                                       pipelineSnapTime: TimeTypes
                                     )(clusterStateDetail: DataFrame): DataFrame = {
    val instanceDetailsDF = instanceDetailsTarget.asDF
    val dbuCostDetailsTSDF = dbuCostDetailsTarget.asDF
      .select(
        'organization_id,
        (unix_timestamp('activeFrom) * 1000).alias("timestamp"),
        'sku, 'contract_price.alias("dbu_rate")
      )
      .toTSDF("timestamp", "organization_id", "sku")

    val driverNodeDetails = instanceDetailsDF
      .select(
        'organization_id.alias("driver_orgid"), 'activeFrom, 'activeUntil,
        'API_Name.alias("driver_node_type_id_lookup"),
        struct(instanceDetailsDF.columns map col: _*).alias("driverSpecs")
      )
      .withColumn("activeFromEpochMillis", unix_timestamp('activeFrom) * 1000)
      .withColumn("activeUntilEpochMillis",
        coalesce(unix_timestamp('activeUntil) * 1000, unix_timestamp(pipelineSnapTime.asColumnTS) * 1000)
      )

    val workerNodeDetails = instanceDetailsDF
      .select(
        'organization_id.alias("worker_orgid"), 'activeFrom, 'activeUntil,
        'API_Name.alias("node_type_id_lookup"),
        struct(instanceDetailsDF.columns map col: _*).alias("workerSpecs")
      )
      .withColumn("activeFromEpochMillis", unix_timestamp('activeFrom) * 1000)
      .withColumn("activeUntilEpochMillis",
        coalesce(unix_timestamp('activeUntil) * 1000, unix_timestamp(pipelineSnapTime.asColumnTS) * 1000)
      )

    val derivedSKU = PipelineFunctions.deriveSKU(isAutomated('cluster_name), 'spark_version, 'cluster_type)
    val nodeTypeLookup = clusterSpec.asDF
      .select('organization_id, 'cluster_id, 'cluster_name, 'custom_tags, 'timestamp, 'driver_node_type_id, 'node_type_id, 'spark_version, 'cluster_type, 'runtime_engine)
      .withColumn("sku", derivedSKU)
      .toTSDF("timestamp", "organization_id", "sku")
      .lookupWhen(dbuCostDetailsTSDF)
      .df


    val nodeTypeLookup2 = clusterSnapshot.asDF
      .withColumn("timestamp", coalesce('terminated_time, 'start_time))
      .select('organization_id, 'cluster_id, 'cluster_name, to_json('custom_tags).alias("custom_tags"), 'timestamp, 'driver_node_type_id, 'node_type_id, 'spark_version, lit(null).cast("string").alias("cluster_type"), 'runtime_engine)
      .withColumn("sku", derivedSKU)
      .toTSDF("timestamp", "organization_id", "sku")
      .lookupWhen(dbuCostDetailsTSDF)
      .df
    val clusterPotMetaToFill = Array(
      "cluster_name", "custom_tags", "driver_node_type_id", "runtime_engine",
      "node_type_id", "spark_version", "sku", "dbu_rate", "driverSpecs", "workerSpecs"
    )
    val clusterPotKeys = Seq("organization_id", "cluster_id")
    val clusterPotIncrementals = Seq("state_start_date", "unixTimeMS_state_start")

    val clusterPotential = clusterStateDetail
      .withColumn("timestamp", 'unixTimeMS_state_start) // time control temporary column
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        nodeTypeLookup.toTSDF("timestamp", "organization_id", "cluster_id"),
        maxLookAhead = 1,
        tsPartitionVal = 16
      ).lookupWhen(
      nodeTypeLookup2.toTSDF("timestamp", "organization_id", "cluster_id"),
      maxLookAhead = 1,
      tsPartitionVal = 16
    ).df.drop("timestamp")
      .alias("clusterPotential")
      .join( // estimated node type details at start time of run. If contract changes during state, not handled but very infrequent and negligible impact
        driverNodeDetails.alias("driverNodeDetails"),
        $"clusterPotential.organization_id" === $"driverNodeDetails.driver_orgid" &&
          trim(lower($"clusterPotential.driver_node_type_id")) === trim(lower($"driverNodeDetails.driver_node_type_id_lookup")) &&
          $"clusterPotential.unixTimeMS_state_start"
            .between($"driverNodeDetails.activeFromEpochMillis", $"driverNodeDetails.activeUntilEpochMillis"),
        "left"
      )
      .drop("activeFrom", "activeUntil", "activeFromEpochMillis", "activeUntilEpochMillis", "activeFromDate", "activeUntilDate", "driver_orgid", "driver_node_type_id_lookup")
      .alias("clusterPotential")
      .join( // estimated node type details at start time of run. If contract changes during state, not handled but very infrequent and negligible impact
        workerNodeDetails.alias("workerNodeDetails"),
        $"clusterPotential.organization_id" === $"workerNodeDetails.worker_orgid" &&
          trim(lower($"clusterPotential.node_type_id")) === trim(lower($"workerNodeDetails.node_type_id_lookup")) &&
          $"clusterPotential.unixTimeMS_state_start"
            .between($"workerNodeDetails.activeFromEpochMillis", $"workerNodeDetails.activeUntilEpochMillis"),
        "left")
      .drop("activeFrom", "activeUntil", "activeFromEpochMillis", "activeUntilEpochMillis", "activeFromDate", "activeUntilDate", "worker_orgid", "node_type_id_lookup")
      // filling data is critical here to ensure jrcp dups don't occur
      // using noise generators to fill with two passes to reduce skew
      .fillMeta(clusterPotMetaToFill, clusterPotKeys, clusterPotIncrementals, noiseBuckets = getTotalCores) // scan back then forward to fill all

    val isSingleNode = when(get_json_object('custom_tags, "$.ResourceClass") === "SingleNode", lit(true)).otherwise(lit(false))
    val workerPotentialCoreS = when('databricks_billable && isSingleNode, $"driverSpecs.vCPUs" * 'uptime_in_state_S)
      .when('databricks_billable, $"workerSpecs.vCPUs" * 'current_num_workers * 'uptime_in_state_S)
      .otherwise(lit(0))
    val isPhotonEnabled = upper('runtime_engine).equalTo("PHOTON")
    val isNotAnSQlWarehouse = !upper('sku).equalTo("SQLCOMPUTE")
    val photonDBUMultiplier = when(isPhotonEnabled && isNotAnSQlWarehouse && isAutomated('cluster_name), lit(2.9))
      .when(isPhotonEnabled && isNotAnSQlWarehouse && !isAutomated('cluster_name), lit(2.0))
      .otherwise(lit(1.0))
    val driverDBUs = when('databricks_billable, $"driverSpecs.Hourly_DBUs" * 'uptime_in_state_H * photonDBUMultiplier).otherwise(lit(0)).alias("driver_dbus")
    val workerDBUs = when('databricks_billable && !isSingleNode, $"workerSpecs.Hourly_DBUs" * 'current_num_workers * 'uptime_in_state_H * photonDBUMultiplier).otherwise(lit(0)).alias("worker_dbus")
    val driverCoreHours = round(TransformFunctions.getNodeInfo("driver", "vCPUs", true) / lit(3600), 2)
    val workerCoreHours = round(TransformFunctions.getNodeInfo("worker", "vCPUs", true) / lit(3600), 2)
    val coreHours = when('isRunning && isSingleNode, driverCoreHours)
      .when('isRunning && !isSingleNode, workerCoreHours)
      .otherwise(lit(0.0)).alias("core_hours")
    val driverComputeCost = Costs.compute('cloud_billable, $"driverSpecs.Compute_Contract_Price", lit(1), 'uptime_in_state_H).alias("driver_compute_cost")
    val workerComputeCost = Costs.compute('cloud_billable, $"workerSpecs.Compute_Contract_Price", 'target_num_workers, 'uptime_in_state_H).alias("worker_compute_cost")
    val driverDBUCost = Costs.dbu(driverDBUs, 'dbu_rate).alias("driver_dbu_cost")
    val workerDBUCost = Costs.dbu(workerDBUs, 'dbu_rate).alias("worker_dbu_cost")

    val clusterStateFactCols: Array[Column] = Array(
      'organization_id,
      'cluster_id,
      'cluster_name,
      'custom_tags,
      'state_start_date,
      'unixTimeMS_state_start,
      'unixTimeMS_state_end,
      'timestamp_state_start,
      'timestamp_state_end,
      'state,
      'driver_node_type_id,
      'node_type_id,
      'current_num_workers,
      'target_num_workers,
      'uptime_since_restart_S,
      'uptime_in_state_S,
      'uptime_in_state_H,
      'cloud_billable,
      'databricks_billable,
      isAutomated('cluster_name).alias("isAutomated"),
      'sku,
      'runtime_engine,
      'dbu_rate,
      'state_dates,
      'days_in_state,
      (workerPotentialCoreS / lit(3600)).alias("worker_potential_core_H"),
      driverDBUs,
      workerDBUs,
      (driverDBUs + workerDBUs).alias("total_dbus"),
      coreHours,
      'driverSpecs,
      'workerSpecs,
      driverComputeCost,
      workerComputeCost,
      driverDBUCost,
      workerDBUCost,
      (driverComputeCost + workerComputeCost).alias("total_compute_cost"),
      (driverDBUCost + workerDBUCost).alias("total_DBU_cost"),
      (driverDBUCost + driverComputeCost).alias("total_driver_cost"),
      (workerDBUCost + workerComputeCost).alias("total_worker_cost"),
      (driverComputeCost + driverDBUCost + workerComputeCost + workerDBUCost).alias("total_cost")
    )

    clusterPotential
      .select(clusterStateFactCols: _*)
  }

  protected def buildJobRunCostPotentialFact(
                                              jrcpLag30D: DataFrame,
                                              clsfLag90D: DataFrame,
                                              sparkJobLag2D: DataFrame,
                                              sparkTaskLag2D: DataFrame,
                                              fromTime: TimeTypes,
                                              untilTime: TimeTypes
                                            )(jrGoldLag30D: DataFrame): DataFrame = {

    val clsfLag90IsEmpty = clsfLag90D.isEmpty
    val jrcpLag30IsEmpty = jrcpLag30D.isEmpty
    val clusterPotentialWCosts = if (clsfLag90IsEmpty) {
      val emptyMsg = s"Dependent on clusterStateFact -- Dependent on clusterEventLogs which only has 30d of data " +
        s"available. You're likely not getting source data for 1 of 2 reasons. 1) the Overwatch account doesn't " +
        s"have access to any clusters or 2) the Overwatch Pipeline this module is loading historical data and only 30d " +
        s"of data is accessible for clusterEvents. Progressing module but will not be able to load clusterEvents prior " +
        s"to today - 30d"
      throw new NoNewDataException(emptyMsg, Level.WARN, allowModuleProgression = true)
    } else {
      clsfLag90D
        .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull)
    }

    val newJrLaunches = jrGoldLag30D
      .filter($"task_runtime.startEpochMS" >= fromTime.asUnixTimeMilli)

    val newAndOpenJobRuns = if (!jrcpLag30IsEmpty) {
      jrcpDeriveNewAndOpenRuns(newJrLaunches, jrGoldLag30D ,jrcpLag30D, fromTime)
        .repartition()
        .cache()
    } else newJrLaunches
      .repartition()
      .cache()

    newAndOpenJobRuns.count() // eager cache as this DF is used several times downstream


    // for states (CREATING and STARTING) OR automated cluster runstate start is same as cluster state start
    // (i.e. not discounted to runStateStart)
    val runStateLastToStart = array_max(array('unixTimeMS_state_start, $"task_runtime.startEpochMS"))

    // use the untilTime for jobRun End if the run is still open
    val taskRunEndOrPipelineEnd = coalesce($"task_runtime.endEpochMS", lit(untilTime.asUnixTimeMilli))
    // use the untilTime for clusterState End if cluster state is still open
    val clusterStateEndOrPipelineEnd = coalesce('unixTimeMS_state_end, lit(untilTime.asUnixTimeMilli))
    val runStateFirstToEnd = array_min(array(clusterStateEndOrPipelineEnd, taskRunEndOrPipelineEnd))

    val jobRunByClusterState = jrcpDeriveRunsByClusterState(
      clusterPotentialWCosts,
      newAndOpenJobRuns,
      runStateFirstToEnd,
      runStateLastToStart,
      taskRunEndOrPipelineEnd,
      clusterStateEndOrPipelineEnd
    )

    val cumulativeRunStateRunTimeByRunState = jrcpDeriveCumulativeRuntimeByRunState(
      jobRunByClusterState,
      runStateLastToStart,
      runStateFirstToEnd
    )

    val jobRunCostPotential = jobRunByClusterState
      .join(
        cumulativeRunStateRunTimeByRunState,
        Seq("organization_id", "run_id", "cluster_id", "unixTimeMS_state_start", "unixTimeMS_state_end"),
        "left"
      )
      .transform(jrcpAppendUtilAndCosts)
      .transform(jrcpAggMetricsToRun)

    // GET UTILIZATION BY KEY
    // IF incremental spark events are present calculate utilization, otherwise just return with NULLS
    // Spark events are commonly missing if no clusters are logging and/or in test environments
    if (!sparkJobLag2D.isEmpty && !sparkTaskLag2D.isEmpty) {

      jrcpDeriveSparkJobUtil(sparkJobLag2D, sparkTaskLag2D)
        .transform(jrcpJoinWithJobRunCostPotential(jobRunCostPotential))

    } else {
      jobRunCostPotential
        .withColumn("spark_task_runtimeMS", lit(null).cast("long"))
        .withColumn("spark_task_runtime_H", lit(null).cast("double"))
        .withColumn("job_run_cluster_util", lit(null).cast("double"))
    }

  }

  protected def buildNotebookCommandsFact(
                                   notebook: PipelineTable,
                                   clsfIncrementalDF : DataFrame,
                                 )(auditIncrementalDF: DataFrame): DataFrame = {

    if (auditIncrementalDF.isEmpty || notebook.asDF.isEmpty || clsfIncrementalDF.isEmpty) {
      throw new NoNewDataException("No New Data", Level.WARN, true)
    }

    val auditDF_base = auditIncrementalDF
      .filter(col("serviceName") === "notebook" && col("actionName") === "runCommand")
      .selectExpr("*", "requestParams.*").drop("requestParams")

    if (auditDF_base.columns.contains("executionTime")){

      val notebookLookupTSDF = notebook.asDF
        .select("organization_id", "notebook_id", "notebook_path", "notebook_name", "unixTimeMS", "date")
        .withColumnRenamed("notebook_id", "notebookId")
        .filter('notebookId.isNotNull)
        .distinct
        .toTSDF("unixTimeMS", "organization_id", "date", "notebookId")

      val clsfDF = clsfIncrementalDF
        .select(
          "organization_id", "state_start_date", "unixTimeMS_state_start", "cluster_id",
          "current_num_workers", "uptime_in_state_H", "total_DBU_cost", "unixTimeMS_state_end", "cluster_name", "custom_tags", "node_type_id")
        .distinct
        .withColumnRenamed("cluster_id", "clusterId")
        .withColumnRenamed("current_num_workers", "node_count")

      val colNames: Array[Column] = Array(
        'organization_id,
        'clusterId.alias("cluster_id"),
        'notebookId.alias("notebook_id"),
        'workspace_name,
        'date,
        'timestamp,
        'notebook_path,
        'notebook_name,
        'commandId.alias("command_id"),
        'commandText.alias("command_text"),
        'executionTime.alias("execution_time_s"),
        'sourceIpAddress.alias("source_ip_address"),
        'userIdentity.alias("user_identity"),
        'estimated_dbu_cost,
        'status,
        'cluster_name,
        'custom_tags,
        'node_type_id,
        'node_count,
        'response,
        'userAgent.alias("user_agent"),
        'unixTimeMS
      )

      val auditDF = auditDF_base
        .withColumnRenamed("timestamp", "unixTimeMSStart")
        .withColumn("timestamp", from_unixtime(col("unixTimeMSStart") / 1000.0).cast("timestamp"))
        .withColumn("executionTime", col("executionTime").cast("double"))
        .withColumn("unixTimeMSEnd", (col("unixTimeMSStart") + (col("executionTime") * 1000)).cast("double"))
        .filter('notebookId.isNotNull)
        .drop("cluster_name", "custom_tags", "node_type_id")


      val joinedDF = clsfDF.join(auditDF, Seq("clusterId", "organization_id"), "inner")

      // Cluster_state started before cmd start time and ended before command end time
      val state_before_before = 'unixTimeMS_state_start < 'unixTimeMSStart && 'unixTimeMS_state_end < 'unixTimeMSEnd

      // Cluster_state started on or after command start time and ended on or before command end time
      val state_after_before = 'unixTimeMS_state_start >= 'unixTimeMSStart && 'unixTimeMS_state_end <= 'unixTimeMSEnd
      // cluster_state started after command start time and ended after command end time
      val state_after = 'unixTimeMS_state_start > 'unixTimeMSStart && 'unixTimeMS_state_end > 'unixTimeMSEnd
      // Cluster State Started before cmd start time and ended after command run time
      val state_before_after = 'unixTimeMS_state_start < 'unixTimeMSStart && 'unixTimeMS_state_end > 'unixTimeMSEnd

      val aggregationWindow = Window.partitionBy("commandId", "clusterId", "unixTimeMSStart", "unixTimeMSEnd")
      val orderingWindow = Window.partitionBy("commandId", "clusterId", "unixTimeMSStart", "unixTimeMSEnd").orderBy(desc("uptime_in_state_H"))

      val joinedDF_before_before = joinedDF.filter(state_before_before)
        .withColumn("sum_total_DBU_cost", sum(col("total_DBU_cost")).over(aggregationWindow))
        .withColumn("sum_uptime_in_state_H", sum(col("uptime_in_state_H")).over(aggregationWindow))
        .withColumn("rn", row_number.over(orderingWindow))
        .withColumn("rnk", rank.over(orderingWindow))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

      val joinedDF_after_before = joinedDF.filter(state_after_before)
        .withColumn("sum_total_DBU_cost", sum(col("total_DBU_cost")).over(aggregationWindow))
        .withColumn("sum_uptime_in_state_H", sum(col("uptime_in_state_H")).over(aggregationWindow))
        .withColumn("rn", row_number.over(orderingWindow))
        .withColumn("rnk", rank.over(orderingWindow))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

      val joinedDF_after = joinedDF.filter(state_after)
        .withColumn("sum_total_DBU_cost", sum(col("total_DBU_cost")).over(aggregationWindow))
        .withColumn("sum_uptime_in_state_H", sum(col("uptime_in_state_H")).over(aggregationWindow))
        .withColumn("rn", row_number.over(orderingWindow))
        .withColumn("rnk", rank.over(orderingWindow))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

      val joinedDF_before_after = joinedDF.filter(state_before_after)
        .withColumn("sum_total_DBU_cost", sum(col("total_DBU_cost")).over(aggregationWindow))
        .withColumn("sum_uptime_in_state_H", sum(col("uptime_in_state_H")).over(aggregationWindow))
        .withColumn("rn", row_number.over(orderingWindow))
        .withColumn("rnk", rank.over(orderingWindow))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

      val unionDF = joinedDF_before_before.union(joinedDF_after_before).union(joinedDF_after).union(joinedDF_before_after)
        .withColumn("total_DBU_cost", sum(col("sum_total_DBU_cost")).over(aggregationWindow))
        .withColumn("uptime_in_state_H", sum(col("sum_uptime_in_state_H")).over(aggregationWindow))
        .withColumn("rn", row_number.over(orderingWindow))
        .withColumn("rnk", rank.over(orderingWindow))
        .drop("sum_total_DBU_cost", "sum_uptime_in_state_H")


      val unionDF_final = unionDF.filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
      val df = joinedDF.join(unionDF_final, Seq("unixTimeMSStart", "unixTimeMSEnd", "commandId"), "leftAnti")
      val notebookClsfDF = unionDF_final.union(df.select(unionDF_final.schema.fieldNames.map(col): _*))
      val notebookCodeAndMetaDF = notebookClsfDF.withColumnRenamed("unixTimeMS_state_start", "unixTimeMS")
        .toTSDF("unixTimeMS", "organization_id", "date", "notebookId")
        .lookupWhen(
          notebookLookupTSDF,
          maxLookAhead = 100
        )
        .df
        .withColumn("dbu_cost_ps", col("total_DBU_cost") / col("uptime_in_state_H") / lit(3600))
        .withColumn("estimated_dbu_cost", col("executionTime") * col("dbu_cost_ps"))
        .drop("cluster_id")
      notebookCodeAndMetaDF.select(colNames: _*)
    }else{
      println("Verbose Audit Log not enabled in workspace")
      throw new ModuleDisabled(3019,s"Verbose Audit Logging is not enabled in the workspace. To get the data in notebookCommands_gold it is necessary you " +
        s"enable verbose audit logging in the workspace and also you should include scope notebookCommands")
    }
  }

  private[overwatch] def extractDBJobId(column: Column): Column = {
    split(regexp_extract(column, "(job-\\d+)", 1), "-")(1)
  }

  private[overwatch] def extractDBIdInJob(column: Column): Column = {
    split(regexp_extract(column, "(-run-\\d+)", 1), "-")(2)
  }

  protected def buildSparkJob(
                               cloudProvider: String
                             )(df: DataFrame): DataFrame = {

    val jobGroupW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.JobGroupID")
    val executionW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.ExecutionID")
    val principalObjectIDW = Window.partitionBy('organization_id, $"PowerProperties.principalIdpObjectId")
    val isolationIDW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.SparkDBIsolationID")
    val replIDW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.SparkDBREPLID")
      .orderBy('startTimestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val cloudSpecificUserImputations = if (cloudProvider == "azure") {
      df.withColumn("user_email", $"PowerProperties.UserEmail")
        .withColumn("user_email",
          when('user_email.isNull,
            first('user_email, ignoreNulls = true).over(principalObjectIDW)).otherwise('user_email))
    } else {
      df.withColumn("user_email", $"PowerProperties.UserEmail")
    }

    val sparkJobsWImputedUser = cloudSpecificUserImputations
      .withColumn("user_email",
        when('user_email.isNull,
          first('user_email, ignoreNulls = true).over(isolationIDW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull,
          first('user_email, ignoreNulls = true).over(jobGroupW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull,
          first('user_email, ignoreNulls = true).over(executionW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull,
          last('user_email, ignoreNulls = true).over(replIDW)).otherwise('user_email))

    val sparkJobCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'JobID.alias("job_id"),
      'JobGroupID.alias("job_group_id"),
      'ExecutionID.alias("execution_id"),
      'StageIDs.alias("stage_ids"),
      'clusterId.alias("cluster_id"),
      $"PowerProperties.ClusterDetails.Name".alias("cluster_name"),
      $"PowerProperties.NotebookID".alias("notebook_id"),
      $"PowerProperties.NotebookPath".alias("notebook_path"),
      $"PowerProperties.SparkDBJobID".alias("db_job_id"),
      $"PowerProperties.SparkDBRunID".alias("db_run_id"),
      $"PowerProperties.sparkDBJobType".alias("db_job_type"),
      'startTimestamp.alias("unixTimeMS"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'JobRunTime.alias("job_runtime"),
      'JobResult.alias("job_result"),
      'startFilenameGroup.alias("event_log_start"),
      'endFilenameGroup.alias("event_log_end"),
      'user_email
    )

    val sparkContextW = Window.partitionBy('organization_id, 'spark_context_id)

    val isDatabricksJob = 'job_group_id.like("%job-%-run-%")
    val isAutomatedCluster = 'cluster_name.like("%job-%-run-%")

    sparkJobsWImputedUser
      .select(sparkJobCols: _*)
      .withColumn("cluster_id", first('cluster_id, ignoreNulls = true).over(sparkContextW))
      .withColumn("jobGroupAr", split('job_group_id, "_")(2))
      .withColumn("db_job_id",
        when(isDatabricksJob && 'db_job_id.isNull, extractDBJobId('jobGroupAr))
          .otherwise(
            when(isAutomatedCluster && 'db_job_id.isNull, extractDBJobId('cluster_name))
            .otherwise('db_job_id)
          )
      )
      .withColumn("db_id_in_job",
        when(isDatabricksJob && 'db_run_id.isNull, extractDBIdInJob('jobGroupAr))
          .otherwise(
            when(isAutomatedCluster && 'db_run_id.isNull, extractDBIdInJob('cluster_name))
            .otherwise('db_run_id)
          )
      )
      .drop("cluster_name")
  }

  protected def buildSparkStage()(df: DataFrame): DataFrame = {
    val sparkStageCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'StageID.alias("stage_id"),
      'StageAttemptID.alias("stage_attempt_id"),
      'clusterId.alias("cluster_id"),
      'startTimestamp.alias("unixTimeMS"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'StageRunTime.alias("stage_runtime"),
      'StageInfo.alias("stage_info"),
      'startFilenameGroup.alias("event_log_start"),
      'endFilenameGroup.alias("event_log_end")
    )
    df.select(sparkStageCols: _*)
  }

  protected def buildSparkTask()(df: DataFrame): DataFrame = {
    val sparkTaskCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'TaskID.alias("task_id"),
      'TaskAttempt.alias("task_attempt_id"),
      'StageID.alias("stage_id"),
      'StageAttemptID.alias("stage_attempt_id"),
      'clusterId.alias("cluster_id"),
      'ExecutorID.alias("executor_id"),
      'Host.alias("host"),
      'startTimestamp.alias("unixTimeMS"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'TaskRunTime.alias("task_runtime"),
      'TaskMetrics.alias("task_metrics"),
      'TaskExecutorMetrics.alias("task_executor_metrics"),
      'TaskInfo.alias("task_info"),
      'TaskType.alias("task_type"),
      'TaskEndReason.alias("task_end_reason"),
      'startFilenameGroup.alias("event_log_start"),
      'endFilenameGroup.alias("event_log_end")
    )
    df.select(sparkTaskCols: _*)
  }

  protected def buildSparkExecution()(df: DataFrame): DataFrame = {
    val sparkExecutionCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'ExecutionID.alias("execution_id"),
      'clusterId.alias("cluster_id"),
      'description,
      'details,
      'startTimestamp.alias("unixTimeMS"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('startTimestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'SqlExecutionRunTime.alias("sql_execution_runtime"),
      'startFilenameGroup.alias("event_log_start"),
      'endFilenameGroup.alias("event_log_end")
    )
    df.select(sparkExecutionCols: _*)
  }

  private def cleanseDesc(colName: String): Column = {
    trim(
      regexp_replace(regexp_replace(col(colName), "<br/>", " "),  "\\n", " ")
    )
  }

  private def deriveStreamDetails(streamArrayCol: Column): Column = {
    val idPos = array_position(streamArrayCol, "id") + 1
    val runIdPos = array_position(streamArrayCol, "runId") + 1
    val batchIdPos = when(array_contains(streamArrayCol, "batchId"), array_position(streamArrayCol, "batchId"))
      .when(array_contains(streamArrayCol, "batch"), array_position(streamArrayCol, "batch"))
      .otherwise(lit(0)) + 1
    struct(
      streamArrayCol(idPos).alias("stream_id"),
      streamArrayCol(runIdPos).alias("stream_run_id"),
      streamArrayCol(batchIdPos).alias("stream_batch_id")
    )
  }

  protected def buildSparkStream(
                                  sparkStreamsGoldTarget: PipelineTable,
                                  sparkExecutionsSilverDFLag30d: DataFrame
                                )(sparkEventsDF: DataFrame): DataFrame = {

    val streamTargetKeys = sparkStreamsGoldTarget.keys
    val streamSegment = when('Event.endsWith("StartedEvent"), "Started")
      .when('Event.endsWith("ProgressEvent"), "Progressed")
      .otherwise(lit(null).cast("string"))

    val deriveStreamingDesc = when('description.like("%id =%runId =%batch =%"), cleanseDesc("description")).otherwise(lit(null))

    val streamRawDF = sparkEventsDF
      .filter('Event.like("org.apache.spark.sql.streaming.StreamingQueryListener%"))
      .filter(!'Event.endsWith("TerminatedEvent")) // no valuable data and no timestamp provided

    // when there is no input data break out of module, progress timeline and continue with pipeline
    val emptyMsg = s"No new streaming data found."
    if (streamRawDF.filter('progress.isNotNull).isEmpty) throw new NoNewDataException(emptyMsg, Level.WARN, allowModuleProgression = true)

    val lastStreamValue = Window.partitionBy('organization_id, 'SparkContextId, 'clusterId, 'stream_id, 'stream_run_id).orderBy('stream_timestamp)
    val onlyOnceEventGuaranteeW = Window.partitionBy(streamTargetKeys map col: _*).orderBy('fileCreateEpochMS.desc)

    val streamBaseCols:Array[Column] = Array(
      'organization_id,
      'SparkContextId.alias("spark_context_id"),
      'clusterId.alias("cluster_id"),
      'stream_timestamp,
      PipelineFunctions.epochMilliToTs("stream_timestamp").cast("date").alias("date"),
      streamSegment.alias("streamSegment"),
      'stream_id,
      PipelineFunctions.fillForward("name", lastStreamValue).alias("stream_name"),
      'stream_run_id,
      coalesce($"streaming_metrics.batchId", lit(-1)).alias("stream_batch_id"), // started events don't have batch id but don't want null keys
      'streaming_metrics,
      'fileCreateEpochMS
    )

    val streamingExecCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'clusterId.alias("cluster_id"),
      'ExecutionID.alias("execution_id"),
      $"streamDetails.*"
    )

    val enhancedStreamsRawDF = streamRawDF
      .withColumn("streaming_metrics", SchemaTools.structFromJson(spark, streamRawDF, "progress"))
      .withColumn("stream_timestamp",
        coalesce(PipelineFunctions.tsToEpochMilli("streaming_metrics.timestamp"), 'Timestamp))
      .withColumn("stream_id", coalesce($"streaming_metrics.id", 'id))
      .withColumn("stream_run_id", coalesce($"streaming_metrics.runId", 'runId))
      .select(streamBaseCols: _*)
      // Events are not guaranteed to be only once so duplicates must be filtered out by key for these events
      .withColumn("rnk", rank().over(onlyOnceEventGuaranteeW))
      .withColumn("rn", row_number().over(onlyOnceEventGuaranteeW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn", "fileCreateEpochMS")

    // Convert several complex columns to JSON string since different sources/sinks offer different metrics
    // the nested structs cannot always be merged; thus, schema on read is required
    val statefulCustomMetrics = to_json(col("streaming_metrics.stateOperators"))
    val changeInventory = Map[String, Column](
      "streaming_metrics.stateOperators" ->
        when(statefulCustomMetrics === "[]", lit(null).cast("string"))
          .otherwise(statefulCustomMetrics),
      "streaming_metrics.durationMs" -> to_json(col("streaming_metrics.durationMs")).alias("durationMs"),
      "streaming_metrics.sink" -> to_json(col("streaming_metrics.sink")).alias("sink"),
      "streaming_metrics.sources" -> to_json(col("streaming_metrics.sources")).alias("sources"),
      "streaming_metrics.metrics" -> to_json(col("streaming_metrics.metrics")).alias("metrics"),
      "streaming_metrics.observedMetrics" -> to_json(col("streaming_metrics.observedMetrics")).alias("observedMetrics")
    )

    val streamExecutionsKeys = streamTargetKeys
      .filterNot(_ =="stream_timestamp")
      .filterNot(_ == "date")
    val streamingExecutions = sparkExecutionsSilverDFLag30d.drop("details")
      .withColumn("streamingDesc", deriveStreamingDesc)
      .withColumn("streamDetailsAR", split('streamingDesc, " "))
      .withColumn("streamDetails", deriveStreamDetails('streamDetailsAR))
      .filter($"streamDetails.stream_id".isNotNull)
      .select(streamingExecCols: _*)
      .groupBy(streamExecutionsKeys map col: _*)
      .agg(collect_set('execution_id).alias("execution_ids"))
      .filter(size('execution_ids) <= 100000) // safety valve for bizarre streams

    enhancedStreamsRawDF
      .select(SchemaTools.modifyStruct(enhancedStreamsRawDF.schema, changeInventory): _*)
      .join(streamingExecutions,
        streamExecutionsKeys,
        "left")
      .verifyMinimumSchema(Schema.streamingGoldMinimumSchema)

  }

  protected def buildSparkExecutor()(df: DataFrame): DataFrame = {
    val sparkExecutorCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'ExecutorID.alias("executor_id"),
      'clusterId.alias("cluster_id"),
      'ExecutorInfo.alias("executor_info"),
      'RemovedReason.alias("removed_reason"),
      'ExecutorAliveTime.alias("executor_alivetime"),
      'addedTimestamp.alias("unixTimeMS"),
      from_unixtime('addedTimestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('addedTimestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'startFilenameGroup.alias("event_log_start"),
      'endFilenameGroup.alias("event_log_end")
    )
    df.select(sparkExecutorCols: _*)
  }

  protected def buildWarehouse()(df: DataFrame): DataFrame = {

    val warehouseCols : Array[Column] = Array(
      'warehouse_id,
      'warehouse_name,
      'organization_id,
      'serviceName.alias("service_name"),
      'actionName.alias("action_name"),
      'userEmail.alias("user_email"),
      'cluster_size,
      'min_num_clusters,
      'max_num_clusters,
      'auto_stop_mins,
      'spot_instance_policy,
      'enable_photon,
      'channel,
      'enable_serverless_compute,
      'warehouse_type,
      'warehouse_state,
      'size,
      'auto_resume,
      'creator_id,
      'tags,
      'num_clusters,
      'num_active_sessions,
      'jdbc_url,
      'timestamp.alias("unixTimeMS"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'createdBy.alias("created_by")
    )

    df.select(warehouseCols: _*)
  }

  protected def buildWarehouseStateFact(
                                       instanceDetailsTarget: PipelineTable,
                                       warehouseDbuDetailsTarget: PipelineTable,
                                       warehouseSpec: PipelineTable
                                     )(warehouseStateDetail: DataFrame): DataFrame = {
    val warehouseDbuDetails = warehouseDbuDetailsTarget.asDF()

    val warehouseDbuDetailsColumns = warehouseDbuDetails.columns.map(colName => col(s"warehouseDbuDetails.$colName"))

    val warehouseClustersDetails = warehouseDbuDetails.alias("warehouseDbuDetails")
      .join(instanceDetailsTarget.asDF.alias("instanceDetails"),
        $"warehouseDbuDetails.organization_id" === $"instanceDetails.organization_id" &&
          $"warehouseDbuDetails.driver_size" === $"instanceDetails.API_Name")
      .select(warehouseDbuDetailsColumns :+ col("instanceDetails.vCPUs"): _*)

    val warehousePotMetaToFill = Array(
      "warehouse_name", "channel", "warehouse_type","cluster_size","driver_size"
    )
    val warehousePotKeys = Seq("organization_id", "warehouse_id")
    val warehousePotIncrementals = Seq("state_start_date", "unixTimeMS_state_start")

    val warehousePotential = warehouseStateDetail
      .withColumn("timestamp", 'unixTimeMS_state_start) // time control temporary column
      .toTSDF("timestamp", "organization_id", "warehouse_id")
      .lookupWhen(
        warehouseSpec.asDF.toTSDF("timestamp", "organization_id", "warehouse_id"),
        maxLookAhead = Window.unboundedFollowing,maxLookback = 0L,
        tsPartitionVal = 4
      ).df
      .alias("warehousePotential")
      .join( // estimated node type details at start time of run. If contract changes during state, not handled but very infrequent and negligible impact
        warehouseClustersDetails
          .withColumnRenamed("organization_id","warehouse_organization_id")
          .withColumnRenamed("workspace_name","warehouse_workspace_name")
          .withColumnRenamed("cluster_size","warehouse_cluster_size")
          .withColumnRenamed("Pipeline_SnapTS","warehouse_Pipeline_SnapTS")
          .withColumnRenamed("Overwatch_RunID","warehouse_Overwatch_RunID")
          .alias("warehouseClustersDetails"),
        $"warehousePotential.organization_id" === $"warehouseClustersDetails.warehouse_organization_id" &&
          trim(lower($"warehousePotential.cluster_size")) === trim(lower($"warehouseClustersDetails.warehouse_cluster_size"))
         &&
         $"warehousePotential.unixTimeMS_state_start"
           .between($"warehouseClustersDetails.activeFromEpochMillis", $"warehouseClustersDetails.activeUntilEpochMillis"),
        "left"
      ).drop("warehouse_organization_id","warehouse_cluster_size","warehouse_Pipeline_SnapTS")
      .fillMeta(warehousePotMetaToFill, warehousePotKeys, warehousePotIncrementals, noiseBuckets = getTotalCores)

    val workerPotentialCoreS = when('databricks_billable, 'vCPUs * 'current_num_clusters * 'uptime_in_state_S)
      .otherwise(lit(0))
    val warehouseDBUs = when('databricks_billable, 'total_dbus * 'uptime_in_state_H ).otherwise(lit(0)).alias("driver_dbus")

    val warehouseStateFactCols: Array[Column] = Array(
      'organization_id,
      'warehouse_id,
      'warehouse_name,
      'tags,
      'state_start_date,
      'unixTimeMS_state_start,
      'unixTimeMS_state_end,
      'timestamp_state_start,
      'timestamp_state_end,
      'state,
      'cluster_size,
      'current_num_clusters,
      'target_num_clusters,
      'uptime_since_restart_S,
      'uptime_in_state_S,
      'uptime_in_state_H,
      'cloud_billable,
      'databricks_billable,
      'warehouse_type,
      'state_dates,
      'days_in_state,
      (workerPotentialCoreS / lit(3600)).alias("worker_potential_core_H"),
      warehouseDBUs
    )

    warehousePotential
      .select(warehouseStateFactCols: _*)
  }

  protected val clusterViewColumnMapping: String =
    """
      |organization_id, workspace_name, cluster_id, action, unixTimeMS, timestamp, date, cluster_name, driver_node_type,
      |node_type, num_workers, autoscale, auto_termination_minutes, enable_elastic_disk, is_automated, cluster_type,
      |security_profile, cluster_log_conf, init_scripts, custom_tags, cluster_source, spark_env_vars, spark_conf,
      |acl_path_prefix, aws_attributes, azure_attributes, gcp_attributes, driver_instance_pool_id, instance_pool_id,
      |driver_instance_pool_name, instance_pool_name, spark_version, runtime_engine, idempotency_token,
      |deleted_by, created_by, last_edited_by
      |""".stripMargin

  protected val poolsViewColumnMapping: String =
    """
      |organization_id, workspace_name, instance_pool_id, serviceName, timestamp, date, actionName, instance_pool_name, node_type_id,
      |idle_instance_autotermination_minutes, min_idle_instances, max_capacity, preloaded_spark_versions,
      |azure_attributes, aws_attributes, gcp_attributes,  custom_tags, create_details, delete_details, request_details
      |""".stripMargin

  protected val jobViewColumnMapping: String =
    """
      |organization_id, workspace_name, job_id, action, date, timestamp, job_name, tags, tasks, job_clusters,
      |libraries, timeout_seconds, max_concurrent_runs, max_retries, retry_on_timeout, min_retry_interval_millis,
      |schedule, existing_cluster_id, new_cluster, git_source, task_detail_legacy, is_from_dlt, aclPermissionSet,
      |targetUserId, session_id, request_id, user_agent, response, source_ip_address, created_by, created_ts,
      |deleted_by, deleted_ts, last_edited_by, last_edited_ts
      |""".stripMargin

  protected val jobRunViewColumnMapping: String =
    """
      |organization_id, workspace_name, job_id, job_name, run_id, run_name, multitask_parent_run_id, job_run_id,
      |task_run_id, repair_id, task_key, cluster_type, cluster_id, cluster_name, job_cluster_key, job_cluster,
      |new_cluster, tags, task_detail, task_dependencies, task_runtime, task_execution_runtime, task_type,
      |terminal_state, job_trigger_type, schedule, libraries, manual_override_params, repair_details, timeout_seconds,
      |retry_on_timeout, max_retries, min_retry_interval_millis, max_concurrent_runs, run_as_user_name, parent_run_id,
      |workflow_context, task_detail_legacy, submitRun_details, created_by, last_edited_by, request_detail, time_detail
      |""".stripMargin

  protected val jobRunCostPotentialFactViewColumnMapping: String =
    """
      |organization_id, workspace_name, job_id, job_name, run_id, job_run_id, task_run_id, task_key, repair_id, run_name,
      |startEpochMS, cluster_id, cluster_name, cluster_type, cluster_tags, driver_node_type_id, node_type_id, dbu_rate,
      |multitask_parent_run_id, parent_run_id, task_runtime, task_execution_runtime, terminal_state,
      |job_trigger_type, task_type, created_by, last_edited_by, running_days, avg_cluster_share,
      |avg_overlapping_runs, max_overlapping_runs, run_cluster_states, worker_potential_core_H, driver_compute_cost,
      |driver_dbu_cost, worker_compute_cost, worker_dbu_cost, total_driver_cost, total_worker_cost,
      |total_compute_cost, total_dbu_cost, total_cost, total_dbus, spark_task_runtimeMS, spark_task_runtime_H,
      |job_run_cluster_util
      |""".stripMargin

  protected val notebookCommandsFactViewColumnMapping: String =
    """
      |organization_id, workspace_name, date, timestamp,
      |notebook_id, notebook_path, notebook_name, command_id, command_text, execution_time_s, source_ip_address,
      |user_identity, estimated_dbu_cost, status, cluster_id, cluster_name, custom_tags,
      |node_type_id, node_count, response, user_agent, unixTimeMS
      |""".stripMargin

  protected val notebookViewColumnMappings: String =
    """
      |organization_id, workspace_name, notebook_id, notebook_name, notebook_path, cluster_id, action, unixTimeMS, timestamp, date, old_name, old_path,
      |new_name, new_path, parent_path, user_email, request_id, response
      |""".stripMargin

  protected val accountModViewColumnMappings: String =
    """
      |organization_id, workspace_name, mod_unixTimeMS, mod_date, action, endpoint, modified_by, user_name, user_id,
      |group_name, group_id, from_ip_address, user_agent, request_id, response
      |""".stripMargin

  protected val accountLoginViewColumnMappings: String =
    """
      |organization_id, workspace_name, login_unixTimeMS, login_date, login_type, login_user, user_email, ssh_login_details
      |from_ip_address, user_agent, request_id, response
      |""".stripMargin

  protected val clusterStateFactViewColumnMappings: String =
    """
      |organization_id, workspace_name, cluster_id, cluster_name, custom_tags, state_start_date, unixTimeMS_state_start, unixTimeMS_state_end,
      |timestamp_state_start, timestamp_state_end, state, driver_node_type_id, node_type_id, current_num_workers,
      |target_num_workers, uptime_since_restart_S, uptime_in_state_S, uptime_in_state_H, cloud_billable,
      |databricks_billable, isAutomated, sku, runtime_engine, dbu_rate, state_dates, days_in_state,
      |worker_potential_core_H, core_hours, driver_dbus, worker_dbus, total_dbus, driver_compute_cost,
      |worker_compute_cost, driver_dbu_cost, worker_dbu_cost, total_compute_cost, total_DBU_cost, total_driver_cost,
      |total_worker_cost, total_cost, driverSpecs, workerSpecs
      |""".stripMargin

  protected val sparkJobViewColumnMapping: String =
    """
      |organization_id, workspace_name, spark_context_id, job_id, job_group_id, execution_id, stage_ids, cluster_id, notebook_id, notebook_path,
      |db_job_id, db_id_in_job, db_job_type, unixTimeMS, timestamp, date, job_runtime, job_result, event_log_start,
      |event_log_end, user_email
      |""".stripMargin

  protected val sparkStageViewColumnMapping: String =
    """
      |organization_id, workspace_name, spark_context_id, stage_id, stage_attempt_id, cluster_id, unixTimeMS, timestamp, date, stage_runtime,
      |stage_info, event_log_start, event_log_end
      |""".stripMargin

  protected val sparkTaskViewColumnMapping: String =
    """
      |organization_id, workspace_name, spark_context_id, task_id, task_attempt_id, stage_id, stage_attempt_id, cluster_id, executor_id, host,
      |unixTimeMS, timestamp, date, task_runtime, task_metrics, task_info, task_type, task_end_reason,
      |event_log_start, event_log_end
      |""".stripMargin

  protected val sparkExecutionViewColumnMapping: String =
    """
      |organization_id, workspace_name, spark_context_id, execution_id, cluster_id, description, details, unixTimeMS, timestamp, date,
      |sql_execution_runtime, event_log_start, event_log_end
      |""".stripMargin

  protected val sparkStreamViewColumnMapping: String =
    """
      |organization_id, workspace_name, spark_context_id, cluster_id, stream_id, stream_run_id, stream_batch_id,
      |stream_timestamp, date, streamSegment, stream_name, streaming_metrics, execution_ids
      |""".stripMargin

  protected val sparkExecutorViewColumnMapping: String =
    """
      |organization_id, workspace_name, spark_context_id, executor_id, cluster_id, executor_info, removed_reason, executor_alivetime,
      |unixTimeMS, timestamp, date, event_log_start, event_log_end
      |""".stripMargin

  protected val sqlQueryHistoryViewColumnMapping: String =
    """
      |organization_id,workspace_name,warehouse_id,query_id,query_end_time_ms,user_name,user_id,
      |executed_as_user_id,executed_as_user_name,duration,error_message,execution_end_time_ms,
      |query_start_time_ms,query_text,rows_produced,spark_ui_url,statement_type,status,
      |compilation_time_ms,execution_time_ms,network_sent_bytes,photon_total_time_ms,
      |pruned_bytes,pruned_files_count,read_bytes,read_cache_bytes,read_files_count,read_partitions_count,
      |read_remote_bytes,result_fetch_time_ms,result_from_cache,rows_produced_count,rows_read_count,
      |spill_to_disk_bytes,task_total_time_ms,total_time_ms,write_remote_bytes
      |""".stripMargin

  protected val warehouseViewColumnMapping: String =
    """
      |organization_id,workspace_name,warehouse_id,warehouse_name,service_name,action_name,
      |user_email,cluster_size,min_num_clusters,max_num_clusters,auto_stop_mins,spot_instance_policy,enable_photon,
      |channel,enable_serverless_compute,warehouse_type,warehouse_state,size,auto_resume,creator_id,tags,num_clusters,
      |num_active_sessions,jdbc_url,unixTimeMS,date,created_by
      |""".stripMargin

  protected val warehouseStateFactViewColumnMappings: String =
    """
      |organization_id,workspace_name,warehouse_id,warehouse_name,unixTimeMS_state_start, tags, state_start_date,
      |unixTimeMS_state_end, timestamp_state_start, timestamp_state_end, state, cluster_size, current_num_clusters,
      |target_num_clusters, uptime_since_restart_S, uptime_in_state_S, uptime_in_state_H, cloud_billable,
      |databricks_billable, warehouse_type, state_dates, days_in_state, worker_potential_core_H, driver_dbus
      |""".stripMargin

}
