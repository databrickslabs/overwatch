package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.{NoNewDataException, SchemaTools, SparkSessionWrapper, TimeTypes}
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

trait GoldTransforms extends SparkSessionWrapper {

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
      'is_automated,
      'cluster_type,
      'security_profile,
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'cluster_source,
      'aws_attributes,
      'azure_attributes,
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'driver_instance_pool_id,
      'instance_pool_id,
      'driver_instance_pool_name,
      'instance_pool_name,
      'spark_version,
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
      'cluster_spec.alias("cluster"),
      'aclPermissionSet,
      'grants,
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
      'runId.alias("run_id"),
      'run_name,
      $"JobRunTime.startEpochMS".alias("startEpochMS"),
      'jobRunTime.alias("job_runtime"),
      'jobId.alias("job_id"),
      'idInJob.alias("id_in_job"),
      'jobClusterType.alias("job_cluster_type"),
      'jobTaskType.alias("job_task_type"),
      'jobTerminalState.alias("job_terminal_state"),
      'jobTriggerType.alias("job_trigger_type"),
      'clusterId.alias("cluster_id"),
      'organization_id,
      'notebook_params,
      'libraries,
      'children,
      'workflow_context,
      'taskDetail.alias("task_detail"),
      'requestDetails.alias("request_detail"),
      'timeDetails.alias("time_detail")
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
      'azure_attributes,
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

    val nodeTypeLookup = clusterSpec.asDF
      .select('organization_id, 'cluster_id, 'cluster_name, 'custom_tags, 'timestamp, 'driver_node_type_id, 'node_type_id, 'spark_version)
      .withColumn("sku", PipelineFunctions.deriveSKU(isAutomated('cluster_name), 'spark_version))
      .toTSDF("timestamp", "organization_id", "sku")
      .lookupWhen(dbuCostDetailsTSDF)
      .df


    val nodeTypeLookup2 = clusterSnapshot.asDF
      .withColumn("timestamp", coalesce('terminated_time, 'start_time))
      .select('organization_id, 'cluster_id, 'cluster_name, to_json('custom_tags).alias("custom_tags"), 'timestamp, 'driver_node_type_id, 'node_type_id, 'spark_version)
      .withColumn("sku", PipelineFunctions.deriveSKU(isAutomated('cluster_name), 'spark_version))
      .toTSDF("timestamp", "organization_id", "sku")
      .lookupWhen(dbuCostDetailsTSDF)
      .df

    val clusterPotMetaToFill = Array(
      "cluster_name", "custom_tags", "driver_node_type_id",
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

    val workerPotentialCoreS = when('databricks_billable, $"workerSpecs.vCPUs" * 'current_num_workers * 'uptime_in_state_S).otherwise(lit(0))
    val driverDBUs = when('databricks_billable, $"driverSpecs.Hourly_DBUs" * 'uptime_in_state_H).otherwise(lit(0)).alias("driver_dbus")
    val workerDBUs = when('databricks_billable, $"workerSpecs.Hourly_DBUs" * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)).alias("worker_dbus")
    val driverComputeCost = Costs.compute('cloud_billable, $"driverSpecs.Compute_Contract_Price", lit(1), 'uptime_in_state_H).alias("driver_compute_cost")
    val workerComputeCost = Costs.compute('cloud_billable, $"workerSpecs.Compute_Contract_Price", 'target_num_workers, 'uptime_in_state_H).alias("worker_compute_cost")
    val driverDBUCost = Costs.dbu('databricks_billable, $"driverSpecs.Hourly_DBUs", 'dbu_rate, lit(1), 'uptime_in_state_H).alias("driver_dbu_cost")
    val workerDBUCost = Costs.dbu('databricks_billable, $"workerSpecs.Hourly_DBUs", 'dbu_rate, 'current_num_workers, 'uptime_in_state_H).alias("worker_dbu_cost")

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
      'dbu_rate,
      'state_dates,
      'days_in_state,
      (workerPotentialCoreS / lit(3600)).alias("worker_potential_core_H"),
      driverDBUs,
      workerDBUs,
      (driverDBUs + workerDBUs).alias("total_dbus"),
      when('isRunning,
        round(TransformFunctions.getNodeInfo("driver", "vCPUs", true) / lit(3600), 2) +
          round(TransformFunctions.getNodeInfo("worker", "vCPUs", true) / lit(3600), 2)
      ).otherwise(lit(0.0)).alias("core_hours"),
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

    val clusterPotentialWCosts = if (clsfLag90D.isEmpty) {
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

    // TODO -- review the neaAndOpenJobRuns with updated jobRun logic to ensure all open runs are accounted for
    val newJrLaunches = jrGoldLag30D
      .filter($"job_runtime.startEpochMS" >= fromTime.asUnixTimeMilli)

    val newAndOpenJobRuns = if (!jrcpLag30D.isEmpty) { // jrcp will be empty if is first run for org_id
      // scan jrcp for records where jobRun has started but not completed to stage for the upsert changes
      // limiting scan here to 30 days to reduce load
      // don't reprocess all 30 days of jobRuns each Overwatch run, only the open records
      val openJRCPRecordsRunIDs = jrcpLag30D
        .filter($"job_runtime.endEpochMS".isNull) // open jrcp records (i.e. not incomplete job runs)
        .select('organization_id, 'run_id).distinct // org_id left to force partition pruning
      val outstandingJrRecordsToClose = jrGoldLag30D.join(openJRCPRecordsRunIDs, Seq("organization_id", "run_id"))
      newJrLaunches.unionByName(outstandingJrRecordsToClose) // combine open records (updates) with new records (inserts)
    } else newJrLaunches

    val clsfKeyColNames = Array("organization_id", "cluster_id", "timestamp")
    val clsfKeys: Array[Column] = Array(clsfKeyColNames map col: _*)
    val clsfLookups: Array[Column] = Array(
      'cluster_name, 'custom_tags, 'unixTimeMS_state_start, 'unixTimeMS_state_end, 'timestamp_state_start,
      'timestamp_state_end, 'state, 'cloud_billable, 'databricks_billable, 'uptime_in_state_H, 'current_num_workers, 'target_num_workers,
      $"driverSpecs.API_Name".alias("driver_node_type_id"),
      $"driverSpecs.Compute_Contract_Price".alias("driver_compute_hourly"),
      $"driverSpecs.Hourly_DBUs".alias("driver_dbu_hourly"),
      $"workerSpecs.API_Name".alias("node_type_id"),
      $"workerSpecs.Compute_Contract_Price".alias("worker_compute_hourly"),
      $"workerSpecs.Hourly_DBUs".alias("worker_dbu_hourly"),
      $"workerSpecs.vCPUs".alias("worker_cores"),
      'isAutomated,
      'dbu_rate,
      'worker_potential_core_H,
      'driver_compute_cost,
      'worker_compute_cost,
      'driver_dbu_cost,
      'worker_dbu_cost,
      'total_compute_cost,
      'total_DBU_cost,
      'total_driver_cost,
      'total_worker_cost,
      'total_cost
    )

    // Get cluster potential with costs by state relative to job run
    // Manipulate "timestamp" column to reflect state handle relative to job run
    val clusterPotentialInitialState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_start)
      .select(clsfKeys ++ clsfLookups: _*)

    val clusterPotentialIntermediateStates = clusterPotentialWCosts
      .select((clsfKeyColNames.filterNot(_ == "timestamp") map col) ++ clsfLookups: _*)

    val clusterPotentialTerminalState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_end)
      .select(clsfKeys ++ clsfLookups: _*)

    // Adjust the uptimeInState to smooth the runtimes over the runPeriod across concurrent runs
    val stateLifecycleKeys = Seq("organization_id", "run_id", "cluster_id", "unixTimeMS_state_start")

    // for states (CREATING and STARTING) OR automated cluster runstate start is same as cluster state start
    // (i.e. not discounted to runStateStart)
    val runStateLastToStartStart = array_max(array('unixTimeMS_state_start, $"job_runtime.startEpochMS"))
    val runStateFirstToEnd = array_min(array('unixTimeMS_state_end, $"job_runtime.endEpochMS"))

    val jobRunInitialState = newAndOpenJobRuns //jobRun_gold
      .withColumn("timestamp", $"job_runtime.startEpochMS")
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialInitialState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 4, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull)
      .withColumn("runtime_in_cluster_state",
        when('state.isin("CREATING", "STARTING") || 'job_cluster_type === "new", 'uptime_in_state_H * 1000 * 3600) // get true cluster time when state is guaranteed fully initial
          .otherwise(runStateFirstToEnd - $"job_runtime.startEpochMS")) // otherwise use jobStart as beginning time and min of stateEnd or jobEnd for end time )
      .withColumn("lifecycleState", lit("init"))

    val jobRunTerminalState = newAndOpenJobRuns
      .withColumn("timestamp", coalesce($"job_runtime.endEpochMS", lit(untilTime.asUnixTimeMilli))) // include currently executing runs and calculate costs through module until time
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialTerminalState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 4, maxLookback = 0L, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull && 'unixTimeMS_state_end > $"job_runtime.endEpochMS")
      .join(jobRunInitialState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out beginning states
      .withColumn("runtime_in_cluster_state", $"job_runtime.endEpochMS" - runStateLastToStartStart)
      .withColumn("lifecycleState", lit("terminal"))

    // PERF -- identify top 40 job counts by cluster to be provided to SKEW JOIN hint
    // Some interactive clusters may receive 90%+ of job runs causing massive skew, skew hint resolves
    val topClusters = newAndOpenJobRuns
      .filter('organization_id.isNotNull && 'cluster_id.isNotNull)
      .groupBy('organization_id, 'cluster_id).count
      .orderBy('count.desc).limit(40)
      .select(array('organization_id, 'cluster_id)).as[Seq[String]].collect.toSeq

    val jobRunIntermediateStates = newAndOpenJobRuns.alias("jr")
      .join(clusterPotentialIntermediateStates.alias("cpot").hint("SKEW", Seq("organization_id", "cluster_id"), topClusters),
        $"jr.organization_id" === $"cpot.organization_id" &&
          $"jr.cluster_id" === $"cpot.cluster_id" &&
          $"cpot.unixTimeMS_state_start" > $"jr.job_runtime.startEpochMS" && // only states beginning after job start and ending before
          $"cpot.unixTimeMS_state_end" < $"jr.job_runtime.endEpochMS"
      )
      .drop($"cpot.cluster_id").drop($"cpot.organization_id")
      .join(jobRunInitialState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out beginning states
      .join(jobRunTerminalState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out ending states
      .withColumn("runtime_in_cluster_state", 'unixTimeMS_state_end - 'unixTimeMS_state_start)
      .withColumn("lifecycleState", lit("intermediate"))


    val jobRunByClusterState = jobRunInitialState
      .unionByName(jobRunIntermediateStates)
      .unionByName(jobRunTerminalState)

    // Derive runStateConcurrency to derive runState fair share or utilization
    // runStateUtilization = runtimeInRunState / sum(overlappingRuntimesInState)

    val runstateKeys = $"obs.organization_id" === $"lookup.organization_id" &&
      $"obs.cluster_id" === $"lookup.cluster_id" &&
      $"obs.unixTimeMS_state_start" === $"lookup.unixTimeMS_state_start" &&
      $"obs.unixTimeMS_state_end" === $"lookup.unixTimeMS_state_end"

    val startsBefore = $"lookup.run_state_start_epochMS" < $"obs.run_state_start_epochMS"
    val startsDuring = $"lookup.run_state_start_epochMS" > $"obs.run_state_start_epochMS" && $"lookup.run_state_start_epochMS" < $"obs.run_state_end_epochMS" // exclusive
    val endsDuring = $"lookup.run_state_end_epochMS" > $"obs.run_state_start_epochMS" && $"lookup.run_state_end_epochMS" < $"obs.run_state_end_epochMS" // exclusive
    val endsAfter = $"lookup.run_state_end_epochMS" > $"obs.run_state_end_epochMS"
    val startsEndsWithin = $"lookup.run_state_start_epochMS".between($"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS") &&
      $"lookup.run_state_end_epochMS".between($"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS") // inclusive

    val simplifiedJobRunByClusterState = jobRunByClusterState
      .filter('job_cluster_type === "existing") // only relevant for interactive clusters
      .withColumn("run_state_start_epochMS", runStateLastToStartStart)
      .withColumn("run_state_end_epochMS", runStateFirstToEnd)
      .select(
        'organization_id, 'run_id, 'cluster_id, 'run_state_start_epochMS, 'run_state_end_epochMS, 'unixTimeMS_state_start, 'unixTimeMS_state_end
      )

    // sum of run_state_times starting before ending during
    val runStateBeforeEndsDuring = simplifiedJobRunByClusterState.alias("obs")
      .join(simplifiedJobRunByClusterState.alias("lookup"), runstateKeys && startsBefore && endsDuring)
      .withColumn("relative_runtime_in_runstate", $"lookup.run_state_end_epochMS" - $"obs.unixTimeMS_state_start") // runStateEnd minus clusterStateStart
      .select(
        $"obs.organization_id", $"obs.run_id", $"obs.cluster_id", $"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS", $"obs.unixTimeMS_state_start", $"obs.unixTimeMS_state_end", 'relative_runtime_in_runstate
      )

    // sum of run_state_times starting during ending after
    val runStateAfterBeginsDuring = simplifiedJobRunByClusterState.alias("obs")
      .join(simplifiedJobRunByClusterState.alias("lookup"), runstateKeys && startsDuring && endsAfter)
      .withColumn("relative_runtime_in_runstate", $"lookup.unixTimeMS_state_end" - $"obs.run_state_start_epochMS") // clusterStateEnd minus runStateStart
      .select(
        $"obs.organization_id", $"obs.run_id", $"obs.cluster_id", $"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS", $"obs.unixTimeMS_state_start", $"obs.unixTimeMS_state_end", 'relative_runtime_in_runstate
      )

    // sum of run_state_times starting and ending during
    val runStateBeginEndDuring = simplifiedJobRunByClusterState.alias("obs")
      .join(simplifiedJobRunByClusterState.alias("lookup"), runstateKeys && startsEndsWithin)
      .withColumn("relative_runtime_in_runstate", $"lookup.run_state_end_epochMS" - $"obs.run_state_start_epochMS") // runStateEnd minus runStateStart
      .select(
        $"obs.organization_id", $"obs.run_id", $"obs.cluster_id", $"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS", $"obs.unixTimeMS_state_start", $"obs.unixTimeMS_state_end", 'relative_runtime_in_runstate
      )

    val cumulativeRunStateRunTimeByRunState = runStateBeforeEndsDuring
      .unionByName(runStateAfterBeginsDuring)
      .unionByName(runStateBeginEndDuring)
      .groupBy('organization_id, 'run_id, 'cluster_id, 'unixTimeMS_state_start, 'unixTimeMS_state_end) // runstate
      .agg(
        sum('relative_runtime_in_runstate).alias("cum_runtime_in_cluster_state"), // runtime in clusterState
        (sum(lit(1)) - lit(1)).alias("overlapping_run_states") // subtract one for self run
      )
      .repartition()
      .cache

    val runStateWithUtilizationAndCosts = jobRunByClusterState
      .join(cumulativeRunStateRunTimeByRunState, Seq("organization_id", "run_id", "cluster_id", "unixTimeMS_state_start", "unixTimeMS_state_end"), "left")
      .withColumn("cluster_type", when('job_cluster_type === "new", lit("automated")).otherwise(lit("interactive")))
      .withColumn("state_utilization_percent", 'runtime_in_cluster_state / 1000 / 3600 / 'uptime_in_state_H) // run runtime as percent of total state time
      .withColumn("run_state_utilization",
        when('cluster_type === "interactive", least('runtime_in_cluster_state / 'cum_runtime_in_cluster_state, lit(1.0)))
          .otherwise(lit(1.0))
      ) // determine share of cluster when interactive as runtime / all overlapping run runtimes
      .withColumn("overlapping_run_states", when('cluster_type === "interactive", 'overlapping_run_states).otherwise(lit(0)))
      //        .withColumn("overlapping_run_states", when('cluster_type === "automated", lit(0)).otherwise('overlapping_run_states)) // removed 4.2
      .withColumn("running_days", sequence($"job_runtime.startTS".cast("date"), $"job_runtime.endTS".cast("date")))
      .withColumn("driver_compute_cost", 'driver_compute_cost * 'state_utilization_percent * 'run_state_utilization)
      .withColumn("driver_dbu_cost", 'driver_dbu_cost * 'state_utilization_percent * 'run_state_utilization)
      .withColumn("worker_compute_cost", 'worker_compute_cost * 'state_utilization_percent * 'run_state_utilization)
      .withColumn("worker_dbu_cost", 'worker_dbu_cost * 'state_utilization_percent * 'run_state_utilization)
      .withColumn("total_driver_cost", 'driver_compute_cost + 'driver_dbu_cost)
      .withColumn("total_worker_cost", 'worker_compute_cost + 'worker_dbu_cost)
      .withColumn("total_compute_cost", 'driver_compute_cost + 'worker_compute_cost)
      .withColumn("total_dbu_cost", 'driver_dbu_cost + 'worker_dbu_cost)
      .withColumn("total_cost", 'total_driver_cost + 'total_worker_cost)

    val jobRunCostPotential = runStateWithUtilizationAndCosts
      .groupBy(
        'organization_id,
        'run_id,
        'job_id,
        'id_in_job,
        'startEpochMS,
        'job_runtime,
        'job_terminal_state.alias("run_terminal_state"),
        'job_trigger_type.alias("run_trigger_type"),
        'job_task_type.alias("run_task_type"),
        'cluster_id,
        'cluster_name,
        'cluster_type,
        'custom_tags,
        'driver_node_type_id,
        'node_type_id,
        'dbu_rate
      )
      .agg(
        first('running_days).alias("running_days"),
        greatest(round(avg('run_state_utilization), 4), lit(0.0)).alias("avg_cluster_share"),
        greatest(round(avg('overlapping_run_states), 2), lit(0.0)).alias("avg_overlapping_runs"),
        greatest(max('overlapping_run_states), lit(0.0)).alias("max_overlapping_runs"),
        sum(lit(1)).alias("run_cluster_states"),
        greatest(round(sum('worker_potential_core_H), 6), lit(0)).alias("worker_potential_core_H"),
        greatest(round(sum('driver_compute_cost), 6), lit(0)).alias("driver_compute_cost"),
        greatest(round(sum('driver_dbu_cost), 6), lit(0)).alias("driver_dbu_cost"),
        greatest(round(sum('worker_compute_cost), 6), lit(0)).alias("worker_compute_cost"),
        greatest(round(sum('worker_dbu_cost), 6), lit(0)).alias("worker_dbu_cost"),
        greatest(round(sum('total_driver_cost), 6), lit(0)).alias("total_driver_cost"),
        greatest(round(sum('total_worker_cost), 6), lit(0)).alias("total_worker_cost"),
        greatest(round(sum('total_compute_cost), 6), lit(0)).alias("total_compute_cost"),
        greatest(round(sum('total_dbu_cost), 6), lit(0)).alias("total_dbu_cost"),
        greatest(round(sum('total_cost), 6), lit(0)).alias("total_cost")
      )

    // GET UTILIZATION BY KEY
    // IF incremental spark events are present calculate utilization, otherwise just return with NULLS
    // Spark events are commonly missing if no clusters are logging and/or in test environments
    if (!sparkJobLag2D.isEmpty && !sparkTaskLag2D.isEmpty) {
      val sparkJobMini = sparkJobLag2D
        .select('organization_id, 'date, 'spark_context_id, 'job_group_id,
          'job_id, explode('stage_ids).alias("stage_id"), 'db_job_id, 'db_id_in_job)
        .filter('db_job_id.isNotNull && 'db_id_in_job.isNotNull)

      val sparkTaskMini = sparkTaskLag2D
        .select('organization_id, 'date, 'spark_context_id, 'stage_id,
          'stage_attempt_id, 'task_id, 'task_attempt_id,
          $"task_runtime.runTimeMS", $"task_runtime.endTS".cast("date").alias("spark_task_termination_date"))

      val jobRunUtilRaw = sparkJobMini.alias("sparkJobMini")
        .joinWithLag(
          sparkTaskMini,
          Seq("organization_id", "date", "spark_context_id", "stage_id"),
          "date"
        )
        .withColumn("spark_task_runtime_H", 'runtimeMS / lit(1000) / lit(3600))
        .withColumnRenamed("job_id", "spark_job_id")
        .withColumnRenamed("stage_id", "spark_stage_id")
        .withColumnRenamed("task_id", "spark_task_id")

      val jobRunSparkUtil = jobRunUtilRaw
        .groupBy('organization_id, 'db_job_id, 'db_id_in_job)
        .agg(
          sum('runTimeMS).alias("spark_task_runtimeMS"),
          round(sum('spark_task_runtime_H), 4).alias("spark_task_runtime_H")
        )

      jobRunCostPotential.alias("jrCostPot")
        .join(
          jobRunSparkUtil.withColumnRenamed("organization_id", "orgId").alias("jrSparkUtil"),
          $"jrCostPot.organization_id" === $"jrSparkUtil.orgId" &&
            $"jrCostPot.job_id" === $"jrSparkUtil.db_job_id" &&
            $"jrCostPot.id_in_job" === $"jrSparkUtil.db_id_in_job",
          "left"
        )
        .drop("db_job_id", "db_id_in_job", "orgId")
        .withColumn("job_run_cluster_util", round(('spark_task_runtime_H / 'worker_potential_core_H), 4))
    } else {
      jobRunCostPotential
        .withColumn("spark_task_runtimeMS", lit(null).cast("long"))
        .withColumn("spark_task_runtime_H", lit(null).cast("double"))
        .withColumn("job_run_cluster_util", lit(null).cast("double"))
    }


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
    val notebookW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.NotebookID")
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
      .withColumn("user_email",
        when('user_email.isNull,
          last('user_email, ignoreNulls = true).over(notebookW)).otherwise('user_email))

    val sparkJobCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'JobID.alias("job_id"),
      'JobGroupID.alias("job_group_id"),
      'ExecutionID.alias("execution_id"),
      'StageIDs.alias("stage_ids"),
      'clusterId.alias("cluster_id"),
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

    sparkJobsWImputedUser
      .select(sparkJobCols: _*)
      .withColumn("cluster_id", first('cluster_id, ignoreNulls = true).over(sparkContextW))
      .withColumn("jobGroupAr", split('job_group_id, "_")(2))
      .withColumn("db_job_id",
        when(isDatabricksJob && 'db_job_id.isNull,
          split(regexp_extract('jobGroupAr, "(job-\\d+)", 1), "-")(1))
          .otherwise('db_job_id)
      )
      .withColumn("db_id_in_job",
        when(isDatabricksJob && 'db_run_id.isNull,
          split(regexp_extract('jobGroupAr, "(-run-\\d+)", 1), "-")(2))
          .otherwise('db_run_id)
      )
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
    if (streamRawDF.isEmpty) throw new NoNewDataException(emptyMsg, Level.WARN, allowModuleProgression = true)

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
      .agg(collect_list('execution_id).alias("execution_ids"))

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

  protected val clusterViewColumnMapping: String =
    """
      |organization_id, workspace_name, cluster_id, action, unixTimeMS, timestamp, date, cluster_name, driver_node_type, node_type, num_workers,
      |autoscale, auto_termination_minutes, enable_elastic_disk, is_automated, cluster_type, security_profile, cluster_log_conf,
      |init_scripts, custom_tags, cluster_source, spark_env_vars, spark_conf, acl_path_prefix, aws_attributes, azure_attributes,
      |driver_instance_pool_id, instance_pool_id, driver_instance_pool_name, instance_pool_name,
      |spark_version, idempotency_token, deleted_by, created_by, last_edited_by
      |""".stripMargin

  protected val poolsViewColumnMapping: String =
    """
      |organization_id, workspace_name, instance_pool_id, serviceName, timestamp, date, actionName, instance_pool_name, node_type_id,
      |idle_instance_autotermination_minutes, min_idle_instances, max_capacity, preloaded_spark_versions,
      |azure_attributes, create_details, delete_details, request_details
      |""".stripMargin

  protected val jobViewColumnMapping: String =
    """
      |organization_id, workspace_name, job_id, action, unixTimeMS, timestamp, date, job_name, job_type, timeout_seconds, schedule,
      |notebook_path, new_settings, cluster, aclPermissionSet, grants, targetUserId, session_id, request_id, user_agent,
      |response, source_ip_address, created_by, created_ts, deleted_by, deleted_ts, last_edited_by, last_edited_ts
      |""".stripMargin

  protected val jobRunViewColumnMapping: String =
    """
      |organization_id, workspace_name, run_id, run_name, job_runtime, job_id, id_in_job, job_cluster_type, job_task_type,
      |job_terminal_state, job_trigger_type, cluster_id, notebook_params, libraries, children, workflow_context,
      |task_detail, request_detail, time_detail
      |""".stripMargin

  protected val jobRunCostPotentialFactViewColumnMapping: String =
    """
      |organization_id, workspace_name, run_id, job_id, id_in_job, job_runtime, run_terminal_state, run_trigger_type, run_task_type, cluster_id,
      |cluster_name, cluster_type, custom_tags, driver_node_type_id, node_type_id, dbu_rate, running_days,
      |run_cluster_states, avg_cluster_share, avg_overlapping_runs, max_overlapping_runs, worker_potential_core_H,
      |driver_compute_cost, driver_dbu_cost, worker_compute_cost, worker_dbu_cost, total_driver_cost, total_worker_cost,
      |total_compute_cost, total_dbu_cost, total_cost, spark_task_runtimeMS, spark_task_runtime_H, job_run_cluster_util
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
      |databricks_billable, isAutomated, dbu_rate, state_dates, days_in_state, worker_potential_core_H, core_hours,
      |driver_compute_cost, worker_compute_cost, driver_dbu_cost, worker_dbu_cost, total_compute_cost, total_DBU_cost,
      |total_driver_cost, total_worker_cost, total_cost, driverSpecs, workerSpecs
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

}
