package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.{Module, SparkSessionWrapper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait GoldTransforms extends SparkSessionWrapper {

  import spark.implicits._
  final private val orgId = dbutils.notebook.getContext.tags("orgId")

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
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'instance_pool_id,
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
      struct(
        'existing_cluster_id,
        'new_cluster
      ).alias("cluster"),
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

  protected def buildJobRuns()(df: DataFrame): DataFrame = {
    val jobRunCols: Array[Column] = Array(
      'runId.alias("run_id"),
      'run_name,
      $"JobRunTime.endEpochMS".alias("endEpochMS"),
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
      'workflow_context,
      'taskDetail.alias("task_detail"),
      'requestDetails.alias("request_detail"),
      'timeDetails.alias("time_detail")
    )
    df.select(jobRunCols: _*)
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

  protected def buildClusterStateFact(
                                       instanceDetails: PipelineTable,
                                       clusterSnapshot: PipelineTable,
                                       clusterSpec: PipelineTable
                                     )(clusterEventsDF: DataFrame): DataFrame = {
    val driverNodeDetails = instanceDetails.asDF
      .select('API_Name.alias("driver_node_type_id"), struct(instanceDetails.asDF.columns map col: _*).alias("driverSpecs"))

    val workerNodeDetails = instanceDetails.asDF
      .select('API_Name.alias("node_type_id"), struct(instanceDetails.asDF.columns map col: _*).alias("workerSpecs"))

    val clusterEventsBaseline = clusterEventsDF
      .selectExpr("*", "details.*")
      .drop("details")
      .withColumn("current_num_workers",
        when('type === "CREATING", coalesce($"cluster_size.num_workers", $"cluster_size.autoscale.min_workers"))
          .otherwise('current_num_workers)
      )
      .withColumn("target_num_workers",
        when('type === "CREATING", coalesce($"cluster_size.num_workers", $"cluster_size.autoscale.min_workers"))
          .otherwise('target_num_workers)
      )

    val clusterBeforeW = Window.partitionBy('cluster_id).orderBy('timestamp)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val stateAsOfW = Window.partitionBy('cluster_id).orderBy('timestamp)
      .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    val stateUnboundW = Window.partitionBy('cluster_id).orderBy('timestamp)
    val uptimeW = Window.partitionBy('cluster_id, 'counter_reset).orderBy('timestamp)

    val billableTypes = Array(
      "INIT_SCRIPTS_FINISHED", "INIT_SCRIPTS_STARTED", "STARTING", "TERMINATING", "CREATING", "RESTARTING"
    )

    val nodeTypeLookup = clusterSpec.asDF
      .withColumn("driver_node_type_id",
        when('driver_node_type_id.isNull, last('driver_node_type_id, true).over(clusterBeforeW))
          .otherwise('driver_node_type_id))
      .withColumn("node_type_id",
        when('node_type_id.isNull, last('node_type_id, true).over(clusterBeforeW))
          .otherwise('node_type_id))
      .select('cluster_id, 'cluster_name, 'timestamp, 'driver_node_type_id, 'node_type_id)

    val nodeTypeLookup2 = clusterSnapshot.asDF
      .withColumn("timestamp", coalesce('terminated_time, 'start_time))
      .select('cluster_id, 'cluster_name, 'timestamp, 'driver_node_type_id, 'node_type_id)

    val nodeTypeLookups = Array(nodeTypeLookup, nodeTypeLookup2)

    val clusterPotential = TransformFunctions.fillFromLookupsByTS(
      clusterEventsBaseline, "type",
      Array("driver_node_type_id", "node_type_id", "cluster_name"), stateAsOfW, nodeTypeLookups: _*)
      .select(
        'cluster_id, 'cluster_name,
        'timestamp, 'type, 'current_num_workers, 'target_num_workers,
        'driver_node_type_id, 'node_type_id
      )
      .withColumn("timestamp", 'timestamp.cast("double") / 1000.0)
      .withColumn("ts", from_unixtime('timestamp).cast("timestamp"))
      .withColumn("date", 'ts.cast("date"))
      .withColumn("counter_reset",
        when(lag('type, 1).over(stateUnboundW).isin("TERMINATING", "RESTARTING"), lit(true))
          .otherwise(lit(false)
          ))
      .withColumn("target_num_workers", last('target_num_workers, true).over(stateAsOfW))
      .withColumn("current_num_workers", last('current_num_workers, true).over(stateAsOfW))
      .withColumn("uptime_since_restart_S",
        coalesce(
          when('counter_reset, lit(0))
            .otherwise(sum('timestamp - lag('timestamp, 1).over(stateUnboundW)).over(uptimeW)),
          lit(0)
        )
      )
      .withColumn("uptime_in_state_S", lead('timestamp, 1).over(stateUnboundW) - 'timestamp)
      .withColumn("cloud_billable", lit(true))
      .withColumn("databricks_billable", when('type.isin(billableTypes: _*), lit(false))
        .otherwise(lit(true)
        ))
      .join(driverNodeDetails, Seq("driver_node_type_id"), "left")
      .join(workerNodeDetails, Seq("node_type_id"), "left")
      .withColumn("core_hours",
        round(TransformFunctions.getNodeInfo("driver", "vCPUs", true) / lit(3600), 2) +
          round(TransformFunctions.getNodeInfo("worker", "vCPUs", true) / lit(3600), 2)
      )
      .withColumn("organization_id", lit(orgId))

    val clusterStateFactCols: Array[Column] = Array(
      'organization_id,
      'cluster_id,
      ('timestamp * lit(1000)).alias("unixTimeMS_state_start"),
      from_unixtime(('timestamp * lit(1000)).cast("double") / 1000).cast("timestamp").alias("timestamp_state_start"),
      from_unixtime(('timestamp * lit(1000)).cast("double") / 1000).cast("timestamp").cast("date").alias("date_state_start"),
      ((lead('timestamp, 1).over(stateUnboundW) * lit(1000) - 1)).alias("unixTimeMS_state_end"),
      from_unixtime(((lead('timestamp, 1).over(stateUnboundW) * lit(1000)).cast("double") - 1.0) / 1000).cast("timestamp").alias("timestamp_state_end"),
      from_unixtime(((lead('timestamp, 1).over(stateUnboundW) * lit(1000)).cast("double") - 1.0) / 1000).cast("timestamp").cast("date").alias("date_state_end"),
      'type.alias("state"),
      'current_num_workers,
      'target_num_workers,
      'counter_reset,
      'uptime_since_restart_S,
      'uptime_in_state_S,
      'driver_node_type_id,
      'node_type_id,
      'cloud_billable,
      'databricks_billable,
      'core_hours
    )

    clusterPotential
      .select(clusterStateFactCols: _*)
  }

  protected def buildJobRunCostPotentialFact(
                                              clusterStateFact: DataFrame,
                                              cluster: DataFrame,
                                              instanceDetails: DataFrame
                                            )(newTerminatedJobRuns: DataFrame): DataFrame = {

    val clusterNameLookup = cluster.select('cluster_id, 'cluster_name).distinct
    val isAutomated = 'cluster_name.like("job-%-run-%")

    val driverCosts = instanceDetails
      .select(
        'API_name.alias("driver_node_type_id"),
        'On_Demand_Cost_Hourly.alias("driver_compute_hourly"),
        'Hourly_DBUs.alias("driver_dbu_hourly")
      )
    val workerCosts = instanceDetails
      .select(
        'API_name.alias("node_type_id"),
        'On_Demand_Cost_Hourly.alias("worker_compute_hourly"),
        'Hourly_DBUs.alias("worker_dbu_hourly"),
        'vCPUs.alias("worker_cores")
      )

    val clusterPotentialWCosts = clusterStateFact
      .join(clusterNameLookup, Seq("cluster_id"), "left")
      .withColumn("dbu_rate",
        when(isAutomated, lit(0.23))
          .otherwise(lit(0.55))
      ) // adding this to instanceDetails table -- placeholder for now
      .withColumn("uptime_in_state_H", 'uptime_in_state_S / 60 / 60)
      .join(driverCosts, Seq("driver_node_type_id"))
      .join(workerCosts, Seq("node_type_id"))

    val clusterByIdAsOfW = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val clusterByIdWhenW = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp.desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val keys = Array("cluster_id", "cluster_name", "timestamp")
    val lookupCols = Array("unixTimeMS_state_start", "unixTimeMS_state_end", "timestamp_state_start",
      "timestamp_state_end", "state", "cloud_billable", "databricks_billable", "current_num_workers",
      "dbu_rate", "driver_compute_hourly", "worker_compute_hourly", "driver_dbu_hourly",
      "worker_dbu_hourly", "worker_cores")

    // GET POTENTIAL WITH COSTS
    val clusterPotentialInitialState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_start)
      .select(keys ++ lookupCols map col: _*)

    val jobRunInitialState = TransformFunctions.fillFromLookupsByTS(
      newTerminatedJobRuns.withColumn("timestamp", $"job_runtime.startEpochMS"), "run_id",
      lookupCols, clusterByIdAsOfW, clusterPotentialInitialState
    )
      .drop("timestamp")
      .withColumn("uptime_in_state_H", (array_min(array('unixTimeMS_state_end, $"job_runtime.endEpochMS")) - $"job_runtime.startEpochMS") / lit(1000) / 3600)
      .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))

    val clusterPotentialTerminalState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_end)
      .select(keys ++ lookupCols map col: _*)

    val jobRunTerminalState = TransformFunctions.fillFromLookupsByTS(
      newTerminatedJobRuns.withColumn("timestamp", $"job_runtime.endEpochMS"), "run_id",
      lookupCols, clusterByIdWhenW, clusterPotentialTerminalState
    )
      .filter('unixTimeMS_state_start > $"job_runtime.startEpochMS" && 'unixTimeMS_state_start < $"job_runtime.endEpochMS")
      .drop("timestamp")
      .withColumn("uptime_in_state_H", ($"job_runtime.endEpochMS" - array_max(array('unixTimeMS_state_start, $"job_runtime.startEpochMS"))) / lit(1000) / 3600)
      .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))

    val clusterPotentialIntermediateStates = clusterPotentialWCosts
      .select(Array("cluster_id", "cluster_name") ++ lookupCols map col: _*)

    val jobRunIntermediateStates = newTerminatedJobRuns.alias("jr")
      .join(clusterPotentialIntermediateStates.alias("cpot"),
        $"jr.cluster_id" === $"cpot.cluster_id" &&
          $"cpot.unixTimeMS_state_start" > $"jr.job_runtime.startEpochMS" && // only states beginning after job start and ending before
          $"cpot.unixTimeMS_state_end" < $"jr.job_runtime.endEpochMS"
      )
      .drop($"cpot.cluster_id")
      .withColumn("uptime_in_state_H", ('unixTimeMS_state_end - 'unixTimeMS_state_start) / lit(1000) / 3600)
      .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))

    val jobRunWPotential = jobRunInitialState
      .unionByName(jobRunIntermediateStates)
      .unionByName(jobRunTerminalState)

    // GET UTILIZATION BY KEY
    val sparkJobMini = spark.table("overwatch_dev.sparkjob")
      .select('organization_id, 'spark_context_id, 'job_group_id, 'job_id, explode('stage_ids).alias("stage_id"), 'db_job_id, 'db_id_in_job)
      .filter('job_group_id.like("%job-%-run-%")) // temporary until all job/run ids are imputed in the gold layer
    val sparkTaskMini = spark.table("overwatch_dev.sparktask")
      .select('organization_id, 'spark_context_id, 'stage_id, 'stage_attempt_id, 'task_id, 'task_attempt_id, $"task_runtime.runTimeMS", $"task_runtime.endTS".cast("date").alias("spark_task_termination_date"))

    val jobRunUtilRaw = sparkJobMini
      .join(sparkTaskMini, Seq("organization_id", "spark_context_id", "stage_id"))
      .withColumn("spark_task_runtime_H", 'runtimeMS / lit(1000) / lit(3600))
      .withColumnRenamed("job_id", "spark_job_id")
      .withColumnRenamed("stage_id", "spark_stage_id")
      .withColumnRenamed("task_id", "spark_task_id")

    val jobRunCostPotential = jobRunWPotential
      .withColumn("cluster_type", when('job_cluster_type === "new", lit("automated")).otherwise(lit("interactive")))
      .withColumn("driver_compute_cost", when('cloud_billable, 'driver_compute_hourly * 'uptime_in_state_H).otherwise(lit(0)))
      .withColumn("driver_dbu_cost", when('databricks_billable, 'driver_dbu_hourly * 'uptime_in_state_H * 'dbu_rate).otherwise(lit(0)))
      .withColumn("worker_compute_cost", when('cloud_billable, 'worker_compute_hourly * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))
      .withColumn("worker_dbu_cost", when('databricks_billable, 'worker_dbu_hourly * 'current_num_workers * 'uptime_in_state_H * 'dbu_rate).otherwise(lit(0)))
      .withColumn("total_driver_cost", 'driver_compute_cost + 'driver_dbu_cost)
      .withColumn("total_worker_cost", 'worker_compute_cost + 'worker_dbu_cost)
      .withColumn("total_cost", 'total_driver_cost + 'total_worker_cost)
      .withColumn("job_start_date", $"job_runtime.startTS".cast("date"))
      .groupBy('organization_id, 'job_id, 'id_in_job, 'job_start_date, 'cluster_id, 'cluster_type, 'job_terminal_state.alias("run_terminal_state"), 'job_trigger_type.alias("run_trigger_type"))
      .agg(
        round(sum('worker_potential_core_H), 4).alias("worker_potential_core_H"),
        round(sum('driver_compute_cost), 2).alias("driver_compute_cost"),
        round(sum('driver_dbu_cost), 2).alias("driver_dbu_cost"),
        round(sum('worker_compute_cost), 2).alias("worker_compute_cost"),
        round(sum('worker_dbu_cost), 2).alias("worker_dbu_cost"),
        round(sum('total_driver_cost), 2).alias("total_driver_cost"),
        round(sum('total_worker_cost), 2).alias("total_worker_cost"),
        round(sum('total_cost), 2).alias("total_cost")
      )

    val jobRunSparkUtil = jobRunUtilRaw
      .groupBy('organization_id, 'db_job_id, 'db_id_in_job, 'spark_task_termination_date)
      .agg(
        sum('runTimeMS).alias("spark_task_runtimeMS"),
        round(sum('spark_task_runtime_H), 4).alias("spark_task_runtime_H")
      )
      .drop("spark_task_termination_date")

    jobRunCostPotential.alias("jrCostPot")
      .join(
        jobRunSparkUtil.withColumnRenamed("organization_id", "orgId").alias("jrSparkUtil"),
        $"jrCostPot.organization_id" === $"jrSparkUtil.orgId" &&
          $"jrCostPot.job_id" === $"jrSparkUtil.db_job_id" &&
          $"jrCostPot.id_in_job" === $"jrSparkUtil.db_id_in_job"

      )
      .drop("db_job_id", "db_id_in_job", "orgId")
      .withColumn("job_run_cluster_util", round(('spark_task_runtime_H / 'worker_potential_core_H), 4))


  }

  protected def buildSparkJob(
                               cloudProvider: String
                             )(df: DataFrame): DataFrame = {

    val jobGroupW = Window.partitionBy('SparkContextID, $"PowerProperties.JobGroupID")
    val executionW = Window.partitionBy('SparkContextID, $"PowerProperties.ExecutionID")
    val isolationIDW = Window.partitionBy('SparkContextID, $"PowerProperties.SparkDBIsolationID")
    val oAuthIDW = Window.partitionBy('SparkContextID, $"PowerProperties.AzureOAuth2ClientID")
    val rddScopeW = Window.partitionBy('SparkContextID, $"PowerProperties.RDDScope")
    val replIDW = Window.partitionBy('SparkContextID, $"PowerProperties.SparkDBREPLID")
    val notebookW = Window.partitionBy('SparkContextID, $"PowerProperties.NotebookID")
      .orderBy('startTimestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)


    val cloudSpecificUserImputations = if (cloudProvider == "azure") {
      df.withColumn("user_email", $"PowerProperties.UserEmail")
        .withColumn("user_email",
          when('user_email.isNull && $"PowerProperties.AzureOAuth2ClientID".isNotNull,
            first('user_email, ignoreNulls = true).over(oAuthIDW)).otherwise('user_email))
    } else {
      df.withColumn("user_email", $"PowerProperties.UserEmail")
    }

    val sparkJobsWImputedUser = cloudSpecificUserImputations
      .withColumn("user_email",
        when('user_email.isNull && $"PowerProperties.SparkDBIsolationID".isNotNull,
          first('user_email, ignoreNulls = true).over(isolationIDW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull && $"PowerProperties.SparkDBREPLID".isNotNull,
          first('user_email, ignoreNulls = true).over(replIDW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull && $"PowerProperties.RDDScope".isNotNull,
          first('user_email, ignoreNulls = true).over(rddScopeW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull && $"PowerProperties.JobGroupID".isNotNull,
          first('user_email, ignoreNulls = true).over(jobGroupW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull && $"PowerProperties.ExecutionID".isNotNull,
          first('user_email, ignoreNulls = true).over(executionW)).otherwise('user_email))
      .withColumn("user_email",
        when('user_email.isNull && $"PowerProperties.NotebookID".isNotNull,
          first('user_email, ignoreNulls = true).over(notebookW)).otherwise('user_email))

    val sparkJobCols: Array[Column] = Array(
      'organization_id,
      'SparkContextID.alias("spark_context_id"),
      'JobID.alias("job_id"),
      'JobGroupID.alias("job_group_id"),
      'ExecutionID.alias("execution_id"),
      'StageIDs.alias("stage_ids"),
      $"PowerProperties.ClusterDetails.ClusterID".alias("cluster_id"),
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

    val sparkContextW = Window.partitionBy('spark_context_id)

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
      .withColumn("db_run_id",
        when(isDatabricksJob && 'db_run_id.isNull,
          split(regexp_extract('jobGroupAr, "(-run-\\d+)", 1), "-")(2))
          .otherwise('db_run_id)
      )
  }

  protected def buildSparkStage()(df: DataFrame): DataFrame = {
    val sparkStageCols: Array[Column] = Array(
      lit(orgId).alias("organization_id"),
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
      lit(orgId).alias("organization_id"),
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
      lit(orgId).alias("organization_id"),
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
  protected def buildSparkExecutor()(df: DataFrame): DataFrame = {
    val sparkExecutorCols: Array[Column] = Array(
      lit(orgId).alias("organization_id"),
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
      |organization_id, cluster_id, action, unixTimeMS, timestamp, date, cluster_name, driver_node_type, node_type, num_workers,
      |autoscale, auto_termination_minutes, enable_elastic_disk, is_automated, cluster_type, security_profile, cluster_log_conf,
      |init_scripts, custom_tags, cluster_source, spark_env_vars, spark_conf, acl_path_prefix, instance_pool_id,
      |spark_version, idempotency_token, deleted_by, created_by, last_edited_by
      |""".stripMargin

  protected val jobViewColumnMapping: String =
    """
      |organization_id, job_id, action, unixTimeMS, timestamp, date, job_name, job_type, timeout_seconds, schedule,
      |notebook_path, new_settings, cluster, aclPermissionSet, grants, targetUserId, session_id, request_id, user_agent,
      |response, source_ip_address, created_by, created_ts, deleted_by, deleted_ts, last_edited_by, last_edited_ts
      |""".stripMargin

  protected val jobRunViewColumnMapping: String =
    """
      |organization_id, run_id, run_name, job_runtime, job_id, id_in_job, job_cluster_type, job_task_type,
      |job_terminal_state, job_trigger_type, cluster_id, notebook_params, libraries, workflow_context, task_detail,
      |request_detail, time_detail
      |""".stripMargin

  protected val jobRunCostPotentialFactViewColumnMapping: String =
    """
      |organization_id, job_id, id_in_job, job_start_date, cluster_id, cluster_type, run_terminal_state,
      |run_trigger_type, worker_potential_core_H, driver_compute_cost, driver_dbu_cost, worker_compute_cost,
      |worker_dbu_cost, total_driver_cost, total_worker_cost, total_cost, spark_task_runtimeMS, spark_task_runtime_H,
      |job_run_cluster_util
      |""".stripMargin

  protected val notebookViewColumnMappings: String =
    """
      |organization_id, notebook_id, notebook_name, notebook_path, cluster_id, action, unixTimeMS, timestamp, date, old_name, old_path,
      |new_name, new_path, parent_path, user_email, request_id, response
      |""".stripMargin

  protected val clusterStateFactViewColumnMappings: String =
    """
      |organization_id, cluster_id, unixTimeMS_state_start, timestamp_state_start, date_state_start, unixTimeMS_state_end,
      |timestamp_state_end, date_state_end, state, current_num_workers, target_num_workers, counter_reset,
      |uptime_since_restart_S, uptime_in_state_S, driver_node_type_id, node_type_id, cloud_billable,
      |databricks_billable, core_hours
      |""".stripMargin

  protected val sparkJobViewColumnMapping: String =
    """
      |organization_id, spark_context_id, job_id, job_group_id, execution_id, stage_ids, cluster_id, notebook_id, notebook_path,
      |db_job_id, db_run_id as db_id_in_job, db_job_type, unixTimeMS, timestamp, date, job_runtime, job_result, event_log_start,
      |event_log_end, user_email
      |""".stripMargin

  protected val sparkStageViewColumnMapping: String =
    """
      |organization_id, spark_context_id, stage_id, stage_attempt_id, cluster_id, unixTimeMS, timestamp, date, stage_runtime,
      |stage_info, event_log_start, event_log_end
      |""".stripMargin

  protected val sparkTaskViewColumnMapping: String =
    """
      |organization_id, spark_context_id, task_id, task_attempt_id, stage_id, stage_attempt_id, cluster_id, executor_id, host,
      |unixTimeMS, timestamp, date, task_runtime, task_metrics, task_info, task_type, task_end_reason,
      |event_log_start, event_log_end
      |""".stripMargin

  protected val sparkExecutionViewColumnMapping: String =
    """
      |organization_id, spark_context_id, execution_id, cluster_id, description, details, unixTimeMS, timestamp, date,
      |sql_execution_runtime, event_log_start, event_log_end
      |""".stripMargin

  protected val sparkExecutorViewColumnMapping: String =
    """
      |organization_id, spark_context_id, executor_id, cluster_id, executor_info, removed_reason, executor_alivetime,
      |unixTimeMS, timestamp, date, event_log_start, event_log_end
      |""".stripMargin


}
