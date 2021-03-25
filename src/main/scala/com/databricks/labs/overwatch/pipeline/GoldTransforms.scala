package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import TransformFunctions._

trait GoldTransforms extends SparkSessionWrapper {

  import spark.implicits._
  //  final private val orgId = dbutils.notebook.getContext.tags("orgId")

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
      'children,
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
        'user_id,
        'user_email,
        'ssh_login_details,
        'sourceIPAddress.alias("from_ip_address"),
        'userAgent.alias("user_agent"),
        'requestId.alias("request_id"),
        'response
      )
  }

  protected def buildClusterStateFact(
                                       instanceDetails: PipelineTable,
                                       clusterSnapshot: PipelineTable,
                                       clusterSpec: PipelineTable,
                                       interactiveDBUPrice: Double,
                                       automatedDBUPrice: Double
                                     )(clusterEventsDF: DataFrame): DataFrame = {
    val driverNodeDetails = instanceDetails.asDF
      .select('organization_id, 'API_Name.alias("driver_node_type_id"), struct(instanceDetails.asDF.columns map col: _*).alias("driverSpecs"))

    val workerNodeDetails = instanceDetails.asDF
      .select('organization_id, 'API_Name.alias("node_type_id"), struct(instanceDetails.asDF.columns map col: _*).alias("workerSpecs"))

    val stateUnboundW = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp)
    val uptimeW = Window.partitionBy('organization_id, 'cluster_id, 'reset_partition).orderBy('unixTimeMS_state_start)

    val clusterEventsBaseline = clusterEventsDF
      .selectExpr("*", "details.*")
      .drop("details")
      .withColumn("isRunning", when('type === "TERMINATING", lit(false)).otherwise(lit(true)))
      .withColumn("isRunning",
        when('type === "TERMINATING" || ('type =!= "STARTING" && lag(!'isRunning, 1).over(stateUnboundW)), lit(false))
          .otherwise(lit(true))
      )
      .withColumn("current_num_workers",
        when('type === "CREATING", coalesce($"cluster_size.num_workers", $"cluster_size.autoscale.min_workers"))
          .otherwise('current_num_workers)
      )
      .withColumn("current_num_workers",
        when( // bug that occasionally results in negative workers when nodes are lost
          'current_num_workers < 0, last('target_num_workers, true).over(stateUnboundW)
        ).otherwise('current_num_workers))
      .withColumn("target_num_workers",
        when('type === "CREATING", coalesce($"cluster_size.num_workers", $"cluster_size.autoscale.min_workers"))
          .otherwise('target_num_workers)
      )
      .select(
        'organization_id, 'cluster_id, 'isRunning,
        'timestamp, 'type, 'current_num_workers, 'target_num_workers
      )

    val nonBillableTypes = Array(
      "STARTING", "TERMINATING", "CREATING", "RESTARTING"
    )

    val nodeTypeLookup = clusterSpec.asDF
      .select('organization_id, 'cluster_id, 'cluster_name, 'custom_tags, 'timestamp, 'driver_node_type_id, 'node_type_id)

    val nodeTypeLookup2 = clusterSnapshot.asDF
      .withColumn("timestamp", coalesce('terminated_time, 'start_time))
      .select('organization_id, 'cluster_id, 'cluster_name, to_json('custom_tags).alias("custom_tags"), 'timestamp, 'driver_node_type_id, 'node_type_id)

    val clusterPotential = clusterEventsBaseline
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        nodeTypeLookup.toTSDF("timestamp", "organization_id", "cluster_id"),
        maxLookAhead = 1,
        tsPartitionVal = 64
      ).lookupWhen(
      nodeTypeLookup2.toTSDF("timestamp", "organization_id", "cluster_id"),
      maxLookAhead = 1,
      tsPartitionVal = 64
    ).df
      .withColumn("ts", from_unixtime('timestamp.cast("double") / lit(1000)).cast("timestamp"))
      .withColumn("date", 'ts.cast("date"))
      .withColumn("counter_reset",
        when(
          lag('type, 1).over(stateUnboundW).isin("TERMINATING", "RESTARTING", "EDITED") ||
            !'isRunning, lit(1)
        ).otherwise(lit(0))
      )
      .withColumn("reset_partition", sum('counter_reset).over(stateUnboundW))
      .withColumn("target_num_workers", last('target_num_workers, true).over(stateUnboundW))
      .withColumn("current_num_workers", last('current_num_workers, true).over(stateUnboundW))
      .withColumn("unixTimeMS_state_start", 'timestamp)
      .withColumn("unixTimeMS_state_end", (lead('timestamp, 1).over(stateUnboundW) - lit(1))) // subtract 1 millis
      .withColumn("timestamp_state_start", from_unixtime('unixTimeMS_state_start.cast("double") / lit(1000)).cast("timestamp"))
      .withColumn("timestamp_state_end", from_unixtime('unixTimeMS_state_end.cast("double") / lit(1000)).cast("timestamp")) // subtract 1.0 millis
      .withColumn("uptime_in_state_S", ('unixTimeMS_state_end - 'unixTimeMS_state_start) / lit(1000))
      .withColumn("uptime_since_restart_S",
        coalesce(
          when('counter_reset === 1, lit(0))
            .otherwise(sum('uptime_in_state_S).over(uptimeW)),
          lit(0)
        )
      )
      .withColumn("cloud_billable", 'isRunning)
      .withColumn("databricks_billable", 'isRunning && !'type.isin(nonBillableTypes: _*))
      .join(driverNodeDetails, Seq("organization_id", "driver_node_type_id"), "left")
      .join(workerNodeDetails, Seq("organization_id", "node_type_id"), "left")
      .withColumn("worker_potential_core_S", when('databricks_billable, $"workerSpecs.vCPUs" * 'current_num_workers * 'uptime_in_state_S).otherwise(lit(0)))
      .withColumn("core_hours", when('isRunning,
        round(TransformFunctions.getNodeInfo("driver", "vCPUs", true) / lit(3600), 2) +
          round(TransformFunctions.getNodeInfo("worker", "vCPUs", true) / lit(3600), 2)
      ))
      .withColumn("uptime_in_state_H", 'uptime_in_state_S / lit(3600))
      .withColumn("isAutomated", isAutomated('cluster_name))
      .withColumn("dbu_rate", when('isAutomated, lit(automatedDBUPrice)).otherwise(lit(interactiveDBUPrice)))
      .withColumn("days_in_state", size(sequence('timestamp_state_start.cast("date"), 'timestamp_state_end.cast("date"))))
      .withColumn("worker_potential_core_H", 'worker_potential_core_S / lit(3600))
      .withColumn("driver_compute_cost", Costs.compute('cloud_billable, $"driverSpecs.Compute_Contract_Price", lit(1), 'uptime_in_state_H))
      .withColumn("worker_compute_cost", Costs.compute('cloud_billable, $"workerSpecs.Compute_Contract_Price", 'target_num_workers, 'uptime_in_state_H))
      .withColumn("driver_dbu_cost", Costs.dbu('databricks_billable, $"driverSpecs.Hourly_DBUs", 'dbu_rate, lit(1), 'uptime_in_state_H))
      .withColumn("worker_dbu_cost", Costs.dbu('databricks_billable, $"workerSpecs.Hourly_DBUs", 'dbu_rate, 'current_num_workers, 'uptime_in_state_H))
      .withColumn("total_compute_cost", 'driver_compute_cost + 'worker_compute_cost)
      .withColumn("total_DBU_cost", 'driver_dbu_cost + 'worker_dbu_cost)
      .withColumn("total_driver_cost", 'driver_compute_cost + 'driver_dbu_cost)
      .withColumn("total_worker_cost", 'worker_compute_cost + 'worker_dbu_cost)
      .withColumn("total_cost", 'total_driver_cost + 'total_worker_cost)

    val clusterStateFactCols: Array[Column] = Array(
      'organization_id,
      'cluster_id,
      'cluster_name,
      'custom_tags,
      'unixTimeMS_state_start,
      'unixTimeMS_state_end,
      'timestamp_state_start,
      'timestamp_state_end,
      'type.alias("state"),
      'driver_node_type_id,
      'node_type_id,
      'current_num_workers,
      'target_num_workers,
      'uptime_since_restart_S,
      'uptime_in_state_S,
      'uptime_in_state_H,
      'cloud_billable,
      'databricks_billable,
      'isAutomated,
      'dbu_rate,
      'days_in_state,
      'worker_potential_core_H,
      'core_hours,
      'driverSpecs,
      'workerSpecs,
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

    clusterPotential
      .select(clusterStateFactCols: _*)
  }

  protected def buildJobRunCostPotentialFact(
                                              clusterStateFact: DataFrame,
                                              incrementalSparkJob: DataFrame,
                                              incrementalSparkTask: DataFrame,
                                              interactiveDBUPrice: Double,
                                              automatedDBUPrice: Double
                                            )(newTerminatedJobRuns: DataFrame): DataFrame = {

    if (clusterStateFact.isEmpty) {
      spark.emptyDataFrame
    } else {

      val clusterPotentialWCosts = clusterStateFact
        .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull)

      val keyColNames = Array("organization_id", "cluster_id", "timestamp")
      val keys: Array[Column] = Array(keyColNames map col: _*)
      val lookupCols: Array[Column] = Array(
        'cluster_name, 'custom_tags, 'unixTimeMS_state_start, 'unixTimeMS_state_end, 'timestamp_state_start,
        'timestamp_state_end, 'state, 'cloud_billable, 'databricks_billable, 'uptime_in_state_H, 'current_num_workers, 'target_num_workers,
        lit(interactiveDBUPrice).alias("interactiveDBUPrice"), lit(automatedDBUPrice).alias("automatedDBUPrice"),
        $"driverSpecs.API_name".alias("driver_node_type_id"),
        $"driverSpecs.Compute_Contract_Price".alias("driver_compute_hourly"),
        $"driverSpecs.Hourly_DBUs".alias("driver_dbu_hourly"),
        $"workerSpecs.API_name".alias("node_type_id"),
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

      // GET POTENTIAL WITH COSTS
      val clusterPotentialInitialState = clusterPotentialWCosts
        .withColumn("timestamp", 'unixTimeMS_state_start)
        .select(keys ++ lookupCols: _*)

      val clusterPotentialIntermediateStates = clusterPotentialWCosts
        .select((keyColNames.filterNot(_ == "timestamp") map col) ++ lookupCols: _*)

      val clusterPotentialTerminalState = clusterPotentialWCosts
        .withColumn("timestamp", 'unixTimeMS_state_end)
        .select(keys ++ lookupCols: _*)

      // Adjust the uptimeInState to smoothe the runtimes over the runPeriod across concurrent runs
      val stateLifecycleKeys = Seq("organization_id", "run_id", "cluster_id", "unixTimeMS_state_start")
      val jobRunInitialState = newTerminatedJobRuns //jobRun_gold
        .withColumn("timestamp", $"job_runtime.startEpochMS")
        .toTSDF("timestamp", "organization_id", "cluster_id")
        .lookupWhen(
          clusterPotentialInitialState
            .toTSDF("timestamp", "organization_id", "cluster_id"),
          tsPartitionVal = 64, maxLookAhead = 1L
        ).df
        .drop("timestamp")
        .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull)
        .withColumn("job_runtime_in_state",
          when('state.isin("CREATING", "STARTING") || 'job_cluster_type === "new", 'uptime_in_state_H) // get true cluster time when state is guaranteed fully initial
            .otherwise((array_min(array('unixTimeMS_state_end, $"job_runtime.endEpochMS")) - $"job_runtime.startEpochMS") / lit(1000) / 3600)) // otherwise use jobStart as beginning time and min of stateEnd or jobEnd for end time
        //   .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))
        .withColumn("lifecycleState", lit("init"))

      val jobRunTerminalState = newTerminatedJobRuns
        .withColumn("timestamp", $"job_runtime.endEpochMS")
        .toTSDF("timestamp", "organization_id", "cluster_id")
        .lookupWhen(
          clusterPotentialTerminalState
            .toTSDF("timestamp", "organization_id", "cluster_id"),
          tsPartitionVal = 64, maxLookback = 0L, maxLookAhead = 1L
        ).df
        .drop("timestamp")
        .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull && 'unixTimeMS_state_end > $"job_runtime.endEpochMS")
        .join(jobRunInitialState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti")
        .withColumn("job_runtime_in_state", ($"job_runtime.endEpochMS" - array_max(array('unixTimeMS_state_start, $"job_runtime.startEpochMS"))) / lit(1000) / 3600)
        //   .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))
        .withColumn("lifecycleState", lit("terminal"))

      val topClusters = newTerminatedJobRuns
        .filter('organization_id.isNotNull && 'cluster_id.isNotNull)
        .groupBy('organization_id, 'cluster_id).count
        .orderBy('count.desc).limit(40)
        .select(array('organization_id, 'cluster_id)).as[Seq[String]].collect.toSeq

      val jobRunIntermediateStates = newTerminatedJobRuns.alias("jr")
        .join(clusterPotentialIntermediateStates.alias("cpot").hint("SKEW", Seq("organization_id", "cluster_id"), topClusters),
          $"jr.organization_id" === $"cpot.organization_id" &&
            $"jr.cluster_id" === $"cpot.cluster_id" &&
            $"cpot.unixTimeMS_state_start" > $"jr.job_runtime.startEpochMS" && // only states beginning after job start and ending before
            $"cpot.unixTimeMS_state_end" < $"jr.job_runtime.endEpochMS"
        )
        .drop($"cpot.cluster_id").drop($"cpot.organization_id")
        .join(jobRunInitialState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti")
        .join(jobRunTerminalState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti")
        .withColumn("job_runtime_in_state", ('unixTimeMS_state_end - 'unixTimeMS_state_start) / lit(1000) / 3600)
        .withColumn("lifecycleState", lit("intermediate"))

      val concState = Window.partitionBy('cluster_id, 'unixTimeMS_state_start, 'unixTimeMS_state_end)
      val jobRunWPotential = jobRunInitialState
        .unionByName(jobRunIntermediateStates)
        .unionByName(jobRunTerminalState)
        .withColumn("dbu_rate", when('job_cluster_type === "new", 'automatedDBUPrice).otherwise('interactiveDBUPrice)) // new | existing
        .withColumn("concurrent_runs", size(collect_set('run_id).over(concState)))
        .withColumn("run_state_utilization", 'job_runtime_in_state / sum('job_runtime_in_state).over(concState)) // given n hours of uptime in state 1/n of said uptime was used by this job job
        .withColumn("running_days", sequence($"job_runtime.startTS".cast("date"), $"job_runtime.endTS".cast("date")))
        .withColumn("cluster_type", when('job_cluster_type === "new", lit("automated")).otherwise(lit("interactive")))
        .withColumn("driver_compute_cost", 'driver_compute_cost * 'run_state_utilization) // smoothing across concurrency and running days
        .withColumn("driver_dbu_cost", 'driver_dbu_cost * 'run_state_utilization)
        .withColumn("worker_compute_cost", 'worker_compute_cost * 'run_state_utilization)
        .withColumn("worker_dbu_cost", 'worker_dbu_cost * 'run_state_utilization)
        .withColumn("total_driver_cost", 'driver_compute_cost + 'driver_dbu_cost)
        .withColumn("total_worker_cost", 'worker_compute_cost + 'worker_dbu_cost)
        .withColumn("total_compute_cost", 'driver_compute_cost + 'worker_compute_cost)
        .withColumn("total_dbu_cost", 'driver_dbu_cost + 'worker_dbu_cost)
        .withColumn("total_cost", 'total_driver_cost + 'total_worker_cost)

      val jobRunCostPotential = jobRunWPotential
        .groupBy(
          'organization_id,
          'job_id,
          'id_in_job,
          'job_runtime,
          'job_terminal_state.alias("run_terminal_state"),
          'job_trigger_type.alias("run_trigger_type"),
          'cluster_id,
          'cluster_name,
          'cluster_type,
          'custom_tags,
          'driver_node_type_id,
          'node_type_id,
          'dbu_rate
        )
        .agg(
          first(size('running_days)).alias("running_days"),
          greatest(avg('concurrent_runs), lit(1.0)).alias("avg_concurrent_runs"),
          sum(lit(1)).alias("cluster_run_states"),
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
      if (!incrementalSparkJob.isEmpty) {
        val sparkJobMini = incrementalSparkJob
          .select('organization_id, 'date, 'spark_context_id, 'job_group_id,
            'job_id, explode('stage_ids).alias("stage_id"), 'db_job_id, 'db_id_in_job)
          //      .filter('job_group_id.like("%job-%-run-%")) // temporary until all job/run ids are imputed in the gold layer
          .filter('db_job_id.isNotNull && 'db_id_in_job.isNotNull)

        val sparkTaskMini = incrementalSparkTask
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


  }

  protected def buildSparkJob(
                               cloudProvider: String
                             )(df: DataFrame): DataFrame = {

    val jobGroupW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.JobGroupID")
    val executionW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.ExecutionID")
    val isolationIDW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.SparkDBIsolationID")
    val oAuthIDW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.AzureOAuth2ClientID")
    val rddScopeW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.RDDScope")
    val replIDW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.SparkDBREPLID")
    val notebookW = Window.partitionBy('organization_id, 'SparkContextID, $"PowerProperties.NotebookID")
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
          last('user_email, ignoreNulls = true).over(notebookW)).otherwise('user_email))

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
      |job_terminal_state, job_trigger_type, cluster_id, notebook_params, libraries, children, workflow_context,
      |task_detail, request_detail, time_detail
      |""".stripMargin

  protected val jobRunCostPotentialFactViewColumnMapping: String =
    """
      |organization_id, job_id, id_in_job, job_runtime, run_terminal_state, run_trigger_type, cluster_id,
      |cluster_name, cluster_type, custom_tags, driver_node_type_id, node_type_id, dbu_rate, running_days,
      |avg_concurrent_runs, cluster_run_states, worker_potential_core_H, driver_compute_cost, driver_dbu_cost,
      |worker_compute_cost, worker_dbu_cost, total_driver_cost, total_worker_cost, total_compute_cost,
      |total_dbu_cost, total_cost, spark_task_runtimeMS, spark_task_runtime_H, job_run_cluster_util
      |""".stripMargin

  protected val notebookViewColumnMappings: String =
    """
      |organization_id, notebook_id, notebook_name, notebook_path, cluster_id, action, unixTimeMS, timestamp, date, old_name, old_path,
      |new_name, new_path, parent_path, user_email, request_id, response
      |""".stripMargin

  protected val accountModViewColumnMappings: String =
    """
      |organization_id, mod_unixTimeMS, mod_date, action, endpoint, modified_by, user_name, user_id,
      |group_name, group_id, from_ip_address, user_agent, request_id, response
      |""".stripMargin

  protected val accountLoginViewColumnMappings: String =
    """
      |organization_id, login_unixTimeMS, login_date, login_type, login_user, user_id, user_email, ssh_login_details
      |from_ip_address, user_agent, request_id, response
      |""".stripMargin

  protected val clusterStateFactViewColumnMappings: String =
    """
      |organization_id, cluster_id, cluster_name, custom_tags, unixTimeMS_state_start, unixTimeMS_state_end,
      |timestamp_state_start, timestamp_state_end, state, driver_node_type_id, node_type_id, current_num_workers,
      |target_num_workers, uptime_since_restart_S, uptime_in_state_S, uptime_in_state_H, cloud_billable,
      |databricks_billable, isAutomated, dbu_rate, days_in_state, worker_potential_core_H, core_hours, driver_compute_cost,
      |worker_compute_cost, driver_dbu_cost, worker_dbu_cost, total_compute_cost, total_DBU_cost,
      |total_driver_cost, total_worker_cost, total_cost, driverSpecs, workerSpecs
      |""".stripMargin

  protected val sparkJobViewColumnMapping: String =
    """
      |organization_id, spark_context_id, job_id, job_group_id, execution_id, stage_ids, cluster_id, notebook_id, notebook_path,
      |db_job_id, db_id_in_job, db_job_type, unixTimeMS, timestamp, date, job_runtime, job_result, event_log_start,
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
