package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.PipelineFunctions.fillForward
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils.{NoNewDataException, SchemaTools, SparkSessionWrapper, TimeTypes}
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

object WorkflowsTransforms extends SparkSessionWrapper {

  import spark.implicits._

  def getJobsBase(df: DataFrame): DataFrame = {
    val onlyOnceJobRecW = Window.partitionBy('organization_id, 'timestamp, 'actionName, 'requestId, $"response.statusCode").orderBy('timestamp)
    df.filter(col("serviceName") === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .withColumn("rnk", rank().over(onlyOnceJobRecW))
      .withColumn("rn", row_number.over(onlyOnceJobRecW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
  }

  /**
   * BEGIN JOB STATUS
   */

  /**
   * Fail fast if jobsSnapshot is missing and/or if there is no new data
   */
  def jobStatusValidateNewJobsStatusHasNewData(
                                                isFirstRun: Boolean,
                                                jobsSnapshot: PipelineTable,
                                                jobsBaseHasRecords: Boolean
                                              ): Unit = {
    if (!jobsSnapshot.exists(dataValidation = true)) {
      throw new NoNewDataException(s"${jobsSnapshot.tableFullName} has no data for this workspace. " +
        s"To continue please ensure that Overwatch has access to see the jobs and that there are jobs present in " +
        s"this workspace. Otherwise, disable the jobs module completely.", Level.WARN, allowModuleProgression = true)
    }

    // not first run but no new pools records from audit -- fast fail OR
    // is first run and no pools records or snapshot records -- fast fail
    if (
      (!isFirstRun && !jobsBaseHasRecords) ||
        (isFirstRun && !jobsSnapshot.exists(dataValidation = true))
    ) {
      throw new NoNewDataException(
        s"""
           |No new jobs data found for this workspace.
           |If this is incorrect, please ensure that Overwatch has access to see the jobs and that there are
           |jobs present in this workspace.
           |Progressing module state
           |""".stripMargin,
        Level.WARN, allowModuleProgression = true
      )
    }
  }

  /**
   * Looks up the job defined metadata from the job snapshot and fills it with the latest value
   * at the time of the audit log record
   */
  def jobStatusLookupJobMeta(jobSnapLookup: DataFrame)(df: DataFrame): DataFrame = {
    df.toTSDF("timestamp", "organization_id", "jobId")
      .lookupWhen(
        jobSnapLookup.toTSDF("timestamp", "organization_id", "jobId"),
        maxLookAhead = Long.MaxValue
      ).df
  }

  /**
   * Extract and append the cluster details from the new_settings field
   * This will be present if it was recorded on the log event, otherwise it will be appended in the
   * next step
   */
  def jobStatusAppendClusterDetails(df: DataFrame): DataFrame = {
    df
      .withColumn("existing_cluster_id",
        coalesce('existing_cluster_id, get_json_object('new_settings, "$.existing_cluster_id"))
      )
      .withColumn("new_cluster",
        coalesce('new_cluster, get_json_object('new_settings, "$.new_cluster"))
      )
  }

  /**
   * When the cluster spec
   */
  def jobStatusBuildAndAppendTemporalClusterSpec(lastJobStatus: WindowSpec)(df: DataFrame): DataFrame = {
    // Following section builds out a "clusterSpec" as it is defined at the timestamp. existing_cluster_id
    // and new_cluster should never both be populated as a job must be one or the other at a timestamp

    val existingClusterMoreRecent = $"x2.last_existing.timestamp" > coalesce($"x2.last_new.timestamp", lit(0))
    val newClusterMoreRecent = $"x2.last_new.timestamp" > coalesce($"x2.last_existing.timestamp", lit(0))
    val existingAndNewClusterNull = $"cluster_spec.existing_cluster_id".isNull && $"cluster_spec.new_cluster".isNull
    df
      .withColumn( // initialize cluster_spec at record timestamp
        "x",
        struct(
          when('existing_cluster_id.isNotNull, struct('timestamp, 'existing_cluster_id))
            .otherwise(lit(null)).alias("last_existing"),
          when('new_cluster.isNotNull, struct('timestamp, 'new_cluster))
            .otherwise(lit(null)).alias("last_new")
        )
      )
      .withColumn( // last non_null cluster id / spec i.e. fill forward
        "x2",
        struct(
          last($"x.last_existing", true).over(lastJobStatus).alias("last_existing"),
          last($"x.last_new", true).over(lastJobStatus).alias("last_new"),
        )
      )
      .withColumn( // derive the latest existing / new cluster spec at log event time
        "cluster_spec",
        struct(
          when(existingClusterMoreRecent, $"x2.last_existing.existing_cluster_id")
            .otherwise(lit(null))
            .alias("existing_cluster_id"),
          when(newClusterMoreRecent, $"x2.last_new.new_cluster")
            .otherwise(lit(null))
            .alias("new_cluster")
        )
      )
      .withColumn( // when both are null in the audit logs fallback to the jobSnapshot lookup in lookup_settings
        "cluster_spec",
        struct(
          when(existingAndNewClusterNull, get_json_object('lookup_settings, "$.existing_cluster_id"))
            .otherwise($"cluster_spec.existing_cluster_id").alias("existing_cluster_id"),
          when(existingAndNewClusterNull, get_json_object('lookup_settings, "$.new_cluster"))
            .otherwise($"cluster_spec.new_cluster").alias("new_cluster")
        )
      ).drop("existing_cluster_id", "new_cluster", "x", "x2") // drop temp columns and old version of clusterSpec components
  }

  /**
   * lookup and fill forward common metadata
   */
  def jobStatusLookupAndFillForwardMetaData(lastJobStatus: WindowSpec)(df: DataFrame): DataFrame = {
    df
      .withColumn("schedule", fillForward("schedule", lastJobStatus, Seq(get_json_object('lookup_settings, "$.schedule"))))
      .withColumn("timeout_seconds", fillForward("timeout_seconds", lastJobStatus, Seq(get_json_object('lookup_settings, "$.timeout_seconds"))).cast("string").alias("timeout_seconds"))
      .withColumn("notebook_path", fillForward("notebook_path", lastJobStatus, Seq(get_json_object('lookup_settings, "$.notebook_task.notebook_path"))))
      .withColumn("jobName", fillForward("jobName", lastJobStatus, Seq(get_json_object('lookup_settings, "$.name"))))
      .withColumn("created_by", when('actionName === "create", $"userIdentity.email"))
      .withColumn("created_by", coalesce(fillForward("created_by", lastJobStatus), 'snap_lookup_created_by))
      .withColumn("created_ts", when('actionName === "create", 'timestamp))
      .withColumn("created_ts", coalesce(fillForward("created_ts", lastJobStatus), 'snap_lookup_created_time))
      .withColumn("deleted_by", when('actionName === "delete", $"userIdentity.email"))
      .withColumn("deleted_ts", when('actionName === "delete", 'timestamp))
      .withColumn("last_edited_by", when('actionName.isin("update", "reset"), $"userIdentity.email"))
      .withColumn("last_edited_by", last('last_edited_by, true).over(lastJobStatus))
      .withColumn("last_edited_ts", when('actionName.isin("update", "reset"), 'timestamp))
      .withColumn("last_edited_ts", last('last_edited_ts, true).over(lastJobStatus))
      .drop("userIdentity", "snap_lookup_created_time", "snap_lookup_created_by", "lookup_settings")
  }

  /**
   * On first run the audit logs may have recently been enabled which means a stagnant job may not have any
   * create/edit events logged. In this case, we still want to provide data for this ID in the historical job status
   * dataframe. To do this, these records are build / imputed from snapshot as best as possible and act as an
   * initializer for the records. This function captures the ids that are missing so they can be imputed / filled
   * in subsequent steps
   */
  def jobStatusDeriveFirstRunMissingJobIDs(
                                            jobsBaseHasRecords: Boolean,
                                            jobsSnapshotDFComplete: DataFrame,
                                            jobStatusBase: DataFrame
                                          ): DataFrame = {
    if (jobsBaseHasRecords) { // if job status records found in audit
      jobsSnapshotDFComplete.select('organization_id, 'job_id).distinct
        .join(jobStatusBase.select('organization_id, 'jobId.alias("job_id")).distinct, Seq("organization_id", "job_id"), "anti")
    } else { // otherwise just load what's available from snap
      jobsSnapshotDFComplete.select('organization_id, 'job_id)
        .distinct
    }
  }

  /**
   * Build first run records from jobSnapshot for ids missing in audit log historical events
   */
  def jobStatusDeriveFirstRunRecordImputesFromSnapshot(
                                                        jobsSnapshotDFComplete: DataFrame,
                                                        missingJobIds: DataFrame,
                                                        fromTime: TimeTypes
                                                      ): DataFrame = {

    val lastJobSnapW = Window.partitionBy('organization_id, 'job_id).orderBy('Pipeline_SnapTS.desc)
    jobsSnapshotDFComplete
      .join(missingJobIds, Seq("organization_id", "job_id")) // filter to only the missing job IDs
      .withColumn("rnk", rank().over(lastJobSnapW))
      .filter('rnk === 1).drop("rnk")
      .withColumn("timestamp", lit(fromTime.asUnixTimeMilli)) // set timestamp as fromtime so it will be included in downstream incrementals
      .select(
        'organization_id,
        'timestamp,
        'job_id.alias("jobId"),
        lit("jobs").alias("serviceName"),
        lit("snapImpute").alias("actionName"),
        lit("-1").alias("requestId"),
        $"settings.name".alias("jobName"),
        $"settings.timeout_seconds".cast("string").alias("timeout_seconds"),
        to_json($"settings.schedule").alias("schedule"),
        $"settings.notebook_task.notebook_path",
        when($"settings.existing_cluster_id".isNotNull,
          struct( // has existing cluster_id, no new_cluster_spec
            $"settings.existing_cluster_id",
            lit(null).alias("new_cluster")
          )
        )
          .otherwise(
            struct(
              lit(null).alias("existing_cluster_id"),
              to_json($"settings.new_cluster").alias("new_cluster")
            )
          )
          .alias("cluster_spec"),
        'creator_user_name.alias("created_by"),
        'created_time.alias("created_ts")
      )
  }

  /**
   * Convert complex json columns to typed structs
   */
  def jobStatusConvertJsonColsToStructs(jobsBaseHasRecords: Boolean)(df: DataFrame): DataFrame = {
    val changeInventory = if (jobsBaseHasRecords) {
      Map(
        "cluster_spec.new_cluster" -> structFromJson(spark, df, "cluster_spec.new_cluster"),
        "new_settings" -> structFromJson(spark, df, "new_settings"),
        "schedule" -> structFromJson(spark, df, "schedule")
      )
    } else { // new_settings is not present from snapshot and is a struct thus it cannot be added with dynamic schema
      Map(
        "cluster_spec.new_cluster" -> structFromJson(spark, df, "cluster_spec.new_cluster"),
        "schedule" -> structFromJson(spark, df, "schedule")
      )
    }
    df
      .select(SchemaTools.modifyStruct(df.schema, changeInventory): _*)
      .scrubSchema
  }

  //  def jobStatusCleanseComplexStructs

  def jobStatusCleanseJobClusters(df: DataFrame, keys: Array[String]): DataFrame = {
    val jobStatusByKeyW = Window.partitionBy(keys map col: _*)
    val jobClustersExplodedWKeys = df
      .filter(size($"new_settings.job_clusters") > 0)
      .select((keys map col) :+ explode($"new_settings.job_clusters").alias("job_clusters"): _*)

    val jobClustersChangeInventory = Map(
      "job_clusters.new_cluster.custom_tags" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "job_clusters.new_cluster.custom_tags"),
      "job_clusters.new_cluster.aws_attributes" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "job_clusters.new_cluster.aws_attributes"),
      "job_clusters.new_cluster.azure_attributes" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "job_clusters.new_cluster.azure_attributes"),
      "job_clusters.new_cluster.spark_conf" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "job_clusters.new_cluster.spark_conf"),
      "job_clusters.new_cluster.spark_env_vars" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "job_clusters.new_cluster.spark_env_vars")
    )

    jobClustersExplodedWKeys
      .select(SchemaTools.modifyStruct(jobClustersExplodedWKeys.schema, jobClustersChangeInventory): _*)
      .withColumn("job_clusters", collect_list('job_clusters).over(jobStatusByKeyW))
  }

  def jobStatusCleanseNewSettingsTasks(df: DataFrame, keys: Array[String]): DataFrame = {
    val jobStatusByKeyW = Window.partitionBy(keys map col: _*)
    val tasksExplodedWKeys = df
      .filter(size($"new_settings.tasks") >= 1)
      .select((keys map col) :+ explode($"new_settings.tasks").alias("newSettingsTask"): _*)

    val tasksChangeInventory = Map(
      "newSettingsTask.new_cluster.custom_tags" -> SchemaTools.structToMap(tasksExplodedWKeys, "newSettingsTask.new_cluster.custom_tags"),
      "newSettingsTask.new_cluster.aws_attributes" -> SchemaTools.structToMap(tasksExplodedWKeys, "newSettingsTask.new_cluster.aws_attributes"),
      "newSettingsTask.new_cluster.azure_attributes" -> SchemaTools.structToMap(tasksExplodedWKeys, "newSettingsTask.new_cluster.azure_attributes"),
      "newSettingsTask.new_cluster.spark_conf" -> SchemaTools.structToMap(tasksExplodedWKeys, "newSettingsTask.new_cluster.spark_conf"),
      "newSettingsTask.new_cluster.spark_env_vars" -> SchemaTools.structToMap(tasksExplodedWKeys, "newSettingsTask.new_cluster.spark_env_vars"),
      "newSettingsTask.notebook_task.base_parameters" -> SchemaTools.structToMap(tasksExplodedWKeys, "newSettingsTask.notebook_task.base_parameters")
    )

    tasksExplodedWKeys
      .select(SchemaTools.modifyStruct(tasksExplodedWKeys.schema, tasksChangeInventory): _*)
      .withColumn("newSettingsTask", collect_list('newSettingsTask).over(jobStatusByKeyW))
  }

  /**
   * BEGIN JOB RUNS SILVER
   */

//  val jobRunsLookups: Map[String, DataFrame] =
  def jobRunsInitializeLookups(lookups: (PipelineTable, DataFrame)*): Map[String, DataFrame] = {
    lookups.map(lookup => {
      (lookup._1.name, lookup._2)
    }).toMap
  }

  def jobRunsDeriveCompletedRuns(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("runSucceeded", "runFailed"))
      .select(
        'serviceName, 'actionName,
        'organization_id,
        'date,
        'timestamp,
        'runId,
        'jobId.alias("completedJobId"),
        'idInJob, 'clusterId,
        'jobClusterType.alias("jobClusterType_Completed"),
        'jobTaskType.alias("jobTaskType_Completed"),
        'jobTriggerType.alias("jobTriggerType_Completed"),
        'jobTerminalState,
        'requestId.alias("completionRequestID"),
        'response.alias("completionResponse"),
        'timestamp.alias("completionTime")
      )
      .filter('runId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveCancelledRuns(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("cancel"))
      .select(
        'organization_id, 'date, 'timestamp,
        'run_id.cast("long").alias("runId"),
        'requestId.alias("cancellationRequestId"),
        'response.alias("cancellationResponse"),
        'sessionId.alias("cancellationSessionId"),
        'sourceIPAddress.alias("cancellationSourceIP"),
        'timestamp.alias("cancellationTime"),
        'userAgent.alias("cancelledUserAgent"),
        'userIdentity.alias("cancelledBy")
      )
      .filter('runId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveRunsLaunched(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("runNow"))
      .select(
        'organization_id, 'date, 'timestamp,
        'job_id.cast("long").alias("runNowJobId"),
        get_json_object($"response.result", "$.run_id").cast("long").alias("runId"),
        lit(null).cast("string").alias("run_name"),
        'timestamp.alias("submissionTime"),
        lit(null).cast("string").alias("new_cluster"),
        lit(null).cast("string").alias("existing_cluster_id"),
        'notebook_params, 'workflow_context,
        struct(
          lit(null).cast("string").alias("notebook_task"),
          lit(null).cast("string").alias("spark_python_task"),
          lit(null).cast("string").alias("spark_jar_task"),
          lit(null).cast("string").alias("shell_command_task")
        ).alias("taskDetail"),
        lit(null).cast("string").alias("libraries"),
        lit(null).cast("string").alias("timeout_seconds"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      )
      .filter('runId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveSubmittedRuns(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("submitRun"))
      .select(
        'organization_id, 'date, 'timestamp,
        lit(null).cast("long").alias("runNowJobId"),
        get_json_object($"response.result", "$.run_id").cast("long").alias("runId"),
        'run_name,
        'timestamp.alias("submissionTime"),
        'new_cluster, 'existing_cluster_id,
        'notebook_params, 'workflow_context,
        struct(
          'notebook_task,
          'spark_python_task,
          'spark_jar_task,
          'shell_command_task
        ).alias("taskDetail"),
        'libraries,
        'timeout_seconds.cast("string").alias("timeout_seconds"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      )
      .filter('runId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveRunStarts(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df.filter('actionName.isin("runStart"))
      .select(
        'organization_id, 'date, 'timestamp,
        'jobId.alias("runStartJobId").cast("long"),
        'runId,
        'jobClusterType.alias("jobClusterType_Started"),
        'jobTaskType.alias("jobTaskType_Started"),
        'jobTriggerType.alias("jobTriggerType_Started"),
        'clusterId.alias("startClusterId"),
        'timestamp.alias("startTime"),
        'requestId.alias("startRequestID")
      )
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveRunsBase(df: DataFrame, etlUntilTime: TimeTypes): DataFrame = {

    val firstRunSemanticsW = Window.partitionBy('organization_id, 'runId).orderBy('timestamp)

    // Completes must be >= etlStartTime as it is the driver endpoint
    // All joiners to Completes may be from the past up to N days as defined in the incremental df
    // Identify all completed jobs in scope for this overwatch run
    val allCompletes = jobRunsDeriveCompletedRuns(df, firstRunSemanticsW)

    // CancelRequests are still lookups from the driver "complete" as a cancel request is a request and still
    // results in a runFailed after the cancellation
    // Identify all cancelled jobs in scope for this overwatch run
    val allCancellations = jobRunsDeriveCancelledRuns(df, firstRunSemanticsW)

    // DF for jobs launched with actionName == "runNow"
    // Lookback 30 days for laggard starts prior to current run
    // only field from runNow that we care about is the response.result.runId
    val runNowStart = jobRunsDeriveRunsLaunched(df, firstRunSemanticsW)

    // DF for jobs launched with actionName == "submitRun"
    // Lookback 30 days for laggard starts prior to current run
    // TODO: move taskDetail and libraries to jobStatus as it only exists for create and submitRun actions
    //  it will be much more complete over there

    /**
     * These are runs submitted using the "submitRun" API endpoint. These runs will have no corresponding job
     * since the job was never scheduled. The entire definition of the job and the cluster must be sumitted
     * in this API call. Does not reference an existing job_id present in the jobsStatus Target
     */
    val runSubmitStart = jobRunsDeriveSubmittedRuns(df, firstRunSemanticsW)

    // DF to pull unify differing schemas from runNow and submitRun and pull all job launches into one DF
    // TODO -- add runTriggered events
    val allSubmissions = runNowStart.unionByName(runSubmitStart)

    // Find the corresponding runStart action for the completed jobs
    // Lookback 30 days for laggard starts prior to current run
    val runStarts = jobRunsDeriveRunStarts(df, firstRunSemanticsW)

    allCompletes
      .join(allCancellations, Seq("runId", "organization_id"), "full")
      .join(allSubmissions, Seq("runId", "organization_id"), "full")
      .join(runStarts, Seq("runId", "organization_id"), "full")
      .withColumn("jobId", coalesce('completedJobId, 'runStartJobId, 'runNowJobId).cast("long"))
      .withColumn("idInJob", coalesce('idInJob, 'runId))
      .withColumn("jobClusterType", coalesce('jobClusterType_Completed, 'jobClusterType_Started))
      .withColumn("jobTerminalState",
        when('cancellationRequestId.isNotNull, "Cancelled")
          .otherwise('jobTerminalState)
      ) //.columns.sorted
      .withColumn("cluster_name",
        when('jobClusterType === "new", concat(lit("job-"), 'jobId, lit("-run-"), 'idInJob))
          .otherwise(lit(null).cast("string"))
      )
      .select(
        'runId.cast("long"),
        'jobId,
        'idInJob,
        TransformFunctions.subtractTime(
          array_min(array('startTime, 'submissionTime)),
          coalesce(array_max(array('completionTime, 'cancellationTime)), lit(etlUntilTime.asUnixTimeMilli)))
          .alias("JobRunTime"),
        'run_name,
        'jobClusterType,
        coalesce('jobTaskType_Completed, 'jobTaskType_Started).alias("jobTaskType"),
        coalesce('jobTriggerType_Completed, 'jobTriggerType_Started).alias("jobTriggerType"),
        'jobTerminalState,
        'new_cluster,
        coalesce('clusterId, 'startClusterId, 'existing_cluster_id).alias("clusterId"),
        'cluster_name,
        'organization_id,
        'notebook_params, 'libraries,
        'workflow_context, 'taskDetail,
        struct(
          'startTime,
          'submissionTime,
          'cancellationTime,
          'completionTime,
          'timeout_seconds
        ).alias("timeDetails"),
        struct(
          struct(
            'submitRequestId,
            'submitResponse,
            'submitSessionId,
            'submitSourceIP,
            'submitUserAgent,
            'submittedBy
          ).alias("submissionRequest"),
          struct(
            'cancellationRequestId,
            'cancellationResponse,
            'cancellationSessionId,
            'cancellationSourceIP,
            'cancelledUserAgent,
            'cancelledBy
          ).alias("cancellationRequest"),
          struct(
            'completionRequestId,
            'completionResponse
          ).alias("completionRequest"),
          struct(
            'startRequestId
          ).alias("startRequest")
        ).alias("requestDetails")
      )
      .withColumn("timestamp", $"JobRunTime.startEpochMS")
  }

  /**
   * Populate existing cluster ids and cluster_names for scheduled jobs present in job_status_silver target
   */
  def jobRunsDeriveRunsOnInteractiveClusters(df: DataFrame, lookups: Map[String, DataFrame]): DataFrame = {
    if (lookups.contains("job_status_silver")) {
      df
        .toTSDF("timestamp", "organization_id", "jobId")
        .lookupWhen(
          lookups("job_status_silver")
            .toTSDF("timestamp", "organization_id", "jobId")
        ).df
    } else df
  }

  /**
   * Lookup and append job names for jobs without names
   * TODO -- append runName if it's a submitRun or job_name is null and run_name is not
   */
  def jobRunsFillMissingInteractiveClusterIDsAndJobNames(jobStatusLookupTarget: PipelineTable)(df: DataFrame): DataFrame = {

    // jobNames from MLFlow Runs
    val experimentName = get_json_object($"taskDetail.notebook_task", "$.base_parameters.EXPERIMENT_NAME")
    // Get JobName for All Jobs runs
    val jobNameLookupFromStatus = jobStatusLookupTarget.asDF
      .select('organization_id, 'timestamp, 'jobId, 'jobName
      )

    df
      .toTSDF("timestamp", "organization_id", "jobId")
      .lookupWhen(
        jobNameLookupFromStatus.toTSDF("timestamp", "organization_id", "jobId")
      ).df
      .withColumn("jobName",
        when('jobName.isNull && experimentName.isNotNull, experimentName)
          .otherwise('jobName)
      )
      .withColumn("jobName",
        when('jobName.isNull && 'run_name.isNotNull, 'run_name)
          .otherwise('jobName)
      )
  }

  def jobRunsFillClusterIDsAndClusterNames(lookups: Map[String, DataFrame])(df: DataFrame): DataFrame = {
    var interactiveRunsWClusterName = df.filter('jobClusterType === "existing")
    var automatedRunsWClusterID = df.filter('jobClusterType === "new")

    // fill missing cluster names for interactive clusters from cluster_spec by clusterId where exists
    interactiveRunsWClusterName = if (lookups.contains("cluster_spec_silver")) {
      interactiveRunsWClusterName
        .toTSDF("timestamp", "organization_id", "clusterId")
        .lookupWhen(
          lookups("cluster_spec_silver")
            .toTSDF("timestamp", "organization_id", "clusterId")
        ).df
    } else interactiveRunsWClusterName

    // fill missing cluster names for interactive clusters from cluster_snapshot by clusterId where exists
    interactiveRunsWClusterName = if (lookups.contains("clusters_snapshot_bronze")) {
      interactiveRunsWClusterName
        .toTSDF("timestamp", "organization_id", "clusterId")
        .lookupWhen(
          lookups("clusters_snapshot_bronze")
            .toTSDF("timestamp", "organization_id", "clusterId")
        ).df
    } else interactiveRunsWClusterName

    // fill missing cluster ids for automated clusters from cluster_spec by cluster_name where exists
    automatedRunsWClusterID = if (lookups.contains("cluster_spec_silver")) {
      automatedRunsWClusterID
        .toTSDF("timestamp", "organization_id", "cluster_name")
        .lookupWhen(
          lookups("cluster_spec_silver")
            .toTSDF("timestamp", "organization_id", "cluster_name")
        ).df
    } else automatedRunsWClusterID

    // fill missing cluster ids for automated clusters from cluster_snap by cluster_name where exists
    automatedRunsWClusterID = if (lookups.contains("clusters_snapshot_bronze")) {
      automatedRunsWClusterID
        .toTSDF("timestamp", "organization_id", "cluster_name")
        .lookupWhen(
          lookups("clusters_snapshot_bronze")
            .toTSDF("timestamp", "organization_id", "cluster_name")
        ).df
    } else automatedRunsWClusterID
    interactiveRunsWClusterName.unionByName(automatedRunsWClusterID)
  }

}
