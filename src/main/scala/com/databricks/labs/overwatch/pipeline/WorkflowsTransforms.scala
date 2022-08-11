package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.PipelineFunctions.fillForward
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils.{NoNewDataException, SchemaTools, SparkSessionWrapper, TimeTypes}
import org.apache.log4j.Level
import org.apache.spark.sql.{Column, DataFrame}
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

  def jobStatusUnifyNewCluster(df: DataFrame): DataFrame = {
    df
      .withColumn("new_cluster", coalesce('new_cluster, get_json_object('new_settings, "$.new_cluster")))
  }

  /**
   * Extract and append the cluster details from the new_settings field
   * This will be present if it was recorded on the log event, otherwise it will be appended in the
   * next step
   */
  def jobStatusAppendClusterDetails(df: DataFrame): DataFrame = {
//    val newCluster = coalesce('new_cluster, get_json_object('new_settings, "$.new_cluster"))
//    df
//      .withColumn("existing_cluster_id",
//        coalesce('existing_cluster_id, get_json_object('new_settings, "$.existing_cluster_id"))
//      )
//      .withColumn("new_cluster", newCluster)
//      .withColumn("job_clusters", get_json_object('new_settings, "$.job_clusters"))
    df
      .withColumn("existing_cluster_id",
        coalesce('existing_cluster_id, $"new_settings.existing_cluster_id")
      )
      .withColumn("job_clusters", $"new_settings.job_clusters")
  }

  /**
   * Generate cluster_spec field and fill it temporally with the last value when
   * the details are not provided
   */
  def jobStatusAppendAndFillTemporalClusterSpec(df: DataFrame): DataFrame = {
    val temporalJobClusterSpec = df
      .select('organization_id, 'jobId, 'timestamp,
        struct(
          'existing_cluster_id,
          'new_cluster,
          'job_clusters
          // TODO - tasks.new_clusters?
        ).alias("temporal_cluster_spec")
      )
      .toTSDF("timestamp", "organization_id", "jobId")

    df
      .drop("existing_cluster_id", "new_cluster", "job_clusters")
      .toTSDF("timestamp", "organization_id", "jobId")
      .lookupWhen(temporalJobClusterSpec)
      .df
      .selectExpr("*", "temporal_cluster_spec.existing_cluster_id", "temporal_cluster_spec.new_cluster", "temporal_cluster_spec.job_clusters")
      .drop("temporal_cluster_spec")
      .withColumn("cluster_spec", struct(
        'existing_cluster_id,
        'new_cluster,
        'job_clusters
      ))
      .scrubSchema // cleans schema after creating structs
      .drop("existing_cluster_id", "new_cluster", "job_clusters")
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
        $"settings.notebook_task.notebook_path", // TODO -- this is now inside $"settings.tasks[*] -- return all tasks
        // TODO -- existing_cluster_id and new_cluster and job_cluster_key are now inside of tasks
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
        "new_cluster" -> structFromJson(spark, df, "new_cluster"),
        "new_settings" -> structFromJson(spark, df, "new_settings"),
        "schedule" -> structFromJson(spark, df, "schedule")
      )
    } else { // new_settings is not present from snapshot and is a struct thus it cannot be added with dynamic schema
      Map( // TODO -- rebuild the way new_cluster gets created from new jobs/list snapshot v2.1
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

  def jobStatusCleanseForPublication(keys: Array[String])(df: DataFrame): DataFrame = {
    val containsMTJTasks = SchemaTools.getAllColumnNames(df.schema).contains("new_settings.tasks")
    val containsMTJJobClusters = SchemaTools.getAllColumnNames(df.schema).contains("new_settings.job_clusters")
    var cleansedDF = df

    val structsCleaner = collection.mutable.Map(
      "new_settings.tags" -> SchemaTools.structToMap(df, "new_settings.tags"),
      "new_cluster.custom_tags" -> SchemaTools.structToMap(df, "new_cluster.custom_tags"),
      "new_cluster.spark_conf" -> SchemaTools.structToMap(df, "new_cluster.spark_conf"),
      "new_cluster.spark_env_vars" -> SchemaTools.structToMap(df, "new_cluster.spark_env_vars"),
      "new_cluster.aws_attributes" -> SchemaTools.structToMap(df, s"new_cluster.aws_attributes"),
      "new_cluster.azure_attributes" -> SchemaTools.structToMap(df, s"new_cluster.azure_attributes"),
      "new_settings.new_cluster.custom_tags" -> SchemaTools.structToMap(df, "new_settings.new_cluster.custom_tags"),
      "new_settings.new_cluster.spark_conf" -> SchemaTools.structToMap(df, "new_settings.new_cluster.spark_conf"),
      "new_settings.new_cluster.spark_env_vars" -> SchemaTools.structToMap(df, "new_settings.new_cluster.spark_env_vars"),
      "new_settings.new_cluster.aws_attributes" -> SchemaTools.structToMap(df, s"new_settings.new_cluster.aws_attributes"),
      "new_settings.new_cluster.azure_attributes" -> SchemaTools.structToMap(df, s"new_settings.new_cluster.azure_attributes"),
      "new_settings.notebook_task.base_parameters" -> SchemaTools.structToMap(df, "new_settings.notebook_task.base_parameters"),
      "cluster_spec.new_cluster.custom_tags" -> SchemaTools.structToMap(df, "cluster_spec.new_cluster.custom_tags"),
      "cluster_spec.new_cluster.spark_conf" -> SchemaTools.structToMap(df, "cluster_spec.new_cluster.spark_conf"),
      "cluster_spec.new_cluster.spark_env_vars" -> SchemaTools.structToMap(df, "cluster_spec.new_cluster.spark_env_vars"),
      "cluster_spec.new_cluster.aws_attributes" -> SchemaTools.structToMap(df, s"cluster_spec.new_cluster.aws_attributes"),
      "cluster_spec.new_cluster.azure_attributes" -> SchemaTools.structToMap(df, s"cluster_spec.new_cluster.azure_attributes")
    )

    cleansedDF = if (containsMTJTasks) {
      structsCleaner("new_settings.tasks") = col("newSettingsTask")
      val cleansedMTJTasksDF = jobStatusCleanseNewSettingsTasks(cleansedDF, keys)
      cleansedDF.join(cleansedMTJTasksDF, keys.toSeq, "left")
    } else cleansedDF

    cleansedDF = if (containsMTJJobClusters) {
      structsCleaner("new_settings.job_clusters") = col("job_clusters")
      val cleansedMTJJobClustersDF = jobStatusCleanseJobClusters(cleansedDF, keys)
      cleansedDF.join(cleansedMTJJobClustersDF, keys.toSeq, "left")
    } else cleansedDF

    cleansedDF
      .modifyStruct(structsCleaner.toMap)
      .drop("newSettingsTask", "job_clusters")
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
    val parentRunId = coalesce('multitaskParentRunId, 'parentRunId)
    df
      .filter('actionName.isin("runSucceeded", "runFailed"))
      .select(
        'serviceName, 'actionName,
        'organization_id,
        'date,
        'timestamp,
        when(parentRunId.isNull, 'runId).otherwise(parentRunId.cast("long")).alias("jobRunId"),
        'runId.alias("taskRunId"),
        'jobId.alias("completedJobId"),
        parentRunId.alias("parentRunId_Completed"),
        'taskKey.alias("taskKey_Completed"),
        'taskDependencies.alias("taskDependencies_Completed"),
        'repairId.alias("repairId_Completed"),
        'idInJob,
        'jobClusterType.alias("jobClusterType_Completed"),
        'jobTaskType.alias("jobTaskType_Completed"),
        'jobTriggerType.alias("jobTriggerType_Completed"),
        'jobTerminalState,
        'requestId.alias("completionRequestID"),
        'response.alias("completionResponse"),
        'timestamp.alias("completionTime")
      )
      .filter('taskRunId.isNotNull)
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
        'run_id.cast("long").alias("jobRunId"), // TODO -- verify -- assume correct since user cannot cancel a taskRun
        'requestId.alias("cancellationRequestId"),
        'response.alias("cancellationResponse"),
        'sessionId.alias("cancellationSessionId"),
        'sourceIPAddress.alias("cancellationSourceIP"),
        'timestamp.alias("cancellationTime"),
        'userAgent.alias("cancelledUserAgent"),
        'userIdentity.alias("cancelledBy")
      )
      .filter('jobRunId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  /**
   * Primarily necessary to get runId from response and capture the submission time
   */
  def jobRunsDeriveRunsLaunched(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("runNow"))
      .select(
        'organization_id, 'date, 'timestamp,
        'job_id.cast("long").alias("submissionJobId"),
        get_json_object($"response.result", "$.run_id").cast("long").alias("jobRunId"),
        'timestamp.alias("submissionTime"),
        lit("manual").alias("jobTriggerType_runNow"),
        'workflow_context.alias("workflow_context_runNow"),
        struct(
          'jar_params,
          'python_params,
          'spark_submit_params,
          'notebook_params
        ).alias("submission_params"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      )
      .filter('jobRunId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  /**
   * Same as runNow but captures runs launched by the jobScheduler (i.e. cron)
   */
  def jobRunsDeriveRunsTriggered(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("runTriggered"))
      .select(
        'organization_id, 'date, 'timestamp,
        'jobId.cast("long").alias("submissionJobId"),
        'runId.alias("jobRunId"),
        'timestamp.alias("submissionTime"),
        'jobTriggerType,
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userIdentity.alias("submittedBy")
      )
      .filter('jobRunId.isNotNull)
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
        lit(null).cast("long").alias("submissionJobId"),
        get_json_object($"response.result", "$.run_id").cast("long").alias("jobRunId"),
        'run_name,
        'timestamp.alias("submissionTime"),
        'job_clusters, // json array struct string
        'new_cluster, // json struct string
        'existing_cluster_id,
        'workflow_context.alias("workflow_context_submitRun"),
        struct(
          'notebook_task,
          'spark_python_task,
          'spark_jar_task,
          'shell_command_task,
          'pipeline_task
        ).alias("taskDetail"),
        'tasks, // json array struct string
        'libraries,
        'access_control_list.alias("submitRun_acls"),
        'git_source.alias("submitRun_git_source"),
        'timeout_seconds.cast("string").alias("timeout_seconds"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      )
      .filter('jobRunId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveRunStarts(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    val parentRunId = coalesce('multitaskParentRunId, 'parentRunId)
    df.filter('actionName.isin("runStart"))
      .select(
        'organization_id, 'date, 'timestamp,
        'jobId.cast("long").alias("runStartJobId"),
        when(parentRunId.isNull, 'runId).otherwise(parentRunId.cast("long")).alias("jobRunId"),
        'runId.alias("taskRunId"),
        parentRunId.alias("parentRunId_Started"),
        'taskKey.alias("taskKey_runStart"),
        'taskDependencies.alias("taskDependencies_runStart"), // json array string
        'repairId.alias("repairId_runStart"),
        'jobClusterType.alias("jobClusterType_Started"),
        'jobTaskType.alias("jobTaskType_Started"),
        'jobTriggerType.alias("jobTriggerType_Started"),
        'clusterId,
        'timestamp.alias("startTime"),
        'requestId.alias("startRequestID")
      )
      .filter('taskRunId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
  }

  def jobRunsDeriveRunsBase(df: DataFrame, etlUntilTime: TimeTypes): DataFrame = {

    val firstTaskRunSemanticsW = Window.partitionBy('organization_id, 'jobRunId, 'taskRunId).orderBy('timestamp)
    val firstJobRunSemanticsW = Window.partitionBy('organization_id, 'jobRunId).orderBy('timestamp)

    // Completes must be >= etlStartTime as it is the driver endpoint
    // All joiners to Completes may be from the past up to N days as defined in the incremental df
    // Identify all completed jobs in scope for this overwatch run
    val allCompletes = jobRunsDeriveCompletedRuns(df, firstTaskRunSemanticsW)

    // CancelRequests are still lookups from the driver "complete" as a cancel request is a request and still
    // results in a runFailed after the cancellation
    // Identify all cancelled jobs in scope for this overwatch run
    val allCancellations = jobRunsDeriveCancelledRuns(df, firstJobRunSemanticsW)

    // DF for jobs launched with actionName == "runNow"
    // Lookback 30 days for laggard starts prior to current run
    // only field from runNow that we care about is the response.result.runId
    val runNowStart = jobRunsDeriveRunsLaunched(df, firstJobRunSemanticsW)

    val runTriggered = jobRunsDeriveRunsTriggered(df, firstJobRunSemanticsW)

    // TODO: move taskDetail and libraries to jobStatus as it only exists for create and submitRun actions
    //  it will be much more complete over there

    /**
     * These are runs submitted using the "submitRun" API endpoint. These runs will have no corresponding job
     * since the job was never scheduled. The entire definition of the job and the cluster must be sumitted
     * in this API call. Does not reference an existing job_id present in the jobsStatus Target
     */
    val runSubmitStart = jobRunsDeriveSubmittedRuns(df, firstJobRunSemanticsW)

    // DF to pull unify differing schemas from runNow and submitRun and pull all job launches into one DF
    // TODO -- add runTriggered events
    val allSubmissions = runNowStart
      .unionByName(runTriggered, allowMissingColumns = true)
      .unionByName(runSubmitStart, allowMissingColumns = true)

    // Find the corresponding runStart action for the completed jobs
    // Lookback 30 days for laggard starts prior to current run
    val runStarts = jobRunsDeriveRunStarts(df, firstTaskRunSemanticsW)

    // TODO -- add repairRuns action
    allCompletes
      .join(allCancellations, Seq("organization_id", "jobRunId"), "full")
      .join(allSubmissions, Seq("organization_id", "jobRunId"), "full")
      .join(runStarts, Seq("organization_id", "jobRunId", "taskRunId"), "full")
//      .withColumn("cluster_name", // TODO -- look this up later
//        when('jobClusterType === "new", concat(lit("job-"), 'jobId, lit("-run-"), 'idInJob))
//          .otherwise(lit(null).cast("string"))
//      )
      .select(
        'organization_id,
        coalesce('runStartJobId, 'completedJobId, 'submissionJobId).cast("long").alias("jobId"),
        'jobRunId.cast("long"),
        'taskRunId.cast("long"),
        coalesce('taskKey_runStart, 'taskKey_Completed).alias("taskKey"),
        coalesce('taskDependencies_runStart, 'taskDependencies_Completed).alias("taskDependencies"),
        coalesce('taskRunId, 'jobRunId).cast("long").alias("runid"), // DEPRECATED 0620
        coalesce('parentRunId_Started, 'parentRunId_Completed).alias("parentJobRunId"),
        coalesce('repairId_runStart, 'repairId_Completed).alias("repairId"),
        coalesce('taskRunId, 'idInJob).alias("idInJob"),
        TransformFunctions.subtractTime(
          'submissionTime,
          coalesce(array_max(array('completionTime, 'cancellationTime)), lit(etlUntilTime.asUnixTimeMilli))
        ).alias("JobRunTime"), // run launch time until terminal event
        TransformFunctions.subtractTime(
          'startTime,
          coalesce(array_max(array('completionTime, 'cancellationTime)), lit(etlUntilTime.asUnixTimeMilli))
        ).alias("JobExecutionRunTime"), // from cluster up and run begin until terminal event
        'run_name, // TODO -- set == to jobName later if runName is Null
        coalesce('jobClusterType_Started, 'jobClusterType_Completed).alias("jobClusterType"),
        coalesce('jobTaskType_Started, 'jobTaskType_Completed).alias("jobTaskType"),
        coalesce('jobTriggerType_Started, 'jobTriggerType_Completed, 'jobTriggerType_runNow).alias("jobTriggerType"),
        when('cancellationRequestId.isNotNull, "Cancelled")
          .otherwise('jobTerminalState)
          .alias("jobTerminalState"),
        'new_cluster.alias("submitRun_newCluster"),
        'existing_cluster_id.alias("submitRun_existing_cluster_id"),
        'clusterId, // TODO -- fill this for each taskRun
//        'cluster_name, // TODO -- look this up later
        'libraries, // TODO -- potentially lookup from jobs or remove completely
        'submission_params,
        coalesce('workflow_context_runNow, 'workflow_context_submitRun).alias("workflow_context"),
        'taskDetail,
        'tasks, // TODO -- structify and cleanse later
        'submitRun_acls,
        'submitRun_git_source,
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

  /**
   * fill missing cluster ids and cluster names for interactive and automated clusters
   * automated clusters -- lookup cluster_id by cluster_name
   * TODO -- automated cluster names -- ensure using job_run_id and not task_run_id
   * interactive clusters -- lookup cluster_name by cluster_id
   */
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

  /**
   * BEGIN JRCP Transforms
   */

  def jrcpDeriveNewAndOpenRuns(df: DataFrame, fromTime: TimeTypes): DataFrame = {
    // TODO -- review the neaAndOpenJobRuns with updated jobRun logic to ensure all open runs are accounted for
    val newJrLaunches = df
      .filter($"job_runtime.startEpochMS" >= fromTime.asUnixTimeMilli)

    val openJRCPRecordsRunIDs = df
      // TODO -- verify endEpochMS is null and not == fromTime -- I believe this is closed with untilTime on prev run
      .filter($"job_runtime.endEpochMS".isNull) // open jrcp records (i.e. not incomplete job runs)
      .select('organization_id, 'run_id).distinct // org_id left to force partition pruning

    val outstandingJrRecordsToClose = df.join(openJRCPRecordsRunIDs, Seq("organization_id", "run_id"))

    // combine open records (updates) with new records (inserts)
    newJrLaunches.unionByName(outstandingJrRecordsToClose)
  }

  def jrcpDeriveRunInitialStates(
                                 clusterPotentialWCosts: DataFrame,
                                 newAndOpenRuns: DataFrame,
                                 runStateFirstToEnd: Column,
                                 clsfKeys: Array[Column],
                                 clsfLookupCols: Array[Column]
                               ): DataFrame = {

    // use state_start_time for initial states
    val clusterPotentialInitialState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_start)
      .select(clsfKeys ++ clsfLookupCols: _*)

    newAndOpenRuns //jobRun_gold
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
  }

  def jrcpDeriveRunTerminalStates(
                                   clusterPotentialWCosts: DataFrame,
                                   newAndOpenRuns: DataFrame,
                                   jobRunInitialStates: DataFrame,
                                   runStateLastToStart: Column,
                                   stateLifecycleKeys: Seq[String],
                                   clsfKeys: Array[Column],
                                   clsfLookupCols: Array[Column],
                                   untilTime: TimeTypes
                               ): DataFrame = {

    // use state_end_time for terminal states
    val clusterPotentialTerminalState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_end)
      .select(clsfKeys ++ clsfLookupCols: _*)

    newAndOpenRuns
      .withColumn("timestamp", coalesce($"job_runtime.endEpochMS", lit(untilTime.asUnixTimeMilli))) // include currently executing runs and calculate costs through module until time
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialTerminalState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 4, maxLookback = 0L, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .filter('unixTimeMS_state_start.isNotNull && 'unixTimeMS_state_end.isNotNull && 'unixTimeMS_state_end > $"job_runtime.endEpochMS")
      .join(jobRunInitialStates.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out beginning states
      .withColumn("runtime_in_cluster_state", $"job_runtime.endEpochMS" - runStateLastToStart)
      .withColumn("lifecycleState", lit("terminal"))
  }

  def jrcpDeriveRunIntermediateStates(
                                       clusterPotentialWCosts: DataFrame,
                                       newAndOpenRuns: DataFrame,
                                       jobRunInitialStates: DataFrame,
                                       jobRunTerminalStates: DataFrame,
                                       stateLifecycleKeys: Seq[String],
                                       clsfKeyColNames: Array[String],
                                       clsfLookupCols: Array[Column]
                                 ): DataFrame = {

    // PERF -- identify top 40 job counts by cluster to be provided to SKEW JOIN hint
    // Some interactive clusters may receive 90%+ of job runs causing massive skew, skew hint resolves
    val topClusters = newAndOpenRuns
      .filter('organization_id.isNotNull && 'cluster_id.isNotNull)
      .groupBy('organization_id, 'cluster_id).count
      .orderBy('count.desc).limit(40)
      .select(array('organization_id, 'cluster_id)).as[Seq[String]].collect.toSeq

    // use the actual timestamp for intermediate states
    val clusterPotentialIntermediateStates = clusterPotentialWCosts
      .select((clsfKeyColNames.filterNot(_ == "timestamp") map col) ++ clsfLookupCols: _*)

    newAndOpenRuns.alias("jr")
      .join(clusterPotentialIntermediateStates.alias("cpot").hint("SKEW", Seq("organization_id", "cluster_id"), topClusters),
        $"jr.organization_id" === $"cpot.organization_id" &&
          $"jr.cluster_id" === $"cpot.cluster_id" &&
          $"cpot.unixTimeMS_state_start" > $"jr.job_runtime.startEpochMS" && // only states beginning after job start and ending before
          $"cpot.unixTimeMS_state_end" < $"jr.job_runtime.endEpochMS"
      )
      .drop($"cpot.cluster_id").drop($"cpot.organization_id")
      .join(jobRunInitialStates.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out beginning states
      .join(jobRunTerminalStates.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out ending states
      .withColumn("runtime_in_cluster_state", 'unixTimeMS_state_end - 'unixTimeMS_state_start)
      .withColumn("lifecycleState", lit("intermediate"))

  }

  def jrcpDeriveRunsByClusterState(
                                    clusterPotentialWCosts: DataFrame,
                                    newAndOpenRuns: DataFrame,
                                    untilTime: TimeTypes,
                                    runStateFirstToEnd: Column,
                                    runStateLastToStart: Column
                                  ): DataFrame = {

    // Adjust the uptimeInState to smooth the runtimes over the runPeriod across concurrent runs
    val stateLifecycleKeys = Seq("organization_id", "run_id", "cluster_id", "unixTimeMS_state_start")
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

    val jobRunInitialStates = jrcpDeriveRunInitialStates(
      clusterPotentialWCosts,
      newAndOpenRuns,
      runStateFirstToEnd,
      clsfKeys,
      clsfLookups
    )
    val jobRunTerminalStates = jrcpDeriveRunTerminalStates(
      clusterPotentialWCosts,
      newAndOpenRuns,
      jobRunInitialStates,
      runStateLastToStart,
      stateLifecycleKeys,
      clsfKeys,
      clsfLookups,
      untilTime
    )
    val jobRunIntermediateStates = jrcpDeriveRunIntermediateStates(
      clusterPotentialWCosts,
      newAndOpenRuns,
      jobRunInitialStates,
      jobRunTerminalStates,
      stateLifecycleKeys,
      clsfKeyColNames,
      clsfLookups
    )

    jobRunInitialStates
      .unionByName(jobRunIntermediateStates)
      .unionByName(jobRunTerminalStates)
  }

  /**
   * Goal - identify the cumulative runtime of all concurrently running runs inside a run state so that run
   * runtime can be divided into cumulative runtime to calculate percent utilization of runstate for a specific run.
   * Runstate == state of a cluster and all concurrent jobs -- each time a cluster state or run state of a job changes
   * a new runstate is created. It's the sumProduct of cluster states and runs in clusterState
   */
  def jrcpDeriveCumulativeRuntimeByRunState(
                                             df: DataFrame,
                                             runStateLastToStart: Column,
                                             runStateFirstToEnd: Column
                                           ): DataFrame = {
    val runstateKeys = $"obs.organization_id" === $"lookup.organization_id" &&
      $"obs.cluster_id" === $"lookup.cluster_id" &&
      $"obs.unixTimeMS_state_start" === $"lookup.unixTimeMS_state_start" &&
      $"obs.unixTimeMS_state_end" === $"lookup.unixTimeMS_state_end"

    val startsBefore = $"lookup.run_state_start_epochMS" < $"obs.run_state_start_epochMS"
    val startsDuring = $"lookup.run_state_start_epochMS" > $"obs.run_state_start_epochMS" &&
      $"lookup.run_state_start_epochMS" < $"obs.run_state_end_epochMS" // exclusive
    val endsDuring = $"lookup.run_state_end_epochMS" > $"obs.run_state_start_epochMS" &&
      $"lookup.run_state_end_epochMS" < $"obs.run_state_end_epochMS" // exclusive
    val endsAfter = $"lookup.run_state_end_epochMS" > $"obs.run_state_end_epochMS"
    val startsEndsWithin = $"lookup.run_state_start_epochMS".between(
      $"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS") &&
      $"lookup.run_state_end_epochMS".between($"obs.run_state_start_epochMS", $"obs.run_state_end_epochMS") // inclusive

    val simplifiedJobRunByClusterState = df
      .filter('job_cluster_type === "existing") // only relevant for interactive clusters
      .withColumn("run_state_start_epochMS", runStateLastToStart)
      .withColumn("run_state_end_epochMS", runStateFirstToEnd)
      .select(
        'organization_id, 'run_id, 'cluster_id, 'run_state_start_epochMS, 'run_state_end_epochMS,
        'unixTimeMS_state_start, 'unixTimeMS_state_end
      )

    // sum of run_state_times starting before ending during
    val runStateBeforeEndsDuring = simplifiedJobRunByClusterState.alias("obs")
      .join(simplifiedJobRunByClusterState.alias("lookup"), runstateKeys && startsBefore && endsDuring)
      .withColumn("relative_runtime_in_runstate", $"lookup.run_state_end_epochMS" - $"obs.unixTimeMS_state_start") // runStateEnd minus clusterStateStart
      .select(
        $"obs.organization_id", $"obs.run_id", $"obs.cluster_id", $"obs.run_state_start_epochMS",
        $"obs.run_state_end_epochMS", $"obs.unixTimeMS_state_start", $"obs.unixTimeMS_state_end",
        'relative_runtime_in_runstate
      )

    // sum of run_state_times starting during ending after
    val runStateAfterBeginsDuring = simplifiedJobRunByClusterState.alias("obs")
      .join(simplifiedJobRunByClusterState.alias("lookup"), runstateKeys && startsDuring && endsAfter)
      .withColumn("relative_runtime_in_runstate", $"lookup.unixTimeMS_state_end" - $"obs.run_state_start_epochMS") // clusterStateEnd minus runStateStart
      .select(
        $"obs.organization_id", $"obs.run_id", $"obs.cluster_id", $"obs.run_state_start_epochMS",
        $"obs.run_state_end_epochMS", $"obs.unixTimeMS_state_start", $"obs.unixTimeMS_state_end",
        'relative_runtime_in_runstate
      )

    // sum of run_state_times starting and ending during
    val runStateBeginEndDuring = simplifiedJobRunByClusterState.alias("obs")
      .join(simplifiedJobRunByClusterState.alias("lookup"), runstateKeys && startsEndsWithin)
      .withColumn("relative_runtime_in_runstate", $"lookup.run_state_end_epochMS" - $"obs.run_state_start_epochMS") // runStateEnd minus runStateStart
      .select(
        $"obs.organization_id", $"obs.run_id", $"obs.cluster_id", $"obs.run_state_start_epochMS",
        $"obs.run_state_end_epochMS", $"obs.unixTimeMS_state_start", $"obs.unixTimeMS_state_end",
        'relative_runtime_in_runstate
      )

    runStateBeforeEndsDuring
      .unionByName(runStateAfterBeginsDuring)
      .unionByName(runStateBeginEndDuring)
      .groupBy('organization_id, 'run_id, 'cluster_id, 'unixTimeMS_state_start, 'unixTimeMS_state_end) // runstate
      .agg(
        sum('relative_runtime_in_runstate).alias("cum_runtime_in_cluster_state"), // runtime in clusterState
        (sum(lit(1)) - lit(1)).alias("overlapping_run_states") // subtract one for self run
      )
  }

  def jrcpAppendUtilAndCosts(df: DataFrame): DataFrame = {
    df
      .withColumn("cluster_type",
        when('job_cluster_type === "new", lit("automated"))
          .otherwise(lit("interactive"))
      )
      .withColumn("state_utilization_percent", 'runtime_in_cluster_state / 1000 / 3600 / 'uptime_in_state_H) // run runtime as percent of total state time
      .withColumn("run_state_utilization",
        when('cluster_type === "interactive", least('runtime_in_cluster_state / 'cum_runtime_in_cluster_state, lit(1.0)))
          .otherwise(lit(1.0))
      ) // determine share of cluster when interactive as runtime / all overlapping run runtimes
      .withColumn("overlapping_run_states", when('cluster_type === "interactive", 'overlapping_run_states).otherwise(lit(0)))
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
  }

  def jrcpAggMetricsToRun(df: DataFrame): DataFrame = {
    df
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
  }

  def jrcpDeriveSparkJobUtil(sparkJobLag2D: DataFrame, sparkTaskLag2D: DataFrame): DataFrame = {
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

    jobRunUtilRaw
      .groupBy('organization_id, 'db_job_id, 'db_id_in_job)
      .agg(
        sum('runTimeMS).alias("spark_task_runtimeMS"),
        round(sum('spark_task_runtime_H), 4).alias("spark_task_runtime_H")
      )
  }

  def jrcpJoinWithJobRunCostPotential(jobRunCostPotential: DataFrame)(df: DataFrame): DataFrame = {
    jobRunCostPotential.alias("jrCostPot")
      .join(
        df.withColumnRenamed("organization_id", "orgId").alias("jrSparkUtil"),
        $"jrCostPot.organization_id" === $"jrSparkUtil.orgId" &&
          $"jrCostPot.job_id" === $"jrSparkUtil.db_job_id" &&
          $"jrCostPot.id_in_job" === $"jrSparkUtil.db_id_in_job",
        "left"
      )
      .drop("db_job_id", "db_id_in_job", "orgId")
      .withColumn("job_run_cluster_util", round(('spark_task_runtime_H / 'worker_potential_core_H), 4))
  }

}
