package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.PipelineFunctions.fillForward
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils.{NoNewDataException, SchemaScrubber, SchemaTools, SparkSessionWrapper, TimeTypes}
import org.apache.log4j.Level
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, LongType, StringType}

object WorkflowsTransforms extends SparkSessionWrapper {

  import spark.implicits._

  /**
   * BEGIN Workflow generic functions
   */


  def getJobsBase(df: DataFrame): DataFrame = {
    val onlyOnceJobRecW = Window.partitionBy('organization_id, 'timestamp, 'actionName, 'requestId, $"response.statusCode").orderBy('timestamp)
    df.filter(col("serviceName") === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .withColumn("rnk", rank().over(onlyOnceJobRecW))
      .withColumn("rn", row_number.over(onlyOnceJobRecW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
  }

  def workflowsCleanseTasks(df: DataFrame, keys: Array[String], pathToTasksField: String): DataFrame = {
    val emptyDFWKeysAndCleansedTasks = Seq.empty[(String, Long, Long)].toDF("organization_id", "runId", "startEpochMS")
      .withColumn("cleansedTasks", lit(null).cast(Schema.minimumTasksSchema))

    if (SchemaTools.getAllColumnNames(df.schema).exists(c => c.startsWith(pathToTasksField))) { // tasks field exists
      if (df.select(col(pathToTasksField)).schema.fields.head.dataType.typeName == "array") { // tasks field is array
        val jobStatusByKeyW = Window.partitionBy(keys map col: _*)
        val tasksExplodedWKeys = df
          .filter(size(col(pathToTasksField)) >= 1)
          .select((keys map col) :+ explode(col(pathToTasksField)).alias("tasksToCleanse"): _*)

        val tasksChangeInventory = Map(
          "tasksToCleanse.new_cluster.custom_tags" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.new_cluster.custom_tags"),
          "tasksToCleanse.new_cluster.aws_attributes" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.new_cluster.aws_attributes"),
          "tasksToCleanse.new_cluster.azure_attributes" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.new_cluster.azure_attributes"),
          "tasksToCleanse.new_cluster.spark_conf" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.new_cluster.spark_conf"),
          "tasksToCleanse.new_cluster.spark_env_vars" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.new_cluster.spark_env_vars"),
          "tasksToCleanse.notebook_task.base_parameters" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.notebook_task.base_parameters")
        )

        tasksExplodedWKeys
          .select(SchemaTools.modifyStruct(tasksExplodedWKeys.schema, tasksChangeInventory): _*)
          .groupBy(keys map col: _*)
          .agg(collect_list('tasksToCleanse).alias("cleansedTasks"))
      } else emptyDFWKeysAndCleansedTasks // build empty DF with keys to allow the subsequent joins
    } else emptyDFWKeysAndCleansedTasks // build empty DF with keys to allow the subsequent joins
  }

  def workflowsCleanseJobClusters(df: DataFrame, keys: Array[String], pathToJobClustersField: String): DataFrame = {
    val emptyDFWKeysAndCleansedJobClusters = Seq.empty[(String, Long, Long)].toDF("organization_id", "runId", "startEpochMS")
      .withColumn("cleansedJobsClusters", lit(null).cast(Schema.minimumJobClustersSchema))

    if (SchemaTools.getAllColumnNames(df.schema).exists(c => c.startsWith(pathToJobClustersField))) { // job_clusters field exists
      if (df.select(col(pathToJobClustersField)).schema.fields.head.dataType.typeName == "array") { // job_clusters field is array
        val jobStatusByKeyW = Window.partitionBy(keys map col: _*)
        val jobClustersExplodedWKeys = df
          .filter(size(col(pathToJobClustersField)) > 0)
          .select((keys map col) :+ explode(col(pathToJobClustersField)).alias("jobClustersToCleanse"): _*)

        val jobClustersChangeInventory = Map(
          "jobClustersToCleanse.new_cluster.custom_tags" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "jobClustersToCleanse.new_cluster.custom_tags"),
          "jobClustersToCleanse.new_cluster.aws_attributes" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "jobClustersToCleanse.new_cluster.aws_attributes"),
          "jobClustersToCleanse.new_cluster.azure_attributes" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "jobClustersToCleanse.new_cluster.azure_attributes"),
          "jobClustersToCleanse.new_cluster.spark_conf" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "jobClustersToCleanse.new_cluster.spark_conf"),
          "jobClustersToCleanse.new_cluster.spark_env_vars" -> SchemaTools.structToMap(jobClustersExplodedWKeys, "jobClustersToCleanse.new_cluster.spark_env_vars")
        )

        jobClustersExplodedWKeys
          .select(SchemaTools.modifyStruct(jobClustersExplodedWKeys.schema, jobClustersChangeInventory): _*)
          .groupBy(keys map col: _*)
          .agg(collect_list('jobClustersToCleanse).alias("cleansedJobsClusters"))
      } else emptyDFWKeysAndCleansedJobClusters // build empty DF with keys to allow the subsequent joins
    } else emptyDFWKeysAndCleansedJobClusters // build empty DF with keys to allow the subsequent joins
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
  // todo -- add new fields to be filled (if there are new ones)
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
      // replace new_settings.tasks with cleansed tasks
      structsCleaner("new_settings.tasks") = col("cleansedTasks")
      val cleansedMTJTasksDF = workflowsCleanseTasks(cleansedDF, keys, "new_settings.tasks")
      cleansedDF.join(cleansedMTJTasksDF, keys.toSeq, "left")
    } else cleansedDF

    cleansedDF = if (containsMTJJobClusters) {
      // replace new_settings.job_clusters with cleansed job_clusters
      structsCleaner("new_settings.job_clusters") = col("cleansedJobsClusters")
      val cleansedMTJJobClustersDF = workflowsCleanseJobClusters(cleansedDF, keys, "new_settings.job_clusters")
      cleansedDF.join(cleansedMTJJobClustersDF, keys.toSeq, "left")
    } else cleansedDF

    cleansedDF
      .modifyStruct(structsCleaner.toMap)
      .drop("cleansedTasks", "cleansedJobsClusters") // cleanup temporary cleaner fields
  }

  /**
   * BEGIN JOB RUNS SILVER
   */

//  val jobRunsLookups: Map[String, DataFrame] =
  def jobRunsInitializeLookups(lookups: (PipelineTable, DataFrame)*): Map[String, DataFrame] = {
    lookups
      .filter(_._1.exists)
      .map(lookup => {
      (lookup._1.name, lookup._2)
    }).toMap
  }

  def jobRunsDeriveCompletedRuns(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName.isin("runSucceeded", "runFailed"))
      .select(
        'organization_id,
        'timestamp,
        when('multitaskParentRunId.isNull, 'runId.cast("long")).otherwise('multitaskParentRunId.cast("long")).alias("jobRunId"),
        'runId.cast("long").alias("taskRunId"),
        'jobId.cast("long").alias("completedJobId"),
        'multitaskParentRunId.alias("multitaskParentRunId_Completed"),
        'parentRunId.alias("parentRunId_Completed"),
        'taskKey.alias("taskKey_Completed"),
        'taskDependencies.alias("taskDependencies_Completed"),
        'repairId.alias("repairId_Completed"),
        'idInJob.cast("long"),
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
        'organization_id,
        'timestamp,
        'run_id.cast("long").alias("jobRunId"), // TODO -- add task_run_id and add to the join -- but we need DBR to emit multitask_parent_id before we can do this
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
  def jobRunsDeriveRunsLaunched(df: DataFrame, firstRunSemanticsW: WindowSpec, arrayStringSchema: ArrayType): DataFrame = {
    df
      .filter('actionName.isin("runNow"))
      .select(
        'organization_id,
        'timestamp,
        'job_id.cast("long").alias("submissionJobId"),
        get_json_object($"response.result", "$.run_id").cast("long").alias("jobRunId"),
        'timestamp.alias("submissionTime"),
        lit("manual").alias("jobTriggerType_runNow"),
        'workflow_context.alias("workflow_context_runNow"),
        struct(
          from_json('jar_params, arrayStringSchema).alias("jar_params"),
          from_json('python_params, arrayStringSchema).alias("python_params"),
          from_json('spark_submit_params, arrayStringSchema).alias("spark_submit_params"),
          from_json('notebook_params, arrayStringSchema).alias("notebook_params")
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
        'organization_id,
        'timestamp,
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
        'organization_id,
        'timestamp,
        get_json_object($"response.result", "$.run_id").cast("long").alias("jobRunId"),
        'run_name,
        'timestamp.alias("submissionTime"),
        'job_clusters, // json array struct string
        'new_cluster, // json struct string
        'existing_cluster_id,
        'workflow_context.alias("workflow_context_submitRun"),
        'notebook_task,
        'spark_python_task,
        'spark_jar_task,
        'shell_command_task,
        'pipeline_task,
        'tasks, // json array struct string
        'libraries,
        'access_control_list,
        'git_source,
        'timeout_seconds.cast("string").alias("timeout_seconds"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      )
      .filter('jobRunId.isNotNull) // removed to capture failed
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
      .scrubSchema // required to remove nasty map chars from structified strings

  }

  def jobRunsDeriveRunStarts(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    // TODO -- ISSUE 488 -- SQL runStarts do not emit clusterId (i.e. sqlEndpointId) need this for run costs
    // TODO -- ISSUE 479 -- DLT runStarts do not emit clusterId -- need this for run costs
    df.filter('actionName.isin("runStart"))
      .select(
        'organization_id,
        'timestamp,
        'jobId.cast("long").alias("runStartJobId"),
        when('multitaskParentRunId.isNull, 'runId).otherwise('multitaskParentRunId.cast("long")).alias("jobRunId"),
        'runId.cast("long").alias("taskRunId"),
        'multitaskParentRunId.alias("multitaskParentRunId_Started"),
        'parentRunId.alias("parentRunId_Started"),
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

    val arrayStringSchema = ArrayType(StringType, containsNull = true)
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
    val runNowStart = jobRunsDeriveRunsLaunched(df, firstJobRunSemanticsW, arrayStringSchema)

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
    val allSubmissions = runNowStart
      .unionByName(runTriggered, allowMissingColumns = true)
      .unionByName(runSubmitStart, allowMissingColumns = true)

    // Find the corresponding runStart action for the completed jobs
    // Lookback 30 days for laggard starts prior to current run
    val runStarts = jobRunsDeriveRunStarts(df, firstTaskRunSemanticsW)

    val jobRunsMaster = allCompletes
      .join(runStarts, Seq("organization_id", "jobRunId", "taskRunId"), "left")
      .join(allSubmissions, Seq("organization_id", "jobRunId"), "left")
      .join(allCancellations, Seq("organization_id", "jobRunId"), "left") // todo -- add support for cancelled tasks (i.e. join on taskRunId??) waiting on DBR update
      .withColumn("runId", coalesce('taskRunId, 'jobRunId).cast("long"))

    // TODO -- add repairRuns action
    jobRunsMaster
      .select(
        'organization_id,
        coalesce('runStartJobId, 'completedJobId, 'submissionJobId).cast("long").alias("jobId"),
        'jobRunId.cast("long"),
        'taskRunId.cast("long"),
        coalesce('taskKey_runStart, 'taskKey_Completed).alias("taskKey"),
        from_json(coalesce('taskDependencies_runStart, 'taskDependencies_Completed), arrayStringSchema).alias("taskDependencies"),
        'runId,
        coalesce('multitaskParentRunId_Started, 'multitaskParentRunId_Completed).cast("long").alias("multitaskParentRunId"),
        coalesce('parentRunId_Started, 'parentRunId_Completed).cast("long").alias("parentRunId"),
        coalesce('repairId_runStart, 'repairId_Completed).cast("long").alias("repairId"),
        coalesce('taskRunId, 'idInJob).cast("long").alias("idInJob"),
        TransformFunctions.subtractTime(
          'submissionTime,
          coalesce(array_max(array('completionTime, 'cancellationTime)), lit(etlUntilTime.asUnixTimeMilli))
        ).alias("JobRunTime"), // run launch time until terminal event
        TransformFunctions.subtractTime(
          'startTime,
          coalesce(array_max(array('completionTime, 'cancellationTime)), lit(etlUntilTime.asUnixTimeMilli))
        ).alias("JobExecutionRunTime"), // from cluster up and run begin until terminal event
        'run_name,
        coalesce('jobClusterType_Started, 'jobClusterType_Completed).alias("jobClusterType"),
        coalesce('jobTaskType_Started, 'jobTaskType_Completed).alias("jobTaskType"),
        coalesce('jobTriggerType_Started, 'jobTriggerType_Completed, 'jobTriggerType_runNow).alias("jobTriggerType"),
        when('cancellationRequestId.isNotNull, "Cancelled")
          .otherwise('jobTerminalState)
          .alias("jobTerminalState"),
        'clusterId,
        struct(
          'existing_cluster_id,
          structFromJson(spark, jobRunsMaster, "new_cluster"),
          structFromJson(spark, jobRunsMaster, "tasks", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumTasksSchema),
          structFromJson(spark, jobRunsMaster, "job_clusters", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumJobClustersSchema),
          structFromJson(spark, jobRunsMaster, "libraries", isArrayWrapped = true),
          structFromJson(spark, jobRunsMaster, "access_control_list", isArrayWrapped = true),
          structFromJson(spark, jobRunsMaster, "git_source")
        ).alias("submitRun_details"),
        'submission_params,
        coalesce('workflow_context_runNow, 'workflow_context_submitRun).alias("workflow_context"),
        'notebook_task,
        'spark_python_task,
        'spark_jar_task,
        'shell_command_task,
        'pipeline_task,
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
      .withColumn("timestamp", $"JobRunTime.startEpochMS") // TS lookup key added for next steps (launch time)
      .withColumn("startEpochMS", $"JobRunTime.startEpochMS") // set launch time as TS key
      .scrubSchema
  }

  def structifyLookupMeta(df: DataFrame): DataFrame = {
    df
      .withColumn("taskDetail",
        struct(
          structFromJson(spark, df, "notebook_task", allNullMinimumSchema = Schema.minimumNotebookTaskSchema),
          structFromJson(spark, df, "spark_python_task"),
          structFromJson(spark, df, "spark_jar_task"),
//          structFromJson(spark, df, "shell_command_task"),
          structFromJson(spark, df, "pipeline_task")
        ).alias("taskDetail")
      )
      .drop("notebook_task", "spark_python_task", "spark_jar_task", "shell_command_task", "pipeline_task")
      .scrubSchema
  }

  def cleanseCreatedNestedStructures(keys: Array[String])(df: DataFrame): DataFrame = {

    val cleansedTasksDF = workflowsCleanseTasks(df, keys, "submitRun_details.tasks")
    val cleansedJobClustersDF = workflowsCleanseJobClusters(df, keys, "submitRun_details.job_clusters")

    val dfWCleansedJobsAndTasks = df
      .join(cleansedTasksDF, keys.toSeq, "left")
      .join(cleansedJobClustersDF, keys.toSeq, "left")

    // todo -- implement the newClusterCleaner everywhere
    val tasksAndJobClustersCleansingInventory = Map(
      "submitRun_details.tasks" -> col("cleansedTasks"),
      "submitRun_details.job_clusters" -> col("cleansedJobsClusters"),
      "submitRun_details.tasks.notebook_task.base_parameters" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "submitRun_details.tasks.notebook_task.base_parameters"),
      "submitRun_details.tasks.shell_command_task.env_vars" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "submitRun_details.tasks.shell_command_task.env_vars"),
      "taskDetail.notebook_task.base_parameters" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "taskDetail.notebook_task.base_parameters"),
      "taskDetail.shell_command_task.env_vars" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "taskDetail.shell_command_task.env_vars")
    ) ++
      PipelineFunctions.newClusterCleaner(dfWCleansedJobsAndTasks, "submitRun_details.new_cluster") ++
      PipelineFunctions.newClusterCleaner(dfWCleansedJobsAndTasks, "submitRun_details.tasks.new_cluster")

    dfWCleansedJobsAndTasks
      .modifyStruct(tasksAndJobClustersCleansingInventory) // overwrite nested complex structures with cleansed structures
      .drop("cleansedTasks", "cleansedJobsClusters") // cleanup temporary cleaner fields
      .scrubSchema(SchemaScrubber(cullNullTypes = true))

  }

  /**
   * looks up the cluster_name based on id first from job_status_silver and if not present there fallback to latest
   * snapshot prior to the run
   */
  def jobRunsAppendClusterName(lookups: Map[String, DataFrame])(df: DataFrame): DataFrame = {

    val runsWClusterNames1 = if (lookups.contains("cluster_spec_silver")) {
      df.toTSDF("timestamp", "organization_id", "clusterId")
        .lookupWhen(
          lookups("cluster_spec_silver")
            .toTSDF("timestamp", "organization_id", "clusterId")
        ).df
    } else df

    val runsWClusterNames2 = if (lookups.contains("clusters_snapshot_bronze")) {
      runsWClusterNames1
        .toTSDF("timestamp", "organization_id", "clusterId")
        .lookupWhen(
          lookups("clusters_snapshot_bronze")
            .toTSDF("timestamp", "organization_id", "clusterId")
        ).df
    } else runsWClusterNames1

    runsWClusterNames2
  }

  /**
   * looks up the job name based on id first from job_status_silver and if not present there fallback to latest
   * snapshot prior to the run
   */
  def jobRunsAppendJobMeta(lookups: Map[String, DataFrame])(df: DataFrame): DataFrame = {

    val runsWithJobName1 = if (lookups.contains("job_status_silver")) {
      df
        .toTSDF("timestamp", "organization_id", "jobId")
        .lookupWhen(
          lookups("job_status_silver")
            .toTSDF("timestamp", "organization_id", "jobId")
        ).df
    } else df

    val runsWithJobName2 = if (lookups.contains("jobs_snapshot_bronze")) {
      runsWithJobName1
        .toTSDF("timestamp", "organization_id", "jobId")
        .lookupWhen(
          lookups("jobs_snapshot_bronze")
            .toTSDF("timestamp", "organization_id", "jobId")
        ).df
    } else df

    runsWithJobName2
      .withColumn("jobName", coalesce('jobName, 'run_name))

  }

  /**
   * It's imperative that all nested runs be nested within the jobRun record to ensure cost accuracy downstream in
   * jrcp -- without it jrcp will double count costs as both the parent and the child will have an associated cost
   *
   * A "workflow" in this context isa dbutils.notebook.run execution -- it does spin up a job run in the back end
   * and will have a workflow_context (field) with root_run_id and parent_run_id. All of these are rolled to the root
   * to avoid the need to multi-layer joining. It's up to the customer to complete this as needed as the depths can
   * get very large.
   *
   * An multi-task job (mtj) is a job that has at least one task identified (all jobs runs after the change in 2022).
   * MTJs can execute notebooks which can also run nested workflows using dbutils.notebook.run.
   *
   * Workflows can be launched interactively from a notebook or through an mtj; thus it's necessary to account for
   * both scenarios hence the double join in the last DF.
   *
   * Nested runs DO NOT mean tasks inside a jobrun as these are still considered root level tasks. A nested run
   * is only launched via dbutils.notebook.run either manually or through an MTJ.
   *
   * It may be possible to utilize a single field to report both of these as it appears there can never be a child
   * without a workflow child but the reverse is not true. This can be reviewed with customer to determine if this
   * is a valid assumption and these can be coalesced, but for now, for safety, they are being kept separate until
   * all scenarios can be identified
   */
  def jobRunsRollupWorkflowsAndChildren(df: DataFrame): DataFrame = {

    // identify root level task runs
    val rootTaskRuns = df
      .filter('parentRunId.isNull && get_json_object('workflow_context, "$.root_run_id").isNull)

    // pull only workflow children as defined by having a workflow_context.root_run_id
    val workflowChildren = df
      .filter(get_json_object('workflow_context, "$.root_run_id").isNotNull)

    // prepare the nesting by getting keys and the entire record as a nested record
    val workflowChildrenForNesting = workflowChildren
      .withColumn("parentRunId", get_json_object('workflow_context, "$.root_run_id").cast("long"))
      .withColumn("workflowChild", struct(workflowChildren.schema.fieldNames map col: _*))
      .groupBy('organization_id, 'parentRunId)
      .agg(collect_list('workflowChild).alias("workflow_children"))

    // get all the children identified as having a parentRunId as they need to be rolled up
    val children = df
      .filter('parentRunId.isNotNull)
      .join(workflowChildrenForNesting, Seq("organization_id", "parentRunId"), "left")

    // prepare the nesting by getting keys and the entire record as a nested record
    val childrenForNesting = children
      .withColumn("child", struct(children.schema.fieldNames map col: _*))
      .groupBy('organization_id, 'parentRunId)
      .agg(collect_list('child).alias("children"))
      .withColumnRenamed("parentRunId", "taskRunId") // for simple joining

    // deliver root task runs with workflows and children nested within the root
    rootTaskRuns
      .join(childrenForNesting, Seq("organization_id", "taskRunId"), "left") // workflows in mtjs
      .join(
        workflowChildrenForNesting.withColumnRenamed("parentRunId", "taskRunId"), // for simple joining
        Seq("organization_id", "taskRunId"),
        "left"
      )

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
