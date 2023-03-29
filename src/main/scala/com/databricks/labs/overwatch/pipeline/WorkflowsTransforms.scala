package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.PipelineFunctions.fillForward
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object WorkflowsTransforms extends SparkSessionWrapper {

  import spark.implicits._

  /**
   * BEGIN Workflow generic functions
   */

  /**
   * When colIfExists expression returns a null type due to conversion to a different type set it to null and
   * type case it to the definedNullType
   * @param df df to fix
   * @param fieldName name of the field to test for null types
   * @param colIfExists expression that creates the field
   * @param definedNullType type to cast to when expression results in nullType
   * @return
   */
  private def handleRootNull(df: DataFrame, fieldName: String, colIfExists: Column, definedNullType: DataType): Column = {
    val nullField = lit(null).cast(definedNullType).alias(fieldName)
    if (SchemaTools.nestedColExists(df.schema, fieldName)) {
      if (df.select(fieldName).schema.exists(f => f.dataType != NullType)) {
        colIfExists.alias(fieldName)
      } else {
        nullField
      }
    } else nullField
  }

  def getJobsBase(df: DataFrame): DataFrame = {
    val onlyOnceJobRecW = Window.partitionBy('organization_id, 'timestamp, 'actionName, 'requestId, $"response.statusCode", 'runId).orderBy('timestamp)
    df.filter(col("serviceName") === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .withColumn("rnk", rank().over(onlyOnceJobRecW))
      .withColumn("rn", row_number.over(onlyOnceJobRecW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
  }

  /**
   * Clean "tasks" field. This is done by exploding the the tasks, cleaning them and then rewrapping them to array
   * using collect_list
   * @param df df that contains the "tasks" field within the jobs / jobruns context
   * @param keys dataframe keys as defined in pipeline_target
   * @param emptyKeysDF empty DF with the typed keys and no data to protect against empty arrays
   * @param pathToTasksField dot-delimited location of tasks such as settings.tasks
   * @param cleansedTaskAlias alias of the clean tasks to return
   * @return
   */
  def workflowsCleanseTasks(
                             df: DataFrame,
                             keys: Array[String],
                             emptyKeysDF: DataFrame,
                             pathToTasksField: String,
                             cleansedTaskAlias: String = "cleansedTasks"
                           ): DataFrame = {
    val emptyDFWKeysAndCleansedTasks = emptyKeysDF
      .withColumn(cleansedTaskAlias, lit(null).cast(Schema.minimumTasksSchema))

    if (SchemaTools.getAllColumnNames(df.schema).exists(c => c.startsWith(pathToTasksField))) { // tasks field exists
      if (df.select(col(pathToTasksField)).schema.fields.head.dataType.typeName == "array") { // tasks field is array
        val recordsWithTasks = df
          .filter(size(col(pathToTasksField)) > 0)

        if (!recordsWithTasks.isEmpty) { // if no tasks to cleanse present
          val tasksExplodedWKeys = recordsWithTasks
            .select((keys map col) :+ explode(col(pathToTasksField)).alias("tasksToCleanse"): _*)

          val tasksChangeInventory = Map(
            "tasksToCleanse.notebook_task.base_parameters" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.notebook_task.base_parameters"),
            "tasksToCleanse.python_wheel_task.named_parameters" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.python_wheel_task.named_parameters"),
            "tasksToCleanse.sql_task.parameters" -> SchemaTools.structToMap(tasksExplodedWKeys, "tasksToCleanse.sql_task.parameters")
          ) ++ PipelineFunctions.newClusterCleaner(tasksExplodedWKeys, "tasksToCleanse.new_cluster")

          tasksExplodedWKeys
            .select(SchemaTools.modifyStruct(tasksExplodedWKeys.schema, tasksChangeInventory): _*)
            .groupBy(keys map col: _*)
            .agg(collect_list('tasksToCleanse).alias(cleansedTaskAlias))
        } else emptyDFWKeysAndCleansedTasks
      } else emptyDFWKeysAndCleansedTasks // build empty DF with keys to allow the subsequent joins
    } else emptyDFWKeysAndCleansedTasks // build empty DF with keys to allow the subsequent joins
  }

  /**
   * Clean "job_clusters" field. This is done by exploding the the job_clusters, cleaning them and then
   * rewrapping them to array using collect_list
   * @param df df that contains the "job_clusters" field within the jobs / jobruns context
   * @param keys dataframe keys as defined in pipeline_target
   * @param emptyKeysDF empty DF with the typed keys and no data to protect against empty arrays
   * @param pathToJobClustersField dot-delimited location of tasks such as settings.job_clusters
   * @param cleansedJobClustersAlias alias of the clean job_clusters to return
   * @return
   */
  def workflowsCleanseJobClusters(
                                   df: DataFrame,
                                   keys: Array[String],
                                   emptyKeysDF: DataFrame,
                                   pathToJobClustersField: String,
                                   cleansedJobClustersAlias: String = "cleansedJobsClusters"
                                 ): DataFrame = {
    val emptyDFWKeysAndCleansedJobClusters = emptyKeysDF
      .withColumn(cleansedJobClustersAlias, lit(null).cast(Schema.minimumJobClustersSchema))

    if (SchemaTools.getAllColumnNames(df.schema).exists(c => c.startsWith(pathToJobClustersField))) { // job_clusters field exists
      if (df.select(col(pathToJobClustersField)).schema.fields.head.dataType.typeName == "array") { // job_clusters field is array
        val recordsWithJobClusters = df
          .filter(size(col(pathToJobClustersField)) > 0)

        if (!recordsWithJobClusters.isEmpty) {
          val jobClustersExplodedWKeys = recordsWithJobClusters
            .select((keys map col) :+ explode(col(pathToJobClustersField)).alias("jobClustersToCleanse"): _*)

          val jobClustersChangeInventory = PipelineFunctions.newClusterCleaner(jobClustersExplodedWKeys, "jobClustersToCleanse.new_cluster")

          jobClustersExplodedWKeys
            .select(SchemaTools.modifyStruct(jobClustersExplodedWKeys.schema, jobClustersChangeInventory): _*)
            .groupBy(keys map col: _*)
            .agg(collect_list('jobClustersToCleanse).alias(cleansedJobClustersAlias))
        } else emptyDFWKeysAndCleansedJobClusters
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

  def jobStatusDeriveJobsStatusBase(df: DataFrame): DataFrame = {
    val jobStatusJobIdBuilder = when('actionName === "create",
      get_json_object($"response.result", "$.job_id").cast("long")
    )
      .when('actionName === "changeJobAcl", 'resourceId.cast("long"))
      .otherwise('job_id).cast("long")

    val jobStatusNameBuilder = when('actionName === "create", 'name)
      .when('actionName.isin("update", "reset"), get_json_object('new_settings, "$.name"))
      .otherwise(lit(null).cast("string"))

    df
      .withColumn("jobId", jobStatusJobIdBuilder)
      .withColumn("jobName", jobStatusNameBuilder)
      .filter('jobId.isNotNull) // create errors and delete errors will be filtered out
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
   * Generate cluster_spec field and fill it temporally with the last value when
   * the details are not provided
   */
  // todo -- is this func needed for jrcp?
  //  it could be that we need the latest cluster spec for jrcp but I believe we can just get the latest
  //  cluster_id since it should be present for all started run
  //  def jobStatusAppendAndFillTemporalClusterSpec(df: DataFrame): DataFrame = {
  //    val temporalJobClusterSpec = df
  //      .select('organization_id, 'jobId, 'timestamp,
  //        struct(
  //          'existing_cluster_id,
  //          'new_cluster,
  //          'job_clusters
  //          // TODO - tasks.new_clusters?
  //        ).alias("temporal_cluster_spec")
  //      )
  //      .toTSDF("timestamp", "organization_id", "jobId")
  //
  //    df
  //      .drop("existing_cluster_id", "new_cluster", "job_clusters")
  //      .toTSDF("timestamp", "organization_id", "jobId")
  //      .lookupWhen(temporalJobClusterSpec)
  //      .df
  //      .selectExpr("*", "temporal_cluster_spec.existing_cluster_id", "temporal_cluster_spec.new_cluster", "temporal_cluster_spec.job_clusters")
  //      .drop("temporal_cluster_spec")
  //      .withColumn("cluster_spec", struct(
  //        'existing_cluster_id,
  //        'new_cluster,
  //        'job_clusters
  //      ))
  //      .scrubSchema // cleans schema after creating structs
  //      .drop("existing_cluster_id", "new_cluster", "job_clusters")
  //  }

  /**
   * Fill the column using many lookups but take into account fields_to_remove
   * fields_to_remove convert an update into a reset but is optional
   * if field name is not present in the array and action is update fill it in logical order (new_settings.x, secondary lookups)
   * if field name IS present in the array do not fill it as it is a reset and must == new_settings.x
   * @param colToFillName Name of column to fill
   * @param w windowSpec for fillForward
   * @param snapLookupSettingsColName Snapshot lookup settings struct col name. If this is left empty no lookup will be performed
   * @param altLookupColName alternate name for lookup column (if different than colToFillName)
   * @param fillForwardColToFill Whether or not to attempt to backfill the colToFillName from it's own history
   * @param extraLookupCols additional places to look as a Seq[Column]
   * @return
   */
  def jobStatusBuildLookupLogic(
                                 colToFillName: String,
                                 w: WindowSpec,
                                 sourceDFSchema: StructType,
                                 snapLookupSettingsColName: Option[String] = None,
                                 altLookupColName: Option[String] = None,
                                 fillForwardColToFill: Boolean = true,
                                 extraLookupCols: Seq[Column] = Seq()
                               ): Column = {
    val drivingColType = sourceDFSchema.filter(_.name == colToFillName).head.dataType
    val lookupColName = altLookupColName.getOrElse(colToFillName)
    val newSettingValue = get_json_object('new_settings, "$." + lookupColName).cast(drivingColType)
    val updateCreateResetFillLogic = if (fillForwardColToFill) {
      coalesce(
        newSettingValue,
        col(colToFillName)
      )
    } else newSettingValue

    val fieldIsReset = array_contains(from_json('fields_to_remove, ArrayType(StringType)), lookupColName)

    val orderedLookups = if (snapLookupSettingsColName.nonEmpty) {
      val snapLookupLogic = get_json_object(col(snapLookupSettingsColName.get), "$." + lookupColName).cast(drivingColType)
      Seq(updateCreateResetFillLogic, snapLookupLogic) ++ extraLookupCols
    } else {
      Seq(updateCreateResetFillLogic) ++ extraLookupCols
    }
    when('actionName.isin("update") && fieldIsReset, newSettingValue)
      .otherwise(fillForward(colToFillName, w, orderedLookups, colToFillHasPriority = false))
      .alias(colToFillName)
  }

  /**
   * lookup and fill forward common metadata
   */
  // TODO -- when update -- fillForward and nullify fieldsToRemove
  // TODO -- when reset -- do not fill forward
  def jobStatusDeriveBaseLookupAndFillForward(lastJobStatus: WindowSpec)(df: DataFrame): DataFrame = {
    val rawDFSchema = df.schema

    df
      .select(
        'organization_id,
        'serviceName,
        'actionName,
        'timestamp,
        'jobId,
        jobStatusBuildLookupLogic("jobName", lastJobStatus, rawDFSchema, Some("lookup_settings"), Some("name")),
        jobStatusBuildLookupLogic("tags", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        fillForward("job_type", lastJobStatus),
        fillForward("format", lastJobStatus, Seq(get_json_object('lookup_settings, "$.format"))),
        jobStatusBuildLookupLogic("schedule", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        jobStatusBuildLookupLogic("email_notifications", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        'notebook_task,
        'spark_python_task,
        'python_wheel_task,
        'spark_jar_task,
        'spark_submit_task,
        'shell_command_task,
        'pipeline_task,
        jobStatusBuildLookupLogic("existing_cluster_id", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        jobStatusBuildLookupLogic("job_clusters", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        jobStatusBuildLookupLogic("new_cluster", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        jobStatusBuildLookupLogic("tasks", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        jobStatusBuildLookupLogic("libraries", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        jobStatusBuildLookupLogic("git_source", lastJobStatus, rawDFSchema, Some("lookup_settings")),
        fillForward("is_from_dlt", lastJobStatus),
        jobStatusBuildLookupLogic("max_concurrent_runs", lastJobStatus, rawDFSchema, Some("lookup_settings")).cast("long"),
        jobStatusBuildLookupLogic("max_retries", lastJobStatus, rawDFSchema, Some("lookup_settings")).cast("long"),
        jobStatusBuildLookupLogic("timeout_seconds", lastJobStatus, rawDFSchema, Some("lookup_settings")).cast("long"),
        jobStatusBuildLookupLogic("retry_on_timeout", lastJobStatus, rawDFSchema, Some("lookup_settings")).cast("boolean"),
        jobStatusBuildLookupLogic("min_retry_interval_millis", lastJobStatus, rawDFSchema, Some("lookup_settings")).cast("long"),
        fillForward("run_as_user_name", lastJobStatus),
        fillForward("access_control_list", lastJobStatus),
        'aclPermissionSet,
        'grants,
        'targetUserId,
        'sessionId,
        'requestId,
        'userAgent,
        'userIdentity,
        'response,
        'sourceIPAddress,
        when('actionName === "create", $"userIdentity.email").alias("created_by"),
        when('actionName === "create", 'timestamp).alias("created_ts"),
        when('actionName === "delete", $"userIdentity.email").alias("deleted_by"),
        when('actionName.isin("update", "reset"), $"userIdentity.email").alias("last_edited_by"),
        when('actionName.isin("update", "reset"), 'timestamp).alias("last_edited_ts"),
        'snap_lookup_created_by,
        'snap_lookup_created_time
      )
      .withColumn("created_by", coalesce(fillForward("created_by", lastJobStatus), 'snap_lookup_created_by))
      .withColumn("created_ts", coalesce(fillForward("created_ts", lastJobStatus), 'snap_lookup_created_time))
      .withColumn("deleted_ts", when('actionName === "delete", 'timestamp))
      .withColumn("last_edited_by", coalesce(fillForward("last_edited_by", lastJobStatus, Seq('created_by))))
      .withColumn("last_edited_ts", when('actionName.isin("update", "reset"), 'timestamp))
      .withColumn("last_edited_ts", coalesce(fillForward("last_edited_ts", lastJobStatus, Seq('created_ts))))
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
        $"settings.tags".alias("tags"),
        $"settings.email_notifications".alias("email_notifications"),
        $"settings.existing_cluster_id".alias("existing_cluster_id"),
        $"settings.job_clusters".alias("job_clusters"),
        $"settings.new_cluster".alias("new_cluster"),
        $"settings.tasks".alias("tasks"),
        $"settings.libraries".alias("libraries"),
        $"settings.git_source".alias("git_source"),
        $"settings.max_concurrent_runs".alias("max_concurrent_runs"),
        $"settings.max_retries".alias("max_retries"),
        $"settings.retry_on_timeout".alias("retry_on_timeout"),
        $"settings.min_retry_interval_millis".alias("min_retry_interval_millis"),
        struct(
          $"settings.notebook_task".alias("notebook_task"),
          $"settings.spark_python_task".alias("spark_python_task"),
          $"settings.python_wheel_task".alias("python_wheel_task"),
          $"settings.spark_jar_task".alias("spark_jar_task"),
          $"settings.spark_submit_task".alias("spark_submit_task"),
          $"settings.shell_command_task".alias("shell_command_task"),
          $"settings.pipeline_task".alias("pipeline_task")
        ).alias("task_detail_legacy"),
        $"settings.timeout_seconds".alias("timeout_seconds"),
        $"settings.schedule".alias("schedule"),
        'creator_user_name.alias("created_by"),
        'created_time.alias("created_ts")
      )
  }

  def jobStatusFirstRunImputeFromSnap(
                                       isFirstRunAndJobsSnapshotHasRecords: Boolean,
                                       jobsBaseHasRecords: Boolean,
                                       jobsSnapshotDFComplete: DataFrame,
                                       fromTime: TimeTypes,
                                       tmpWorkingDir: String
                                     )(df: DataFrame): DataFrame = {
    // get job ids that are present in snapshot but not present in historical audit logs
    if (isFirstRunAndJobsSnapshotHasRecords) { // is first run and snapshot is populated
      val missingJobIds = jobStatusDeriveFirstRunMissingJobIDs(jobsBaseHasRecords, jobsSnapshotDFComplete, df)

      // impute records for jobs in snapshot not in audit (i.e. pre-existing pools prior to audit logs capture)
      val imputedFirstRunJobRecords = jobStatusDeriveFirstRunRecordImputesFromSnapshot(
        jobsSnapshotDFComplete, missingJobIds, fromTime
      )

      if (jobsBaseHasRecords) {
        // using delta writer merge nested complex structs -- not possible with a union
        val schemaMergePath = s"${tmpWorkingDir}/jobStatus/firstRun/${fromTime.asUnixTimeMilli}"
        spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(schemaMergePath)
        imputedFirstRunJobRecords.write.format("delta").mode("append").option("mergeSchema", "true").save(schemaMergePath)
        spark.conf.set("spark.databricks.delta.formatCheck.enabled", "true")
        spark.read.format("delta").load(schemaMergePath)
      } else imputedFirstRunJobRecords
    } else if (jobsBaseHasRecords) df
    else { // not first run AND no new audit data break out and progress timeline
      val msg = s"No new jobs audit records found, progressing timeline and appending no new records"
      throw new NoNewDataException(msg, Level.WARN, allowModuleProgression = true)
    }


  }

  /**
   * Convert complex json columns to typed structs
   */
  def jobStatusStructifyJsonCols(cacheParts: Int)(df: DataFrame): DataFrame = {
    val dfc = df.repartition(cacheParts).cache() // persisting to speed up schema inference
    dfc.count()
    val dfCols = dfc.columns
    val colsToRebuild = Array(
      "email_notifications", "tags", "schedule", "libraries", "job_clusters", "tasks", "new_cluster",
      "git_source", "access_control_list", "grants", "notebook_task", "spark_python_task", "spark_jar_task",
      "python_wheel_task", "spark_submit_task", "pipeline_task", "shell_command_task"
    ).map(_.toLowerCase)
    val baseSelects = dfCols.filterNot(cName => colsToRebuild.contains(cName.toLowerCase)) map col
    val structifiedCols: Array[Column] = Array(
      structFromJson(spark, dfc, "job_clusters", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumJobClustersSchema),
      structFromJson(spark, dfc, "email_notifications", allNullMinimumSchema = Schema.minimumEmailNotificationsSchema),
      structFromJson(spark, dfc, "tags"),
      structFromJson(spark, dfc, "schedule", allNullMinimumSchema = Schema.minimumScheduleSchema),
      structFromJson(spark, dfc, "new_cluster", allNullMinimumSchema = Schema.minimumNewClusterSchema),
      structFromJson(spark, dfc, "tasks", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumTasksSchema),
      structFromJson(spark, dfc, "libraries", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumLibrariesSchema),
      structFromJson(spark, dfc, "git_source", allNullMinimumSchema = Schema.minimumGitSourceSchema),
      structFromJson(spark, dfc, "access_control_list", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumAccessControlListSchema),
      structFromJson(spark, dfc, "grants", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumGrantsSchema),
      structFromJson(spark, dfc, "notebook_task", allNullMinimumSchema = Schema.minimumNotebookTaskSchema),
      structFromJson(spark, dfc, "spark_python_task", allNullMinimumSchema = Schema.minimumSparkPythonTaskSchema),
      structFromJson(spark, dfc, "spark_jar_task", allNullMinimumSchema = Schema.minimumSparkJarTaskSchema),
      structFromJson(spark, dfc, "spark_submit_task", allNullMinimumSchema = Schema.minimumSparkSubmitTaskSchema),
      structFromJson(spark, dfc, "shell_command_task", allNullMinimumSchema = Schema.minimumShellCommandTaskSchema),
      structFromJson(spark, dfc, "pipeline_task", allNullMinimumSchema = Schema.minimumPipelineTaskSchema)
    )


    // Done this way for performance -- using with columns creates massive plan
    dfc
      .select((baseSelects ++ structifiedCols): _*)
      .scrubSchema
      //      .withColumn("tags", structFromJson(spark, df, "tags")) // having to do this after -- some issue with removing all null tags
      .withColumn("task_detail_legacy",
        struct(
          'notebook_task,
          'spark_python_task,
          'spark_jar_task,
          'spark_submit_task,
          'shell_command_task,
          'pipeline_task
        )
      )
      .drop(
        "notebook_task", "spark_python_task", "python_wheel_task", "spark_jar_task",
        "spark_submit_task", "shell_command_task", "pipeline_task"
      )
  }

  def jobStatusCleanseForPublication(
                                      keys: Array[String],
                                      cacheParts: Int
                                    )(df: DataFrame): DataFrame = {
    val dfc = df.repartition(cacheParts).cache()
    dfc.count()
    val emptyKeysDF = Seq.empty[(String, Long, Long, String, String)].toDF("organization_id", "timestamp", "jobId", "actionName", "requestId")

    val rootCleansedTasksDF = workflowsCleanseTasks(dfc, keys, emptyKeysDF, "tasks", "rootCleansedTasks")
    val rootCleansedJobClustersDF = workflowsCleanseJobClusters(dfc, keys, emptyKeysDF, "job_clusters", "rootCleansedJobClusters")

    val dfWCleansedJobClustersAndTasks = dfc
      .join(rootCleansedTasksDF, keys.toSeq, "left")
      .join(rootCleansedJobClustersDF, keys.toSeq, "left")

    val structsCleaner = Map(
      "tags" -> handleRootNull(
        dfWCleansedJobClustersAndTasks,
        "tags",
        SchemaTools.structToMap(dfWCleansedJobClustersAndTasks, "tags"),
        MapType(StringType, StringType)
      ),
      "tasks" -> col("rootCleansedTasks"),
      "job_clusters" -> col("rootCleansedJobClusters"),
      "task_detail_legacy.notebook_task.base_parameters" -> SchemaTools.structToMap(dfWCleansedJobClustersAndTasks, "task_detail_legacy.notebook_task.base_parameters"),
      "task_detail_legacy.shell_command_task.env_vars" -> SchemaTools.structToMap(dfWCleansedJobClustersAndTasks, "task_detail_legacy.shell_command_task.env_vars")
    ) ++ PipelineFunctions.newClusterCleaner(dfWCleansedJobClustersAndTasks, "new_cluster")

    dfWCleansedJobClustersAndTasks
      .modifyStruct(structsCleaner)
      .drop("rootCleansedTasks", "rootCleansedJobClusters", "newSettingsCleansedTasks", "newSettingsCleansedJobClusters")
      .scrubSchema(SchemaScrubber(cullNullTypes = true))
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
        'run_id.cast("long").alias("runId"), // lowest level -- could be taskRunId or jobRunId
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
          'pipeline_params,
          'notebook_params,
          'python_named_params,
          'sql_params,
          from_json('dbt_commands, arrayStringSchema).alias("dbt_commands")
        ).alias("manual_override_params"),
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
        'jobTriggerType.alias("jobTriggerType_Triggered"),
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
        'python_wheel_task,
        'spark_jar_task,
        'shell_command_task,
        'spark_submit_task,
        'pipeline_task,
        'tasks, // json array struct string
        'libraries,
        'access_control_list,
        'git_source,
        'timeout_seconds.alias("timeout_seconds"),
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

  def jobRunsDeriveRepairRunsDetail(df: DataFrame, firstRunSemanticsW: WindowSpec): DataFrame = {
    df
      .filter('actionName === "repairRun")
      .select(
        'organization_id,
        'timestamp,
        'run_id.cast("long").alias("runId"),
        get_json_object($"response.result", "$.repair_id").cast("long").alias("repairId"),
        struct(
          'timestamp.alias("repair_timestamp"),
          'rerun_tasks,
          'latest_repair_id,
          struct(
            'jar_params.alias("jar_params"),
            'python_params.alias("python_params"),
            'spark_submit_params.alias("spark_submit_params"),
            'notebook_params.alias("notebook_params")
          ).alias("repair_params"),
          'response.alias("repair_response"),
          'userIdentity.alias("repairedBy"),
          'userAgent.alias("repairUserAgent"),
          'requestId.alias("repairRequestId")
        ).alias("repair_details")
      )
      .filter('runId.isNotNull)
      .withColumn("rnk", rank().over(firstRunSemanticsW))
      .withColumn("rn", row_number().over(firstRunSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .drop("rnk", "rn", "timestamp")
      .groupBy('organization_id, 'runId, 'repairId)
      .agg(collect_list('repair_details).alias("repair_details"))
  }

  def jobRunsDeriveRunsBase(df: DataFrame, etlUntilTime: TimeTypes): DataFrame = {

    val arrayStringSchema = ArrayType(StringType, containsNull = true)
    val firstTaskRunSemanticsW = Window.partitionBy('organization_id, 'jobRunId, 'taskRunId).orderBy('timestamp)
    val firstJobRunSemanticsW = Window.partitionBy('organization_id, 'jobRunId).orderBy('timestamp)
    val firstRunSemanticsW = Window.partitionBy('organization_id, 'runId).orderBy('timestamp)

    // Completes must be >= etlStartTime as it is the driver endpoint
    // All joiners to Completes may be from the past up to N days as defined in the incremental df
    // Identify all completed jobs in scope for this overwatch run
    val allCompletes = jobRunsDeriveCompletedRuns(df, firstTaskRunSemanticsW)

    // CancelRequests are still lookups from the driver "complete" as a cancel request is a request and still
    // results in a runFailed after the cancellation
    // Identify all cancelled jobs in scope for this overwatch run
    val allCancellations = jobRunsDeriveCancelledRuns(df, firstRunSemanticsW)

    // DF for jobs launched with actionName == "runNow"
    // Lookback 30 days for laggard starts prior to current run
    // only field from runNow that we care about is the response.result.runId
    val runNowStart = jobRunsDeriveRunsLaunched(df, firstJobRunSemanticsW, arrayStringSchema)

    val runTriggered = jobRunsDeriveRunsTriggered(df, firstJobRunSemanticsW)

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

    val repairDetails = jobRunsDeriveRepairRunsDetail(df, firstRunSemanticsW)

    val jobRunsMaster = allSubmissions
      .join(runStarts, Seq("organization_id", "jobRunId"), "left")
      .join(allCompletes, Seq("organization_id", "jobRunId", "taskRunId"), "left")
      .withColumn("runId", coalesce('taskRunId, 'jobRunId).cast("long"))
      .join(allCancellations, Seq("organization_id", "runId"), "left")
      .withColumn("repairId", coalesce('repairId_runStart, 'repairId_Completed).cast("long"))
      .join(repairDetails, Seq("organization_id", "runId", "repairId"), "left")
      .cache() // caching to speed up schema inference

    jobRunsMaster.count

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
        coalesce('taskRunId, 'idInJob).cast("long").alias("idInJob"),
        TransformFunctions.subtractTime(
          'startTime,
          array_max(array('completionTime, 'cancellationTime)) // endTS must remain null if still open
        ).alias("TaskRunTime"), // run launch time until terminal event
        TransformFunctions.subtractTime(
          'startTime,
          array_max(array('completionTime, 'cancellationTime)) // endTS must remain null if still open
        ).alias("TaskExecutionRunTime"), // from cluster up and run begin until terminal event
        'run_name,
        coalesce('jobClusterType_Started, 'jobClusterType_Completed).alias("clusterType"),
        coalesce('jobTaskType_Started, 'jobTaskType_Completed).alias("taskType"),
        coalesce('jobTriggerType_Triggered,'jobTriggerType_Started, 'jobTriggerType_Completed, 'jobTriggerType_runNow).alias("jobTriggerType"),
        when('cancellationRequestId.isNotNull, "Cancelled")
          .otherwise('jobTerminalState)
          .alias("terminalState"),
        'clusterId,
        'existing_cluster_id,
        'new_cluster,
        'tasks.alias("submitRun_tasks"),
        'job_clusters.alias("submitRun_job_clusters"),
        'libraries,
        'access_control_list,
        'git_source,
        'manual_override_params,
        coalesce('workflow_context_runNow, 'workflow_context_submitRun).alias("workflow_context"),
        'notebook_task,
        'spark_python_task,
        'python_wheel_task,
        'spark_jar_task,
        'spark_submit_task,
        'shell_command_task,
        'pipeline_task,
        'repairId,
        'repair_details,
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
      .withColumn("timestamp", $"TaskRunTime.startEpochMS") // TS lookup key added for next steps (launch time)
      .withColumn("startEpochMS", $"TaskRunTime.startEpochMS") // set launch time as TS key
//      .scrubSchema
  }

  def jobRunsStructifyLookupMeta(cacheParts: Int)(df: DataFrame): DataFrame = {
    val dfc = df.repartition(cacheParts).cache() // caching to speed up schema inference
    dfc.count()
    val colsToOverride = Array("tasks", "job_clusters", "tags").toSet
    val dfOrigCols = (dfc.columns.toSet -- colsToOverride).toArray map col
    val colsToAppend: Array[Column] = Array(
      structFromJson(spark, dfc, "tasks", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumTasksSchema).alias("tasks"),
      structFromJson(spark, dfc, "job_clusters", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumJobClustersSchema).alias("job_clusters"),
      struct(
        structFromJson(spark, dfc, "new_cluster", allNullMinimumSchema = Schema.minimumNewClusterSchema),
        structFromJson(spark, dfc, "submitRun_tasks", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumTasksSchema).alias("tasks"),
        structFromJson(spark, dfc, "submitRun_job_clusters", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumJobClustersSchema).alias("job_clusters"),
        structFromJson(spark, dfc, "libraries", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumLibrariesSchema),
        structFromJson(spark, dfc, "access_control_list", isArrayWrapped = true, allNullMinimumSchema = Schema.minimumAccessControlListSchema),
        structFromJson(spark, dfc, "git_source", allNullMinimumSchema = Schema.minimumGitSourceSchema)
      ).alias("submitRun_details"),
      struct(
        structFromJson(spark, dfc, "notebook_task", allNullMinimumSchema = Schema.minimumNotebookTaskSchema),
        structFromJson(spark, dfc, "spark_python_task", allNullMinimumSchema = Schema.minimumSparkPythonTaskSchema),
        structFromJson(spark, dfc, "python_wheel_task", allNullMinimumSchema = Schema.minimumPythonWheelTaskSchema),
        structFromJson(spark, dfc, "spark_jar_task", allNullMinimumSchema = Schema.minimumSparkJarTaskSchema),
        structFromJson(spark, dfc, "spark_submit_task", allNullMinimumSchema = Schema.minimumSparkSubmitTaskSchema),
        structFromJson(spark, dfc, "shell_command_task", allNullMinimumSchema = Schema.minimumShellCommandTaskSchema),
        structFromJson(spark, dfc, "pipeline_task", allNullMinimumSchema = Schema.minimumPipelineTaskSchema)
      ).alias("task_detail_legacy"),
      structFromJson(spark, dfc, "manual_override_params.notebook_params").alias("notebook_params_overwatch_ctrl"),
      structFromJson(spark, dfc, "manual_override_params.python_named_params").alias("python_named_params_overwatch_ctrl"),
      structFromJson(spark, dfc, "manual_override_params.sql_params").alias("sql_params_overwatch_ctrl"),
      structFromJson(spark, dfc, "manual_override_params.pipeline_params").alias("pipeline_params_overwatch_ctrl"),
      structFromJson(spark, dfc, "tags").alias("tags")
    )
    val selectCols = dfOrigCols ++ colsToAppend
    dfc.select(selectCols: _*)
      .drop(
        "notebook_task", "spark_python_task", "spark_jar_task", "python_wheel_task",
        "spark_submit_task", "shell_command_task", "pipeline_task", "new_cluster", "libraries", "access_control_list",
        "git_source", "submitRun_tasks", "submitRun_job_clusters"
      )
      .scrubSchema

  }

  def jobRunsCleanseCreatedNestedStructures(keys: Array[String])(df: DataFrame): DataFrame = {
    val emptyKeysDF = Seq.empty[(String, Long, Long)].toDF("organization_id", "runId", "startEpochMS")

    val cleansedTasksDF = workflowsCleanseTasks(df, keys, emptyKeysDF, "submitRun_details.tasks")
    val cleansedJobClustersDF = workflowsCleanseJobClusters(df, keys, emptyKeysDF, "submitRun_details.job_clusters")

    val dfWCleansedJobsAndTasks = df
      .join(cleansedTasksDF, keys.toSeq, "left")
      .join(cleansedJobClustersDF, keys.toSeq, "left")

    val tasksAndJobClustersCleansingInventory = Map(
      "tags" -> handleRootNull(dfWCleansedJobsAndTasks, "tags", SchemaTools.structToMap(dfWCleansedJobsAndTasks, "tags"), MapType(StringType, StringType)),
      "submitRun_details.tasks" -> col("cleansedTasks"),
      "submitRun_details.job_clusters" -> col("cleansedJobsClusters"),
      "task_detail.notebook_task.base_parameters" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "task_detail.notebook_task.base_parameters"),
      "task_detail.shell_command_task.env_vars" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "task_detail.shell_command_task.env_vars"),
      "task_detail_legacy.notebook_task.base_parameters" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "task_detail_legacy.notebook_task.base_parameters"),
      "task_detail_legacy.shell_command_task.env_vars" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "task_detail_legacy.shell_command_task.env_vars"),
      "manual_override_params.notebook_params" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "notebook_params_overwatch_ctrl"),
      "manual_override_params.python_named_params" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "python_named_params_overwatch_ctrl"),
      "manual_override_params.sql_params" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "sql_params_overwatch_ctrl"),
      "manual_override_params.pipeline_params" -> SchemaTools.structToMap(dfWCleansedJobsAndTasks, "pipeline_params_overwatch_ctrl")
    ) ++
      PipelineFunctions.newClusterCleaner(dfWCleansedJobsAndTasks, "submitRun_details.new_cluster") ++
      PipelineFunctions.newClusterCleaner(dfWCleansedJobsAndTasks, "new_cluster") ++
      PipelineFunctions.newClusterCleaner(dfWCleansedJobsAndTasks, "job_cluster")

//    val dfWStructedTasksAndCleansedJobs =
    dfWCleansedJobsAndTasks
      .modifyStruct(tasksAndJobClustersCleansingInventory) // overwrite nested complex structures with cleansed structures
      .drop(
        "cleansedTasks",
        "cleansedJobsClusters",
        "notebook_params_overwatch_ctrl",
        "python_named_params_overwatch_ctrl",
        "sql_params_overwatch_ctrl",
        "pipeline_params_overwatch_ctrl"
      ) // cleanup temporary cleaner fields
      .scrubSchema

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
      .withColumn("tasks", coalesce('tasks, 'submitRun_tasks))
      .withColumn("job_clusters", coalesce('job_clusters, 'submitRun_job_clusters))

  }

  def jobRunsAppendTaskAndClusterDetails(df: DataFrame): DataFrame = {
    val computeIsSQLWarehouse = $"task_detail.sql_task.warehouse_id".isNotNull

    val dfHasTasks = SchemaTools.nestedColExists(df.schema, "tasks")
    val dfHasJobClusters = SchemaTools.nestedColExists(df.schema, "job_clusters")

    if (dfHasTasks) { // todo -- temp until 503 resolved
      val isSingleTaskMTJ = 'taskKey.isNull && size('tasks) === 1
      val jobRunsWithImprovedKeys = df
        .withColumn("taskKey", when(isSingleTaskMTJ, $"tasks"(0)("task_key")).otherwise('taskKey)) // ES-427957 -- singleTask MTJs don't emit taskKey as of 0.6.2.0

      val tasksExploded = jobRunsWithImprovedKeys
        .select('jobId, 'taskKey, 'runId, explode('tasks).alias("task"))
        .filter('taskKey === $"task.task_key")
        .appendToStruct("task", Array(NamedColumn("libraries", lit(null).cast(Schema.minimumLibrariesSchema)))) // todo -- temp until 503 resolved
        .selectExpr("*", "task.*").drop("task", "task_key")
        .verifyMinimumSchema(Schema.minimumExplodedTaskLookupMetaSchema)
        .select(
          'jobId,
          'taskKey,
          'runId,
          'job_cluster_key,
          'new_cluster,
          'libraries,
          'max_retries,
          'min_retry_interval_millis,
          'retry_on_timeout,
          struct(
            'notebook_task,
            'pipeline_task,
            'spark_jar_task,
            'spark_submit_task,
            'spark_python_task,
            'python_wheel_task,
            'shell_command_task,
            'sql_task
          ).alias("task_detail")
        )

      if (dfHasJobClusters) { // has tasks AND job_clusters
        val jobClustersExploded = jobRunsWithImprovedKeys
          .join(tasksExploded, Seq("jobId", "taskKey", "runId"))
          .select('jobId, 'runId, 'job_cluster_key.alias("jobClusterKey"), explode('job_clusters).alias("job_cluster"))
          .filter('jobClusterKey === $"job_cluster.job_cluster_key")
          // ensure job_cluster contains new_cluster field but don't override it if it exists already
          .appendToStruct("job_cluster", Array(NamedColumn("new_cluster", lit(null).cast(Schema.minimumNewClusterSchema))))
          .selectExpr("*", "job_cluster.*").drop("job_cluster", "job_cluster_key")
          .withColumnRenamed("jobClusterKey", "job_cluster_key")
          .select('jobId, 'runId, 'job_cluster_key, 'new_cluster.alias("job_cluster"))

        jobRunsWithImprovedKeys
          .join(tasksExploded, Seq("jobId", "taskKey", "runId"), "left")
          .join(jobClustersExploded, Seq("jobId", "runId", "job_cluster_key"), "left")
          .withColumn("cluster_name", coalesce('cluster_name, 'job_cluster_key, 'taskKey))
          .withColumn("clusterId", when(computeIsSQLWarehouse, $"task_detail.sql_task.warehouse_id").otherwise('clusterId))
          .withColumn("clusterType", when(computeIsSQLWarehouse, lit("sqlWarehouse")).otherwise('clusterType))
          .withColumn("run_name", coalesce('run_name, 'taskKey))
          .drop("tasks", "job_clusters")
      } else { // has tasks BUT NO job_clusters in schema
        jobRunsWithImprovedKeys
          .join(tasksExploded, Seq("jobId", "taskKey", "runId"), "left")
          .withColumn("cluster_name", coalesce('cluster_name, 'job_cluster_key, 'taskKey))
          .withColumn("clusterId", when(computeIsSQLWarehouse, $"task_detail.sql_task.warehouse_id").otherwise('clusterId))
          .withColumn("clusterType", when(computeIsSQLWarehouse, lit("sqlWarehouse")).otherwise('clusterType))
          .withColumn("run_name", coalesce('run_name, 'taskKey))
          .drop("tasks")
      }
    } else df // tasks not present in schema
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

  def jrcpDeriveNewAndOpenRuns(newJrLaunches: DataFrame, jrLag30: DataFrame, jrcpLag30: DataFrame, fromTime: TimeTypes): DataFrame = {

    val openJRCPRecordsRunIDs = jrcpLag30
      .filter('openRun) // open jrcp records (i.e. run was still executing at the start of the last jrcp execution)
      .select('organization_id, 'run_id).distinct // org_id left to force partition pruning

    val outstandingJrRecordsToClose = jrLag30.join(openJRCPRecordsRunIDs, Seq("organization_id", "run_id"))

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
      .withColumn("openRun", when($"task_runtime.endEpochMS".isNull, lit(true)).otherwise(lit(false)))
      .withColumn("timestamp", $"task_runtime.startEpochMS")
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialInitialState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 4, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .filter('unixTimeMS_state_start.isNotNull)
      .withColumn("runtime_in_cluster_state", // all runs have an initial cluster state
        when('state.isin("CREATING", "STARTING") || 'cluster_type === "new", 'uptime_in_state_H * 1000 * 3600) // get true cluster time when state is guaranteed fully initial
          .otherwise(runStateFirstToEnd - $"task_runtime.startEpochMS")) // otherwise use jobStart as beginning time and min of stateEnd or jobEnd for end time )
      .withColumn("hourly_core_potential", 'cluster_state_worker_potential_core_H / 'uptime_in_state_H) // executor potential per hour
      .withColumn("worker_potential_core_H", ('runtime_in_cluster_state / 1000.0 / 60.0 / 60.0) * 'hourly_core_potential)
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
                                   taskRunEndOrPipelineEnd: Column
                                 ): DataFrame = {

    // use state_end_time for terminal states
    val clusterPotentialTerminalState = clusterPotentialWCosts
      .withColumn("timestamp", 'unixTimeMS_state_end)
      .select(clsfKeys ++ clsfLookupCols: _*)

    newAndOpenRuns
      .withColumn("openRun", when($"task_runtime.endEpochMS".isNull, lit(true)).otherwise(lit(false)))
      .withColumn("timestamp", taskRunEndOrPipelineEnd) // include currently executing runs and calculate costs through module until time
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialTerminalState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 4, maxLookback = 0L, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .filter( // cluster state end equal to or after task run and not in initialState (leftanti)
        'unixTimeMS_state_start.isNotNull &&
          'unixTimeMS_state_end >= taskRunEndOrPipelineEnd // >= in case both the state and the run are open and closed with untilTime
      )
      .join(jobRunInitialStates.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out beginning states
      .withColumn("runtime_in_cluster_state", taskRunEndOrPipelineEnd - runStateLastToStart)
      .withColumn("hourly_core_potential", 'cluster_state_worker_potential_core_H / 'uptime_in_state_H) // executor potential per hour
      .withColumn("worker_potential_core_H", ('runtime_in_cluster_state / 1000.0 / 60.0 / 60.0) * 'hourly_core_potential)
      .withColumn("lifecycleState", lit("terminal"))
  }

  def jrcpDeriveRunIntermediateStates(
                                       clusterPotentialWCosts: DataFrame,
                                       newAndOpenRuns: DataFrame,
                                       jobRunInitialStates: DataFrame,
                                       jobRunTerminalStates: DataFrame,
                                       stateLifecycleKeys: Seq[String],
                                       clsfKeyColNames: Array[String],
                                       clsfLookupCols: Array[Column],
                                       taskRunEndOrPipelineEnd: Column
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
      .withColumn("openRun", when($"task_runtime.endEpochMS".isNull, lit(true)).otherwise(lit(false)))
      // cluster state started after and ended before task run
      .join(clusterPotentialIntermediateStates.alias("cpot").hint("SKEW", Seq("organization_id", "cluster_id"), topClusters),
        $"jr.organization_id" === $"cpot.organization_id" &&
          $"jr.cluster_id" === $"cpot.cluster_id" &&
          $"cpot.unixTimeMS_state_start" > $"jr.task_runtime.startEpochMS" && // only states beginning after job start and ending before
          $"cpot.unixTimeMS_state_end" < taskRunEndOrPipelineEnd
      )
      .drop($"cpot.cluster_id").drop($"cpot.organization_id")
      .join(jobRunInitialStates.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out beginning states
      .join(jobRunTerminalStates.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti") // filter out ending states
      .withColumn("runtime_in_cluster_state", 'unixTimeMS_state_end - 'unixTimeMS_state_start)
      .withColumn("hourly_core_potential", 'cluster_state_worker_potential_core_H / 'uptime_in_state_H) // executor potential per hour
      .withColumn("worker_potential_core_H", ('runtime_in_cluster_state / 1000.0 / 60.0 / 60.0) * 'hourly_core_potential)
      .withColumn("lifecycleState", lit("intermediate"))

  }

  def jrcpDeriveRunsByClusterState(
                                    clusterPotentialWCosts: DataFrame,
                                    newAndOpenRuns: DataFrame,
                                    runStateFirstToEnd: Column,
                                    runStateLastToStart: Column,
                                    taskRunEndOrPipelineEnd: Column,
                                    clusterStateEndOrPipelineEnd: Column
                                  ): DataFrame = {

    // Adjust the uptimeInState to smooth the runtimes over the runPeriod across concurrent runs
    val stateLifecycleKeys = Seq("organization_id", "run_id", "cluster_id", "unixTimeMS_state_start")
    val clsfKeyColNames = Array("organization_id", "cluster_id", "timestamp")
    val clsfKeys: Array[Column] = Array(clsfKeyColNames map col: _*)
    val clsfLookups: Array[Column] = Array(
      'custom_tags.alias("cluster_tags"),
      'unixTimeMS_state_start,
      clusterStateEndOrPipelineEnd.alias("unixTimeMS_state_end"), // if clusterState still open -- close it for calculations
      'timestamp_state_start,
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
      'worker_potential_core_H.alias("cluster_state_worker_potential_core_H"),
      'driver_compute_cost,
      'worker_compute_cost,
      'driver_dbu_cost,
      'worker_dbu_cost,
      'total_compute_cost,
      'total_DBUs,
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
      taskRunEndOrPipelineEnd
    )
    val jobRunIntermediateStates = jrcpDeriveRunIntermediateStates(
      clusterPotentialWCosts,
      newAndOpenRuns,
      jobRunInitialStates,
      jobRunTerminalStates,
      stateLifecycleKeys,
      clsfKeyColNames,
      clsfLookups,
      taskRunEndOrPipelineEnd
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
      .filter('cluster_type === "existing") // only relevant for interactive clusters
      .withColumn("run_state_start_epochMS", runStateLastToStart)
      .withColumn("run_state_end_epochMS", runStateFirstToEnd)
      .select(
        'organization_id, 'run_id, 'cluster_id, 'run_state_start_epochMS, 'run_state_end_epochMS,
        'unixTimeMS_state_start, 'unixTimeMS_state_end
      )
      .repartition()
      .cache()

    simplifiedJobRunByClusterState.count() // eager cache as its used several times below

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
        when('cluster_type === "new", lit("automated"))
          .otherwise(lit("interactive"))
      )
      .withColumn("state_utilization_percent", 'runtime_in_cluster_state / 1000 / 3600 / 'uptime_in_state_H) // run runtime as percent of total state time
      .withColumn("run_state_utilization",
        when('cluster_type === "interactive", least('runtime_in_cluster_state / 'cum_runtime_in_cluster_state, lit(1.0)))
          .otherwise(lit(1.0))
      ) // determine share of cluster when interactive as runtime / all overlapping run runtimes
      .withColumn("overlapping_run_states", when('cluster_type === "interactive", 'overlapping_run_states).otherwise(lit(0)))
      .withColumn("running_days", sequence($"task_runtime.startTS".cast("date"), $"task_runtime.endTS".cast("date")))
      .withColumn("total_dbus", 'total_dbus * 'state_utilization_percent * 'run_state_utilization)
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
        'workspace_name,
        'job_id,
        'job_name,
        'run_id,
        'job_run_id,
        'task_run_id,
        'task_key,
        'repair_id,
        'run_name,
        'startEpochMS,
        'cluster_id,
        'cluster_name,
        'cluster_tags,
        'cluster_type,
        'driver_node_type_id,
        'node_type_id,
        'dbu_rate,
        'multitask_parent_run_id,
        'parent_run_id,
        'task_runtime,
        'task_execution_runtime,
        'terminal_state,
        'job_trigger_type,
        'task_type,
        'created_by,
        'last_edited_by,
        'openRun
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
        greatest(round(sum('total_dbus), 6), lit(0)).alias("total_dbus"),
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
          $"jrCostPot.run_id" === $"jrSparkUtil.db_id_in_job",
        "left"
      )
      .drop("db_job_id", "db_id_in_job", "orgId")
      .withColumn("job_run_cluster_util", round(('spark_task_runtime_H / 'worker_potential_core_H), 4))
  }

}
