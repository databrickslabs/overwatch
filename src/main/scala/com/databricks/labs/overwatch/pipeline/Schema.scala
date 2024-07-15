package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.types._

/**
 * The purpose of this Object is to validate that AT LEAST the columns required for ETL are present. Furthermore,
 * if all values of a target struct are null when a schema is derived, the output type of the structure won't be a
 * structure at all but rather a simple string, thus in these cases, the required complex structure must be generated
 * with all nulls so that it can be inserted into the target. To do this, the necessary null structs are created so
 * that source DFs can match the target even when schema inference is used in the reader. The entry function is the
 * "verifyDF" function.
 */
object Schema extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * KEYS MUST BE LOWER CASE
   */
  private val commonSchemas: Map[String, StructField] = Map(
    "filenamegroup" -> StructField("filenameGroup",
      StructType(Seq(
        StructField("filename", StringType, nullable = true),
        StructField("byCluster", StringType, nullable = true),
        StructField("byDriverHost", StringType, nullable = true),
        StructField("bySparkContext", StringType, nullable = true)
      )), nullable = true),
    "response" -> StructField("response",
      StructType(Seq(
        StructField("errorMessage", StringType, nullable = true),
        StructField("result", StringType, nullable = true),
        StructField("statusCode", LongType, nullable = true)
      )), nullable = true),
    "useridentity" -> StructField("userIdentity",
      StructType(Seq(
        StructField("email", StringType, nullable = true)
      )), nullable = true),
    "startfilenamegroup" -> StructField("startFilenameGroup",
      StructType(Seq(
        StructField("filename", StringType, nullable = true),
        StructField("byCluster", StringType, nullable = true),
        StructField("byDriverHost", StringType, nullable = true),
        StructField("bySparkContext", StringType, nullable = true)
      )), nullable = true),
    "endfilenamegroup" -> StructField("endFilenameGroup",
      StructType(Seq(
        StructField("filename", StringType, nullable = true),
        StructField("byCluster", StringType, nullable = true),
        StructField("byDriverHost", StringType, nullable = true),
        StructField("bySparkContext", StringType, nullable = true)
      )), nullable = true)
  )

  private def runtimeField(fieldName: String): StructField = {
    StructField(fieldName,
      StructType(Seq(
        StructField("startEpochMS", LongType, nullable = true),
        StructField("startTS", TimestampType, nullable = true),
        StructField("endEpochMS", LongType, nullable = true),
        StructField("endTS", TimestampType, nullable = true),
        StructField("runTimeMS", LongType, nullable = true),
        StructField("runTimeS", DoubleType, nullable = true),
        StructField("runTimeM", DoubleType, nullable = true),
        StructField("runTimeH", DoubleType, nullable = true)
      )), nullable = true)
  }

  private def common(name: String): StructField = {
    commonSchemas(name.toLowerCase)
  }

  val badRecordsSchema: StructType = StructType(Seq(
    StructField("path", StringType, nullable = true),
    StructField("reason", StringType, nullable = true),
    StructField("record", StringType, nullable = true)
  ))

  val auditMasterSchema: StructType = StructType(Seq(
    StructField("serviceName", StringType, nullable = false),
    StructField("actionName", StringType, nullable = false),
    StructField("organization_id", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = true),
    StructField("date", DateType, nullable = true),
    StructField("sourceIPAddress", StringType, nullable = true),
    StructField("userAgent", StringType, nullable = true),
    StructField("requestId", StringType, nullable = true),
    StructField("sessionId", StringType, nullable = true),
    StructField("version", StringType, nullable = true),
    StructField("hashKey", LongType, nullable = true),
    StructField("requestParams",
      StructType(Seq(
        StructField("clusterId", StringType, nullable = true),
        StructField("cluster_id", StringType, nullable = true),
        StructField("clusterName", StringType, nullable = true),
        StructField("cluster_name", StringType, nullable = true),
        StructField("clusterState", StringType, nullable = true),
        StructField("dataSourceId", StringType, nullable = true),
        StructField("dashboardId", StringType, nullable = true),
        StructField("alertId", StringType, nullable = true),
        StructField("jobId", LongType, nullable = true),
        StructField("job_id", LongType, nullable = true),
        StructField("jobTaskType", StringType, nullable = true),
        StructField("job_type", StringType, nullable = true),
        StructField("format", StringType, nullable = true),
        StructField("runId", LongType, nullable = true),
        StructField("run_id", LongType, nullable = true),
        StructField("tags", StringType, nullable = true),
        StructField("multitaskParentRunId", StringType, nullable = true),
        StructField("parentRunId", StringType, nullable = true),
        StructField("taskKey", StringType, nullable = true),
        StructField("repairId", StringType, nullable = true),
        StructField("rerun_tasks", StringType, nullable = true),
        StructField("latest_repair_id", StringType, nullable = true),
        StructField("jar_params", StringType, nullable = true),
        StructField("python_params", StringType, nullable = true),
        StructField("spark_submit_params", StringType, nullable = true),
        StructField("notebook_params", StringType, nullable = true),
        StructField("python_named_params", StringType, nullable = true),
        StructField("pipeline_params", StringType, nullable = true),
        StructField("sql_params", StringType, nullable = true),
        StructField("dbt_commands", StringType, nullable = true),
        StructField("tasks", StringType, nullable = true),
        StructField("access_control_list", StringType, nullable = true),
        StructField("git_source", StringType, nullable = true),
        StructField("is_from_dlt", BooleanType, nullable = true),
        StructField("taskDependencies", StringType, nullable = true),
        StructField("idInJob", LongType, nullable = true),
        StructField("jobClusterType", StringType, nullable = true),
        StructField("jobTerminalState", StringType, nullable = true),
        StructField("jobTriggerType", StringType, nullable = true),
        StructField("workflow_context", StringType, nullable = true),
        StructField("libraries", StringType, nullable = true),
        StructField("run_name", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("timeout_seconds", LongType, nullable = true),
        StructField("max_concurrent_runs", LongType, nullable = true),
        StructField("max_retries", LongType, nullable = true),
        StructField("retry_on_timeout", BooleanType, nullable = true),
        StructField("min_retry_interval_millis", LongType, nullable = true),
        StructField("schedule", StringType, nullable = true),
        StructField("email_notifications", StringType, nullable = true),
        StructField("notebook_task", StringType, nullable = true),
        StructField("spark_python_task", StringType, nullable = true),
        StructField("spark_jar_task", StringType, nullable = true),
        StructField("python_wheel_task", StringType, nullable = true),
        StructField("spark_submit_task", StringType, nullable = true),
        StructField("shell_command_task", StringType, nullable = true),
        StructField("pipeline_task", StringType, nullable = true),
        StructField("new_settings", StringType, nullable = true),
        StructField("existing_cluster_id", StringType, nullable = true),
        StructField("job_clusters", StringType, nullable = true),
        StructField("new_cluster", StringType, nullable = true),
        StructField("run_as_user_name", StringType, nullable = true),
        StructField("aclPermissionSet", StringType, nullable = true),
        StructField("grants", StringType, nullable = true),
        StructField("targetUserId", StringType, nullable = true),
        StructField("aws_attributes", StringType, nullable = true),
        StructField("azure_attributes", StringType, nullable = true),
        StructField("gcp_attributes", StringType, nullable = true),
        StructField("instanceId", StringType, nullable = true),
        StructField("port", StringType, nullable = true),
        StructField("publicKey", StringType, nullable = true),
        StructField("containerId", StringType, nullable = true),
        StructField("userName", StringType, nullable = true),
        StructField("user", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("endpoint", StringType, nullable = true),
        StructField("targetUserName", StringType, nullable = true),
        StructField("targetGroupName", StringType, nullable = true),
        StructField("targetGroupId", StringType, nullable = true),
        StructField("notebookId", StringType, nullable = true),
        StructField("notebookName", StringType, nullable = true),
        StructField("path", StringType, nullable = true),
        StructField("oldName", StringType, nullable = true),
        StructField("oldPath", StringType, nullable = true),
        StructField("newName", StringType, nullable = true),
        StructField("newPath", StringType, nullable = true),
        StructField("parentPath", StringType, nullable = true),
        StructField("driver_node_type_id", StringType, nullable = true),
        StructField("node_type_id", StringType, nullable = true),
        StructField("num_workers", IntegerType, nullable = true),
        StructField("autoscale", StringType, nullable = true),
        StructField("clusterWorkers", StringType, nullable = true),
        StructField("autotermination_minutes", IntegerType, nullable = true),
        StructField("enable_elastic_disk", BooleanType, nullable = true),
        StructField("start_cluster", BooleanType, nullable = true),
        StructField("clusterOwnerUserId", StringType, nullable = true),
        StructField("cluster_log_conf", StringType, nullable = true),
        StructField("init_scripts", StringType, nullable = true),
        StructField("custom_tags", StringType, nullable = true),
        StructField("cluster_source", StringType, nullable = true),
        StructField("spark_env_vars", StringType, nullable = true),
        StructField("spark_conf", StringType, nullable = true),
        StructField("acl_path_prefix", StringType, nullable = true),
        StructField("driver_instance_pool_id", StringType, nullable = true),
        StructField("instance_pool_id", StringType, nullable = true),
        StructField("instance_pool_name", StringType, nullable = true),
        StructField("preloaded_spark_versions", StringType, nullable = true),
        StructField("spark_version", StringType, nullable = true),
        StructField("idle_instance_autotermination_minutes", LongType, nullable = true),
        StructField("max_capacity", LongType, nullable = true),
        StructField("min_idle_instances", LongType, nullable = true),
        StructField("cluster_creator", StringType, nullable = true),
        StructField("idempotency_token", StringType, nullable = true),
        StructField("user_id", StringType, nullable = true),
        StructField("ssh_public_keys", StringType, nullable = true),
        StructField("single_user_name", StringType, nullable = true),
        StructField("resourceId", LongType, nullable = true),
        StructField("cluster_size", StringType, nullable = true),
        StructField("min_num_clusters", LongType, nullable = true),
        StructField("max_num_clusters", LongType, nullable = true),
        StructField("auto_stop_mins", LongType, nullable = true),
        StructField("spot_instance_policy", StringType, nullable = true),
        StructField("enable_photon", BooleanType, nullable = true),
        StructField("channel", StringType, nullable = true),
        StructField("enable_serverless_compute", BooleanType, nullable = true),
        StructField("warehouse_type", StringType, nullable = true),
        // EXPLICIT EXCLUSIONS -- fields will not be in targetDF
        StructField("organization_id", NullType, nullable = true),
        StructField("shardName", NullType, nullable = true),
        StructField("orgId", NullType, nullable = true),
        StructField("version", NullType, nullable = true),
        //adding schema used for photon evolution
        StructField("effective_spark_version", StringType, nullable = true),
        StructField("runtime_engine", StringType, nullable = true),
        StructField("fields_to_remove", StringType, nullable = true),
        StructField("commandText", StringType, nullable = true),
        StructField("commandId", StringType, nullable = true),
      )), nullable = true),
    StructField("instanceId", NullType, nullable = true),
    StructField("containerId", NullType, nullable = true),
    common("response"),
    common("useridentity")
  ))

  val sparkEventsRawMasterSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("Event", StringType, nullable = false),
    StructField("clusterId", StringType, nullable = false),
    StructField("SparkContextID", StringType, nullable = false),
    StructField("JobID", LongType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("details", StringType, nullable = true),
    StructField("executionId", LongType, nullable = true),
    StructField("JobResult", StructType(Seq(
      StructField("Exception", StructType(Seq(
        StructField("Message", StringType, nullable = true)
      )), nullable = true)
    )), nullable = true),
    StructField("StageID", LongType, nullable = true),
    StructField("StageAttemptID", LongType, nullable = true),
    StructField("ExecutorID", StringType, nullable = true),
    StructField("RemovedReason", StringType, nullable = true),
    StructField("executorInfo",
      StructType(Seq(
        StructField("Host", StringType, nullable = true),
        StructField("TotalCores", LongType, nullable = true)
      )), nullable = true),
    StructField("TaskMetrics",
      StructType(Seq(
        StructField("ExecutorCPUTime", LongType, nullable = true),
        StructField("ExecutorRunTime", LongType, nullable = true),
        StructField("MemoryBytesSpilled", LongType, nullable = true),
        StructField("DiskBytesSpilled", LongType, nullable = true),
        StructField("ResultSize", LongType, nullable = true)
      )), nullable = true),
    StructField("TaskExecutorMetrics",
      StructType(Seq(
        StructField("JVMHeapMemory", LongType, nullable = true)
      )), nullable = true),
    StructField("TaskType", StringType, nullable = true),
    StructField("TaskEndReason",
      StructType(Seq(
        StructField("AccumulatorUpdates", ArrayType(
          StructType(Seq(
            StructField("CountFailedValues",BooleanType, nullable = true),
            StructField("ID",LongType, nullable = true),
            StructField("Internal",BooleanType, nullable = true),
            StructField("Update",StringType, nullable = true)
          )), containsNull = true
        )),
        StructField("StackTrace", ArrayType(
          StructType(Seq(
            StructField("DeclaringClass",StringType, nullable = true),
            StructField("FileName",StringType, nullable = true),
            StructField("LineNumber",LongType, nullable = true),
            StructField("MethodName",StringType, nullable = true)
          )), containsNull = true
        )),
        StructField("BlockManagerAddress",
          StructType(Seq(
            StructField("ExecutorID",StringType, nullable = true),
            StructField("Host",StringType, nullable = true),
            StructField("Port",LongType, nullable = true)
        )), nullable = true),
        StructField("ClassName", StringType, nullable = true),
        StructField("Description", StringType, nullable = true),
        StructField("FullStackTrace", StringType, nullable = true),
        StructField("KillReason", StringType, nullable = true),
        StructField("Reason", StringType, nullable = true)
      )), nullable = true),
    StructField("TaskInfo",
      StructType(Seq(
        StructField("TaskID", LongType, nullable = true),
        StructField("Attempt", LongType, nullable = true),
        StructField("Host", StringType, nullable = true),
        StructField("LaunchTime", LongType, nullable = true),
        StructField("ExecutorID", StringType, nullable = true),
        StructField("FinishTime", LongType, nullable = true),
        StructField("ParentIDs", StringType, nullable = true)
      )), nullable = true),
    StructField("StageInfo",
      StructType(Seq(
        StructField("StageID", LongType, nullable = true),
        StructField("SubmissionTime", LongType, nullable = true),
        StructField("StageAttemptID", LongType, nullable = true),
        StructField("CompletionTime", LongType, nullable = true),
        StructField("Details", StringType, nullable = true),
        StructField("FailureReason", StringType, nullable = true),
        StructField("NumberofTasks", LongType, nullable = true),
        StructField("ParentIDs", ArrayType(LongType, containsNull = true), nullable = true)
      )), nullable = true),
    StructField("fileCreateDate", DateType, nullable = false),
    StructField("SubmissionTime", LongType, nullable = true),
    StructField("CompletionTime", LongType, nullable = true),
    StructField("Timestamp", LongType, nullable = true),
    StructField("time", LongType, nullable = true),
    common("filenameGroup")
  ))

  val minimumLibrariesSchema: ArrayType = ArrayType(
    StructType(Seq(
      StructField("jar",StringType, nullable = true),
      StructField("maven",
        StructType(
          Seq(StructField("coordinates",StringType, nullable = true)
          )),nullable = true),
      StructField("pypi",StructType(Seq(
        StructField("package",StringType, nullable = true)
      )), nullable = true),
      StructField("whl",StringType, nullable = true)
    )), containsNull = true
  )

  val minimumManualOverrideParamsSchema: StructType = StructType(Seq(
    StructField("jar_params",ArrayType(StringType, containsNull = true), nullable = true),
    StructField("python_params",ArrayType(StringType, containsNull = true), nullable = true),
    StructField("spark_submit_params",ArrayType(StringType, containsNull = true), nullable = true),
    StructField("notebook_params",MapType(StringType, StringType, valueContainsNull = true), nullable = true)
  ))

  val minimumSQLTaskSchema: StructType = StructType(Seq(
    StructField("warehouse_id", StringType, nullable = true),
    StructField("query",StructType(Seq(
      StructField("query_id",StringType, nullable = true))
    ), nullable = true)
  ))

  val minimumSubmitRunDetailsSchema: StructType = StructType(Seq(
    StructField("existing_cluster_id",StringType, nullable = true),
    StructField("sql_task",minimumSQLTaskSchema, nullable = true)
  ))

  val minimumRepairDetailsSchema: ArrayType = ArrayType(
    StructType(Seq(
      StructField("repair_timestamp",LongType, nullable = true),
      StructField("rerun_tasks",StringType, nullable = true),
      StructField("latest_repair_id",StringType, nullable = true)
    ))
  )

  val minimumNotebookTaskSchema: StructType = StructType(Seq(
    StructField("notebook_path",StringType, nullable = true),
    StructField("revision_timestamp",LongType, nullable = true)
  ))

  val minimumSparkPythonTaskSchema: StructType = StructType(Seq(
    StructField("spark_python_task",StructType(Seq(
      StructField("parameters",ArrayType(StringType, containsNull = true), nullable = true),
      StructField("python_file",StringType)
    )),nullable = true)
  ))

  val minimumSparkSubmitTaskSchema: StructType = StructType(Seq(
    StructField("parameters",ArrayType(StringType, containsNull = true), nullable = true)
  ))

  val minimumSparkJarTaskSchema: StructType = StructType(Seq(
    StructField("jar_uri",StringType,nullable = true),
    StructField("main_class_name",StringType,nullable = true),
    StructField("parameters",ArrayType(StringType, containsNull = true), nullable = true)
  ))

  val minimumShellCommandTaskSchema: StructType = StructType(Seq(
    StructField("command",ArrayType(StringType,containsNull = true),nullable = true)
  ))

  val minimumPipelineTaskSchema: StructType = StructType(Seq(StructField("pipeline_id",StringType,nullable = true)))

  val minimumPythonWheelTaskSchema: StructType = StructType(Seq(
    StructField("entry_point",StringType, nullable = true),
    StructField("package_name",StringType, nullable = true),
    StructField("parameters",ArrayType(StringType, containsNull = true), nullable = true)
  ))

  val minimumDBTTask: StructType = StructType(Seq(
    StructField("commands", ArrayType(StringType, containsNull = true), nullable = true)
  ))

  val minimumTaskDetailSchema: StructType = StructType(Seq(
    StructField("notebook_task",minimumNotebookTaskSchema, nullable = true),
    StructField("spark_python_task",minimumSparkPythonTaskSchema, nullable = true),
    StructField("python_wheel_task",minimumPythonWheelTaskSchema, nullable = true),
    StructField("spark_jar_task",minimumSparkJarTaskSchema, nullable = true),
    StructField("spark_submit_task",minimumSparkSubmitTaskSchema, nullable = true),
    StructField("shell_command_task",minimumShellCommandTaskSchema, nullable = true),
    StructField("pipeline_task",minimumPipelineTaskSchema, nullable = true),
  ))

  // minimum new cluster struct
  val minimumNewClusterSchema: StructType = StructType(Seq(
    StructField("autoscale",
      StructType(Seq(
        StructField("max_workers", LongType, true),
        StructField("min_workers", LongType, true)
      )), true),
    StructField("cluster_name", StringType, true),
    StructField("driver_instance_pool_id", StringType, true),
    StructField("driver_node_type_id", StringType, true),
    StructField("enable_elastic_disk", BooleanType, true),
    StructField("instance_pool_id", StringType, true),
    StructField("node_type_id", StringType, true),
    StructField("num_workers", LongType, true),
    StructField("policy_id", StringType, true),
    StructField("spark_version", StringType, true)
  ))

  // after tasks exploded
  val minimumExplodedTaskLookupMetaSchema: StructType = StructType(Seq(
    StructField("taskKey",StringType, nullable = true),
    StructField("job_cluster_key",StringType, nullable = true),
    StructField("libraries",minimumLibrariesSchema, nullable = true),
    StructField("max_retries", LongType, nullable = true),
    StructField("retry_on_timeout", BooleanType, nullable = true),
    StructField("min_retry_interval_millis", LongType, nullable = true),
    StructField("new_cluster", minimumNewClusterSchema, nullable = true),
    StructField("notebook_task",minimumNotebookTaskSchema, nullable = true),
    StructField("spark_python_task",minimumSparkPythonTaskSchema, nullable = true),
    StructField("python_wheel_task",minimumPythonWheelTaskSchema, nullable = true),
    StructField("spark_jar_task",minimumSparkJarTaskSchema, nullable = true),
    StructField("spark_submit_task",minimumSparkSubmitTaskSchema, nullable = true),
    StructField("shell_command_task",minimumShellCommandTaskSchema, nullable = true),
    StructField("pipeline_task",minimumPipelineTaskSchema, nullable = true),
    StructField("sql_task",minimumSQLTaskSchema, nullable = true)
  ))

  // Minimum required Schedule Schema
  val minimumScheduleSchema: StructType = StructType(Seq(
    StructField("pause_status", StringType, true),
    StructField("quartz_cron_expression", StringType, true),
    StructField("timezone_id", StringType, true)
  ))

  val minimumEmailNotificationsSchema: StructType = StructType(Seq(
    StructField("no_alert_for_skipped_runs", BooleanType, nullable = true),
    StructField("on_failure", ArrayType(StringType, containsNull = true), nullable = true)
  ))

  val minimumGitSourceSchema: StructType = StructType(Seq(
    StructField("git_branch",StringType, nullable = true),
    StructField("git_provider",StringType, nullable = true),
    StructField("git_url",StringType, nullable = true),
    StructField("git_tag",StringType, nullable = true),
    StructField("git_commit",StringType, nullable = true)
  ))

  val minimumAccessControlListSchema: ArrayType = ArrayType(
      StructType(Seq(
        StructField("user_name",StringType, nullable = true),
        StructField("group_name",StringType, nullable = true),
        StructField("permission_level",StringType, nullable = true),
      ))
    , containsNull = true)

  val minimumGrantsSchema: ArrayType = ArrayType(
    StructType(Seq(
      StructField("permission",StringType, nullable = true),
      StructField("user_id",LongType, nullable = true)
    ))
    ,containsNull = true)

  val minimumJobClustersSchema: ArrayType = ArrayType(
    StructType(Seq(StructField("job_cluster_key",StringType,nullable = true))),
    containsNull = true
  )

  val minimumTasksSchema: ArrayType = ArrayType(StructType(Seq(
    StructField("task_key",StringType, nullable = true),
  )))

  // minimum new jobs settings struct
  val minimumNewSettingsSchema: StructType = StructType(Seq(
    StructField("existing_cluster_id", StringType, nullable = true),
    StructField("max_concurrent_runs", LongType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("new_cluster", minimumNewClusterSchema, nullable = true),
    StructField("timeout_seconds", LongType, nullable = true),
    StructField("notebook_task", minimumNotebookTaskSchema, nullable = true),
    StructField("spark_python_task", minimumSparkPythonTaskSchema, nullable = true),
    StructField("python_wheel_task", minimumPythonWheelTaskSchema, nullable = true),
    StructField("spark_jar_task", minimumSparkJarTaskSchema, nullable = true),
    StructField("spark_submit_task", minimumSparkSubmitTaskSchema, nullable = true),
    StructField("shell_command_task", minimumShellCommandTaskSchema, nullable = true),
    StructField("pipeline_task", minimumPipelineTaskSchema, nullable = true),
  ))

  val minimumJobStatusSilverMetaLookupSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("jobId", LongType, nullable = true),
    StructField("jobName", StringType, nullable = true),
    StructField("tasks", minimumTasksSchema, nullable = true),
    StructField("job_clusters", minimumJobClustersSchema, nullable = true),
    StructField("tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("schedule", minimumScheduleSchema, nullable = true),
    StructField("max_concurrent_runs", LongType, nullable = true),
    StructField("run_as_user_name", StringType, nullable = true),
    StructField("timeout_seconds", LongType, nullable = true),
    StructField("created_by", StringType, nullable = true),
    StructField("last_edited_by", StringType, nullable = true),
    StructField("task_detail_legacy", minimumTaskDetailSchema, nullable = true),
  ))

  // simplified new settings struct
  private[overwatch] val simplifiedNewSettingsSchema = StructType(Seq(
    StructField("email_notifications", minimumEmailNotificationsSchema, nullable = true),
    StructField("existing_cluster_id", StringType, nullable = true),
    StructField("max_concurrent_runs", LongType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("new_cluster", minimumNewClusterSchema, nullable = true),
    StructField("notebook_task", minimumNotebookTaskSchema, nullable = true),
    StructField("schedule", minimumScheduleSchema, nullable = true),
    StructField("notebook_task",minimumNotebookTaskSchema, nullable = true),
    StructField("spark_python_task",minimumSparkPythonTaskSchema, nullable = true),
    StructField("python_wheel_task", minimumPythonWheelTaskSchema, nullable = true),
    StructField("spark_jar_task",minimumSparkJarTaskSchema, nullable = true),
    StructField("spark_submit_task", minimumSparkSubmitTaskSchema, nullable = true),
    StructField("shell_command_task",minimumShellCommandTaskSchema, nullable = true),
    StructField("pipeline_task", minimumPipelineTaskSchema, nullable = true),
    StructField("timeout_seconds", LongType, nullable = true)
  ))

  val streamingGoldMinimumSchema: StructType = StructType(Seq(
    StructField("cluster_id", StringType, nullable = false),
    StructField("organization_id", StringType, nullable = false),
    StructField("spark_context_id", StringType, nullable = false),
    StructField("stream_id", StringType, nullable = false),
    StructField("stream_run_id", StringType, nullable = false),
    StructField("stream_batch_id", LongType, nullable = false),
    StructField("stream_timestamp", DoubleType, nullable = true),
    StructField("date", DateType, nullable = false),
    StructField("streamSegment", StringType, nullable = true),
    StructField("stream_name", StringType, nullable = true),
    StructField("streaming_metrics",
      StructType(Seq(
        StructField("batchDuration", LongType, nullable = true),
        StructField("batchId", LongType, nullable = true),
        StructField("durationMs", StringType, nullable = true),
        StructField("eventTime", StructType(Seq(
          StructField("avg", StringType, nullable = true),
          StructField("max", StringType, nullable = true),
          StructField("min", StringType, nullable = true),
          StructField("watermark", StringType, nullable = true)
        )), nullable = true),
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("runId", StringType, nullable = true),
        StructField("sink", StringType, nullable = true),
        StructField("sources", StringType, nullable = true),
        StructField("stateOperators", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true)
      )), nullable = true),
    StructField("execution_ids", ArrayType(LongType, containsNull = true), nullable = true)
  ))

  val poolsSnapMinimumSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("Pipeline_SnapTS", TimestampType, nullable = false),
    StructField("instance_pool_id", StringType, nullable = false),
    StructField("instance_pool_name", StringType, nullable = true),
    StructField("node_type_id", StringType, nullable = true),
    StructField("idle_instance_autotermination_minutes", LongType, nullable = true),
    StructField("min_idle_instances", LongType, nullable = true),
    StructField("max_capacity", LongType, nullable = true),
    StructField("preloaded_spark_versions", ArrayType(StringType, containsNull = true), nullable = true),
    StructField("aws_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("azure_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("gcp_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
  ))

  val poolsDeleteSchema: StructType = StructType(Seq(
    StructField("deleted_by", StringType, nullable = true),
    StructField("deleted_at_epochMillis", LongType, nullable = true),
    StructField("deleted_at", TimestampType, nullable = true),
  ))

  val poolsCreateSchema: StructType = StructType(Seq(
    StructField("created_by", StringType, nullable = true),
    StructField("created_at_epochMillis", LongType, nullable = true),
    StructField("created_at", TimestampType, nullable = true),
  ))

  val poolsRequestDetails: StructType = StructType(Seq(
    StructField("requestId", StringType, nullable = true),
    common("response"),
    StructField("sessionId", StringType, nullable = true),
    StructField("sourceIPAddress", StringType, nullable = true),
    StructField("userAgent", StringType, nullable = true)
  ))

  private val s3LogSchema = StructType(Seq(
    StructField("canned_acl", StringType, nullable = true),
    StructField("destination", StringType, nullable = true),
    StructField("enable_encryption", BooleanType, nullable = true),
    StructField("region", StringType, nullable = true)
  ))

  private val dbfsLogSchema = StructType(Seq(
    StructField("destination", StringType, true)
  ))

  private val logConfSchema = StructType(Seq(
    StructField("dbfs", dbfsLogSchema, true),
    StructField("s3", s3LogSchema, true)
  ))

  val clusterSnapMinimumSchema: StructType = StructType(Seq(
    StructField("autoscale",
      StructType(Seq(
        StructField("max_workers", LongType, nullable = true),
        StructField("min_workers", LongType, nullable = true)
      )), nullable = true),
    StructField("autotermination_minutes", LongType, nullable = true),
    StructField("cluster_id", StringType, nullable = true),
    StructField("cluster_log_conf", logConfSchema, nullable = true),
    StructField("cluster_name", StringType, nullable = true),
    StructField("cluster_source", StringType, nullable = true),
    StructField("creator_user_name", StringType, nullable = true),
    StructField("driver_instance_pool_id", StringType, nullable = true),
    StructField("driver_node_type_id", StringType, nullable = true),
    StructField("enable_elastic_disk", BooleanType, nullable = true),
    StructField("enable_local_disk_encryption", BooleanType, nullable = true),
    StructField("instance_pool_id", StringType, nullable = true),
    StructField("init_scripts", ArrayType(StructType(Seq(
      StructField("dbfs", StructType(Seq(
        StructField("destination", StringType, nullable = true)
      )), nullable = true)
    )), containsNull = true), nullable = true),
    StructField("node_type_id", StringType, nullable = true),
    StructField("num_workers", LongType, nullable = true),
    StructField("single_user_name", StringType, nullable = true),
    StructField("spark_version", StringType, nullable = true),
    StructField("runtime_engine", StringType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("default_tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("custom_tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("start_time", LongType, nullable = true),
    StructField("terminated_time", LongType, nullable = true),
    StructField("organization_id", StringType, nullable = false),
    StructField("Pipeline_SnapTS", TimestampType, nullable = true),
    StructField("Overwatch_RunID", StringType, nullable = true),
    StructField("workspace_name", StringType, nullable = true)
  ))

  val clusterEventsMinimumSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("cluster_id", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("type", StringType, nullable = true),
    StructField("details",
      StructType(Seq(
        StructField("cluster_size",
          StructType(Seq(
            StructField("autoscale",
              StructType(Seq(
                StructField("max_workers", LongType, nullable = true),
                StructField("min_workers", LongType, nullable = true)
              )), nullable = true),
            StructField("num_workers", LongType, nullable = true)
          )), nullable = true),
        StructField("current_num_workers", LongType, nullable = true),
        StructField("target_num_workers", LongType, nullable = true),
        StructField("user", StringType, nullable = true),
        StructField("disk_size", LongType, nullable = true),
        StructField("free_space", LongType, nullable = true),
        StructField("instance_id", StringType, nullable = true),
        StructField("previous_disk_size", LongType, nullable = true),
        StructField("driver_state_message", StringType, nullable = true)
      )), nullable = true)
  ))

  // commented fields have been removed as of 2.1
  // todo - update for v2.1
  val jobSnapMinimumSchema: StructType = StructType(Seq(
    StructField("created_time", LongType, nullable = true),
    StructField("creator_user_name", StringType, nullable = true),
    StructField("job_id", LongType, nullable = true),
    StructField("settings", StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("existing_cluster_id", StringType, nullable = true),
      StructField("job_clusters", minimumJobClustersSchema, nullable = true),
      StructField("tasks", minimumTasksSchema, nullable = true),
      StructField("new_cluster", minimumNewClusterSchema, nullable = true),
      StructField("libraries", minimumLibrariesSchema, nullable = true),
      StructField("git_source", minimumGitSourceSchema, nullable = true),
      StructField("max_concurrent_runs", LongType, nullable = true),
      StructField("max_retries", LongType, nullable = true),
      StructField("timeout_seconds", LongType, nullable = true),
      StructField("retry_on_timeout", BooleanType, nullable = true),
      StructField("min_retry_interval_millis", LongType, nullable = true),
      StructField("schedule", minimumScheduleSchema, nullable = true),
      StructField("tags", MapType(StringType, StringType), nullable = true),
      StructField("email_notifications", minimumEmailNotificationsSchema, nullable = true),
      StructField("notebook_task", minimumNotebookTaskSchema, nullable = true),
      StructField("spark_python_task", minimumSparkPythonTaskSchema, nullable = true),
      StructField("python_wheel_task", minimumPythonWheelTaskSchema, nullable = true),
      StructField("spark_jar_task", minimumSparkJarTaskSchema, nullable = true),
      StructField("spark_submit_task", minimumSparkSubmitTaskSchema, nullable = true),
      StructField("shell_command_task", minimumShellCommandTaskSchema, nullable = true),
      StructField("pipeline_task", minimumPipelineTaskSchema, nullable = true)
    )), nullable = true),
    StructField("organization_id", StringType, nullable = false),
    StructField("Pipeline_SnapTS", TimestampType, nullable = true),
    StructField("Overwatch_RunID", StringType, nullable = true),
    StructField("job_type", StringType, nullable = true),
    StructField("workspace_name", StringType, nullable = true)
  ))

  val warehouseSnapMinimumSchema: StructType = StructType(Seq(
    StructField("warehouse_id", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("size", StringType, nullable = true),
    StructField("cluster_size", StringType, nullable = true),
    StructField("min_num_clusters", LongType, nullable = true),
    StructField("max_num_clusters", LongType, nullable = true),
    StructField("auto_stop_mins", LongType, nullable = true),
    StructField("auto_resume", BooleanType, nullable = true),
    StructField("creator_name", StringType, nullable = true),
    StructField("creator_id", LongType, nullable = true),
    StructField("tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("spot_instance_policy", StringType, nullable = true),
    StructField("enable_photon", BooleanType, nullable = true),
    StructField("enable_serverless_compute", BooleanType, nullable = true),
    StructField("warehouse_type", StringType, nullable = true),
    StructField("num_clusters", LongType, nullable = true),
    StructField("num_active_sessions", LongType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("organization_id", StringType, nullable = false),
    StructField("Pipeline_SnapTS", TimestampType, nullable = true),
    StructField("Overwatch_RunID", StringType, nullable = true),
    StructField("workspace_name", StringType, nullable = true)
  ))

  val instanceDetailsMinimumSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("workspace_name", StringType, nullable = false),
    StructField("Hourly_DBUs", DoubleType, nullable = false),
    StructField("isActive", BooleanType, nullable = false),
    StructField("activeFrom", DateType, nullable = false),
    StructField("activeUntil", DateType, nullable = true),
    StructField("Compute_Contract_Price", DoubleType, nullable = false),
    StructField("Memory_GB", DoubleType, nullable = false),
    StructField("vCPUs", IntegerType, nullable = false),
    StructField("API_Name", StringType, nullable = false)
  ))

  val dbuCostDetailsMinimumSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("workspace_name", StringType, nullable = false),
    StructField("isActive", BooleanType, nullable = false),
    StructField("activeFrom", DateType, nullable = false),
    StructField("activeUntil", DateType, nullable = true),
    StructField("sku", StringType, nullable = false),
    StructField("contract_price", DoubleType, nullable = false)
  ))

  /**
   * Minimum required schema by module. "Minimum Requierd Schema" means that at least these columns of these types
   * must exist for the downstream ETLs to function.
   * The preceeding 2 in module ID 2XXX key integer maps to 2 being silver.
   * Silver Layer modules 2xxx
   * Gold Layer 3xxx
   */
  private[overwatch] val minimumSchemasByModule: Map[Int, StructType] = Map(
    1005 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("state", StringType, nullable = true),
      StructField("cluster_id", StringType, nullable = true)
    )),
    1006 -> auditMasterSchema,
    // SparkExecutors
    2003 -> sparkEventsRawMasterSchema,
    // SparkExecutions
    2005 -> sparkEventsRawMasterSchema,
    // SparkJobs
    2006 -> sparkEventsRawMasterSchema,
    // SparkStages
    2007 -> sparkEventsRawMasterSchema,
    // SparkTasks
    2008 -> sparkEventsRawMasterSchema,
    // PoolsSpec
    2009 -> auditMasterSchema,
    // JobStatus
    2010 -> auditMasterSchema,
    // JobRuns
    2011 -> auditMasterSchema,
    // ClusterSpec
    2014 -> auditMasterSchema,
    // User Account Login
    2016 -> auditMasterSchema,
    // User Account Mods
    2017 -> auditMasterSchema,
    // Notebook Summary
    2018 -> auditMasterSchema,
    // Warehouse Spec
    2021 -> auditMasterSchema,
    // jobStatus
    3002 -> StructType(Seq(
      StructField("timestamp", LongType, nullable = false),
      StructField("organization_id", StringType, nullable = false),
      StructField("workspace_name", StringType, nullable = true),
      StructField("jobId", LongType, nullable = false),
      StructField("actionName", StringType, nullable = false),
      StructField("jobName", StringType, nullable = true),
      StructField("tags", MapType(StringType, StringType), nullable = true),
      StructField("job_type", StringType, nullable = true),
      StructField("format", StringType, nullable = true),
      StructField("existing_cluster_id", StringType, nullable = true),
      StructField("new_cluster", minimumNewClusterSchema, nullable = true),
      StructField("tasks", minimumTasksSchema, nullable = true),
      StructField("job_clusters", minimumJobClustersSchema, nullable = true),
      StructField("libraries", minimumLibrariesSchema, nullable = true),
      StructField("git_source", minimumGitSourceSchema, nullable = true),
      StructField("timeout_seconds", LongType, nullable = true),
      StructField("max_concurrent_runs", LongType, nullable = true),
      StructField("max_retries", LongType, nullable = true),
      StructField("retry_on_timeout", BooleanType, nullable = true),
      StructField("min_retry_interval_millis", LongType, nullable = true),
      StructField("schedule", minimumScheduleSchema, nullable = true),
      StructField("task_detail_legacy", minimumTaskDetailSchema, nullable = true),
      StructField("is_from_dlt", BooleanType, nullable = true),
      StructField("access_control_list", minimumAccessControlListSchema, nullable = true),
      StructField("aclPermissionSet", StringType, nullable = true),
      StructField("grants", minimumGrantsSchema, nullable = true),
      StructField("targetUserId", StringType, nullable = true),
      StructField("sessionId", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      common("response"),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("Pipeline_SnapTS", TimestampType, nullable = true),
      StructField("created_by", StringType, nullable = true),
      StructField("created_ts", StringType, nullable = true),
      StructField("deleted_by", StringType, nullable = true),
      StructField("deleted_ts", LongType, nullable = true),
      StructField("last_edited_by", StringType, nullable = true),
      StructField("last_edited_ts", LongType, nullable = true)
    )),
    // jobRunGold
    3003 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("workspace_name", StringType, nullable = true),
      StructField("jobId", LongType, nullable = true),
      StructField("runId", LongType, nullable = false),
      StructField("startEpochMS", LongType, nullable = false),
      StructField("jobName", StringType, nullable = true),
      StructField("tags", MapType(StringType, StringType), nullable = true),
      StructField("jobRunId", LongType, nullable = true),
      StructField("taskRunId", LongType, nullable = true),
      StructField("taskKey", StringType, nullable = true),
      StructField("clusterId", StringType, nullable = true),
      StructField("cluster_name", StringType, nullable = true),
      StructField("multitaskParentRunId", LongType, nullable = true),
      StructField("parentRunId", LongType, nullable = true),
      StructField("task_detail", minimumTaskDetailSchema, nullable = true),
      StructField("taskDependencies", ArrayType(StringType), nullable = true),
      runtimeField("TaskRunTime"),
      runtimeField("TaskExecutionRunTime"),
      StructField("job_cluster_key", StringType, nullable = true),
      StructField("clusterType", StringType, nullable = true),
      StructField("job_cluster", minimumNewClusterSchema, nullable = true), // use newCluster Schema here since the job_cluster has already been exploded and structured as a cluster spec
      StructField("new_cluster", minimumNewClusterSchema, nullable = true),
      StructField("taskType", StringType, nullable = true),
      StructField("terminalState", StringType, nullable = true),
      StructField("jobTriggerType", StringType, nullable = true),
      StructField("schedule", minimumScheduleSchema, nullable = true),
      StructField("libraries", minimumLibrariesSchema, nullable = true),
      StructField("manual_override_params", minimumManualOverrideParamsSchema, nullable = true),
      StructField("repairId", LongType, nullable = true),
      StructField("repair_details", minimumRepairDetailsSchema, nullable = true),
      StructField("run_name", StringType, nullable = true),
      StructField("timeout_seconds", LongType, nullable = true),
      StructField("retry_on_timeout", BooleanType, nullable = true),
      StructField("max_retries", LongType, nullable = true),
      StructField("min_retry_interval_millis", LongType, nullable = true),
      StructField("max_concurrent_runs", LongType, nullable = true),
      StructField("run_as_user_name", StringType, nullable = true),
      StructField("workflow_context", StringType, nullable = true),
      StructField("task_detail_legacy", minimumTaskDetailSchema, nullable = true),
      StructField("submitRun_details", minimumSubmitRunDetailsSchema, nullable = true),
      StructField("created_by", StringType, nullable = true),
      StructField("last_edited_by", StringType, nullable = true)
    )),
    // clusterStateFact
    3005 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("cluster_id", StringType, nullable = true),
      StructField("isRunning", BooleanType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("current_num_workers", LongType, nullable = true),
      StructField("target_num_workers", LongType, nullable = true),
      StructField("counter_reset", IntegerType, nullable = true),
      StructField("reset_partition", LongType, nullable = true),
      StructField("unixTimeMS_state_start", LongType, nullable = true),
      StructField("unixTimeMS_state_end", LongType, nullable = true),
      StructField("timestamp_state_start", TimestampType, nullable = true),
      StructField("timestamp_state_end", TimestampType, nullable = true),
      StructField("state_start_date", DateType, nullable = true),
      StructField("uptime_in_state_S", DoubleType, nullable = true),
      StructField("uptime_since_restart_S", DoubleType, nullable = true),
      StructField("cloud_billable", BooleanType, nullable = true),
      StructField("databricks_billable", BooleanType, nullable = true),
      StructField("uptime_in_state_H", DoubleType, nullable = true),
      StructField("state_dates", ArrayType(DateType, containsNull = true), nullable = true),
      StructField("days_in_state", IntegerType, nullable = true)
    )),
    // Account Mod Gold
    3007 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("endpoint", StringType, nullable = true),
      StructField("modified_by", StringType, nullable = true),
      StructField("user_name", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("group_name", StringType, nullable = true),
      StructField("group_id", StringType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      common("response")
    )),
    // Account Login Gold
    3008 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = true),
      StructField("login_date", DateType, nullable = true),
      StructField("login_type", StringType, nullable = true),
      StructField("login_user", StringType, nullable = true),
      StructField("user_email", StringType, nullable = true),
      StructField("ssh_login_details", StructType(Seq(
        StructField("instance_id", StringType, nullable = true)
      )), nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      common("response")
    )),
    // poolsGold
    3009 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = false),
      StructField("date", DateType, nullable = true),
      StructField("instance_pool_id", StringType, nullable = false),
      StructField("instance_pool_name", StringType, nullable = true),
      StructField("node_type_id", StringType, nullable = true),
      StructField("idle_instance_autotermination_minutes", LongType, nullable = true),
      StructField("min_idle_instances", LongType, nullable = true),
      StructField("max_capacity", LongType, nullable = true),
      StructField("preloaded_spark_versions", StringType, nullable = true),
      StructField("aws_attributes",MapType(StringType,StringType, valueContainsNull = true), nullable = true),
      StructField("azure_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
      StructField("gcp_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
      StructField("custom_tags",MapType(StringType,StringType, valueContainsNull = true), nullable = true),
      StructField("create_details", poolsCreateSchema, nullable = true),
      StructField("delete_details", poolsDeleteSchema, nullable = true),
      StructField("request_details", poolsRequestDetails, nullable = true)
    )),
    // sparkJob
    3010 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("startTimestamp", LongType, nullable = true),
      StructField("user_email", StringType, nullable = true),
      StructField("JobID", LongType, nullable = true),
      StructField("JobGroupID", StringType, nullable = true),
      StructField("StageIDs", ArrayType(LongType, containsNull = true), nullable = true),
      StructField("ExecutionID", StringType, nullable = true),
      StructField("JobResult", StructType(Seq(
        StructField("Exception", StructType(Seq(
          StructField("Message", StringType, nullable = true)
        )), nullable = true)
      )), nullable = true),
      StructField("PowerProperties",
        StructType(Seq(
          StructField("JobGroupID", StringType, nullable = true),
          StructField("ExecutionID", StringType, nullable = true),
          StructField("SparkDBIsolationID", StringType, nullable = true),
          StructField("AzureOAuth2ClientID", StringType, nullable = true),
          StructField("RDDScope", StringType, nullable = true),
          StructField("SparkDBREPLID", StringType, nullable = true),
          StructField("NotebookID", StringType, nullable = true),
          StructField("NotebookPath", StringType, nullable = true),
          StructField("UserEmail", StringType, nullable = true),
          StructField("SparkDBJobID", StringType, nullable = true),
          StructField("SparkDBRunID", StringType, nullable = true),
          StructField("sparkDBJobType", StringType, nullable = true),
          StructField("clusterDetails",
            StructType(Seq(
              StructField("ClusterID", StringType, nullable = true)
            )), nullable = true)
        )), nullable = true),
      runtimeField("JobRunTime"),
      common("startfilenamegroup"),
      common("endfilenamegroup")
    )),
    // sparkStage
    3011 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("StageID", LongType, nullable = true),
      StructField("StageAttemptID", LongType, nullable = true),
      StructField("startTimestamp", LongType, nullable = true),
      StructField("StageInfo",
        StructType(Seq(
          StructField("CompletionTime", LongType, nullable = true),
          StructField("Details", StringType, nullable = true),
          StructField("FailureReason", StringType, nullable = true),
          StructField("NumberofTasks", LongType, nullable = true),
          StructField("ParentIDs", ArrayType(LongType, containsNull = true), nullable = true),
          StructField("SubmissionTime", LongType, nullable = true)
        )), nullable = true),
      StructField("startDate", DateType, nullable = false),
      runtimeField("StageRunTime"),
      common("startfilenamegroup"),
      common("endfilenamegroup")
    )),
    // sparkTasks
    3012 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("StageID", LongType, nullable = true),
      StructField("StageAttemptID", LongType, nullable = true),
      StructField("TaskID", LongType, nullable = true),
      StructField("TaskAttempt", LongType, nullable = true),
      StructField("ExecutorID", LongType, nullable = true),
      StructField("Host", StringType, nullable = true),
      StructField("TaskEndReason",
        StructType(Seq(
          StructField("ClassName", StringType, nullable = true),
          StructField("Description", StringType, nullable = true),
          StructField("FullStackTrace", StringType, nullable = true),
          StructField("KillReason", StringType, nullable = true),
          StructField("Reason", StringType, nullable = true)
        )), nullable = true),
      StructField("TaskMetrics",
        StructType(Seq(
          StructField("ExecutorCPUTime", LongType, nullable = true),
          StructField("ExecutorRunTime", LongType, nullable = true),
          StructField("MemoryBytesSpilled", LongType, nullable = true),
          StructField("DiskBytesSpilled", LongType, nullable = true),
          StructField("ResultSize", LongType, nullable = true)
        )), nullable = true),
      StructField("TaskExecutorMetrics",
        StructType(Seq(
          StructField("JVMHeapMemory", LongType, nullable = true)
        )), nullable = true),
      StructField("TaskType", StringType, nullable = true),
      StructField("startDate", DateType, nullable = false),
      runtimeField("TaskRunTime"),
      common("startfilenamegroup"),
      common("endfilenamegroup")
    ))
  )

  def get(module: Module): Option[StructType] = minimumSchemasByModule.get(module.moduleId)

  def get(moduleId: Int): Option[StructType] = minimumSchemasByModule.get(moduleId)

  val deployementMinimumSchema:StructType = StructType(Seq(
    StructField("workspace_name", StringType, nullable = false),
    StructField("workspace_id", StringType, nullable = false),
    StructField("workspace_url", StringType, nullable = false),
    StructField("api_url", StringType, nullable = false),
    StructField("cloud", StringType, nullable = false),
    StructField("primordial_date", DateType, nullable = false),
    StructField("storage_prefix", StringType, nullable = false),
    StructField("etl_database_name", StringType, nullable = false),
    StructField("consumer_database_name", StringType, nullable = false),
    StructField("secret_scope", StringType, nullable = false),
    StructField("secret_key_dbpat", StringType, nullable = false),
    StructField("auditlogprefix_source_path", StringType, nullable = true),
    StructField("eh_name", StringType, nullable = true),
    StructField("eh_scope_key", StringType, nullable = true),
    StructField("interactive_dbu_price", DoubleType, nullable = false),
    StructField("automated_dbu_price", DoubleType, nullable = false),
    StructField("sql_compute_dbu_price", DoubleType, nullable = false),
    StructField("jobs_light_dbu_price", DoubleType, nullable = false),
    StructField("max_days", IntegerType, nullable = false),
    StructField("excluded_scopes", StringType, nullable = true),
    StructField("active", BooleanType, nullable = false),
    StructField("proxy_host", StringType, nullable = true),
    StructField("proxy_port", IntegerType, nullable = true),
    StructField("proxy_user_name", StringType, nullable = true),
    StructField("proxy_password_scope", StringType, nullable = true),
    StructField("proxy_password_key", StringType, nullable = true),
    StructField("success_batch_size", IntegerType, nullable = true),
    StructField("error_batch_size", IntegerType, nullable = true),
    StructField("enable_unsafe_SSL", BooleanType, nullable = true),
    StructField("thread_pool_size", IntegerType, nullable = true),
    StructField("api_waiting_time", LongType, nullable = true),
    StructField("mount_mapping_path", StringType, nullable = true),
    StructField("temp_dir_path", StringType, nullable = true),
    StructField("eh_conn_string", StringType, nullable = true),
    StructField("aad_tenant_id", StringType, nullable = true),
    StructField("aad_client_id", StringType, nullable = true),
    StructField("aad_client_secret_key", StringType, nullable = true),
    StructField("aad_authority_endpoint", StringType, nullable = true),
    StructField("sql_endpoint", StringType, nullable = true)
  ))

  val mountMinimumSchema: StructType = StructType(Seq(
    StructField("mountPoint", StringType, nullable = false),
    StructField("source", StringType, nullable = false),
    StructField("workspace_id", StringType, nullable = false)
  ))

  val warehouseDbuDetailsMinimumSchema: StructType = StructType(Seq(
    StructField("organization_id", StringType, nullable = false),
    StructField("workspace_name", StringType, nullable = false),
    StructField("cloud", StringType, nullable = false),
    StructField("cluster_size", StringType, nullable = false),
    StructField("driver_size", StringType, nullable = false),
    StructField("worker_count", IntegerType, nullable = false),
    StructField("total_dbus", IntegerType, nullable = false),
    StructField("activeFrom", DateType, nullable = false),
    StructField("activeUntil", DateType, nullable = true)
  ))
}
