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
        StructField("runId", LongType, nullable = true),
        StructField("run_id", LongType, nullable = true),
        StructField("idInJob", LongType, nullable = true),
        StructField("jobClusterType", StringType, nullable = true),
        StructField("jobTerminalState", StringType, nullable = true),
        StructField("jobTriggerType", StringType, nullable = true),
        StructField("notebook_params", StringType, nullable = true),
        StructField("workflow_context", StringType, nullable = true),
        StructField("libraries", StringType, nullable = true),
        StructField("run_name", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("timeout_seconds", StringType, nullable = true),
        StructField("schedule", StringType, nullable = true),
        StructField("notebook_task", StringType, nullable = true),
        StructField("spark_python_task", StringType, nullable = true),
        StructField("spark_jar_task", StringType, nullable = true),
        StructField("shell_command_task", StringType, nullable = true),
        StructField("new_settings", StringType, nullable = true),
        StructField("existing_cluster_id", StringType, nullable = true),
        StructField("new_cluster", StringType, nullable = true),
        StructField("aclPermissionSet", StringType, nullable = true),
        StructField("grants", StringType, nullable = true),
        StructField("targetUserId", StringType, nullable = true),
        StructField("aws_attributes", StringType, nullable = true),
        StructField("azure_attributes", StringType, nullable = true),
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
        // EXPLICIT EXCLUSIONS -- fields will not be in targetDF
        StructField("organization_id", NullType, nullable = true),
        StructField("orgId", NullType, nullable = true),
        StructField("version", NullType, nullable = true)
      )), nullable = true),
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
    StructField("ExecutorID", LongType, nullable = true),
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
        StructField("ExecutorID", LongType, nullable = true),
        StructField("FinishTime", LongType, nullable = true),
        StructField("ParentIDs", StringType, nullable = true)
      )), nullable = true),
    StructField("StageInfo",
      StructType(Seq(
        StructField("StageID", StringType, nullable = true),
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

  // Minimum required Schedule Schema
  val minimumScheduleSchema: StructType = StructType(Seq(
    StructField("pause_status", StringType, true),
    StructField("quartz_cron_expression", StringType, true),
    StructField("timezone_id", StringType, true)
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

  // minimum new jobs settings struct
  val minimumNewSettingsSchema: StructType = StructType(Seq(
    StructField("existing_cluster_id", StringType, true),
    StructField("max_concurrent_runs", LongType, true),
    StructField("name", StringType, true),
    StructField("new_cluster", minimumNewClusterSchema, true),
    StructField("timeout_seconds", LongType, true)
  ))

  // simplified new settings struct
  private[overwatch] val simplifiedNewSettingsSchema = StructType(Seq(
    StructField("email_notifications",
      StructType(Seq(
        StructField("no_alert_for_skipped_runs", BooleanType, true),
        StructField("on_failure", ArrayType(StringType, true), true)
      )), true),
    StructField("existing_cluster_id", StringType, true),
    StructField("max_concurrent_runs", LongType, true),
    StructField("name", StringType, true),
    StructField("new_cluster", minimumNewClusterSchema, true),
    StructField("notebook_task",
      StructType(Seq(
        StructField("base_parameters", MapType(StringType, StringType, true), true),
        StructField("notebook_path", StringType, true)
      )), true),
    StructField("schedule", minimumScheduleSchema, true),
    StructField("spark_jar_task",
      StructType(Seq(
        StructField("jar_uri", StringType, true),
        StructField("main_class_name", StringType, true),
        StructField("parameters", ArrayType(StringType, true), true)
      )), true),
    StructField("timeout_seconds", LongType, true)
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
    StructField("organization_id",StringType, nullable = false),
    StructField("Pipeline_SnapTS",TimestampType, nullable = false),
    StructField("instance_pool_id",StringType, nullable = false),
    StructField("instance_pool_name",StringType, nullable = true),
    StructField("node_type_id",StringType, nullable = true),
    StructField("idle_instance_autotermination_minutes",LongType, nullable = true),
    StructField("min_idle_instances",LongType, nullable = true),
    StructField("max_capacity",LongType, nullable = true),
    StructField("preloaded_spark_versions",ArrayType(StringType, containsNull = true), nullable = true),
    StructField("aws_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("azure_attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
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
    StructField("requestId",StringType, nullable = true),
    common("response"),
    StructField("sessionId",StringType, nullable = true),
    StructField("sourceIPAddress",StringType, nullable = true),
    StructField("userAgent",StringType, nullable = true)
  ))

  private val s3LogSchema = StructType(Seq(
    StructField("canned_acl",StringType,nullable = true),
    StructField("destination",StringType,nullable = true),
    StructField("enable_encryption",BooleanType,nullable = true),
    StructField("region",StringType,nullable = true)
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
        StructField("max_workers",LongType,nullable = true),
        StructField("min_workers",LongType,nullable = true)
      )),nullable = true),
    StructField("autotermination_minutes",LongType,nullable = true),
    StructField("cluster_id",StringType,nullable = true),
    StructField("cluster_log_conf",logConfSchema,nullable = true),
    StructField("cluster_name",StringType,nullable = true),
    StructField("cluster_source",StringType,nullable = true),
    StructField("creator_user_name",StringType,nullable = true),
    StructField("driver_instance_pool_id",StringType,nullable = true),
    StructField("driver_node_type_id",StringType,nullable = true),
    StructField("enable_elastic_disk",BooleanType,nullable = true),
    StructField("enable_local_disk_encryption",BooleanType,nullable = true),
    StructField("instance_pool_id",StringType,nullable = true),
    StructField("init_scripts",ArrayType(StructType(Seq(
      StructField("dbfs",StructType(Seq(
        StructField("destination",StringType,nullable = true)
      )),nullable = true)
    )),containsNull = true),nullable = true),
    StructField("node_type_id",StringType,nullable = true),
    StructField("num_workers",LongType,nullable = true),
    StructField("single_user_name",StringType,nullable = true),
    StructField("spark_version",StringType,nullable = true),
    StructField("state",StringType,nullable = true),
    StructField("default_tags",MapType(StringType,StringType,valueContainsNull = true),nullable = true),
    StructField("custom_tags",MapType(StringType,StringType,valueContainsNull = true),nullable = true),
    StructField("start_time",LongType,nullable = true),
    StructField("terminated_time",LongType,nullable = true),
    StructField("organization_id",StringType,nullable = false),
    StructField("Pipeline_SnapTS",TimestampType,nullable = true),
    StructField("Overwatch_RunID",StringType,nullable = true),
    StructField("workspace_name",StringType,nullable = true)
  ))

  val clusterEventsMinimumSchema: StructType = StructType(Seq(
    StructField("organization_id",StringType,nullable = false),
    StructField("cluster_id",StringType,nullable = false),
    StructField("timestamp",LongType,nullable = false),
    StructField("type",StringType,nullable = true),
    StructField("details",
      StructType(Seq(
        StructField("cluster_size",
          StructType(Seq(
            StructField("autoscale",
              StructType(Seq(
                StructField("max_workers",LongType,nullable = true),
                StructField("min_workers",LongType,nullable = true)
              )),nullable = true),
            StructField("num_workers",LongType,nullable = true)
          )),nullable = true),
        StructField("current_num_workers",LongType,nullable = true),
        StructField("target_num_workers",LongType,nullable = true),
        StructField("user",StringType,nullable = true),
        StructField("disk_size",LongType,nullable = true),
        StructField("free_space",LongType,nullable = true),
        StructField("instance_id",StringType,nullable = true),
        StructField("previous_disk_size",LongType,nullable = true),
        StructField("driver_state_message",StringType,nullable = true)
      )),nullable = true)
  ))

  val jobSnapMinimumSchema: StructType = StructType(Seq(
    StructField("created_time",LongType,nullable = true),
    StructField("creator_user_name",StringType,nullable = true),
    StructField("job_id",LongType,nullable = true),
    StructField("settings",StructType(Seq(
      StructField("existing_cluster_id",StringType,nullable = true),
      StructField("max_concurrent_runs",LongType,nullable = true),
      StructField("name",StringType,nullable = true),
      StructField("new_cluster",minimumNewClusterSchema,nullable = true),
      StructField("notebook_task",
        StructType(Seq(
          StructField("base_parameters", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
          StructField("notebook_path", StringType, nullable = true)
        )), nullable = true),
        StructField("schedule",minimumScheduleSchema,nullable = true),
      StructField("spark_jar_task",
        StructType(Seq(
          StructField("jar_uri", StringType, nullable = true),
          StructField("main_class_name", StringType, nullable = true),
          StructField("parameters", ArrayType(StringType, containsNull = true), nullable = true)
        )), nullable = true),
      StructField("spark_python_task",
        StructType(Seq(
          StructField("parameters",ArrayType(StringType,containsNull = true),nullable = true),
          StructField("python_file",StringType,nullable = true)
        )),nullable = true),
      StructField("timeout_seconds",LongType,nullable = true),
      StructField("format",StringType,nullable = true),
      StructField("max_retries",LongType,nullable = true),
      StructField("min_retry_interval_millis",LongType,nullable = true),
      StructField("retry_on_timeout",BooleanType,nullable = true)
    )),nullable = true),
    StructField("organization_id",StringType,nullable = false),
    StructField("Pipeline_SnapTS",TimestampType,nullable = true),
    StructField("Overwatch_RunID",StringType,nullable = true),
    StructField("job_type",StringType,nullable = true),
    StructField("workspace_name",StringType,nullable = true)
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
    // jobStatus
    3002 -> StructType(Seq(
      StructField("timestamp", LongType, true),
      StructField("organization_id", StringType, false),
      StructField("jobId", LongType, true),
      StructField("serviceName", StringType, true),
      StructField("actionName", StringType, true),
      StructField("jobName", StringType, true),
      StructField("timeout_seconds", StringType, true),
      StructField("notebook_path", StringType, true),
      StructField("schedule", minimumScheduleSchema, true),
      StructField("new_settings", minimumNewSettingsSchema, true),
      StructField("aclPermissionSet", StringType, true),
      StructField("grants", StringType, true),
      StructField("targetUserId", StringType, true),
      StructField("sessionId", StringType, true),
      StructField("requestId", StringType, true),
      StructField("userAgent", StringType, true),
      common("response"),
      StructField("sourceIPAddress", StringType, true),
      StructField("version", StringType, true),
      StructField("Pipeline_SnapTS", TimestampType, true),
      StructField("job_type", StringType, true),
      StructField("cluster_spec",
        StructType(Seq(
          StructField("existing_cluster_id", StringType, true),
          StructField("new_cluster", minimumNewClusterSchema, true)
        )), true),
      StructField("created_by", StringType, true),
      StructField("created_ts", StringType, true),
      StructField("deleted_by", StringType, true),
      StructField("deleted_ts", LongType, true),
      StructField("last_edited_by", StringType, true),
      StructField("last_edited_ts", LongType, true)
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
      StructField("organization_id",StringType, nullable = false),
      StructField("serviceName",StringType, nullable = true),
      StructField("actionName",StringType, nullable = true),
      StructField("timestamp",LongType, nullable = false),
      StructField("date",DateType, nullable = true),
      StructField("instance_pool_id",StringType, nullable = false),
      StructField("instance_pool_name",StringType, nullable = true),
      StructField("node_type_id",StringType, nullable = true),
      StructField("idle_instance_autotermination_minutes",LongType, nullable = true),
      StructField("min_idle_instances",LongType, nullable = true),
      StructField("max_capacity",LongType, nullable = true),
      StructField("preloaded_spark_versions",StringType, nullable = true),
      StructField("azure_attributes",MapType(StringType,StringType, valueContainsNull = true), nullable = true),
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
      StructField("TaskType", StringType, nullable = true),
      StructField("startDate", DateType, nullable = false),
      runtimeField("TaskRunTime"),
      common("startfilenamegroup"),
      common("endfilenamegroup")
    ))
  )

  def get(module: Module): Option[StructType] = minimumSchemasByModule.get(module.moduleId)

  def get(moduleId: Int): Option[StructType] = minimumSchemasByModule.get(moduleId)
}
