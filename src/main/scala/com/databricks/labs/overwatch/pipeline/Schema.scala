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
    StructField("path",StringType, nullable = true),
    StructField("reason",StringType, nullable = true),
    StructField("record",StringType, nullable = true)
  ))

  lazy private[overwatch] val auditMasterSchema = StructType(Seq(
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
        StructField("instance_pool_id", StringType, nullable = true),
        StructField("instance_pool_name", StringType, nullable = true),
        StructField("spark_version", StringType, nullable = true),
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

  lazy private[overwatch] val sparkEventsRawMasterSchema = StructType(Seq(
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

  //  Array(DiskBytesSpilled, ExecutorCPUTime, ExecutorDeserializeCPUTime,
  //  ExecutorDeserializeTime, ExecutorRunTime, InputMetrics, JVMGCTime,
  //  MemoryBytesSpilled, OutputMetrics, PeakExecutionMemory,
  //  ResultSerializationTime, ResultSize, ShuffleReadMetrics,
  //  ShuffleWriteMetrics, UpdatedBlocks)

  /**
   * Minimum required schema by module. "Minimum Requierd Schema" means that at least these columns of these types
   * must exist for the downstream ETLs to function.
   * The preceeding 2 in module ID 2XXX key integer maps to 2 being silver.
   * Silver Layer modules 2xxx
   * Gold Layer 3xxx
   */
  private[overwatch] val minimumSchemasByModule: Map[Int, StructType] = Map(
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
    // clusterStateFact
    3005 -> StructType(Seq(
      StructField("organization_id", StringType, nullable = false),
      StructField("cluster_id", StringType, nullable = true),
      StructField("details", StructType(Seq(
        StructField("cluster_size",
          StructType(Seq(
            StructField("autoscale",
              StructType(Seq(
                StructField("max_workers", LongType, nullable = true),
                StructField("min_workers", LongType, nullable = true)
              )), nullable = true),
            StructField("num_workers", LongType, nullable = true)
          )), nullable = true),
        StructField("current_num_workers", LongType, nullable = true)
      )), nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("type", StringType, nullable = true)
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
