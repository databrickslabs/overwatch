package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.WorkflowsTransforms._
import com.databricks.labs.overwatch.pipeline.DbsqlTransforms._
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


trait SilverTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val responseSuccessFilter: Column = $"response.statusCode" === 200

  private def appendPowerProperties: Column = {
    struct(
      element_at('Properties, "sparkappname").alias("AppName"),
      element_at('Properties, "sparkdatabricksapiurl").alias("WorkspaceURL"),
      element_at('Properties, "sparkjobGroupid").alias("JobGroupID"),
      element_at('Properties, "sparkdatabrickscloudProvider").alias("CloudProvider"),
      element_at('Properties, "principalIdpObjectId").alias("principalIdpObjectId"),
      struct(
        element_at('Properties, "sparkdatabricksclusterSource").alias("ClusterSource"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsautoTerminationMinutes").alias("AutoTerminationMinutes"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterAllTags").alias("ClusterTags"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterAvailability").alias("ClusterAvailability"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterId").alias("ClusterID"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterInstancePoolId").alias("InstancePoolID"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterMaxWorkers").alias("MaxWorkers"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterMinWorkers").alias("MinWorkers"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterName").alias("Name"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterOwnerUserId").alias("OwnerUserID"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterScalingType").alias("ScalingType"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterSpotBidPricePercent").alias("SpotBidPricePercent"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterTargetWorkers").alias("TargetWorkers"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsclusterWorkers").alias("ActualWorkers"),
        element_at('Properties, "sparkdatabricksclusterUsageTagscontainerZoneId").alias("ZoneID"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsdataPlaneRegion").alias("Region"),
        element_at('Properties, "sparkdatabricksclusterUsageTagsdriverNodeType").alias("DriverNodeType"),
        element_at('Properties, "sparkdatabricksworkerNodeTypeId").alias("WorkerNodeType"),
        element_at('Properties, "sparkdatabricksclusterUsageTagssparkVersion").alias("SparkVersion")
      ).alias("ClusterDetails"),
      element_at('Properties, "sparkdatabricksnotebookid").alias("NotebookID"),
      element_at('Properties, "sparkdatabricksnotebookpath").alias("NotebookPath"),
      element_at('Properties, "sparkdatabrickssparkContextId").alias("SparkContextID"),
      element_at('Properties, "sparkdriverhost").alias("DriverHostIP"),
      element_at('Properties, "sparkdrivermaxResultSize").alias("DriverMaxResults"),
      element_at('Properties, "sparkexecutorid").alias("ExecutorID"),
      element_at('Properties, "sparkexecutormemory").alias("ExecutorMemory"),
      element_at('Properties, "sparksqlexecutionid").alias("ExecutionID"),
      element_at('Properties, "sparksqlexecutionparent").alias("ExecutionParent"),
      element_at('Properties, "sparkdatabricksisolationID").alias("SparkDBIsolationID"),
      element_at('Properties, "sparkhadoopfsazureaccountoauth2clientid").alias("AzureOAuth2ClientID"),
      element_at('Properties, "sparkrddscope").alias("RDDScope"),
      element_at('Properties, "sparkdatabricksreplId").alias("SparkDBREPLID"),
      element_at('Properties, "sparkdatabricksjobid").alias("SparkDBJobID"),
      element_at('Properties, "sparkdatabricksjobrunId").alias("SparkDBRunID"),
      element_at('Properties, "sparkdatabricksjobtype").alias("sparkDBJobType"),
      element_at('Properties, "sparksqlshufflepartitions").alias("ShufflePartitions"),
      element_at('Properties, "user").alias("UserEmail"),
      element_at('Properties, "userID").alias("UserID")
    )
  }

  // TODO -- Value Opportunity -- Issue_84

  // TODO -- Issue_81

  //  protected def getJDBCSession(sessionStartDF: DataFrame, sessionEndDF: DataFrame): DataFrame = {
  //    sessionStartDF
  //      .join(sessionEndDF, Seq("SparkContextID", "sessionId"))
  //      .withColumn("ServerSessionRunTime",
  //        TransformFunctions.subtractTime('startTime, 'finishTime))
  //      .drop("startTime", "finishTime")
  //  }
  //
  //  protected def getJDBCOperation(operationStartDF: DataFrame, operationEndDF: DataFrame): DataFrame = {
  //    operationStartDF
  //      .join(operationEndDF, Seq("SparkContextID", "id"))
  //      .withColumn("ServerOperationRunTime",
  //        TransformFunctions.subtractTime('startTime, 'closeTime))
  //      .drop("startTime", "finishTime")
  //  }

  private def simplifyExecutorAdded(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerExecutorAdded")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID, 'ExecutorID, 'executorInfo, 'Timestamp.alias("executorAddedTS"),
        'filenameGroup.alias("startFilenameGroup"))
  }

  private def simplifyExecutorRemoved(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerExecutorRemoved")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID, 'ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"),
        'filenameGroup.alias("endFilenameGroup"))
  }

  protected def executor(
                          sparkEventsLag30D: DataFrame,
                          fromTime: TimeTypes
                        )(newSparkEvents: DataFrame): DataFrame = {


    // look back up to 30 days to match long living cluster node and match removal with added row
    val executorAdded = simplifyExecutorAdded(sparkEventsLag30D).alias("executorAdded")
    // executor removed events are not always captured. Perhaps ignored when decommissioned at end of cluster lifecycle
    // remove events do occur during node lost and decommissioned during cluster alive
    val executorRemoved = simplifyExecutorRemoved(newSparkEvents).alias("executorRemoved")
    val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutorID")

    executorAdded
      .joinWithLag(executorRemoved, joinKeys, "fileCreateDate", lagDays = 30, joinType = "left")
      .dropDuplicates(joinKeys) // events are emitted at least once -- remove potential duplicate keys
      .filter(coalesce('executorRemovedTS, 'executorAddedTS, lit(0)) >= fromTime.asUnixTimeMilli)
      .withColumn("ExecutorAliveTime",
        TransformFunctions.subtractTime('executorAddedTS, 'executorRemovedTS))
      .drop("executorAddedTS", "executorRemovedTS", "fileCreateDate")
      .withColumn("ExecutorAliveTime", struct(
        $"ExecutorAliveTime.startEpochMS".alias("AddedEpochMS"),
        $"ExecutorAliveTime.startTS".alias("AddedTS"),
        $"ExecutorAliveTime.endEpochMS".alias("RemovedEpochMS"),
        $"ExecutorAliveTime.endTS".alias("RemovedTS"),
        $"ExecutorAliveTime.runTimeMS".alias("uptimeMS"),
        $"ExecutorAliveTime.runTimeS".alias("uptimeS"),
        $"ExecutorAliveTime.runTimeM".alias("uptimeM"),
        $"ExecutorAliveTime.runTimeH".alias("uptimeH")
      ))
      .withColumn("addedTimestamp", $"ExecutorAliveTime.AddedEpochMS")
  }

  protected def enhanceApplication()(df: DataFrame): DataFrame = {
    df.select('organization_id, 'SparkContextID, 'AppID, 'AppName,
      TransformFunctions.toTS('Timestamp).alias("AppStartTime"),
      'Pipeline_SnapTS, 'filenameGroup)
  }

  private val uniqueTimeWindow = Window.partitionBy('organization_id, 'SparkContextID, 'executionId)

  private def simplifyExecutionsStart(df: DataFrame): DataFrame = {
    df.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID, 'description, 'details, 'executionId.alias("ExecutionID"),
        'time.alias("SqlExecStartTime"),
        'filenameGroup.alias("startFilenameGroup"))
      .withColumn("timeRnk", rank().over(uniqueTimeWindow.orderBy('SqlExecStartTime)))
      .withColumn("timeRn", row_number().over(uniqueTimeWindow.orderBy('SqlExecStartTime)))
      .filter('timeRnk === 1 && 'timeRn === 1)
      .drop("timeRnk", "timeRn")
  }

  private def simplifyExecutionsEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID, 'executionId.alias("ExecutionID"),
        'time.alias("SqlExecEndTime"),
        'filenameGroup.alias("endFilenameGroup"))
      .withColumn("timeRnk", rank().over(uniqueTimeWindow.orderBy('SqlExecEndTime)))
      .withColumn("timeRn", row_number().over(uniqueTimeWindow.orderBy('SqlExecEndTime.desc)))
      .filter('timeRnk === 1 && 'timeRn === 1)
      .drop("timeRnk", "timeRn")
  }

  protected def sqlExecutions(
                               sparkEventsLag3D: DataFrame,
                               fromTime: TimeTypes,
                               untilTime: TimeTypes
                             )(newSparkEvents: DataFrame): DataFrame = {
    val executionsStart = simplifyExecutionsStart(sparkEventsLag3D).alias("executionsStart")
    val executionsEnd = simplifyExecutionsEnd(newSparkEvents).alias("executionsEnd")
    val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "ExecutionID")

    //TODO -- review if skew is necessary -- on all DFs
    executionsStart
      .joinWithLag(executionsEnd, joinKeys, "fileCreateDate", joinType = "left")
      .dropDuplicates(joinKeys) // events are emitted at least once -- remove potential duplicate keys
      .filter(coalesce('SqlExecEndTime, 'SqlExecStartTime, lit(0)) >= fromTime.asUnixTimeMilli)
      .withColumn("SqlExecutionRunTime",
        TransformFunctions.subtractTime('SqlExecStartTime, 'SqlExecEndTime))
      .drop("SqlExecStartTime", "SqlExecEndTime", "fileCreateDate")
      .withColumn("startTimestamp", $"SqlExecutionRunTime.startEpochMS")
  }


  protected def simplifyJobStart(sparkEventsBronze: DataFrame): DataFrame = {
    sparkEventsBronze.filter('Event.isin("SparkListenerJobStart"))
      .withColumn("PowerProperties", appendPowerProperties)
      .select(
        'organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID,
        $"PowerProperties.JobGroupID", $"PowerProperties.ExecutionID",
        'JobID, 'StageIDs, 'SubmissionTime, 'PowerProperties,
        'filenameGroup.alias("startFilenameGroup")
      )
  }

  protected def simplifyJobEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerJobEnd")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID, 'JobID, 'JobResult, 'CompletionTime,
        'filenameGroup.alias("endFilenameGroup"))
  }

  protected def sparkJobs(
                           sparkEventsLag3D: DataFrame,
                           fromTime: TimeTypes,
                           untilTime: TimeTypes
                         )(newSparkEvents: DataFrame): DataFrame = {
    val jobStart = simplifyJobStart(sparkEventsLag3D).alias("jobStart")
    val jobEnd = simplifyJobEnd(newSparkEvents).alias("jobEnd")
    val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "JobID")

    jobStart
      .joinWithLag(jobEnd, joinKeys, "fileCreateDate", joinType = "left")
      .dropDuplicates(joinKeys) // events are emitted at least once -- remove potential duplicate keys
      .filter(coalesce('CompletionTime, 'SubmissionTime, lit(0)) >= fromTime.asUnixTimeMilli)
      .withColumn("JobRunTime", TransformFunctions.subtractTime('SubmissionTime, 'CompletionTime))
      .drop("SubmissionTime", "CompletionTime", "fileCreateDate")
      .withColumn("startTimestamp", $"JobRunTime.startEpochMS")
      .withColumn("startDate", $"JobRunTime.startTS".cast("date"))
  }

  protected def simplifyStageStart(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerStageSubmitted")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID,
        $"StageInfo.StageID", $"StageInfo.SubmissionTime", $"StageInfo.StageAttemptID",
        'StageInfo.alias("StageStartInfo"),
        'filenameGroup.alias("startFilenameGroup"))
  }

  protected def simplifyStageEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerStageCompleted")
      .select('organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID,
        $"StageInfo.StageID", $"StageInfo.StageAttemptID", $"StageInfo.CompletionTime",
        'StageInfo.alias("StageEndInfo"), 'filenameGroup.alias("endFilenameGroup")
      )
  }

  protected def sparkStages(
                             sparkEventsLag3D: DataFrame,
                             fromTime: TimeTypes,
                             untilTime: TimeTypes
                           )(newSparkEvents: DataFrame): DataFrame = {
    val stageStart = simplifyStageStart(sparkEventsLag3D).alias("stageStart")
    val stageEnd = simplifyStageEnd(newSparkEvents).alias("stageEnd")
    val joinKeys = Seq("organization_id", "clusterId", "SparkContextID", "StageID", "StageAttemptID")

    stageStart
      .joinWithLag(stageEnd, joinKeys, "fileCreateDate", joinType = "left")
      .dropDuplicates(joinKeys) // events are emitted at least once -- remove potential duplicate keys
      .filter(coalesce('CompletionTime, 'SubmissionTime, lit(0)) >= fromTime.asUnixTimeMilli)
      .withColumn("StageInfo", struct(
        $"StageEndInfo.Accumulables", $"StageEndInfo.CompletionTime", $"StageStartInfo.Details",
        $"StageStartInfo.FailureReason", $"StageEndInfo.NumberofTasks",
        $"StageStartInfo.ParentIDs", $"StageStartInfo.SubmissionTime"
      )).drop("StageEndInfo", "StageStartInfo")
      .withColumn("StageRunTime", TransformFunctions.subtractTime('SubmissionTime, 'CompletionTime))
      .drop("SubmissionTime", "CompletionTime", "fileCreateDate")
      .withColumn("startTimestamp", $"StageRunTime.startEpochMS")
      .withColumn("startDate", $"StageRunTime.startTS".cast("date"))
  }

  // TODO -- Add in Specualtive Tasks Event == org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted
  protected def simplifyTaskStart(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerTaskStart")
      .select(
        // keys
        'organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID,
        'StageID, 'StageAttemptID, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID",
        $"TaskInfo.Attempt".alias("TaskAttempt"),
        $"TaskInfo.Host", $"TaskInfo.LaunchTime",
        // mesasures
        'TaskInfo.alias("TaskStartInfo"),
        'filenameGroup.alias("startFilenameGroup")
      )
  }

  protected def simplifyTaskEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerTaskEnd")
      .select(
        // keys
        'organization_id, 'fileCreateDate, 'clusterId, 'SparkContextID,
        'StageID, 'StageAttemptID, 'TaskEndReason, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID",
        $"TaskInfo.Attempt".alias("TaskAttempt"),
        $"TaskInfo.Host",
        // measures
        $"TaskInfo.FinishTime",
        'TaskInfo.alias("TaskEndInfo"),
        'TaskMetrics,
        'TaskType,
        'TaskExecutorMetrics,
        'filenameGroup.alias("endFilenameGroup")
      )
  }

  // Orphan tasks are a result of "TaskEndReason.Reason" != "Success"
  // Failed tasks lose association with their chain
  protected def sparkTasks(
                            sparkEventsLag3D: DataFrame,
                            fromTime: TimeTypes
                          )(newSparkEvents: DataFrame): DataFrame = {
    val taskStart = simplifyTaskStart(sparkEventsLag3D).alias("taskStart")
    val taskEnd = simplifyTaskEnd(newSparkEvents).alias("taskEnd")
    val joinKeys = Seq(
      "organization_id", "clusterId", "SparkContextID",
      "StageID", "StageAttemptID", "TaskID", "TaskAttempt", "ExecutorID", "Host"
    )

    taskStart
      .joinWithLag(taskEnd, joinKeys, "fileCreateDate", joinType = "left")
      .dropDuplicates(joinKeys) // events are emitted at least once -- remove potential duplicate keys
      .filter(coalesce('FinishTime, 'LaunchTime, lit(0)) >= fromTime.asUnixTimeMilli)
      .withColumn("TaskInfo", struct(
        $"TaskEndInfo.Accumulables", $"TaskEndInfo.Failed", $"TaskEndInfo.FinishTime",
        $"TaskEndInfo.GettingResultTime",
        $"TaskEndInfo.Host", $"TaskEndInfo.Index", $"TaskEndInfo.Killed", $"TaskStartInfo.LaunchTime",
        $"TaskEndInfo.Locality", $"TaskEndInfo.Speculative"
      )).drop("TaskStartInfo", "TaskEndInfo", "fileCreateDate")
      .withColumn("TaskRunTime", TransformFunctions.subtractTime('LaunchTime, 'FinishTime))
      .drop("LaunchTime", "FinishTime")
      .withColumn("startTimestamp", $"TaskRunTime.startEpochMS")
      .withColumn("startDate", $"TaskRunTime.startTS".cast("date"))
  }

  // TODO -- Azure Review
  //  Tested in AWS, azure account logic may be slightly different
  protected def accountLogins()(df: DataFrame): DataFrame = {
    val prunedDF = df
      .select(
        'timestamp,
        'organization_id,
        'serviceName,
        'actionName,
        'date,
        'requestId,
        'sessionId,
        'sourceIPAddress,
        'userAgent,
        'userIdentity,
        'response,
        $"requestParams.instanceId".alias("instance_id"),
        $"requestParams.port".alias("login_port"),
        $"requestParams.publicKey".alias("login_public_key"),
        $"requestParams.containerId".alias("container_id"),
        $"requestParams.userName".alias("container_user_name"),
        $"requestParams.user".alias("login_user")
      )
    val endpointLogins = prunedDF.filter(
      'serviceName === "accounts" &&
        lower('actionName).like("%login%") &&
        $"userIdentity.email" =!= "dbadmin")

    val sshLoginRaw = prunedDF.filter('serviceName === "ssh" && 'actionName === "login")
      .withColumn("actionName", lit("ssh"))

    val accountLogins = endpointLogins.unionByName(sshLoginRaw)

    val sshDetails = sshLoginRaw.select(
      'timestamp,
      'organization_id,
      'date,
      'requestId,
      lit("ssh").alias("login_type"),
      struct(
        'instance_id,
        'login_port,
        'sessionId.alias("session_id"),
        'login_public_key,
        'container_id,
        'container_user_name
      ).alias("ssh_login_details")
    )

    if (!accountLogins.isEmpty) {
      accountLogins
        .select(
          'timestamp,
          'organization_id,
          'date,
          'date.alias("login_date"),
          'serviceName,
          'actionName.alias("login_type"),
          'login_user,
          $"userIdentity.email".alias("user_email"),
          'sourceIPAddress, 'userAgent, 'requestId, 'response
        )
        .join(sshDetails, Seq("organization_id", "date", "timestamp", "login_type", "requestId"), "left")
        .drop("date")
    } else
      throw new NoNewDataException("No New Data", Level.INFO, allowModuleProgression = true)
  }

  // TODO -- Azure Review
  //  Tested in AWS, azure account logic may be slightly different
  protected def accountMods()(df: DataFrame): DataFrame = {
    val uidLookup = Window.partitionBy('organization_id, 'user_name).orderBy('timestamp)
      .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val modifiedAccountsDF = df.filter('serviceName === "accounts" &&
      'actionName.isin("add", "addPrincipalToGroup", "removePrincipalFromGroup", "setAdmin", "updateUser", "delete"))
    if (!modifiedAccountsDF.isEmpty) {
      modifiedAccountsDF
        .select(
          'date, 'timestamp, 'organization_id, 'serviceName, 'actionName,
          $"requestParams.endpoint",
          'requestId,
          $"userIdentity.email".alias("modified_by"),
          $"requestParams.targetUserName".alias("user_name"),
          $"requestParams.targetUserId".alias("user_id"),
          $"requestParams.targetGroupName".alias("group_name"),
          $"requestParams.targetGroupId".alias("group_id"),
          'sourceIPAddress, 'userAgent, 'response
        )
        .withColumn("user_id", when('user_id.isNull, first('user_id, true).over(uidLookup))
          .otherwise('user_id))

    } else
      throw new NoNewDataException("No New Data", Level.INFO, allowModuleProgression = true)
  }

  private val auditBaseCols: Array[Column] = Array(
    'timestamp, 'date, 'organization_id, 'serviceName, 'actionName,
    $"userIdentity.email".alias("userEmail"), 'requestId, 'response)

  private def clusterBase(auditRawDF: DataFrame): DataFrame = {
    val isWarehouse = get_json_object('custom_tags, "$.SqlEndpointId").isNotNull
    val cluster_id_gen_w = Window.partitionBy('organization_id, 'cluster_name).orderBy('timestamp).rowsBetween(Window.currentRow, 1000)
    val cluster_name_gen_w = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp).rowsBetween(Window.currentRow, 1000)
    val cluster_id_gen = first('cluster_id, true).over(cluster_id_gen_w)
    val cluster_name_gen = first('cluster_name, true).over(cluster_name_gen_w)

    val clusterSummaryCols = auditBaseCols ++ Array[Column](
      when('actionName === "create", get_json_object($"response.result", "$.cluster_id"))
        .when('actionName =!= "create" && 'cluster_id.isNull, 'clusterId)
        .otherwise('cluster_id).alias("cluster_id"),
      when('cluster_name.isNull, 'clusterName).otherwise('cluster_name).alias("cluster_name"),
      'clusterState.alias("cluster_state"),
      'driver_node_type_id,
      'node_type_id,
      'num_workers,
      'autoscale,
      'clusterWorkers.alias("actual_workers"),
      'autotermination_minutes,
      'enable_elastic_disk,
      'start_cluster,
      'clusterOwnerUserId,
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'ssh_public_keys,
      'cluster_source,
      'aws_attributes,
      'azure_attributes,
      'gcp_attributes,
      'spark_env_vars,
      'spark_conf,
      when('ssh_public_keys.isNotNull, true).otherwise(false).alias("has_ssh_keys"),
      'acl_path_prefix,
      'driver_instance_pool_id,
      'instance_pool_id,
      'instance_pool_name,
      coalesce('effective_spark_version, 'spark_version).alias("spark_version"),
      'runtime_engine,
      'cluster_creator,
      'idempotency_token,
      'user_id,
      'sourceIPAddress,
      'single_user_name
    )

    val clusterRaw = auditRawDF
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .filter(responseSuccessFilter) // only publish successful edits into the spec table
      .select(clusterSummaryCols: _*)
      .filter(!isWarehouse)
      .withColumn("cluster_id", cluster_id_gen)
      .withColumn("cluster_name", cluster_name_gen)

    val clusterWithStructs = clusterRaw
      .withColumn("aws_attributes", SchemaTools.structFromJson(spark, clusterRaw, "aws_attributes"))
      .withColumn("azure_attributes", SchemaTools.structFromJson(spark, clusterRaw, "azure_attributes"))
      .withColumn("gcp_attributes", SchemaTools.structFromJson(spark, clusterRaw, "gcp_attributes"))
      .scrubSchema

    clusterWithStructs
      .withColumn("aws_attributes", SchemaTools.structToMap(clusterWithStructs, "aws_attributes"))
      .withColumn("azure_attributes", SchemaTools.structToMap(clusterWithStructs, "azure_attributes"))
      .withColumn("gcp_attributes", SchemaTools.structToMap(clusterWithStructs, "gcp_attributes"))
  }

  private def warehouseBase(auditLogDf: DataFrame): DataFrame = {

    val warehouse_id_gen_w = Window.partitionBy('organization_id, 'warehouse_name).orderBy('timestamp).rowsBetween(Window.currentRow, 1000)
    val warehouse_name_gen_w = Window.partitionBy('organization_id, 'warehouse_id).orderBy('timestamp).rowsBetween(Window.currentRow, 1000)
    val warehouse_id_gen = first('warehouse_id, true).over(warehouse_id_gen_w)
    val warehouse_name_gen = first('warehouse_name, true).over(warehouse_name_gen_w)

    val warehouseSummaryCols = auditBaseCols ++ Array[Column](
      deriveWarehouseId.alias("warehouse_id"),
      'name.alias("warehouse_name"),
      'cluster_size,
      'min_num_clusters,
      'max_num_clusters,
      'auto_stop_mins,
      'spot_instance_policy,
      'enable_photon,
      get_json_object('channel, "$.name").alias("channel"),
      'tags,
      'enable_serverless_compute,
      'warehouse_type
    )

    val rawAuditLogDf = auditLogDf
      .filter('actionName.isin("createEndpoint", "editEndpoint", "createWarehouse",
        "editWarehouse", "deleteEndpoint", "deleteWarehouse")
        && responseSuccessFilter
        && 'serviceName === "databrickssql")


    if(rawAuditLogDf.isEmpty)
      throw new NoNewDataException("No New Data", Level.INFO, allowModuleProgression = true)

    val auditLogDfWithStructs = rawAuditLogDf
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .select(warehouseSummaryCols: _*)
      .withColumn("warehouse_id", warehouse_id_gen)
      .withColumn("warehouse_name", warehouse_name_gen)

    val auditLogDfWithStructsToMap = auditLogDfWithStructs
      .withColumn("tags", SchemaTools.structFromJson(spark, auditLogDfWithStructs, "tags"))
      .scrubSchema

    val filteredAuditLogDf = auditLogDfWithStructsToMap
      .withColumn("tags", SchemaTools.structToMap(auditLogDfWithStructsToMap, "tags"))
      .withColumn("source_table",lit("audit_log_bronze"))
    filteredAuditLogDf
  }

  protected def buildPoolsSpec(
                                poolSnapDF: DataFrame,
                                isFirstRun: Boolean,
                                fromTime: TimeTypes
                              )(auditIncrementalDF: DataFrame): DataFrame = {

    // If no pools snap table present, fast fail
    if (poolSnapDF.isEmpty) throw new NoNewDataException(
      s"""
         |No pools currently exist in this workspace. As such this module is being skipped and the pools module
         |state will be moved forward. If this workspace is not using pools, it's recommended that the pools scope
         |be disabled for performance improvements.
         |""".stripMargin, Level.WARN, allowModuleProgression = true)

    val lastPoolValue = Window.partitionBy('organization_id, 'instance_pool_id)
      .orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // audit logs semantics for pools are at least once, this is to filter out duplicate publications
    val exactlyOnceFilterW = Window.partitionBy('organization_id, 'instance_pool_id)
      .orderBy('timestamp)

    val poolsSnapDFUntilCurrent = poolSnapDF
      .withColumn("preloaded_spark_versions", to_json('preloaded_spark_versions))
      .withColumn("poolSnapDetails", struct(expr("*")))

    val poolSnapLookup = poolsSnapDFUntilCurrent
      .select(
        'organization_id,
        (unix_timestamp('Pipeline_SnapTS) * 1000).alias("timestamp"),
        'instance_pool_id,
        'poolSnapDetails
      )

    val deleteCol = when('actionName === "delete" && responseSuccessFilter,
      struct(
        $"userIdentity.email".alias("deleted_by"),
        'timestamp.alias("deleted_at_epochMillis"),
        from_unixtime('timestamp / 1000).cast("timestamp").alias("deleted_at")
      )).otherwise(lit(null).cast(Schema.poolsDeleteSchema)).alias("delete_details")

    val createCol = when('actionName === "create" && responseSuccessFilter,
      struct(
        $"userIdentity.email".alias("created_by"),
        'timestamp.alias("created_at_epochMillis"),
        from_unixtime('timestamp / 1000).cast("timestamp").alias("created_at")
      )).otherwise(lit(null).cast(Schema.poolsCreateSchema)).alias("create_details")

    val poolsSelects = Array[Column](
      'serviceName,
      'actionName,
      'organization_id,
      'timestamp,
      'date,
      'instance_pool_id,
      PipelineFunctions.fillForward("instance_pool_name", lastPoolValue, Seq($"poolSnapDetails.instance_pool_name")),
      PipelineFunctions.fillForward("node_type_id", lastPoolValue, Seq($"poolSnapDetails.node_type_id")),
      PipelineFunctions.fillForward("idle_instance_autotermination_minutes", lastPoolValue, Seq($"poolSnapDetails.idle_instance_autotermination_minutes")),
      PipelineFunctions.fillForward("min_idle_instances", lastPoolValue, Seq($"poolSnapDetails.min_idle_instances")),
      PipelineFunctions.fillForward("max_capacity", lastPoolValue, Seq($"poolSnapDetails.max_capacity")),
      PipelineFunctions.fillForward("preloaded_spark_versions", lastPoolValue, Seq($"poolSnapDetails.preloaded_spark_versions")),
      //   fillForward("preloaded_docker_images", lastPoolValue, Seq($"poolSnapDetails.preloaded_docker_images")), // SEC-6198 - DO NOT populate or test until closed
      PipelineFunctions.fillForward(s"aws_attributes", lastPoolValue, Seq(col(s"poolSnapDetails.aws_attributes"))),
      PipelineFunctions.fillForward(s"azure_attributes", lastPoolValue, Seq(col(s"poolSnapDetails.azure_attributes"))),
      PipelineFunctions.fillForward(s"gcp_attributes", lastPoolValue, Seq(col(s"poolSnapDetails.gcp_attributes"))),
      'custom_tags,
      createCol,
      deleteCol,
      struct(
        'requestId,
        'response,
        'sessionId,
        'sourceIPAddress,
        'userAgent
      ).alias("request_details")
    )

    val poolsRawPruned = auditIncrementalDF
      .filter('serviceName === "instancePools")
      .select(
        'organization_id,
        'serviceName,
        'actionName,
        'date,
        'timestamp,
        $"requestParams.aws_attributes".alias("aws_attributes"),
        $"requestParams.azure_attributes".alias("azure_attributes"),
        $"requestParams.gcp_attributes".alias("gcp_attributes"),
        $"requestParams.instance_pool_id",
        $"requestParams.preloaded_spark_versions",
        $"requestParams.instance_pool_name",
        $"requestParams.node_type_id",
        $"requestParams.idle_instance_autotermination_minutes",
        $"requestParams.min_idle_instances",
        $"requestParams.max_capacity",
        $"requestParams.custom_tags",
        'requestId,
        'response,
        'sessionId,
        'sourceIPAddress,
        'userAgent,
        'userIdentity
      )

    val poolsRawPrunedIsEmpty = poolsRawPruned.isEmpty // bool - true == no pools data from

    // not first run but no new pools records from audit -- fast fail OR
    // is first run and no pools records or snapshot records -- fast fail
    if (
      (!isFirstRun && poolsRawPrunedIsEmpty) ||
        (isFirstRun && poolSnapDF.isEmpty)
    ) throw new NoNewDataException("No new instance pools data found. Progressing module state",
      Level.WARN, allowModuleProgression = true
    )

    val newPoolsRecords = if (!poolsRawPrunedIsEmpty) {
      val poolsRawWithStructs = poolsRawPruned
        .withColumn("aws_attributes", SchemaTools.structFromJson(spark, poolsRawPruned, "aws_attributes"))
        .withColumn("azure_attributes", SchemaTools.structFromJson(spark, poolsRawPruned, "azure_attributes"))
        .withColumn("gcp_attributes", SchemaTools.structFromJson(spark, poolsRawPruned, "gcp_attributes"))
        .withColumn("custom_tags", SchemaTools.structFromJson(spark, poolsRawPruned, "custom_tags"))

      val changeInventory = Map[String, Column](
        "aws_attributes" -> SchemaTools.structToMap(poolsRawWithStructs, "aws_attributes"),
        "azure_attributes" -> SchemaTools.structToMap(poolsRawWithStructs, "azure_attributes"),
        "gcp_attributes" -> SchemaTools.structToMap(poolsRawWithStructs, "gcp_attributes"),
        "custom_tags" -> SchemaTools.structToMap(poolsRawWithStructs, "custom_tags")
      )

      val poolsBase = poolsRawWithStructs
        .select(SchemaTools.modifyStruct(poolsRawWithStructs.schema, changeInventory): _*)

      poolsBase
        .filter('actionName.isin("create", "edit", "delete"))
        .withColumn("instance_pool_id", when('actionName === "create", get_json_object($"response.result", "$.instance_pool_id")).otherwise('instance_pool_id))
        .withColumn("preloaded_spark_versions", get_json_object('preloaded_spark_versions, "$."))
        .withColumn("rnk", rank().over(exactlyOnceFilterW))
        .withColumn("rn", row_number().over(exactlyOnceFilterW))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
          .toTSDF("timestamp", "organization_id", "instance_pool_id")
          .lookupWhen(
            poolSnapLookup.toTSDF("timestamp", "organization_id", "instance_pool_id"),
            maxLookAhead = Long.MaxValue
          ).df
    } else spark.emptyDataFrame

    //    val newPoolsHasRecords = !newPoolsRecords.isEmpty
    val poolsStatusFilled = if (isFirstRun) { // on first run initialize pools_silver from pools_snapshot data

      // get missing pool ids
      val missingPoolIds = if (!poolsRawPrunedIsEmpty) { // when pools audit has records, find missing pool_ids for snapImpute
        poolsSnapDFUntilCurrent.select('organization_id, 'instance_pool_id).distinct
          .join(poolsRawPruned.select('organization_id, 'instance_pool_id).distinct, Seq("organization_id", "instance_pool_id"), "anti")
      } else {
        poolsSnapDFUntilCurrent.select('organization_id, 'instance_pool_id).distinct // no audit records, get all pools_snap IDs for impute
      }

      // impute records for pools in snapshot not in audit (i.e. pre-existing pools prior to audit logs capture)
      val lastPoolSnapW = Window.partitionBy('organization_id, 'instance_pool_id).orderBy('Pipeline_SnapTS.desc)
      val poolSnapMissingPools = poolsSnapDFUntilCurrent
        .join(missingPoolIds, Seq("organization_id", "instance_pool_id"))
        .withColumn("rnk", rank().over(lastPoolSnapW))
        .filter('rnk === 1).drop("rnk")
        .select(
          lit("instancePools").alias("serviceName"),
          lit("snapImpute").alias("actionName"),
          'organization_id,
          lit(fromTime.asUnixTimeMilli).alias("timestamp"), // set timestamp as fromtime so it will be included in downstream incrementals
          'Pipeline_SnapTS.cast("date").alias("date"),
          'instance_pool_id,
          'instance_pool_name,
          'node_type_id,
          'idle_instance_autotermination_minutes,
          'min_idle_instances,
          'max_capacity,
          'preloaded_spark_versions,
          'aws_attributes,
          'azure_attributes,
          'gcp_attributes,
          'custom_tags,
          lit(null).cast(Schema.poolsCreateSchema).alias("create_details"),
          lit(null).cast(Schema.poolsDeleteSchema).alias("delete_details"),
          lit(null).cast(Schema.poolsRequestDetails).alias("request_details")
        )

      // union existing and missing (imputed) pool ids (when exists)
      if (!poolsRawPrunedIsEmpty) { // first run and new audit records present -- union the two together
        unionWithMissingAsNull(newPoolsRecords, poolSnapMissingPools).select(poolsSelects: _*)
      } else poolSnapMissingPools // first run but no new audit records -- return only init

    } else if (!poolsRawPrunedIsEmpty) { // not first run but new audit records exist, continue
      newPoolsRecords.select(poolsSelects: _*)
    } else { // not first run and no new audit records -- break out -- nothing to update
      val msg = s"No new pools audit records found, progressing timeline and appending no new records"
      throw new NoNewDataException(msg, Level.WARN, allowModuleProgression = true)
    }

    poolsStatusFilled
      .withColumn("create_details", PipelineFunctions.fillForward("create_details", lastPoolValue))
      .drop("poolSnapDetails")

  }

  protected def buildClusterSpec(
                                  bronze_cluster_snap: PipelineTable,
                                  pools_snapshot: PipelineTable,
                                  auditRawTable: PipelineTable,
                                  isFirstRun: Boolean,
                                  untilTime: TimeTypes
                                )(df: DataFrame): DataFrame = {
    val lastClusterSnap = Window.partitionBy('organization_id, 'cluster_id).orderBy('Pipeline_SnapTS.desc)
    val clusterBefore = Window.partitionBy('organization_id, 'cluster_id)
      .orderBy('timestamp).rowsBetween(-1000, Window.currentRow)
    val isSingleNode = get_json_object(regexp_replace('spark_conf, "\\.", "_"),
      "$.spark_databricks_cluster_profile") === lit("singleNode")
    val isHC = get_json_object(regexp_replace('spark_conf, "\\.", "_"),
      "$.spark_databricks_cluster_profile") === lit("serverless")
    val isSQLAnalytics = get_json_object('custom_tags, "$.SqlEndpointId").isNotNull
    val tableAcls = coalesce(get_json_object(regexp_replace('spark_conf, "\\.", "_"), "$.spark_databricks_acl_dfAclsEnabled").cast("boolean"), lit(false)).alias("table_acls_enabled")
    val passthrough = coalesce(get_json_object(regexp_replace('spark_conf, "\\.", "_"), "$.spark_databricks_passthrough_enabled").cast("boolean"), lit(false)).alias("credential_passthrough_enabled")
    val isolation = coalesce(get_json_object(regexp_replace('spark_conf, "\\.", "_"), "$.spark_databricks_pyspark_enableProcessIsolation").cast("boolean"), lit(false)).alias("isolation_enabled")
    val languagesAllowed = coalesce(split(get_json_object(regexp_replace('spark_conf, "\\.", "_"), "$.spark_databricks_repl_allowedLanguages"), ","), array(lit("All"))).alias("repl_languages_permitted")
    val isSingleUser = 'single_user_name.isNotNull
    val numWorkers = when(isSingleNode, lit(0).cast("int")).otherwise('num_workers.cast("int"))
    val startCluster = when('start_cluster === "false", lit(false))
      .otherwise(lit(true))
    val enableElasticDisk = when('enable_elastic_disk === "false", lit(false))
      .otherwise(lit(true))
    val deriveClusterType = when(isSingleNode, lit("Single Node"))
      .when(isHC, lit("High-Concurrency"))
      .when(isSQLAnalytics, lit("SQL Analytics"))
      .otherwise("Standard").alias("cluster_type")

    val clusterBaseDF = clusterBase(df)
    val clusterBaseWMetaDF = clusterBaseDF
      // remove start, startResults, and permanentDelete as they do not contain sufficient metadata
      .filter('actionName.isin("create", "edit", "resize"))

    val lastClusterSnapW = Window.partitionBy('organization_id, 'cluster_id)
      .orderBy('Pipeline_SnapTS.desc)
    val bronzeClusterSnapLatest = bronze_cluster_snap.asDF
      .withColumn("rnk", rank().over(lastClusterSnapW))
      .withColumn("rn", row_number().over(lastClusterSnapW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

    /**
     * clusterBaseFilled - if first run, baseline cluster spec for existing clusters that haven't been edited since
     * commencement of audit logs. Allows for joins directly to gold cluster work even if they haven't yet been edited.
     * Several of the fields are unavailable through this method but many are and they are very valuable when
     * present in gold
     */
    val clusterBaseFilled = if (isFirstRun) {
      val firstRunMsg = "Silver_ClusterSpec -- First run detected, will impute cluster state from bronze to derive " +
        "current initial state for all existing clusters."
      logger.log(Level.INFO, firstRunMsg)
      println(firstRunMsg)
      val missingClusterIds = bronzeClusterSnapLatest.select('organization_id, 'cluster_id).distinct
        .join(
          clusterBaseWMetaDF
            .select('organization_id, 'cluster_id).distinct,
          Seq("organization_id", "cluster_id"), "anti"
        )
      val latestClusterSnapW = Window.partitionBy('organization_id, 'cluster_id).orderBy('Pipeline_SnapTS.desc)
      val missingClusterBaseFromSnap = bronzeClusterSnapLatest
        .join(missingClusterIds, Seq("organization_id", "cluster_id"))
        .withColumn("rnk", rank().over(latestClusterSnapW))
        .filter('rnk === 1).drop("rnk")
        .withColumn("spark_conf", to_json('spark_conf))
        .withColumn("custom_tags", to_json('custom_tags))
        .select(
          'organization_id,
          'cluster_id,
          lit("clusters").alias("serviceName"),
          lit("snapImpute").alias("actionName"),
          'cluster_name,
          'driver_node_type_id,
          'node_type_id,
          'num_workers,
          to_json('autoscale).alias("autoscale"),
          'autotermination_minutes.cast("int").alias("autotermination_minutes"),
          'enable_elastic_disk,
          'state.alias("cluster_state"),
          isAutomated('cluster_name).alias("is_automated"),
          deriveClusterType,
          to_json('cluster_log_conf).alias("cluster_log_conf"),
          to_json('init_scripts).alias("init_scripts"),
          'custom_tags,
          'cluster_source,
          'aws_attributes,
          'azure_attributes,
          'gcp_attributes,
          to_json('spark_env_vars).alias("spark_env_vars"),
          'spark_conf,
          'driver_instance_pool_id,
          'instance_pool_id,
          coalesce('effective_spark_version, 'spark_version).alias("spark_version"),
          (unix_timestamp('Pipeline_SnapTS) * 1000).alias("timestamp"),
          'Pipeline_SnapTS.cast("date").alias("date"),
          'creator_user_name.alias("createdBy"),
          'runtime_engine
        )

      unionWithMissingAsNull(clusterBaseWMetaDF, missingClusterBaseFromSnap)
    } else clusterBaseWMetaDF

    val (driverPoolSnapLookup: Option[DataFrame], workerPoolSnapLookup: Option[DataFrame]) = if (pools_snapshot.exists) {
      val poolSnapBase = pools_snapshot.asDF
        .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS) * 1000)
      (
        Some(poolSnapBase.select(
          'timestamp, 'organization_id,
          'instance_pool_id.alias("driver_instance_pool_id"),
          'instance_pool_name.alias("pool_snap_driver_instance_pool_name"),
          'node_type_id.alias("pool_snap_driver_node_type"))),
        Some(poolSnapBase.select(
          'timestamp, 'organization_id,
          'instance_pool_id,
          'instance_pool_name.alias("pool_snap_instance_pool_name"),
          'node_type_id.alias("pool_snap_node_type"))
        ))
    } else (None, None)

    val basePoolsDF = auditRawTable.asDF
      .filter('serviceName === "instancePools" && 'actionName.isin("create", "edit"))
      .cache()

    basePoolsDF.count

    val (driverPoolLookup: Option[DataFrame], workerPoolLookup: Option[DataFrame]) = if (!basePoolsDF.isEmpty) { // if instance pools found in audit logs
      (
        Some(basePoolsDF.select(
          'timestamp, 'organization_id,
          when('actionName === "create", get_json_object($"response.result", "$.instance_pool_id"))
            .otherwise($"requestParams.instance_pool_id") // actionName == edit
            .alias("driver_instance_pool_id"),
          $"requestParams.instance_pool_name".alias("driver_instance_pool_name"),
          $"requestParams.node_type_id".alias("pool_driver_node_type")
        )),
        Some(basePoolsDF.select(
          'timestamp, 'organization_id,
          when('actionName === "create", get_json_object($"response.result", "$.instance_pool_id"))
            .otherwise($"requestParams.instance_pool_id") // actionName == edit
            .alias("instance_pool_id"),
          $"requestParams.instance_pool_name",
          $"requestParams.node_type_id".alias("pool_node_type")
        ))
      )
    } else (None, None)

    val filledDriverType =
      when('driver_instance_pool_id.isNotNull, coalesce('pool_driver_node_type, 'pool_snap_driver_node_type, 'driver_node_type_id))
        .when('instance_pool_id.isNotNull && 'driver_instance_pool_id.isNull, coalesce('pool_node_type, 'pool_snap_node_type, 'node_type_id))
        .when('cluster_name.like("job-%-run-%"), coalesce('driver_node_type_id, 'node_type_id)) // when jobs clusters workers == driver driver node type is not defined
        .when(isSingleNode, 'node_type_id) // null
        .otherwise(coalesce('driver_node_type_id, first('driver_node_type_id, true).over(clusterBefore), 'node_type_id))

    val filledWorkerType = when(isSingleNode, lit(null).cast("string")) // singleNode clusters don't have worker nodes
      .when('instance_pool_id.isNotNull, coalesce('pool_node_type, 'pool_snap_node_type, 'node_type_id))
      .otherwise('node_type_id)

    val clusterSpecBaseCols = Array[Column](
      'serviceName,
      'actionName,
      'organization_id,
      'cluster_id,
      when(isSQLAnalytics, lit("SQLAnalytics")).otherwise('cluster_name).alias("cluster_name"),
      filledDriverType.alias("driver_node_type_id"),
      filledWorkerType.alias("node_type_id"),
      numWorkers.alias("num_workers"),
      'autoscale,
      'autotermination_minutes.cast("int").alias("autotermination_minutes"),
      enableElasticDisk.alias("enable_elastic_disk"),
      isAutomated('cluster_name).alias("is_automated"),
      deriveClusterType,
      'single_user_name,
      startCluster.alias("start_cluster"),
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'cluster_source,
      'aws_attributes,
      'azure_attributes,
      'gcp_attributes,
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'driver_instance_pool_id,
      'instance_pool_id,
      when('instance_pool_id.isNotNull, coalesce('instance_pool_name, 'pool_snap_instance_pool_name))
        .otherwise(lit(null).cast("string"))
        .alias("instance_pool_name"),
      when('driver_instance_pool_id.isNotNull, coalesce('driver_instance_pool_name, 'pool_snap_driver_instance_pool_name))
        .otherwise(lit(null).cast("string"))
        .alias("driver_instance_pool_name"),
      PipelineFunctions.fillForward("spark_version", clusterBefore),
      'idempotency_token,
      'timestamp,
      'userEmail,
      'runtime_engine
    )

    val clustersRemoved = clusterBaseDF
      .filter('actionName.isin("permanentDelete"))
      .select('organization_id, 'cluster_id, 'userEmail.alias("deleted_by"))

    val creatorLookup = bronze_cluster_snap.asDF
      .withColumn("rnk", rank().over(lastClusterSnap))
      .withColumn("rn", row_number().over(lastClusterSnap))
      .filter('rnk === 1 && 'rn === 1)
      .select('organization_id, 'cluster_id, $"default_tags.Creator".alias("cluster_creator_lookup"))


    // lookup pools node types from audit logs if records present
    val clusterBaseWithPools = if (driverPoolLookup.nonEmpty) {
      clusterBaseFilled
        .filter('actionName.isin("create", "edit", "snapImpute"))
        .toTSDF("timestamp", "organization_id", "instance_pool_id")
        .lookupWhen(
          workerPoolLookup.get
            .toTSDF("timestamp", "organization_id", "instance_pool_id")
        ).df
        .toTSDF("timestamp", "organization_id", "driver_instance_pool_id")
        .lookupWhen(
          driverPoolLookup.get
            .toTSDF("timestamp", "organization_id", "driver_instance_pool_id")
        ).df
    } else { // driver pool does not exist -- filter and add null lookup cols
      clusterBaseFilled.filter('actionName.isin("create", "edit", "snapImpute"))
        .withColumn("driver_instance_pool_name", lit(null).cast("string"))
        .withColumn("instance_pool_name", lit(null).cast("string"))
        .withColumn("pool_driver_node_type", lit(null).cast("string"))
        .withColumn("pool_node_type", lit(null).cast("string"))
    }

    // lookup pools node types from pools snapshots when snapshots exist
    val clusterBaseWithPoolsAndSnapPools = if (driverPoolSnapLookup.nonEmpty) {
      // initialize snap pools for when snap is after pool creation set timestamp to 0 to provide a historical record
      // without needing to look ahead
      // the following will copy the first record but set the timestamp to 0 to ensure the first record will appear before
      // the cluster action even if the snap date is after the cluster action timestamp
      val initPoolW = Window.partitionBy('organization_id, 'instance_pool_id).orderBy('timestamp)
      val initDriverPoolW = Window.partitionBy('organization_id, 'driver_instance_pool_id).orderBy('timestamp)

      val initSnapPool = workerPoolSnapLookup.get
        .withColumn("rnk", rank().over(initPoolW))
        .withColumn("rn", row_number().over(initPoolW))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
        .withColumn("timestamp", lit(0))

      val initDriverSnapPool = driverPoolSnapLookup.get
        .withColumn("rnk", rank().over(initDriverPoolW))
        .withColumn("rn", row_number().over(initDriverPoolW))
        .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
        .withColumn("timestamp", lit(0))

      clusterBaseWithPools
        .toTSDF("timestamp", "organization_id", "instance_pool_id")
        .lookupWhen(
          workerPoolSnapLookup.get.unionByName(initSnapPool)
            .toTSDF("timestamp", "organization_id", "instance_pool_id")
        ).df
        .toTSDF("timestamp", "organization_id", "driver_instance_pool_id")
        .lookupWhen(
          driverPoolSnapLookup.get.unionByName(initDriverSnapPool, allowMissingColumns = true)
            .toTSDF("timestamp", "organization_id", "driver_instance_pool_id")
        ).df
    } else {
      clusterBaseWithPools
        .withColumn("pool_snap_driver_instance_pool_name", lit(null).cast("string"))
        .withColumn("pool_snap_instance_pool_name", lit(null).cast("string"))
        .withColumn("pool_snap_driver_node_type", lit(null).cast("string"))
        .withColumn("pool_snap_node_type", lit(null).cast("string"))
    }

    val onlyOnceSemanticsW = Window.partitionBy('organization_id, 'cluster_id, 'actionName,'timestamp).orderBy('timestamp)
    clusterBaseWithPoolsAndSnapPools
      .select(clusterSpecBaseCols: _*)
      .join(creatorLookup, Seq("organization_id", "cluster_id"), "left")
      .join(clustersRemoved, Seq("organization_id", "cluster_id"), "left")
      .withColumn("security_profile",
        struct(
          tableAcls,
          passthrough,
          isolation,
          languagesAllowed,
          struct(
            isSingleUser.alias("is_single_user"),
            'single_user_name
          ).alias("single_user_profile"),
          'aws_attributes("instance_profile_arn").alias("instance_profile_arn")
        )
      )
      .withColumn("rnk", rank().over(onlyOnceSemanticsW))
      .withColumn("rn", row_number().over(onlyOnceSemanticsW))
      .filter('rnk === 1 && 'rn === 1)
      .withColumn("createdBy",
        when(isAutomated('cluster_name) && 'actionName === "create", lit("JobsService"))
          .when(!isAutomated('cluster_name) && 'actionName === "create", 'userEmail))
      .withColumn("createdBy", when(!isAutomated('cluster_name) && 'createdBy.isNull, last('createdBy, true).over(clusterBefore)).otherwise('createdBy))
      .withColumn("createdBy", when('createdBy.isNull && 'cluster_creator_lookup.isNotNull, 'cluster_creator_lookup).otherwise('createdBy))
      .withColumn("lastEditedBy", when(!isAutomated('cluster_name) && 'actionName === "edit", 'userEmail))
      .withColumn("lastEditedBy", when('lastEditedBy.isNull, last('lastEditedBy, true).over(clusterBefore)).otherwise('lastEditedBy))
      .withColumn("runtime_engine",
        when('runtime_engine.isNotNull, 'runtime_engine)
          .when('spark_version.like("%_photon_%") && 'runtime_engine.isNull,"PHOTON")
          .when(!'spark_version.like("%_photon_%") && 'runtime_engine.isNull,"STANDARD")
          .otherwise(lit("UNKNOWN")))
      .drop("userEmail", "cluster_creator_lookup", "single_user_name", "rnk", "rn")
  }

  def buildClusterStateDetail(
                               untilTime: TimeTypes,
                               auditLogDF: DataFrame,
                               jrsilverDF: DataFrame,
                               clusterSpec: PipelineTable,
                             )(clusterEventsDF: DataFrame): DataFrame = {
    val stateUnboundW = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp)
    val stateFromCurrentW = Window.partitionBy('organization_id, 'cluster_id).rowsBetween(1L, 1000L).orderBy('timestamp)
    val stateUntilCurrentW = Window.partitionBy('organization_id, 'cluster_id).rowsBetween(-1000L, -1L).orderBy('timestamp)
    val stateUntilPreviousRowW = Window.partitionBy('organization_id, 'cluster_id).rowsBetween(Window.unboundedPreceding, -1L).orderBy('timestamp)
    val uptimeW = Window.partitionBy('organization_id, 'cluster_id, 'reset_partition).orderBy('unixTimeMS_state_start)
    val orderingWindow = Window.partitionBy('organization_id, 'cluster_id).orderBy(desc("timestamp"))

    val nonBillableTypes = Array(
      "STARTING", "TERMINATING", "CREATING", "RESTARTING" , "TERMINATING_IMPUTED"
    )

    // some states like EXPANDED_DISK and NODES_LOST, etc are excluded because
    // they occasionally do come after the cluster has been terminated; thus they are not a guaranteed event
    // goal is to be certain about the 99th percentile
    val runningStates = Array(
      "STARTING", "INIT_SCRIPTS_STARTED", "RUNNING", "CREATING",
      "RESIZING", "UPSIZE_COMPLETED", "DRIVER_HEALTHY"
    )

    val invalidEventChain = lead('runningSwitch, 1).over(stateUnboundW).isNotNull && lead('runningSwitch, 1)
      .over(stateUnboundW) === lead('previousSwitch, 1).over(stateUnboundW)


    val refinedClusterEventsDF = clusterEventsDF
      .selectExpr("*", "details.*")
      .drop("details")
      .withColumnRenamed("type", "state")

    val clusterEventsFinal  = if (jrsilverDF.isEmpty || clusterSpec.asDF.isEmpty) {
      refinedClusterEventsDF
    }else{
      val refinedClusterEventsDFFiltered = refinedClusterEventsDF
        .withColumn("row", row_number().over(orderingWindow))
        .filter('state =!= "TERMINATING" && 'row === 1)

      val exceptClusterEventsDF1 = refinedClusterEventsDF.join(refinedClusterEventsDFFiltered.select("cluster_id","timestamp","state"),Seq("cluster_id","timestamp","state"),"leftAnti")

      val jrSilverAgg= jrsilverDF
        .groupBy("clusterID")
        .agg(max("TaskExecutionRunTime.endTS").alias("end_run_time"))
        .filter('end_run_time.isNotNull)

      val joined = refinedClusterEventsDFFiltered.join(jrSilverAgg, refinedClusterEventsDFFiltered("cluster_id") === jrSilverAgg("clusterID"), "inner")
        .withColumn("state", lit("TERMINATING_IMPUTED"))


      // Join with Cluster Spec to get filter on automated cluster
      val clusterSpecDF = clusterSpec.asDF.withColumnRenamed("cluster_id","clusterID")
        .withColumn("isAutomated",isAutomated('cluster_name))
        .select("clusterID","cluster_name","isAutomated")
        .filter('isAutomated).dropDuplicates()

      val jobClusterImputed = joined.join(clusterSpecDF,Seq("clusterID"),"inner")
        .drop("row","clusterID","end_run_time","cluster_name","isAutomated")

      refinedClusterEventsDF.union(jobClusterImputed)
    }

    val clusterEventsBaseline = clusterEventsFinal
      .withColumn(
        "runningSwitch",
        when('state.isin("TERMINATING","TERMINATING_IMPUTED"), lit(false))
          .when('state.isin("CREATING", "STARTING"), lit(true))
          .otherwise(lit(null).cast("boolean")))
      .withColumn(
        "previousSwitch",
        when('runningSwitch.isNotNull, last('runningSwitch, true).over(stateUntilPreviousRowW))
      )
      .withColumn(
        "invalidEventChainHandler",
        when(invalidEventChain, array(lit(false), lit(true))).otherwise(array(lit(false)))
      )
      .selectExpr("*", "explode(invalidEventChainHandler) as imputedTerminationEvent").drop("invalidEventChainHandler")
      .withColumn("state", when('imputedTerminationEvent, "TERMINATING").otherwise('state))
      .withColumn("timestamp", when('imputedTerminationEvent, lag('timestamp, 1).over(stateUnboundW) + 1L).otherwise('timestamp))
      .withColumn("lastRunningSwitch", last('runningSwitch, true).over(stateUntilCurrentW)) // previous on/off switch
      .withColumn("nextRunningSwitch", first('runningSwitch, true).over(stateFromCurrentW)) // next on/off switch
      // given no anomaly, set on/off state to current state
      // if no current state use previous state
      // if no previous state found, assume opposite of next state switch
      .withColumn("isRunning", coalesce(
        when('imputedTerminationEvent, lit(false)).otherwise(lit(null).cast("boolean")),
        'runningSwitch,
        'lastRunningSwitch,
        !'nextRunningSwitch
      ))
      // if isRunning still undetermined, use guaranteed events to create state anchors to identify isRunning anchors
      .withColumn("isRunning", when('isRunning.isNull && 'state.isin(runningStates: _*), lit(true)).otherwise('isRunning))
      // use the anchors to fill in the null gaps between the state changes to determine if running
      // if ultimately unable to be determined, assume not isRunning
      .withColumn("isRunning", coalesce(
        when('isRunning.isNull, last('isRunning, true).over(stateUntilCurrentW)).otherwise('isRunning),
        when('isRunning.isNull, !first('isRunning, true).over(stateFromCurrentW)).otherwise('isRunning),
        lit(false)
      )).drop("lastRunningSwitch", "nextRunningSwitch")

      .withColumn("previousIsRunning",lag($"isRunning", 1, null).over(stateUnboundW))
      .withColumn("isRunning",when(col("previousIsRunning") === "false" && col("state") === "EXPANDED_DISK",lit(false)).otherwise('isRunning))
      .drop("previousIsRunning")

      .withColumn(
        "current_num_workers",
        coalesce(
          when(!'isRunning || 'isRunning.isNull, lit(null).cast("long"))
            .otherwise(
              coalesce( // get current_num_workers no matter where the value is stored based on business rules
                'current_num_workers,
                $"cluster_size.num_workers",
                $"cluster_size.autoscale.min_workers",
                last(coalesce( // look for the last non-null value when current value isn't present
                  'current_num_workers,
                  $"cluster_size.num_workers",
                  $"cluster_size.autoscale.min_workers"
                ), true).over(stateUntilCurrentW)
              )
            ),
          lit(0) // don't allow null returns
        )
      )
      .withColumn(
        "target_num_workers",
        coalesce(
          when(!'isRunning || 'isRunning.isNull, lit(null).cast("long"))
            .when('state === "CREATING",
              coalesce($"cluster_size.num_workers", $"cluster_size.autoscale.min_workers"))
            .otherwise(coalesce('target_num_workers, 'current_num_workers)),
          lit(0) // don't allow null returns
        )
      )
      .select(
        'organization_id, 'cluster_id, 'isRunning,
        'timestamp, 'state, 'current_num_workers, 'target_num_workers
      )
      .withColumn("unixTimeMS_state_start", 'timestamp)
      .withColumn("unixTimeMS_state_end", coalesce( // if state end open, use pipelineSnapTime, will be merged when state end is received
        lead('timestamp, 1).over(stateUnboundW) - lit(1), // subtract 1 millis
        lit(untilTime.asUnixTimeMilli)
      ))

    // Change for PR -934
    // Get the ClusterID that has been Permenantly_Deleted
    val clusterBaseDF = clusterBase(auditLogDF)
    val removedClusterID = clusterBaseDF
      .filter('actionName.isin("permanentDelete"))
      .select('cluster_id,'timestamp.alias("deletion_timestamp")).distinct()

    val clusterEventsBaselineForRemovedCluster = clusterEventsBaseline.join(removedClusterID,Seq("cluster_id"))

    val window = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp.desc)
    val stateBeforeRemoval = clusterEventsBaselineForRemovedCluster
      .withColumn("rnk",rank().over(window))
      .withColumn("rn", row_number().over(window))
      .withColumn("unixTimeMS_state_end",when('state.isin("TERMINATING","TERMINATING_IMPUTED"),'unixTimeMS_state_end).otherwise('deletion_timestamp))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

    val stateDuringRemoval = stateBeforeRemoval
      .withColumn("timestamp",when('state.isin("TERMINATING","TERMINATING_IMPUTED"),'unixTimeMS_state_end+1).otherwise(col("deletion_timestamp")+1))
      .withColumn("isRunning",lit(false))
      .withColumn("unixTimeMS_state_start",('timestamp))
      .withColumn("unixTimeMS_state_end",('timestamp))
      .withColumn("state",lit("PERMENANT_DELETE"))
      .withColumn("current_num_workers",lit(0))
      .withColumn("target_num_workers",lit(0))
      .drop("deletion_timestamp")

    val columns: Array[String] = clusterEventsBaseline.columns

    val stateDuringRemovalFinal = stateBeforeRemoval.drop("deletion_timestamp")
      .unionByName(stateDuringRemoval, allowMissingColumns = true)
      .select(columns.map(col): _*)

    val clusterEventsBaselineFinal = clusterEventsBaseline.join(stateDuringRemovalFinal,Seq("cluster_id","timestamp"),"anti")
      .select(columns.map(col): _*)
      .unionByName(stateDuringRemovalFinal, allowMissingColumns = true)

    clusterEventsBaselineFinal
      .withColumn("counter_reset",
        when(
          lag('state, 1).over(stateUnboundW).isin("TERMINATING", "RESTARTING", "EDITED","TERMINATING_IMPUTED") ||
            !'isRunning, lit(1)
        ).otherwise(lit(0))
      )
      .withColumn("reset_partition", sum('counter_reset).over(stateUnboundW))
      .withColumn("target_num_workers", last('target_num_workers, true).over(stateUnboundW))
      .withColumn("current_num_workers", last('current_num_workers, true).over(stateUnboundW))
      .withColumn("timestamp_state_start", from_unixtime('unixTimeMS_state_start.cast("double") / lit(1000)).cast("timestamp"))
      .withColumn("timestamp_state_end", from_unixtime('unixTimeMS_state_end.cast("double") / lit(1000)).cast("timestamp")) // subtract 1.0 millis
      .withColumn("state_start_date", 'timestamp_state_start.cast("date"))
      .withColumn("uptime_in_state_S", ('unixTimeMS_state_end - 'unixTimeMS_state_start) / lit(1000))
      .withColumn("uptime_since_restart_S",
        coalesce(
          when('counter_reset === 1, lit(0))
            .otherwise(sum('uptime_in_state_S).over(uptimeW)),
          lit(0)
        )
      )
      .withColumn("cloud_billable", 'isRunning)
      .withColumn("databricks_billable", 'isRunning && !'state.isin(nonBillableTypes: _*))
      .withColumn("uptime_in_state_H", 'uptime_in_state_S / lit(3600))
      .withColumn("state_dates", sequence('timestamp_state_start.cast("date"), 'timestamp_state_end.cast("date")))
      .withColumn("days_in_state", size('state_dates))

  }

  protected def dbJobsStatusSummary(
                                     jobsSnapshotTargetComplete: PipelineTable,
                                     isFirstRun: Boolean,
                                     targetKeys: Array[String],
                                     fromTime: TimeTypes,
                                     tempWorkingDir: String,
                                     daysToProcess: Int
                                   )(df: DataFrame): DataFrame = {

    val jobsBase = getJobsBase(df)
      .filter('actionName.isin("create", "delete", "reset", "update", "resetJobAcl", "changeJobAcl"))
      .filter(responseSuccessFilter) // only promote successful events

    val jobsBaseHasRecords = !jobsBase.isEmpty
    jobStatusValidateNewJobsStatusHasNewData(isFirstRun, jobsSnapshotTargetComplete, jobsBaseHasRecords)

    val optimalCacheParts = Math.min(daysToProcess * getTotalCores * 2, 1000)

    // TODO -- temp -- not necessary after 503 fix
    //  adding arrays to lookup will ensure they are added on first run if they don't exist until 503 is resolved
    val colsToAddIfNotExists = Array(
      NamedColumn("tasks", lit(null).cast(Schema.minimumTasksSchema)),
      NamedColumn("job_clusters", lit(null).cast(Schema.minimumJobClustersSchema)),
      NamedColumn("libraries", lit(null).cast(Schema.minimumLibrariesSchema)),
      NamedColumn("access_control_list", lit(null).cast(Schema.minimumAccessControlListSchema)),
      NamedColumn("grants", lit(null).cast(Schema.minimumGrantsSchema)),
    )

    val jobsSnapshotDFComplete = jobsSnapshotTargetComplete.asDF
      .appendToStruct("settings", colsToAddIfNotExists)

    // Simplified DF for snapshot lookupWhen
    val jobSnapLookup = jobsSnapshotDFComplete
      .withColumnRenamed("created_time", "snap_lookup_created_time")
      .withColumnRenamed("creator_user_name", "snap_lookup_created_by")
      .withColumnRenamed("job_id", "jobId")
      .withColumn("lookup_settings", to_json('settings))
      .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS))
      .drop("Overwatch_RunID", "settings")

    val isFirstRunAndJobsSnapshotHasRecords = isFirstRun && !jobsSnapshotDFComplete.isEmpty

    val lastJobStatus = Window.partitionBy('organization_id, 'jobId).orderBy('timestamp)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    jobStatusDeriveJobsStatusBase(jobsBase)
      .transform(jobStatusLookupJobMeta(jobSnapLookup))
      .transform(jobStatusDeriveBaseLookupAndFillForward(lastJobStatus))
      .transform(jobStatusStructifyJsonCols(optimalCacheParts))
      .transform(jobStatusCleanseForPublication(targetKeys, optimalCacheParts))
      .transform(
        jobStatusFirstRunImputeFromSnap(
          isFirstRunAndJobsSnapshotHasRecords,
          jobsBaseHasRecords,
          jobsSnapshotDFComplete,
          fromTime,
          tempWorkingDir
        )
      )
  }

  /**
   * Depending on the actionName, the runID will be found in one of the following columns, "run_id",
   * "runId", or in the "response.result.run_id" fields.
   *
   * The lineage of a run is as follows:
   * runNow or submitRun: RPC service, happens at RPC request received, this RPC request does not happen when a
   * job is started via databricks cron, in these cases only a runStart is emitted. A manual runstart via the UI
   * emits a runNow
   * runStart: JobRunner service, happens when a run starts on a cluster
   * runSucceeded or runFailed, JobRunner service, happens when job ends
   *
   * To build the logs, the incremental audit logs are reviewed for jobs COMPLETED and CANCELLED in the time scope
   * of this overwatch run. If completed during this run then the entire audit log jobs service is searched for
   * the start of that run
   *
   * @param completeAuditTable
   * @param clusterSpec
   * @param jobsStatus
   * @param jobsSnapshot
   * @param etlStartTime Actual start timestamp of the Overwatch run
   * @param auditLogLag30D
   * @return
   */
  protected def dbJobRunsSummary(
                                  clusterSpec: PipelineTable,
                                  clusterSnapshot: PipelineTable,
                                  jobsStatus: PipelineTable,
                                  jobsSnapshot: PipelineTable,
                                  etlStartTime: TimeTypes,
                                  etlUntilTime: TimeTypes,
                                  targetKeys: Array[String],
                                  daysToProcess: Int
                                )(auditLogLag30D: DataFrame): DataFrame = {

    val jobRunActions = Array(
      "runSucceeded", "runFailed", "runTriggered", "runNow", "runStart", "submitRun", "cancel", "repairRun"
    )
    val optimalCacheParts = Math.min(daysToProcess * getTotalCores * 2, 1000)
    val jobRunsLag30D = getJobsBase(auditLogLag30D)
      .filter('actionName.isin(jobRunActions: _*))
      .repartition(optimalCacheParts)
      .cache() // cached df removed at end of module run

    // eagerly force this highly reused DF into cache()
    jobRunsLag30D.count()

    // Lookup to populate the clusterID/clusterName where missing from jobs
    lazy val clusterSpecNameLookup = clusterSpec.asDF
      .select('organization_id, 'timestamp, 'cluster_name, 'cluster_id.alias("clusterId"))
      .filter('clusterId.isNotNull && 'cluster_name.isNotNull)

    // Lookup to populate the clusterID/clusterName where missing from jobs
    lazy val clusterSnapNameLookup = clusterSnapshot.asDF
      .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS) * lit(1000))
      .select('organization_id, 'timestamp, 'cluster_name, 'cluster_id.alias("clusterId"))
      .filter('clusterId.isNotNull && 'cluster_name.isNotNull)

    // Lookup to populate the existing_cluster_id where missing from jobs -- it can be derived from name
    lazy val jobStatusMetaLookup = jobsStatus.asDF
      .verifyMinimumSchema(Schema.minimumJobStatusSilverMetaLookupSchema)
      .select(
        'organization_id,
        'timestamp,
        'jobId,
        'jobName,
        to_json('tags).alias("tags"),
        'schedule,
        'max_concurrent_runs,
        'run_as_user_name,
        'timeout_seconds,
        'created_by,
        'last_edited_by,
        to_json('tasks).alias("tasks"),
        to_json('job_clusters).alias("job_clusters"),
        to_json($"task_detail_legacy.notebook_task").alias("notebook_task"),
        to_json($"task_detail_legacy.spark_python_task").alias("spark_python_task"),
        to_json($"task_detail_legacy.python_wheel_task").alias("python_wheel_task"),
        to_json($"task_detail_legacy.spark_jar_task").alias("spark_jar_task"),
        to_json($"task_detail_legacy.spark_submit_task").alias("spark_submit_task"),
        to_json($"task_detail_legacy.shell_command_task").alias("shell_command_task"),
        to_json($"task_detail_legacy.pipeline_task").alias("pipeline_task")
      )

    lazy val jobSnapNameLookup = jobsSnapshot.asDF
      .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS) * lit(1000))
      .select('organization_id, 'timestamp, 'job_id.alias("jobId"), $"settings.name".alias("jobName"))

    val jobRunsLookups = jobRunsInitializeLookups(
      (clusterSpec, clusterSpecNameLookup),
      (clusterSnapshot, clusterSnapNameLookup),
      (jobsStatus, jobStatusMetaLookup),
      (jobsSnapshot, jobSnapNameLookup)
    )

    // caching before structifying
    jobRunsDeriveRunsBase(jobRunsLag30D, etlUntilTime)
      .transform(jobRunsAppendClusterName(jobRunsLookups))
      .transform(jobRunsAppendJobMeta(jobRunsLookups))
      .transform(jobRunsStructifyLookupMeta(optimalCacheParts))
      .transform(jobRunsAppendTaskAndClusterDetails)
      .transform(jobRunsCleanseCreatedNestedStructures(targetKeys))
      //      .transform(jobRunsRollupWorkflowsAndChildren)
      .drop("timestamp") // could be duplicated to enable asOf Lookups, dropping to clean up
  }

  protected def notebookSummary()(df: DataFrame): DataFrame = {
    val notebookCols = auditBaseCols ++ Array[Column]('notebookId, 'notebookName, 'path, 'oldName, 'oldPath,
      'newName, 'newPath, 'parentPath, 'clusterId)

    df.filter('serviceName === "notebook")
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .filter(responseSuccessFilter)
      .withColumn("pathLength", when('notebookName.isNull, size(split('path, "/"))))
      .withColumn("notebookName", when('notebookName.isNull, split('path, "/")('pathLength - 1)).otherwise('notebookName))
      .select(notebookCols: _*)
  }

  protected def enhanceSqlQueryHistory(df: DataFrame): DataFrame = {
    df
      .selectExpr("*", "metrics.*")
      .drop("metrics")

  }

  protected def buildWarehouseSpec(
                                  bronze_warehouse_snap: PipelineTable,
                                  isFirstRun: Boolean,
                                  silver_warehouse_spec: PipelineTable
                                )(df: DataFrame): DataFrame = {
    val lastWarehouseSnapW = Window.partitionBy('organization_id, 'warehouse_id)
      .orderBy('Pipeline_SnapTS.desc)

    val bronzeWarehouseSnapLatest = bronze_warehouse_snap.asDF
      .withColumn("rnk", rank().over(lastWarehouseSnapW))
      .withColumn("rn", row_number().over(lastWarehouseSnapW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

    deriveInputForWarehouseBase(df,silver_warehouse_spec,auditBaseCols)
    .transform(deriveWarehouseBase())
      .transform(deriveWarehouseBaseFilled(isFirstRun, bronzeWarehouseSnapLatest, silver_warehouse_spec))
  }

  protected def buildWarehouseStateDetail(
                                           untilTime: TimeTypes,
                                           auditLogDF: DataFrame,
                                           jrsilverDF: DataFrame,
                                           warehousesSpec: PipelineTable,
                                         )(warehouseEventsDF: DataFrame): DataFrame = {

    val stateUnboundW = Window.partitionBy('organization_id, 'warehouse_id).orderBy('timestamp)
    val stateFromCurrentW = Window.partitionBy('organization_id, 'warehouse_id).rowsBetween(1L, 1000L).orderBy('timestamp)
    val stateUntilCurrentW = Window.partitionBy('organization_id, 'warehouse_id).rowsBetween(-1000L, -1L).orderBy('timestamp)
    val stateUntilPreviousRowW = Window.partitionBy('organization_id, 'warehouse_id).rowsBetween(Window.unboundedPreceding, -1L).orderBy('timestamp)
    val uptimeW = Window.partitionBy('organization_id, 'warehouse_id, 'reset_partition).orderBy('unixTimeMS_state_start)
    val orderingWindow = Window.partitionBy('organization_id, 'warehouse_id).orderBy(desc("timestamp"))

    val nonBillableTypes = Array(
      "STARTING", "TERMINATING", "CREATING", "RESTARTING" , "TERMINATING_IMPUTED"
      ,"STOPPING","STOPPED" //new states from warehouse_events
    )

    val runningStates = Array(
      "STARTING", "INIT_SCRIPTS_STARTED", "RUNNING", "CREATING",
      "RESIZING", "UPSIZE_COMPLETED", "DRIVER_HEALTHY"
      ,"SCALED_UP","SCALED_DOWN" //new states from warehouse_events
    )

    val invalidEventChain = lead('runningSwitch, 1).over(stateUnboundW).isNotNull && lead('runningSwitch, 1)
      .over(stateUnboundW) === lead('previousSwitch, 1).over(stateUnboundW)

    val warehouseEventsFinal  = if (jrsilverDF.isEmpty || warehousesSpec.asDF.isEmpty) {
      warehouseEventsDF // need to add "min_num_clusters","max_num_clusters"
    }else{
      val refinedWarehouseEventsDFFiltered = warehouseEventsDF
        .withColumn("row", row_number().over(orderingWindow))
        .filter('state =!= "TERMINATING" && 'row === 1)

      val jrSilverAgg= jrsilverDF
        .filter('clusterType === "sqlWarehouse")
        .groupBy("clusterID")
        .agg(max("TaskExecutionRunTime.endTS").alias("end_run_time"))
        .withColumnRenamed("clusterID","warehouseId")
        .filter('end_run_time.isNotNull)

      val joined = refinedWarehouseEventsDFFiltered.join(jrSilverAgg,
          refinedWarehouseEventsDFFiltered("warehouse_id") === jrSilverAgg("warehouseId"), "inner")
        .withColumn("state", lit("TERMINATING_IMPUTED")) // check if STOPPING_IMPUTED can be used ?

      // Join with Cluster Spec to get filter on automated cluster
      val warehousesSpecDF = warehousesSpec.asDF
        .select("warehouse_id","warehouse_name","min_num_clusters","max_num_clusters")
        .dropDuplicates()

      val jobClusterImputed = joined.join(warehousesSpecDF,Seq("warehouse_id"),"inner")
        .drop("row","warehouseId","end_run_time","warehouse_name")

      warehouseEventsDF
        .withColumn("min_num_clusters",lit(0))
        .withColumn("max_num_clusters",lit(0))
        .union(jobClusterImputed)
        .withColumn("timestamp", unix_timestamp($"timestamp")*1000) // need to add "min_num_clusters","max_num_clusters"
    }

    val warehouseEventsBaseline = warehouseEventsFinal
      .withColumn(
        "runningSwitch",
        when('state.isin("TERMINATING","TERMINATING_IMPUTED","STOPPING"), lit(false)) //added STOPPING
          .when('state.isin("CREATING", "STARTING"), lit(true))
          .otherwise(lit(null).cast("boolean")))
      .withColumn(
        "previousSwitch",
        when('runningSwitch.isNotNull, last('runningSwitch, true).over(stateUntilPreviousRowW))
      )
      .withColumn(
        "invalidEventChainHandler",
        when(invalidEventChain, array(lit(false), lit(true))).otherwise(array(lit(false)))
      )
      .selectExpr("*", "explode(invalidEventChainHandler) as imputedTerminationEvent").drop("invalidEventChainHandler")
      .withColumn("state", when('imputedTerminationEvent, "STOPPING").otherwise('state)) // replaced TERMINATED with STOPPING
      .withColumn("timestamp", when('imputedTerminationEvent, lag('timestamp, 1).over(stateUnboundW) + 1L).otherwise('timestamp))
      .withColumn("lastRunningSwitch", last('runningSwitch, true).over(stateUntilCurrentW)) // previous on/off switch
      .withColumn("nextRunningSwitch", first('runningSwitch, true).over(stateFromCurrentW)) // next on/off switch
      // given no anomaly, set on/off state to current state
      // if no current state use previous state
      // if no previous state found, assume opposite of next state switch
      .withColumn("isRunning", coalesce(
        when('imputedTerminationEvent, lit(false)).otherwise(lit(null).cast("boolean")),
        'runningSwitch,
        'lastRunningSwitch,
        !'nextRunningSwitch
      ))
      // if isRunning still undetermined, use guaranteed events to create state anchors to identify isRunning anchors
      .withColumn("isRunning", when('isRunning.isNull && 'state.isin(runningStates: _*), lit(true)).otherwise('isRunning))
      // use the anchors to fill in the null gaps between the state changes to determine if running
      // if ultimately unable to be determined, assume not isRunning
      .withColumn("isRunning", coalesce(
        when('isRunning.isNull, last('isRunning, true).over(stateUntilCurrentW)).otherwise('isRunning),
        when('isRunning.isNull, !first('isRunning, true).over(stateFromCurrentW)).otherwise('isRunning),
        lit(false)
      )).drop("lastRunningSwitch", "nextRunningSwitch")
      .withColumn("previousIsRunning",lag($"isRunning", 1, null).over(stateUnboundW))
      .withColumn("isRunning",when(col("previousIsRunning") === "false" && col("state") === "EXPANDED_DISK",lit(false)).otherwise('isRunning))
      .drop("previousIsRunning")
      .withColumn(
        "current_num_clusters",
        coalesce(
          when(!'isRunning || 'isRunning.isNull, lit(null).cast("long"))
            .otherwise(
              coalesce( // get current_num_workers no matter where the value is stored based on business rules
                'cluster_count,
                'min_num_clusters,
                last(coalesce( // look for the last non-null value when current value isn't present
                  'cluster_count,
                  'min_num_clusters
                ), true).over(stateUntilCurrentW)
              )
            ),
          lit(0) // don't allow null returns
        )
      )
      .withColumn(
        "target_num_clusters", // need to check this logic and rename column
        coalesce(
          when(!'isRunning || 'isRunning.isNull, lit(null).cast("long"))
            .when('state === "CREATING",
              coalesce('min_num_clusters, 'current_num_clusters))
            .otherwise(coalesce('max_num_clusters, 'current_num_clusters)),
          lit(0) // don't allow null returns
        )
      )
      .select(
        'organization_id, 'warehouse_id, 'isRunning,
        'timestamp, 'state, 'current_num_clusters, 'target_num_clusters
      )
      .withColumn("unixTimeMS_state_start", 'timestamp)
      .withColumn("unixTimeMS_state_end", coalesce( // if state end open, use pipelineSnapTime, will be merged when state end is received
        lead('timestamp, 1).over(stateUnboundW) - lit(1), // subtract 1 millis
        lit(untilTime.asUnixTimeMilli)
      ))

    // Start changes from here
    // Get the warehouseID that has been Permenantly_Deleted
    val warehouseBaseDF = warehouseBase(auditLogDF) //need to rewrite warehouseBase function
    val removedWarehouseID = warehouseBaseDF
      .filter('actionName.isin("deleteEndpoint"))
      .select('warehouse_id,'timestamp.alias("deletion_timestamp")).distinct()

    val warehouseEventsBaselineForRemovedCluster = warehouseEventsBaseline.join(removedWarehouseID,Seq("warehouse_id"))

    val window = Window.partitionBy('organization_id, 'warehouse_id).orderBy('timestamp.desc)
    val stateBeforeRemoval = warehouseEventsBaselineForRemovedCluster
      .withColumn("rnk",rank().over(window))
      .withColumn("rn", row_number().over(window))
      .withColumn("unixTimeMS_state_end",when('state.isin("STOPPING","TERMINATING_IMPUTED"),'unixTimeMS_state_end).otherwise('deletion_timestamp))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")

    val stateDuringRemoval = stateBeforeRemoval
      .withColumn("timestamp",when('state.isin("STOPPING","TERMINATING","TERMINATING_IMPUTED"),'unixTimeMS_state_end+1).otherwise(col("deletion_timestamp")+1))
      .withColumn("isRunning",lit(false))
      .withColumn("unixTimeMS_state_start",('timestamp))
      .withColumn("unixTimeMS_state_end",('timestamp))
      .withColumn("state",lit("PERMENANT_DELETE"))
      .withColumn("current_num_clusters",lit(0))
      .withColumn("target_num_clusters",lit(0))
      .drop("deletion_timestamp")

    val columns: Array[String] = warehouseEventsBaseline.columns
    val stateDuringRemovalFinal = stateBeforeRemoval.drop("deletion_timestamp")
      .unionByName(stateDuringRemoval, allowMissingColumns = true)
      .select(columns.map(col): _*)

    val warehouseEventsBaselineFinal = warehouseEventsBaseline.join(stateDuringRemovalFinal,Seq("warehouse_id","timestamp"),"anti")
      .select(columns.map(col): _*)
      .unionByName(stateDuringRemovalFinal, allowMissingColumns = true)

    warehouseEventsBaselineFinal
      .withColumn("counter_reset",
        when(
          lag('state, 1).over(stateUnboundW).isin("STOPPING","TERMINATING", "RESTARTING", "EDITED","TERMINATING_IMPUTED") ||
            !'isRunning, lit(1)
        ).otherwise(lit(0))
      )
      .withColumn("reset_partition", sum('counter_reset).over(stateUnboundW))
      .withColumn("target_num_clusters", last('target_num_clusters, true).over(stateUnboundW))
      .withColumn("current_num_clusters", last('current_num_clusters, true).over(stateUnboundW))
      .withColumn("timestamp_state_start", from_unixtime('unixTimeMS_state_start.cast("double") / lit(1000)).cast("timestamp"))
      .withColumn("timestamp_state_end", from_unixtime('unixTimeMS_state_end.cast("double") / lit(1000)).cast("timestamp")) // subtract 1.0 millis
      .withColumn("state_start_date", 'timestamp_state_start.cast("date"))
      .withColumn("uptime_in_state_S", ('unixTimeMS_state_end - 'unixTimeMS_state_start) / lit(1000))
      .withColumn("uptime_since_restart_S",
        coalesce(
          when('counter_reset === 1, lit(0))
            .otherwise(sum('uptime_in_state_S).over(uptimeW)),
          lit(0)
        )
      )
      .withColumn("cloud_billable", 'isRunning)
      .withColumn("databricks_billable", 'isRunning && !'state.isin(nonBillableTypes: _*))
      .withColumn("uptime_in_state_H", 'uptime_in_state_S / lit(3600))
      .withColumn("state_dates", sequence('timestamp_state_start.cast("date"), 'timestamp_state_end.cast("date")))
      .withColumn("days_in_state", size('state_dates))
  }
}
