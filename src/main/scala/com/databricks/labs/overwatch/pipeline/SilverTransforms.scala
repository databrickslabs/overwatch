package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.PipelineFunctions.fillForward
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

trait SilverTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

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
                            fromTime: TimeTypes,
                            untilTime: TimeTypes
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
      coalesce('aws_attributes, lit("""{"emptyKey": ""}""")).alias("aws_attributes"),
      coalesce('azure_attributes, lit("""{"emptyKey": ""}""")).alias("azure_attributes"),
      'spark_env_vars,
      'spark_conf,
      when('ssh_public_keys.isNotNull, true).otherwise(false).alias("has_ssh_keys"),
      'acl_path_prefix,
      'driver_instance_pool_id,
      'instance_pool_id,
      'instance_pool_name,
      'spark_version,
      'cluster_creator,
      'idempotency_token,
      'user_id,
      'sourceIPAddress,
      'single_user_name
    )

    val clusterRaw = auditRawDF
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .select(clusterSummaryCols: _*)
      .withColumn("cluster_id", cluster_id_gen)
      .withColumn("cluster_name", cluster_name_gen)

    val clusterWithStructs = clusterRaw
      .withColumn("aws_attributes", SchemaTools.structFromJson(spark, clusterRaw, "aws_attributes"))
      .withColumn("azure_attributes", SchemaTools.structFromJson(spark, clusterRaw, "azure_attributes"))
      .scrubSchema

    clusterWithStructs
      .withColumn("aws_attributes", SchemaTools.structToMap(clusterWithStructs, "aws_attributes"))
      .withColumn("azure_attributes", SchemaTools.structToMap(clusterWithStructs, "azure_attributes"))
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

    val deleteCol = when('actionName === "delete" && $"response.statusCode" === 200,
      struct(
        $"userIdentity.email".alias("deleted_by"),
        'timestamp.alias("deleted_at_epochMillis"),
        from_unixtime('timestamp / 1000).cast("timestamp").alias("deleted_at")
      )).otherwise(lit(null).cast(Schema.poolsDeleteSchema)).alias("delete_details")

    val createCol = when('actionName === "create" && $"response.statusCode" === 200,
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
      .select(
        'organization_id,
        'serviceName,
        'actionName,
        'date,
        'timestamp,
        coalesce($"requestParams.aws_attributes", lit("""{"emptyKey": ""}""")).alias("aws_attributes"),
        coalesce($"requestParams.azure_attributes", lit("""{"emptyKey": ""}""")).alias("azure_attributes"),
        $"requestParams.instance_pool_id",
        $"requestParams.preloaded_spark_versions",
        $"requestParams.instance_pool_name",
        $"requestParams.node_type_id",
        $"requestParams.idle_instance_autotermination_minutes",
        $"requestParams.min_idle_instances",
        $"requestParams.max_capacity",
        'requestId,
        'response,
        'sessionId,
        'sourceIPAddress,
        'userAgent,
        'userIdentity
      )
      .filter('serviceName === "instancePools")

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

      val changeInventory = Map[String, Column](
        "aws_attributes" -> SchemaTools.structToMap(poolsRawWithStructs, "aws_attributes"),
        "azure_attributes" -> SchemaTools.structToMap(poolsRawWithStructs, "azure_attributes")
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
    val isServerless = get_json_object(regexp_replace('spark_conf, "\\.", "_"),
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
      .when(isServerless, lit("Serverless"))
      .when(isSQLAnalytics, lit("SQL Analytics"))
      .otherwise("Standard").alias("cluster_type")

    val clusterBaseDF = clusterBase(df)
    val clusterBaseWMetaDF = clusterBaseDF
      // remove start, startResults, and permanentDelete as they do not contain sufficient metadata
      .filter('actionName.isin("create", "edit"))
    val bronzeClusterSnapUntilCurrent = bronze_cluster_snap.asDF
      .filter('Pipeline_SnapTS <= untilTime.asColumnTS)

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
      val missingClusterIds = bronzeClusterSnapUntilCurrent.select('organization_id, 'cluster_id).distinct
        .join(
          clusterBaseWMetaDF
            .select('organization_id, 'cluster_id).distinct,
          Seq("organization_id", "cluster_id"), "anti"
        )
      val latestClusterSnapW = Window.partitionBy('organization_id, 'cluster_id).orderBy('Pipeline_SnapTS.desc)
      val missingClusterBaseFromSnap = bronzeClusterSnapUntilCurrent
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
          to_json('spark_env_vars).alias("spark_env_vars"),
          'spark_conf,
          'driver_instance_pool_id,
          'instance_pool_id,
          'spark_version,
          (unix_timestamp('Pipeline_SnapTS) * 1000).alias("timestamp"),
          'Pipeline_SnapTS.cast("date").alias("date"),
          'creator_user_name.alias("createdBy")
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


    val (driverPoolLookup: Option[DataFrame], workerPoolLookup: Option[DataFrame]) = if ( // if instance pools found in audit logs
      !auditRawTable.asDF
        .filter('serviceName === "instancePools" && 'actionName.isin("create", "edit")).isEmpty) {
      val basePoolsDF = auditRawTable.asDF
        .filter('serviceName === "instancePools" && 'actionName.isin("create", "edit"))
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
      when('driver_instance_pool_id.isNotNull, coalesce('pool_driver_node_type, 'pool_snap_driver_node_type))
        .when('instance_pool_id.isNotNull && 'driver_instance_pool_id.isNull, coalesce('pool_node_type, 'pool_snap_node_type))
        .when('cluster_name.like("job-%-run-%"), coalesce('driver_node_type_id, 'node_type_id)) // when jobs clusters workers == driver driver node type is not defined
        .when(isSingleNode, 'node_type_id) // null
        .otherwise(coalesce('driver_node_type_id, first('driver_node_type_id, true).over(clusterBefore), 'node_type_id))

    val filledWorkerType = when(isSingleNode, lit(null).cast("string")) // singleNode clusters don't have worker nodes
      .when('instance_pool_id.isNotNull, coalesce('pool_node_type, 'pool_snap_node_type))
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
      'spark_version,
      'idempotency_token,
      'timestamp,
      'userEmail)

    val clustersRemoved = clusterBaseDF
      .filter($"response.statusCode" === 200) // only successful delete statements get applied
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
        .withColumn("driver_instance_pool_id", lit(null).cast("string"))
        .withColumn("driver_instance_pool_name", lit(null).cast("string"))
        .withColumn("pool_driver_node_type", lit(null).cast("string"))
        .withColumn("pool_node_type", lit(null).cast("string"))
    }

    // lookup pools node types from pools snapshots when snapshots exist
    val clusterBaseWithPoolsAndSnapPools = if (driverPoolSnapLookup.nonEmpty) {
      clusterBaseWithPools
        .toTSDF("timestamp", "organization_id", "instance_pool_id")
        .lookupWhen(
          workerPoolSnapLookup.get
            .toTSDF("timestamp", "organization_id", "instance_pool_id")
        ).df
        .toTSDF("timestamp", "organization_id", "driver_instance_pool_id")
        .lookupWhen(
          driverPoolSnapLookup.get
            .toTSDF("timestamp", "organization_id", "driver_instance_pool_id")
        ).df
    } else {
      clusterBaseWithPools
        .withColumn("pool_snap_driver_instance_pool_name", lit(null).cast("string"))
        .withColumn("pool_snap_driver_node_type", lit(null).cast("string"))
        .withColumn("pool_snap_instance_pool_name", lit(null).cast("string"))
        .withColumn("pool_snap_node_type", lit(null).cast("string"))
    }

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
      .withColumn("createdBy",
        when(isAutomated('cluster_name) && 'actionName === "create", lit("JobsService"))
          .when(!isAutomated('cluster_name) && 'actionName === "create", 'userEmail))
      .withColumn("createdBy", when(!isAutomated('cluster_name) && 'createdBy.isNull, last('createdBy, true).over(clusterBefore)).otherwise('createdBy))
      .withColumn("createdBy", when('createdBy.isNull && 'cluster_creator_lookup.isNotNull, 'cluster_creator_lookup).otherwise('createdBy))
      .withColumn("lastEditedBy", when(!isAutomated('cluster_name) && 'actionName === "edit", 'userEmail))
      .withColumn("lastEditedBy", when('lastEditedBy.isNull, last('lastEditedBy, true).over(clusterBefore)).otherwise('lastEditedBy))
      .drop("userEmail", "cluster_creator_lookup", "single_user_name")
  }

  def buildClusterStateDetail(
                               pipelineSnapTime: TimeTypes
                             )(clusterEventsDF: DataFrame): DataFrame = {
    val stateUnboundW = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp)
    val stateFromCurrentW = Window.partitionBy('organization_id, 'cluster_id).rowsBetween(1L, 1000L).orderBy('timestamp)
    val stateUntilCurrentW = Window.partitionBy('organization_id, 'cluster_id).rowsBetween(-1000L, -1L).orderBy('timestamp)
    val stateUntilPreviousRowW = Window.partitionBy('organization_id, 'cluster_id).rowsBetween(Window.unboundedPreceding, -1L).orderBy('timestamp)
    val uptimeW = Window.partitionBy('organization_id, 'cluster_id, 'reset_partition).orderBy('unixTimeMS_state_start)

    val nonBillableTypes = Array(
      "STARTING", "TERMINATING", "CREATING", "RESTARTING"
    )

    // some states like EXPANDED_DISK and NODES_LOST, etc are excluded because they
    // occasionally do come after the cluster has been terminated; thus they are not a guaranteed event
    // goal is to be certain about the 99th percentile
    val runningStates = Array(
      "STARTING", "INIT_SCRIPTS_STARTED", "RUNNING", "CREATING",
      "RESIZING", "UPSIZE_COMPLETED", "DRIVER_HEALTHY"
    )

    val invalidEventChain = lead('runningSwitch, 1).over(stateUnboundW).isNotNull && lead('runningSwitch, 1)
      .over(stateUnboundW) === lead('previousSwitch, 1).over(stateUnboundW)
    val clusterEventsBaseline = clusterEventsDF
      .selectExpr("*", "details.*")
      .drop("details")
      .withColumnRenamed("type", "state")
      .withColumn(
        "runningSwitch",
        when('state === "TERMINATING", lit(false))
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

    clusterEventsBaseline
      .withColumn("counter_reset",
        when(
          lag('state, 1).over(stateUnboundW).isin("TERMINATING", "RESTARTING", "EDITED") ||
            !'isRunning, lit(1)
        ).otherwise(lit(0))
      )
      .withColumn("reset_partition", sum('counter_reset).over(stateUnboundW))
      .withColumn("target_num_workers", last('target_num_workers, true).over(stateUnboundW))
      .withColumn("current_num_workers", last('current_num_workers, true).over(stateUnboundW))
      .withColumn("unixTimeMS_state_start", 'timestamp)
      .withColumn("unixTimeMS_state_end", coalesce( // if state end open, use pipelineSnapTime, will be merged when state end is received
        lead('timestamp, 1).over(stateUnboundW) - lit(1), // subtract 1 millis
        lit(pipelineSnapTime.asUnixTimeMilli)
      ))
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

  private def getJobsBase(df: DataFrame): DataFrame = {
    val onlyOnceJobRecW = Window.partitionBy('organization_id, 'timestamp, 'actionName, 'requestId, $"response.statusCode").orderBy('timestamp)
    df.filter(col("serviceName") === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .withColumn("rnk", rank().over(onlyOnceJobRecW))
      .withColumn("rn", row_number.over(onlyOnceJobRecW))
      .filter('rnk === 1 && 'rn === 1).drop("rnk", "rn")
  }

  private def cleanseNewSettingsTasks(df: DataFrame, keys: Array[String]): DataFrame = {
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

  private def cleanseJobClusters(df: DataFrame, keys: Array[String]): DataFrame = {
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

  protected def dbJobsStatusSummary(
                                     jobsSnapshotTargetComplete: PipelineTable,
                                     isFirstRun: Boolean,
                                     targetKeys: Array[String],
                                     fromTime: TimeTypes
                                   )(df: DataFrame): DataFrame = {

    if (!jobsSnapshotTargetComplete.exists(dataValidation = true)) {
      throw new NoNewDataException(s"${jobsSnapshotTargetComplete.tableFullName} has no data for this workspace. " +
        s"To continue please ensure that Overwatch has access to see the jobs and that there are jobs present in " +
        s"this workspace. Otherwise, disable the jobs module completely.", Level.WARN, allowModuleProgression = true)
    }
    val jobsBase = getJobsBase(df)
      .filter('actionName.isin("create", "delete", "reset", "update", "resetJobAcl", "changeJobAcl"))

    val jobsBaseHasRecords = !jobsBase.isEmpty
    // not first run but no new pools records from audit -- fast fail OR
    // is first run and no pools records or snapshot records -- fast fail
    if (
      (!isFirstRun && !jobsBaseHasRecords) ||
        (isFirstRun && !jobsSnapshotTargetComplete.exists(dataValidation = true))
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

    val jobsSnapshotDFComplete = jobsSnapshotTargetComplete.asDF

    // Simplified DF for snapshot lookupWhen
    val jobSnapLookup = jobsSnapshotDFComplete
      .withColumnRenamed("created_time", "snap_lookup_created_time")
      .withColumnRenamed("creator_user_name", "snap_lookup_created_by")
      .withColumnRenamed("job_id", "jobId")
      .withColumn("lookup_settings", to_json('settings))
      .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS))
      .drop("Overwatch_RunID", "settings")

    val jobs_statusCols: Array[Column] = Array(
      'organization_id, 'serviceName, 'actionName, 'timestamp,
      when('actionName === "create", get_json_object($"response.result", "$.job_id").cast("long"))
        .when('actionName === "changeJobAcl", 'resourceId.cast("long"))
        .otherwise('job_id).cast("long").alias("jobId"),
      when('actionName === "create", 'name)
        .when('actionName.isin("update", "reset"), get_json_object('new_settings, "$.name"))
        .otherwise(lit(null).cast("string")).alias("jobName"),
      'job_type,
      'timeout_seconds.cast("string").alias("timeout_seconds"),
      'schedule,
      get_json_object('notebook_task, "$.notebook_path").alias("notebook_path"),
      'new_settings, 'existing_cluster_id, 'new_cluster, 'aclPermissionSet, 'grants, 'targetUserId,
      'sessionId, 'requestId, 'userAgent, 'userIdentity, 'response, 'sourceIPAddress, 'version
    )

    val lastJobStatus = Window.partitionBy('organization_id, 'jobId).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val jobStatusBase = jobsBase
      .select(jobs_statusCols: _*)
      .toTSDF("timestamp", "organization_id", "jobId")
      .lookupWhen(
        jobSnapLookup.toTSDF("timestamp", "organization_id", "jobId"),
        maxLookAhead = Long.MaxValue
      ).df
      .withColumn("existing_cluster_id", coalesce('existing_cluster_id, get_json_object('new_settings, "$.existing_cluster_id")))
      .withColumn("new_cluster", coalesce('new_cluster, get_json_object('new_settings, "$.new_cluster")))
      // Following section builds out a "clusterSpec" as it is defined at the timestamp. existing_cluster_id
      // and new_cluster should never both be populated as a job must be one or the other at a timestamp
      .withColumn( // initialize cluster_spec at record timestamp
        "x",
        struct(
          when('existing_cluster_id.isNotNull, struct('timestamp, 'existing_cluster_id)).otherwise(lit(null)).alias("last_existing"),
          when('new_cluster.isNotNull, struct('timestamp, 'new_cluster)).otherwise(lit(null)).alias("last_new")
        )
      )
      .withColumn( // last non_null cluster id / spec
        "x2",
        struct(
          last($"x.last_existing", true).over(lastJobStatus).alias("last_existing"),
          last($"x.last_new", true).over(lastJobStatus).alias("last_new"),
        )
      )
      .withColumn( //
        "cluster_spec",
        struct(
          when($"x2.last_existing.timestamp" > coalesce($"x2.last_new.timestamp", lit(0)), $"x2.last_existing.existing_cluster_id").otherwise(lit(null)).alias("existing_cluster_id"),
          when($"x2.last_new.timestamp" > coalesce($"x2.last_existing.timestamp", lit(0)), $"x2.last_new.new_cluster").otherwise(lit(null)).alias("new_cluster")
        )
      )
      .withColumn(
        "cluster_spec",
        struct(
          when($"cluster_spec.existing_cluster_id".isNull && $"cluster_spec.new_cluster".isNull, get_json_object('lookup_settings, "$.existing_cluster_id")).otherwise($"cluster_spec.existing_cluster_id").alias("existing_cluster_id"),
          when($"cluster_spec.existing_cluster_id".isNull && $"cluster_spec.new_cluster".isNull, get_json_object('lookup_settings, "$.new_cluster")).otherwise($"cluster_spec.new_cluster").alias("new_cluster")
        )
      ).drop("existing_cluster_id", "new_cluster", "x", "x2") // drop temp columns and old version of clusterSpec components
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

    /**
     * jobStatusBaseFilled - if first run, baseline jobs statuses for existing jobs that haven't been edited since
     * commencement of audit logs. Allows for joins directly to gold jobs to work even if they haven't been modified
     * lately. Several of the fields are unavailable through this method but many are and they are very valuable when
     * present in gold
     */
    val jobStatusBaseFilled = if (isFirstRun && !jobsSnapshotDFComplete.isEmpty) {

      // get missing job ids
      val missingJobIds = if (jobsBaseHasRecords) { // if job status records found in audit
        jobsSnapshotDFComplete.select('organization_id, 'job_id).distinct
          .join(jobStatusBase.select('organization_id, 'jobId.alias("job_id")).distinct, Seq("organization_id", "job_id"), "anti")
      } else jobsSnapshotDFComplete.select('organization_id, 'job_id).distinct // otherwise just load what's available from snap

      // impute records for jobs in snapshot not in audit (i.e. pre-existing pools prior to audit logs capture)
      val lastJobSnapW = Window.partitionBy('organization_id, 'job_id).orderBy('Pipeline_SnapTS.desc)
      val jSnapMissingJobs = jobsSnapshotDFComplete
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
      if (jobsBaseHasRecords) unionWithMissingAsNull(jobStatusBase, jSnapMissingJobs) else jSnapMissingJobs
    } else if (jobsBaseHasRecords) { // not first run but new audit records exist, continue
      jobStatusBase
    } else { // not first run AND no new audit data break out and progress timeline
      val msg = s"No new jobs audit records found, progressing timeline and appending no new records"
      throw new NoNewDataException(msg, Level.WARN, allowModuleProgression = true)
    }

    val changeInventory = if (!jobStatusBase.isEmpty) {
      Map(
        "cluster_spec.new_cluster" -> structFromJson(spark, jobStatusBaseFilled, "cluster_spec.new_cluster"),
        "new_settings" -> structFromJson(spark, jobStatusBaseFilled, "new_settings"),
        "schedule" -> structFromJson(spark, jobStatusBaseFilled, "schedule")
      )
    } else { // new_settings is not present from snapshot and is a struct thus it cannot be added with dynamic schema
      Map(
        "cluster_spec.new_cluster" -> structFromJson(spark, jobStatusBaseFilled, "cluster_spec.new_cluster"),
        "schedule" -> structFromJson(spark, jobStatusBaseFilled, "schedule")
      )
    }

    // create structs from json strings and cleanse schema
    val jobStatusEnhanced = jobStatusBaseFilled
      .select(SchemaTools.modifyStruct(jobStatusBaseFilled.schema, changeInventory): _*)
      .scrubSchema

    // convert structs to maps where the structs' keys are allowed to be typed by the user to avoid
    // bad duplicate keys
    val structsCleaner = collection.mutable.Map(
      "new_settings.new_cluster.custom_tags" -> SchemaTools.structToMap(jobStatusEnhanced, "new_settings.new_cluster.custom_tags"),
      "new_settings.new_cluster.spark_conf" -> SchemaTools.structToMap(jobStatusEnhanced, "new_settings.new_cluster.spark_conf"),
      "new_settings.new_cluster.spark_env_vars" -> SchemaTools.structToMap(jobStatusEnhanced, "new_settings.new_cluster.spark_env_vars"),
      s"new_settings.new_cluster.aws_attributes" -> SchemaTools.structToMap(jobStatusEnhanced, s"new_settings.new_cluster.aws_attributes"),
      s"new_settings.new_cluster.azure_attributes" -> SchemaTools.structToMap(jobStatusEnhanced, s"new_settings.new_cluster.azure_attributes"),
      "new_settings.notebook_task.base_parameters" -> SchemaTools.structToMap(jobStatusEnhanced, "new_settings.notebook_task.base_parameters"),
      "cluster_spec.new_cluster.custom_tags" -> SchemaTools.structToMap(jobStatusEnhanced, "cluster_spec.new_cluster.custom_tags"),
      "cluster_spec.new_cluster.spark_conf" -> SchemaTools.structToMap(jobStatusEnhanced, "cluster_spec.new_cluster.spark_conf"),
      "cluster_spec.new_cluster.spark_env_vars" -> SchemaTools.structToMap(jobStatusEnhanced, "cluster_spec.new_cluster.spark_env_vars"),
      s"cluster_spec.new_cluster.aws_attributes" -> SchemaTools.structToMap(jobStatusEnhanced, s"cluster_spec.new_cluster.aws_attributes"),
      s"cluster_spec.new_cluster.azure_attributes" -> SchemaTools.structToMap(jobStatusEnhanced, s"cluster_spec.new_cluster.azure_attributes")
    )

    // if new tasks are used cleanse the nested tasks columns as they contain additional nested structs
    // with user-defined keys -- they should be converted to a map and the new_settings column rebuilt
    val withCleansedTasks = if (SchemaTools.getAllColumnNames(jobStatusEnhanced.schema).contains("new_settings.tasks")) {
      val cleansedNewSettingsTasksDF = cleanseNewSettingsTasks(jobStatusEnhanced, targetKeys)

      structsCleaner("new_settings.tasks") = col("newSettingsTask")

      jobStatusEnhanced
        .join(cleansedNewSettingsTasksDF, targetKeys.toSeq, "left")

    } else { // new_settings.tasks column doesn't exist (i.e. jobs pipelines not used in workspace)
      jobStatusEnhanced
    }

    val withCleansedJobClusters = if (SchemaTools.getAllColumnNames(jobStatusEnhanced.schema).contains("new_settings.job_clusters")) {
      val cleansedJobClustersDF = cleanseJobClusters(jobStatusEnhanced, targetKeys)
      structsCleaner("new_settings.job_clusters") = col("job_clusters")
      withCleansedTasks
        .join(cleansedJobClustersDF, targetKeys.toSeq, "left")
    } else {
      withCleansedTasks
    }

    withCleansedJobClusters
      .select(SchemaTools.modifyStruct(withCleansedJobClusters.schema, structsCleaner.toMap): _*)
      .drop("newSettingsTask", "job_clusters")
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
                                  etlUntilTime: TimeTypes
                                )(auditLogLag30D: DataFrame): DataFrame = {

    val jobRunActions = Array("runSucceeded", "runFailed", "runNow", "runStart", "submitRun", "cancel")
    val jobRunsLag30D = getJobsBase(auditLogLag30D)
      .filter('actionName.isin(jobRunActions: _*))

    // TODO -- identify the subset of jobs that need to be updated in the final merge
    //  this includes newly completed runs, newly started runs, and ongoing runs from previous run
    //  this requires additional testing and is generally for perf so putting this off until a bit later.
    //  look at jrcp logic as it seems to handle this
    val newJobRuns = jobRunsLag30D
      .filter('date >= etlStartTime.asColumnTS.cast("date") && 'timestamp >= lit(etlStartTime.asUnixTimeMilli))

    /**
     * JOBS-1709 -- JAWS team should fix multiple event emissions. If they do, this can be removed for a perf gain
     * but for now, this is required or multiple records will be created in the fact due to the multiple events.
     * Confirmed that the first event emitted is the correct event as per ES-65402
     */
    val firstRunSemanticsW = Window.partitionBy('organization_id, 'runId).orderBy('timestamp)

    // Completes must be >= etlStartTime as it is the driver endpoint
    // All joiners to Completes may be from the past up to N days as defined in the incremental df
    // Identify all completed jobs in scope for this overwatch run
    val allCompletes = jobRunsLag30D
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

    // CancelRequests are still lookups from the driver "complete" as a cancel request is a request and still
    // results in a runFailed after the cancellation
    // Identify all cancelled jobs in scope for this overwatch run
    val allCancellations = jobRunsLag30D
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

    // DF for jobs launched with actionName == "runNow"
    // Lookback 30 days for laggard starts prior to current run
    // only field from runNow that we care about is the response.result.runId
    val runNowStart = jobRunsLag30D
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

    // DF for jobs launched with actionName == "submitRun"
    // Lookback 30 days for laggard starts prior to current run

    // TODO: move taskDetail and libraries to jobStatus as it only exists for create and submitRun actions
    //  it will be much more complete over there
    val runSubmitStart = jobRunsLag30D
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

    // DF to pull unify differing schemas from runNow and submitRun and pull all job launches into one DF
    val allSubmissions = runNowStart
      .unionByName(runSubmitStart)

    // Find the corresponding runStart action for the completed jobs
    // Lookback 30 days for laggard starts prior to current run
    val runStarts = jobRunsLag30D
      .filter('actionName.isin("runStart"))
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

    // Lookup to populate the clusterID/clusterName where missing from jobs
    lazy val clusterSpecLookup = clusterSpec.asDF
      .select('organization_id, 'timestamp, 'cluster_name, 'cluster_id.alias("clusterId"))
      .filter('clusterId.isNotNull && 'cluster_name.isNotNull)

    // Lookup to populate the clusterID/clusterName where missing from jobs
    lazy val clusterSnapLookup = clusterSnapshot.asDF
      .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS) * lit(1000))
      .select('organization_id, 'timestamp, 'cluster_name, 'cluster_id.alias("clusterId"))
      .filter('clusterId.isNotNull && 'cluster_name.isNotNull)

    // Lookup to populate the existing_cluster_id where missing from jobs -- it can be derived from name
    lazy val existingClusterLookup = jobsStatus.asDF
      .select('organization_id, 'timestamp, 'jobId, $"cluster_spec.existing_cluster_id".alias("clusterId"))
      .filter('clusterId.isNotNull)

    // Ensure the lookups have data -- this will be improved when the module unification occurs during the next
    // scheduled refactor
    val clusterLookups: scala.collection.mutable.Map[String, DataFrame] = scala.collection.mutable.Map()
    val jobStatusClusterLookups: scala.collection.mutable.Map[String, DataFrame] = scala.collection.mutable.Map()

    if (clusterSpec.exists) clusterLookups.put("clusterSpecLookup", clusterSpecLookup)
    if (clusterSnapshot.exists) clusterLookups.put("clusterSnapLookup", clusterSnapLookup)
    if (jobsStatus.exists) jobStatusClusterLookups.put("jobStatusLookup", existingClusterLookup)

    // match all completed and cancelled (i.e. terminated) jobRuns with their launch assuming launch was in now - 30d
    val jobRunsBase = allCompletes
      .join(allCancellations, Seq("runId", "organization_id"), "full")
      .join(allSubmissions, Seq("runId", "organization_id"), "full")
      .join(runStarts, Seq("runId", "organization_id"), "full")
      .withColumn("jobId", coalesce('completedJobId, 'runStartJobId, 'runNowJobId).cast("long"))
      .withColumn("idInJob", coalesce('idInJob, 'runId))
      .withColumn("jobClusterType", coalesce('jobClusterType_Completed, 'jobClusterType_Started))
      .withColumn("jobTerminalState", when('cancellationRequestId.isNotNull, "Cancelled").otherwise('jobTerminalState)) //.columns.sorted
      .withColumn("cluster_name", when('jobClusterType === "new", concat(lit("job-"), 'jobId, lit("-run-"), 'idInJob)).otherwise(lit(null).cast("string")))
      .select(
        'runId.cast("long"),
        'jobId,
        'idInJob,
        TransformFunctions.subtractTime(array_min(array('startTime, 'submissionTime)), coalesce(array_max(array('completionTime, 'cancellationTime)), lit(etlUntilTime.asUnixTimeMilli))).alias("JobRunTime"),
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

    val jrBaseExisting = jobRunsBase.filter('jobClusterType === "existing")

    val automatedJobRunsBase = jobRunsBase
      .filter('jobClusterType === "new")

    val jobsClusterJobRunsBase = jobRunsBase
      .filter('jobClusterType === "job_cluster")

    // LOOKUP - ClusterID By JobID -- EXISTING CLUSTER JOB RUNS
    // JobRuns with interactive clusters
    // If the lookups are present (likely will be unless is first run or modules are turned off somehow)
    // use the lookups to populate the null clusterIds and cluster_names
    val interactiveRunsWID = if (jobStatusClusterLookups.nonEmpty) {
      var jrBaseExistingBuilder = jrBaseExisting

      if (jobStatusClusterLookups.contains("jobStatusLookup")) {
        jrBaseExistingBuilder = jrBaseExistingBuilder
          .toTSDF("timestamp", "organization_id", "jobId")
          .lookupWhen(
            jobStatusClusterLookups("jobStatusLookup")
              .toTSDF("timestamp", "organization_id", "jobId")
          ).df
      }

      jrBaseExistingBuilder

    } else jrBaseExisting

    // Re-Combine Interactive and Automated Job Run Clusters
    val jobRunsBaseWIDs = interactiveRunsWID
      .unionByName(automatedJobRunsBase)
      .unionByName(jobsClusterJobRunsBase)

    // Get JobName for All Jobs

    val jobNameLookupFromStatus = jobsStatus.asDF
      .select('organization_id, 'timestamp, 'jobId, 'jobName
      )

    // jobNames from MLFlow Runs
    val experimentName = get_json_object($"taskDetail.notebook_task", "$.base_parameters.EXPERIMENT_NAME")

    val jobRunsWIdsAndNames = jobRunsBaseWIDs
      .toTSDF("timestamp", "organization_id", "jobId")
      .lookupWhen(
        jobNameLookupFromStatus.toTSDF("timestamp", "organization_id", "jobId")
      ).df
      .withColumn("jobName", when('jobName.isNull && experimentName.isNotNull, experimentName).otherwise('jobName))
      .withColumn("jobName", when('jobName.isNull && 'run_name.isNotNull, 'run_name).otherwise('jobName))

    /**
     * Child job run times are double counted when kept at the top level with their parents, thus this step is to
     * nest the children under the parent. Currently, only nesting one layer.
     */
    val jobRunsWJobMetaWithoutChildren = jobRunsWIdsAndNames
      .filter(get_json_object('workflow_context, "$.root_run_id").isNull)

    val jobRunsWJobMetaWithoutChildrenInteractive = jobRunsWJobMetaWithoutChildren.filter('jobClusterType === "existing")
    val jobRunsWJobMetaWithoutChildrenAutomated = jobRunsWJobMetaWithoutChildren.filter('jobClusterType === "new")

    // clusters -- get clusterID by derived name AND/OR clusterName by clusterID
    val (interactiveRunsWJobClusterMeta, automatedRunsWJobClusterMeta) = if (clusterLookups.nonEmpty) {

      var interactiveClusterMetaLookupBuilder = jobRunsWJobMetaWithoutChildrenInteractive
      var automatedClusterMetaLookupBuilder = jobRunsWJobMetaWithoutChildrenAutomated

      if (clusterLookups.contains("clusterSpecLookup")) {
        // GET cluster_name by clusterID for interactive clusters
        interactiveClusterMetaLookupBuilder = interactiveClusterMetaLookupBuilder
          .toTSDF("timestamp", "organization_id", "clusterId")
          .lookupWhen(
            clusterLookups("clusterSpecLookup")
              .toTSDF("timestamp", "organization_id", "clusterId")
          ).df

        // get cluster_id by cluster_name (derived)
        automatedClusterMetaLookupBuilder = automatedClusterMetaLookupBuilder
          .toTSDF("timestamp", "organization_id", "cluster_name")
          .lookupWhen(
            clusterLookups("clusterSpecLookup")
              .toTSDF("timestamp", "organization_id", "cluster_name")
          ).df
      }

      // look in run snapshots when values still null attempt to fill them from the snapshots
      if (clusterLookups.contains("clusterSnapLookup")) {
        interactiveClusterMetaLookupBuilder = interactiveClusterMetaLookupBuilder
          .toTSDF("timestamp", "organization_id", "clusterId")
          .lookupWhen(
            clusterLookups("clusterSnapLookup")
              .toTSDF("timestamp", "organization_id", "clusterId")
          ).df

        automatedClusterMetaLookupBuilder = automatedClusterMetaLookupBuilder
          .toTSDF("timestamp", "organization_id", "cluster_name")
          .lookupWhen(
            clusterLookups("clusterSnapLookup")
              .toTSDF("timestamp", "organization_id", "cluster_name")
          ).df
      }

      (interactiveClusterMetaLookupBuilder, automatedClusterMetaLookupBuilder)
    } else (jobRunsWJobMetaWithoutChildrenInteractive, jobRunsWJobMetaWithoutChildrenAutomated)

    // Union the interactive and automated back together with the cluster ids and names
    val jobRunsWMeta = interactiveRunsWJobClusterMeta.unionByName(automatedRunsWJobClusterMeta)

    // Get Ephemeral Notebook Job Run Details to be nested into the parent job runs
    val childJobRuns = jobRunsWIdsAndNames
      .filter(get_json_object('workflow_context, "$.root_run_id").isNotNull)

    val childRunsForNesting = childJobRuns
      .withColumn("child", struct(childJobRuns.schema.fieldNames map col: _*))
      .select(
        get_json_object('workflow_context, "$.root_run_id").cast("long").alias("runId"), 'child
      )
      .groupBy('runId)
      .agg(collect_list('child).alias("children"))

    val jobRunsFinal = jobRunsWMeta
      .join(childRunsForNesting, Seq("runId"), "left")
      .drop("timestamp") // duplicated to enable asOf Lookups, dropping to clean up
      .withColumn("startEpochMS", $"JobRunTime.startEpochMS") // for incremental downstream after job termination
      .withColumn("runId", 'runId.cast("long"))
      .withColumn("jobId", 'jobId.cast("long"))
      .withColumn("idInJob", 'idInJob.cast("long"))

    jobRunsFinal
  }

  protected def notebookSummary()(df: DataFrame): DataFrame = {
    val notebookCols = auditBaseCols ++ Array[Column]('notebookId, 'notebookName, 'path, 'oldName, 'oldPath,
      'newName, 'newPath, 'parentPath, 'clusterId)

    df.filter('serviceName === "notebook")
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .withColumn("pathLength", when('notebookName.isNull, size(split('path, "/"))))
      .withColumn("notebookName", when('notebookName.isNull, split('path, "/")('pathLength - 1)).otherwise('notebookName))
      .select(notebookCols: _*)
  }

}
