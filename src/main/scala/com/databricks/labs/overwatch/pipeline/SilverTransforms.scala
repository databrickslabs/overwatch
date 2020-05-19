package com.databricks.labs.overwatch.pipeline

import java.util.UUID

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

trait SilverTransforms extends SparkSessionWrapper {

  import spark.implicits._

  //  protected val sparkEventsDF: DataFrame = spark.table(s"${_dbName}.spark_events_bronze")
  //    .drop("ClasspathEntries", "HadoopProperties", "SparkProperties", "SystemProperties", "SparkPlanInfo") // TODO - TEMP
  //    .withColumn("filenameGroup", groupFilename('filename))
  //      .repartition().cache
  //    sparkEventsDF.count

  case class DerivedCluster(jobLevel: DataFrame, stageLevel: DataFrame)

  object UDF {
    // eventlog default path
    // Path == uri:/prefix/<cluster_id>/eventlog/<hostName>.replace("-", "_")/sparkContextId/<eventlog> || <eventlog-yyyy-MM-dd--HH-00.gz>
    def groupFilename(filename: Column): Column = {
      val segmentArray = split(filename, "/")
      val byCluster = array_join(slice(segmentArray, 1, 3), "/").alias("byCluster")
      val byClusterHost = array_join(slice(segmentArray, 1, 5), "/").alias("byDriverHost")
      val bySparkContextID = array_join(slice(segmentArray, 1, 6), "/").alias("bySparkContext")
      struct(filename, byCluster, byClusterHost, bySparkContextID).alias("filnameGroup")
    }

    def subtractTime(start: Column, end: Column): Column = {
      val runTimeMS = end - start
      val runTimeS = runTimeMS / 1000
      val runTimeM = runTimeS / 60
      val runTimeH = runTimeM / 60
      struct(
        start.alias("startEpochMS"),
        from_unixtime(start / 1000).cast("timestamp").alias("startTS"),
        end.alias("endEpochMS"),
        from_unixtime(end / 1000).cast("timestamp").alias("endTS"),
        lit(runTimeMS).alias("runTimeMS"),
        lit(runTimeS).alias("runTimeS"),
        lit(runTimeM).alias("runTimeM"),
        lit(runTimeH).alias("runTimeH")
      ).alias("RunTime")
    }

    // Does not remove null structs
    def removeNullCols(df: DataFrame): (Array[Column], DataFrame) = {
      val cntsDF = df.summary("count").drop("summary")
      val nonNullCols = cntsDF
        .collect().flatMap(r => r.getValuesMap(cntsDF.columns).filter(_._2 != "0").keys).map(col)
      val complexTypeFields = df.schema.fields
        .filter(f => f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType])
        .map(_.name).map(col)
      val cleanDF = df.select(nonNullCols ++ complexTypeFields: _*)
      (nonNullCols ++ complexTypeFields, cleanDF)
    }

    def clusterIDLookup(df: DataFrame): DerivedCluster = {
      val jobGroupIDW = Window.partitionBy('SparkContextID, 'JobGroupID).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

      val jobsProps = df.filter('event === "SparkListenerJobStart")
        .withColumn("JobGroupID", $"Properties.sparkjobGroupid")
        .withColumn("ClusterID", $"Properties.sparkdatabricksclusterUsageTagsclusterId")
        .select('SparkContextID, 'JobGroupID, 'JobID, explode('StageIDs).alias("StageID"), 'ClusterID)
        .withColumn("ClusterID", when('ClusterID.isNull, first('ClusterID).over(jobGroupIDW)).otherwise('ClusterID))
        .withColumn("JobsClusterID", when('ClusterID.isNull, last('ClusterID).over(jobGroupIDW)).otherwise('ClusterID))
        .drop("ClusterID")
      val stagesProps = df.filter('event === "SparkListenerStageSubmitted")
        .withColumn("JobGroupID", $"Properties.sparkjobGroupid")
        .withColumn("ClusterID", $"Properties.sparkdatabricksclusterUsageTagsclusterId")
        .select('SparkContextID, 'JobGroupID, 'StageID, 'ClusterID)
        .withColumn("ClusterID", when('ClusterID.isNull, first('ClusterID).over(jobGroupIDW)).otherwise('ClusterID))
        .withColumn("StagesClusterID", when('ClusterID.isNull, last('ClusterID).over(jobGroupIDW)).otherwise('ClusterID))
        .drop("ClusterID")

      val clusterIDByJobGroup = jobsProps.join(stagesProps, Seq("SparkContextID", "JobGroupID", "StageID"), "left")
        .withColumn("ClusterID", when('JobsClusterID.isNull, 'StagesClusterID).otherwise('JobsClusterID))

      val clusterIDAtStageLevel = clusterIDByJobGroup
        .select('SparkContextID, 'StageID, 'ClusterID)
        .distinct

      val clusterIDAtJobLevel = clusterIDByJobGroup
        .select('SparkContextID, 'JobID, 'ClusterID)
        .distinct

      // TODO -- Add inference from logic of surrounding (time) records
      DerivedCluster(clusterIDAtJobLevel, clusterIDAtStageLevel)
    }

    def appendPowerProperties: Column = {
      struct(
        $"Properties.sparkappname".alias("AppName"),
        $"Properties.sparkdatabricksapiurl".alias("WorkspaceURL"),
        $"Properties.sparkjobGroupid".alias("JobGroupID"),
        $"Properties.sparkdatabrickscloudProvider".alias("CloudProvider"),
        struct(
          $"Properties.sparkdatabricksclusterSource".alias("ClusterSource"),
          $"Properties.sparkdatabricksclusterUsageTagsautoTerminationMinutes".alias("AutoTerminationMinutes"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterAllTags".alias("ClusterTags"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterAvailability".alias("CluserAvailability"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterId".alias("ClusterID"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterInstancePoolId".alias("InstancePoolID"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterMaxWorkers".alias("MaxWorkers"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterMinWorkers".alias("MinWorkers"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterName".alias("Name"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterOwnerUserId".alias("OwnerUserID"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterScalingType".alias("ScalingType"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterSpotBidPricePercent".alias("SpotBidPricePercent"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterTargetWorkers".alias("TargetWorkers"),
          $"Properties.sparkdatabricksclusterUsageTagsclusterWorkers".alias("ActualWorkers"),
          $"Properties.sparkdatabricksclusterUsageTagscontainerZoneId".alias("ZoneID"),
          $"Properties.sparkdatabricksclusterUsageTagsdataPlaneRegion".alias("Region"),
          $"Properties.sparkdatabricksclusterUsageTagsdriverNodeType".alias("DriverNodeType"),
          $"Properties.sparkdatabricksworkerNodeTypeId".alias("WorkerNodeType"),
          $"Properties.sparkdatabricksclusterUsageTagssparkVersion".alias("SparkVersion")
        ).alias("ClusterDetails"),
        $"Properties.sparkdatabricksnotebookid".alias("NotebookID"),
        $"Properties.sparkdatabricksnotebookpath".alias("NotebookPath"),
        $"Properties.sparkdatabrickssparkContextId".alias("SparkContextID"),
        $"Properties.sparkdriverhost".alias("DriverHostIP"),
        $"Properties.sparkdrivermaxResultSize".alias("DriverMaxResults"),
        $"Properties.sparkexecutorid".alias("ExecutorID"),
        $"Properties.sparkexecutormemory".alias("ExecutorMemory"),
        $"Properties.sparksqlexecutionid".alias("ExecutionID"),
        $"Properties.sparksqlexecutionparent".alias("ExecutionParent"),
        $"Properties.sparksqlshufflepartitions".alias("ShufflePartitions"),
        $"Properties.user".alias("UserEmail"),
        $"Properties.userID".alias("UserID")
      )
    }
  }

  protected def getJDBCSession(sessionStartDF: DataFrame, sessionEndDF: DataFrame): DataFrame = {
    sessionStartDF
      .join(sessionEndDF, Seq("SparkContextID", "sessionId"))
      .withColumn("ServerSessionRunTime", UDF.subtractTime('startTime, 'finishTime))
      .drop("startTime", "finishTime")
  }

  protected def getJDBCOperation(operationStartDF: DataFrame, operationEndDF: DataFrame): DataFrame = {
    operationStartDF
      .join(operationEndDF, Seq("SparkContextID", "id"))
      .withColumn("ServerOperationRunTime", UDF.subtractTime('startTime, 'closeTime))
      .drop("startTime", "finishTime")
  }

  private def simplifyExecutorAdded(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerExecutorAdded")
      .select('SparkContextID, 'ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"),
        'Pipeline_SnapTS, 'filenameGroup.alias("startFilenameGroup"))
  }

  private def simplifyExecutorRemoved(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerExecutorRemoved")
      .select('SparkContextID, 'ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"),
        'filenameGroup.alias("endFilenameGroup"))
  }

  protected def executor()(df: DataFrame): DataFrame = {

    val executorAdded = simplifyExecutorAdded(df)
    val executorRemoved = simplifyExecutorRemoved(df)

    executorAdded.join(executorRemoved, Seq("SparkContextID", "ExecutorID"))
      .withColumn("TaskRunTime", UDF.subtractTime('executorAddedTS, 'executorRemovedTS))
      .drop("executorAddedTS", "executorRemovedTS")
      .withColumn("ExecutorAliveTime", struct(
        $"TaskRunTime.startEpochMS".alias("AddedEpochMS"),
        $"TaskRunTime.startTS".alias("AddedTS"),
        $"TaskRunTime.endEpochMS".alias("RemovedTS"),
        $"TaskRunTime.endTS".alias("RemovedTS"),
        $"TaskRunTime.runTimeMS".alias("uptimeMS"),
        $"TaskRunTime.runTimeS".alias("uptimeS"),
        $"TaskRunTime.runTimeM".alias("uptimeM"),
        $"TaskRunTime.runTimeH".alias("uptimeH")
      ))
      .withColumn("addedTimestamp", $"ExecutorAliveTime.AddedEpochMS")
  }

  protected def enhanceApplication()(df: DataFrame): DataFrame = {
    df.select('SparkContextID, 'AppID, 'AppName,
      from_unixtime('Timestamp / 1000).cast("timestamp").alias("AppStartTime"),
      'Pipeline_SnapTS, 'filenameGroup)
  }

  private val uniqueTimeWindow = Window.partitionBy("SparkContextID", "executionId")

  private def simplifyExecutionsStart(df: DataFrame): DataFrame = {
    df.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")
      .select('SparkContextID, 'description, 'details, 'executionId.alias("ExecutionID"),
        'time.alias("SqlExecStartTime"),
        'Pipeline_SnapTS, 'filenameGroup.alias("startFilenameGroup"))
      .withColumn("timeRnk", rank().over(uniqueTimeWindow.orderBy('SqlExecStartTime)))
      .withColumn("timeRn", row_number().over(uniqueTimeWindow.orderBy('SqlExecStartTime)))
      .filter('timeRnk === 1 && 'timeRn === 1)
      .drop("timeRnk", "timeRn")
  }

  private def simplifyExecutionsEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")
      .select('SparkContextID, 'executionId.alias("ExecutionID"),
        'time.alias("SqlExecEndTime"),
        'filenameGroup.alias("endFilenameGroup"))
      .withColumn("timeRnk", rank().over(uniqueTimeWindow.orderBy('SqlExecEndTime)))
      .withColumn("timeRn", row_number().over(uniqueTimeWindow.orderBy('SqlExecEndTime.desc)))
      .filter('timeRnk === 1 && 'timeRn === 1)
      .drop("timeRnk", "timeRn")
  }

  protected def sqlExecutions()(df: DataFrame): DataFrame = {
    val executionsStart = simplifyExecutionsStart(df)
    val executionsEnd = simplifyExecutionsEnd(df)

    //TODO -- review if skew is necessary -- on all DFs
    executionsStart
      .join(executionsEnd.hint("SKEW", Seq("SparkContextID")), Seq("SparkContextID", "ExecutionID"))
      .withColumn("SqlExecutionRunTime", UDF.subtractTime('SqlExecStartTime, 'SqlExecEndTime))
      .drop("SqlExecStartTime", "SqlExecEndTime")
      .withColumn("startTimestamp", $"SqlExecutionRunTime.startEpochMS")
  }

  protected def simplifyJobStart(df: DataFrame, eventsRawDF: DataFrame): DataFrame = {
    df.filter('Event.isin("SparkListenerJobStart"))
      .withColumn("PowerProperties", UDF.appendPowerProperties)
      .select(
        'SparkContextID, $"PowerProperties.ClusterDetails.ClusterID",
        $"PowerProperties.JobGroupID", $"PowerProperties.ExecutionID",
        'JobID, 'StageIDs, 'SubmissionTime, 'PowerProperties,
        'Properties.alias("OriginalProperties"),
        'Pipeline_SnapTS, 'filenameGroup.alias("startFilenameGroup")
      )
      .join(UDF.clusterIDLookup(eventsRawDF).jobLevel, Seq("SparkContextID", "JobID"), "left")
  }

  protected def simplifyJobEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerJobEnd")
      .select('SparkContextID, 'JobID, 'JobResult, 'CompletionTime,
        'filenameGroup.alias("endFilenameGroup"))
  }

  protected def jobs(clusterLookupDF: DataFrame)(df: DataFrame): DataFrame = {
    val jobStart = simplifyJobStart(df, clusterLookupDF)
    val jobEnd = simplifyJobEnd(df)

    jobStart.join(jobEnd.hint("SKEW", Seq("SparkContextID")), Seq("SparkContextID", "JobId"))
      .withColumn("JobRunTime", UDF.subtractTime('SubmissionTime, 'CompletionTime))
      .drop("SubmissionTime", "CompletionTime")
      .withColumn("startTimestamp", $"JobRunTime.startEpochMS")

  }

  protected def simplifyStageStart(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerStageSubmitted")
      .select('SparkContextID,
        $"StageInfo.StageID", $"StageInfo.SubmissionTime", $"StageInfo.StageAttemptID",
        'StageInfo.alias("StageStartInfo"),
        'Pipeline_SnapTS, 'filenameGroup.alias("startFilenameGroup"))
  }

  protected def simplifyStageEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerStageCompleted")
      .select('SparkContextID,
        $"StageInfo.StageID", $"StageInfo.StageAttemptID", $"StageInfo.CompletionTime",
        'StageInfo.alias("StageEndInfo"), 'filenameGroup.alias("endFilenameGroup")
      )
  }

  protected def stages()(df: DataFrame): DataFrame = {
    val stageStart = simplifyStageStart(df)
    val stageEnd = simplifyStageEnd(df)

    stageStart
      .join(stageEnd.hint("SKEW", Seq("SparkContextID")), Seq("SparkContextID", "StageID", "StageAttemptID"))
      .withColumn("StageInfo", struct(
        $"StageEndInfo.Accumulables", $"StageEndInfo.CompletionTime", $"StageStartInfo.Details",
        $"StageStartInfo.FailureReason", $"StageEndInfo.NumberofTasks",
        $"StageStartInfo.ParentIDs", $"StageStartInfo.SubmissionTime"
      )).drop("StageEndInfo", "StageStartInfo")
      .withColumn("StageRunTime", UDF.subtractTime('SubmissionTime, 'CompletionTime))
      .drop("SubmissionTime", "CompletionTime")
      .withColumn("startTimestamp", $"StageRunTime.startEpochMS")
  }

  protected def simplifyTaskStart(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerTaskStart")
      .select('SparkContextID,
        'StageID, 'StageAttemptID, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID",
        $"TaskInfo.Attempt".alias("TaskAttempt"),
        $"TaskInfo.Host", $"TaskInfo.LaunchTime", 'TaskInfo.alias("TaskStartInfo"),
        'Pipeline_SnapTS, 'filenameGroup.alias("startFilenameGroup")
      )
  }

  protected def simplifyTaskEnd(df: DataFrame): DataFrame = {
    df.filter('Event === "SparkListenerTaskEnd")
      .select('SparkContextID,
        'StageID, 'StageAttemptID, 'TaskEndReason, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID",
        $"TaskInfo.Attempt".alias("TaskAttempt"),
        $"TaskInfo.Host", $"TaskInfo.FinishTime", 'TaskInfo.alias("TaskEndInfo"),
        'TaskMetrics, 'TaskType, 'filenameGroup.alias("endFilenameGroup")
      )
  }

  // Orphan tasks are a result of "TaskEndReason.Reason" != "Success"
  // Failed tasks lose association with their chain
  protected def tasks()(df: DataFrame): DataFrame = {
    val taskStart = simplifyTaskStart(df)
    val taskEnd = simplifyTaskEnd(df)

    taskStart.join(
      taskEnd.hint("SKEW", Seq("SparkContextID")), Seq(
        "SparkContextID", "StageID", "StageAttemptID", "TaskID", "TaskAttempt", "ExecutorID", "Host"
      ))
      .withColumn("TaskInfo", struct(
        $"TaskEndInfo.Accumulables", $"TaskEndInfo.Failed", $"TaskEndInfo.FinishTime",
        $"TaskEndInfo.GettingResultTime",
        $"TaskEndInfo.Host", $"TaskEndInfo.Index", $"TaskEndInfo.Killed", $"TaskStartInfo.LaunchTime",
        $"TaskEndInfo.Locality", $"TaskEndInfo.Speculative"
      )).drop("TaskStartInfo", "TaskEndInfo")
      .withColumn("TaskRunTime", UDF.subtractTime('LaunchTime, 'FinishTime)).drop("LaunchTime", "FinishTime")
      .withColumn("startTimestamp", $"TaskRunTime.startEpochMS")
  }

  protected def userLogins()(df: DataFrame): DataFrame = {
    df.filter(
      'serviceName === "accounts" &&
      'actionName.isin("login", "tokenLogin") &&
        $"userIdentity.email" =!= "dbadmin")
      .select('date, $"userIdentity.email".alias("userEmail"),
        'serviceName, 'actionName, 'sourceIPAddress, 'timestamp, 'userAgent)
  }

  protected def newAccounts()(df: DataFrame): DataFrame = {
    df.filter('serviceName === "accounts" && 'actionName.isin("add"))
      .select('date, 'timestamp, 'serviceName, 'actionName, $"userIdentity.email".alias("userEmail"),
        $"requestParams.targetUserName", 'sourceIPAddress, 'userAgent)
  }

  private val audidBaseCols: Array[Column] = Array(
    'timestamp, 'serviceName, 'actionName, $"userIdentity.email".alias("userEmail"))

  protected def clustersStatusSummary()(df: DataFrame): DataFrame = {
    val cluster_id_gen_w = Window.partitionBy('cluster_name).orderBy('timestamp).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val cluster_name_gen_w = Window.partitionBy('cluster_id).orderBy('timestamp).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val cluster_state_gen_w = Window.partitionBy('cluster_id).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val cluster_id_gen = first('cluster_id, true).over(cluster_id_gen_w)
    val cluster_name_gen = first('cluster_name, true).over(cluster_name_gen_w)
    val cluster_state_gen = last('cluster_state, true).over(cluster_state_gen_w)

    val clusterSummaryCols = audidBaseCols ++ Array[Column](
      when('cluster_id.isNull, 'clusterId).otherwise('cluster_id).alias("cluster_id"),
      when('cluster_name.isNull, 'clusterName).otherwise('cluster_name).alias("cluster_name"),
      'clusterState.alias("cluster_state"), 'driver_node_type_id, 'node_type_id, 'num_workers, 'autoscale,
      'clusterWorkers.alias("actual_workers"), 'autotermination_minutes, 'enable_elastic_disk, 'start_cluster,
      'aws_attributes, 'clusterOwnerUserId, 'cluster_log_conf, 'init_scripts, 'custom_tags,
      'cluster_source, 'spark_env_vars, 'spark_conf,
      when('ssh_public_keys.isNotNull, true).otherwise(false).alias("has_ssh_keys"),
      'acl_path_prefix, 'instance_pool_id, 'spark_version, 'cluster_creator, 'idempotency_token,
      'organization_id, 'user_id, 'sourceIPAddress, 'ssh_public_keys)

    df
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .select(clusterSummaryCols: _*)
      .withColumn("cluster_id", cluster_id_gen)
      .withColumn("cluster_name", cluster_name_gen)
      .withColumn("cluster_state", cluster_state_gen)
  }

  protected def jobsStatusSummary()(df: DataFrame): DataFrame = {
    val jobChangeCols = audidBaseCols ++ Array[Column](when('name.isNull && 'new_settings.isNotNull,
      get_json_object('new_settings, "$.name")).otherwise('name).alias("job_name"),
      when('job_id.isNull && 'actionName === "create",
        regexp_replace(split($"response.result", ":")(1),"}", ""))
        .otherwise('job_id).alias("job_id"),
      'notebook_task, 'existing_cluster_id, 'job_type, 'schedule, 'timeout_seconds, 'new_settings, 'new_cluster)

    df
      .filter('serviceName === "jobs" && 'actionName.isin("reset", "create", "update"))
      .selectExpr("*", "requestParams.*")
      .select(jobChangeCols: _*)
      .orderBy('timestamp)
  }


  //  object Session {
  //
  //    private val serverSessionStartDF = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionCreated")
  //      .select('SparkContextID, 'ip, 'sessionId, 'startTime, 'userName, 'filenameGroup.alias("startFilenameGroup"))
  //
  //    private val serverSessionEndDF = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionClosed")
  //      .select('SparkContextID, 'sessionId, 'finishTime, 'filenameGroup.alias("endFilenameGroup"))
  //
  //    // Server Operation --> start -> parsed -> finish -> closed
  //    // Parsed adds string execution plan
  //    // Finish adds finishTime
  //    private val serverOperationStartDF = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationStart")
  //      .select('SparkContextID, 'groupId, 'id, 'sessionId, 'startTime, 'statement, 'userName,
  //        'filenameGroup.alias("startFilenameGroup"))
  //
  //    private val serverOperationEndDF = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationClosed")
  //      .select('SparkContextID, 'id, 'closeTime, 'filenameGroup.alias("endFilenameGroup"))
  //
  //    private val serverSessionDF: DataFrame = serverSessionStartDF
  //      .join(serverSessionEndDF, Seq("SparkContextID", "sessionId"))
  //      .withColumn("ServerSessionRunTime", subtractTime('startTime, 'finishTime))
  //      .drop("startTime", "finishTime")
  //
  //    private val serverOperationDF: DataFrame = serverOperationStartDF
  //      .join(serverOperationEndDF, Seq("SparkContextID", "id"))
  //      .withColumn("ServerOperationRunTime", subtractTime('startTime, 'closeTime))
  //      .drop("startTime", "finishTime")
  //
  //    def getSession: DataFrame = {
  //      serverSessionDF
  //    }
  //
  //    def getOperation: DataFrame = {
  //      serverOperationDF
  //    }
  //
  //  }

  //  object Executor {
  //
  //    // TODO -- Can there be multiple instances of ExecutorN in a single spark context?
  //    def compositeKey: Array[Column] = {
  //      Array('SparkContextID, col("ExecutorInfo.Host"), 'ExecutorID)
  //    }
  //
  //      private val executorAddedDF = sparkEventsDF.filter('Event === "SparkListenerExecutorAdded")
  //        .select('SparkContextID, 'ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"),
  //          'filenameGroup.alias("startFilenameGroup"))
  //
  //      private val executorRemovedDF = sparkEventsDF.filter('Event === "SparkListenerExecutorRemoved")
  //        .select('SparkContextID, 'ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"),
  //          'filenameGroup.alias("endFilenameGroup"))
  //  //
  //      val executorDF: DataFrame = executorAddedDF.join(executorRemovedDF, Seq("SparkContextID", "ExecutorID"))
  //        .withColumn("TaskRunTime", subtractTime('executorAddedTS, 'executorRemovedTS))
  //          .drop("executorAddedTS", "executorRemovedTS")
  //
  //  }

  //  object Application {
  //
  //    private val appDF: DataFrame = sparkEventsDF.filter('Event === "SparkListenerApplicationStart")
  //      .select('SparkContextID, 'AppID, 'AppName,
  //        from_unixtime('Timestamp / 1000).cast("timestamp").alias("AppStartTime"), 'filenameGroup)
  //
  //    def get: DataFrame = appDF
  //
  //  }

  //  object Executions {
  //
  //    // This window is necessary to guarantee unique Execution IDs
  //    // Apparently the Spark listener can occasionally place two events in the log with differing times
  //    private val uniqueTimeWindow = Window.partitionBy("SparkContextID", "executionId")
  //
  //    // Include these with specific request only
  //    // 'physicalPlanDescription, 'sparkPlanInfo, 'details
  //    private val sqlExecStartDF = sparkEventsDF
  //      .filter('Event.isin("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"))
  //      .select('SparkContextID, 'description, 'details, 'executionId.alias("ExecutionID"),
  //        'time.alias("SqlExecStartTime"),
  //        'filenameGroup.alias("startFilenameGroup"))
  //      .withColumn("timeRnk", rank().over(uniqueTimeWindow.orderBy('SqlExecStartTime)))
  //      .withColumn("timeRn", row_number().over(uniqueTimeWindow.orderBy('SqlExecStartTime)))
  //      .filter('timeRnk === 1 && 'timeRn === 1)
  //      .drop("timeRnk", "timeRn")
  //
  //    private val sqlExecEndDF = sparkEventsDF
  //      .filter('Event.isin("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"))
  //      .select('SparkContextID, 'executionId.alias("ExecutionID"),
  //        'time.alias("SqlExecEndTime"),
  //        'filenameGroup.alias("endFilenameGroup"))
  //      .withColumn("timeRnk", rank().over(uniqueTimeWindow.orderBy('SqlExecEndTime)))
  //      .withColumn("timeRn", row_number().over(uniqueTimeWindow.orderBy('SqlExecEndTime.desc)))
  //      .filter('timeRnk === 1 && 'timeRn === 1)
  //      .drop("timeRnk", "timeRn")
  //
  //    private val sqlExecutionsDF: DataFrame = sqlExecStartDF
  //      .join(sqlExecEndDF.hint("SKEW", Seq("SparkContextID")), Seq("SparkContextID", "ExecutionID"))
  //      .withColumn("SqlExecutionRunTime", subtractTime('SqlExecStartTime, 'SqlExecEndTime))
  //      .drop("SqlExecStartTime", "SqlExecEndTime")
  //
  //    private var returnDF: DataFrame = sqlExecutionsDF
  //
  //    def get: DataFrame = {
  //      returnDF
  //    }

  //    def withJobs: this.type = {
  //      returnDF = returnDF
  //        .join(Jobs.get, Seq("SparkContextID", "ExecutionID"))
  //      this
  //    }
  //
  //  }

  //  object Jobs {
  //
  //    // Window attempting to get ClusterID from JobGroup
  //    private val jobGroupIDW = Window.partitionBy('SparkContextID, 'JobGroupID)
  //      .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  //
  //    private val jobStartDF = sparkEventsDF.filter('Event.isin("SparkListenerJobStart"))
  //      .withColumn("PowerProperties", appendPowerProperties)
  //      .select(
  //        'SparkContextID, 'JobGroupID, 'JobID, 'StageIDs, 'JobGroupID,
  //        'SubmissionTime, 'ExecutionID, 'PowerProperties,
  //        'Properties.alias("OriginalProperties"),
  //        'filenameGroup.alias("startFilenameGroup")
  //      )
  //      .join(clusterIDLookup.jobLevel, Seq("SparkContextID", "JobID"), "left")
  //
  //    private val jobEndDF = sparkEventsDF.filter('Event.isin("SparkListenerJobEnd"))
  //      .select('SparkContextID, 'JobID, 'JobResult, 'CompletionTime,
  //        'filenameGroup.alias("endFilenameGroup"))
  //
  //    private val jobsDF: DataFrame = jobStartDF.join(jobEndDF.hint("SKEW", Seq("SparkContextID")), Seq("SparkContextID", "JobId"))
  //      .withColumn("JobRunTime", subtractTime('SubmissionTime, 'CompletionTime))
  //      .drop("SubmissionTime", "CompletionTime")
  //
  //    private var returnDF: DataFrame = jobsDF
  //    private var _joiner: Seq[String] = Seq("SparkContextID")
  //
  //    private def withStageID: Column = {
  //      if (returnDF.columns.contains("StageID")) {
  //        col("StageID")
  //      } else {
  //        explode(col("StageIDs"))
  //      }
  //    }
  //
  //    def get: DataFrame = {
  //      returnDF
  //    }
  //
  //    def withStages: this.type = {
  //      _joiner ++ Seq("StageID")
  //      returnDF = returnDF
  //        .withColumn("StageID", explode('StageIDs))
  //        .join(Stages.get, _joiner)
  //      this
  //    }
  //
  //    def withTasks: this.type = {
  //      _joiner ++ Seq("StageID", "StageAttemptID")
  //
  //      returnDF = returnDF
  //        .withColumn("StageID", withStageID)
  //        .join(Tasks.get, _joiner.distinct)
  //      this
  //    }
  //
  //  }

  //  object Stages {
  //
  //    // TODO - What causes a null AppID
  //    // TODO - Why do some stages start but not finish?
  //    private val stageStartDF = sparkEventsDF.filter('Event === "SparkListenerStageSubmitted")
  //      .select('SparkContextID,
  //        $"StageInfo.StageID", $"StageInfo.SubmissionTime", $"StageInfo.StageAttemptID",
  //        'StageInfo.alias("StageStartInfo"), 'filenameGroup.alias("startFilenameGroup")
  //      )
  //
  //    private val stageEndDF = sparkEventsDF.filter('Event === "SparkListenerStageCompleted")
  //      .select('SparkContextID,
  //        $"StageInfo.StageID", $"StageInfo.StageAttemptID", $"StageInfo.CompletionTime",
  //        'StageInfo.alias("StageEndInfo"), 'filenameGroup.alias("endFilenameGroup")
  //      )
  //
  //    private val stagesDF: DataFrame = stageStartDF
  //      .join(stageEndDF.hint("SKEW", Seq("SparkContextID")), Seq("SparkContextID", "StageID", "StageAttemptID"))
  //      .withColumn("StageInfo", struct(
  //        $"StageEndInfo.Accumulables", $"StageEndInfo.CompletionTime", $"StageStartInfo.Details",
  //        $"StageStartInfo.FailureReason", $"StageEndInfo.NumberofTasks",
  //        $"StageStartInfo.ParentIDs", $"StageStartInfo.SubmissionTime"
  //      )).drop("StageEndInfo", "StageStartInfo")
  //      .withColumn("StageRunTime", subtractTime('SubmissionTime, 'CompletionTime))
  //      .drop("SubmissionTime", "CompletionTime")
  //      .withColumn("startTimestamp", $"StageRunTime.startEpochMS")
  //
  //    private var returnDF: DataFrame = stagesDF
  //
  //    def get: DataFrame = {
  //      returnDF
  //    }
  //
  //    def withTasks: this.type = {
  //      returnDF = returnDF
  //        .join(Tasks.get, Seq("SparkContextID", "StageID", "StageAttemptID"))
  //      this
  //    }
  //
  //  }


  //  object Tasks {
  //
  //    private val taskStartEvents = sparkEventsDF.filter('Event === "SparkListenerTaskStart")
  //      .select('SparkContextID,
  //        'StageID, 'StageAttemptID, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID",
  //        $"TaskInfo.Attempt".alias("TaskAttempt"),
  //        $"TaskInfo.Host", $"TaskInfo.LaunchTime", 'TaskInfo.alias("TaskStartInfo"),
  //        'filenameGroup.alias("startFilenameGroup")
  //      )
  //
  //    private val taskEndEvents = sparkEventsDF.filter('Event === "SparkListenerTaskEnd")
  //      .select('SparkContextID,
  //        'StageID, 'StageAttemptID, 'TaskEndReason, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID",
  //        $"TaskInfo.Attempt".alias("TaskAttempt"),
  //        $"TaskInfo.Host", $"TaskInfo.FinishTime", 'TaskInfo.alias("TaskEndInfo"),
  //        'TaskMetrics, 'TaskType, 'filenameGroup.alias("endFilenameGroup")
  //      )
  //
  //    // TODO -- The join here is overly powerful, it appears to be possible to reduce to SparkContextID, TaskID, TaskAttempt
  //    private val tasksDF: DataFrame = taskStartEvents.join(
  //      taskEndEvents.hint("SKEW", Seq("SparkContextID")), Seq(
  //        "SparkContextID", "StageID", "StageAttemptID", "TaskID", "TaskAttempt", "ExecutorID", "Host"
  //      ))
  //      .withColumn("TaskInfo", struct(
  //        $"TaskEndInfo.Accumulables", $"TaskEndInfo.Failed", $"TaskEndInfo.FinishTime",
  //        $"TaskEndInfo.GettingResultTime",
  //        $"TaskEndInfo.Host", $"TaskEndInfo.Index", $"TaskEndInfo.Killed", $"TaskStartInfo.LaunchTime",
  //        $"TaskEndInfo.Locality", $"TaskEndInfo.Speculative"
  //      )).drop("TaskStartInfo", "TaskEndInfo")
  //      .withColumn("TaskRunTime", subtractTime('LaunchTime, 'FinishTime)).drop("LaunchTime", "FinishTime")
  //
  //    private var returnDF: DataFrame = tasksDF
  //
  //    def get: DataFrame = {
  //      returnDF
  //    }
  //
  //  }

}
