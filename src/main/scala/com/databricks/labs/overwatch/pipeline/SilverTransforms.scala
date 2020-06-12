package com.databricks.labs.overwatch.pipeline

import java.util.UUID

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

trait SilverTransforms extends SparkSessionWrapper {

  import spark.implicits._

  //  protected val sparkEventsDF: DataFrame = spark.table(s"${_dbName}.spark_events_bronze")
  //    .drop("ClasspathEntries", "HadoopProperties", "SparkProperties", "SystemProperties", "SparkPlanInfo") // TODO - TEMP
  //    .withColumn("filenameGroup", groupFilename('filename))
  //      .repartition().cache
  //    sparkEventsDF.count

  case class DerivedCluster(jobLevel: DataFrame, stageLevel: DataFrame)
  private val isAutomatedCluster = 'cluster_name.like("job-%-run-%")

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

    private def unionWithMissingAsNull(baseDF: DataFrame, lookupDF: DataFrame): DataFrame = {
      val baseCols = baseDF.columns
      val lookupCols = lookupDF.columns
      val missingBaseCols = lookupCols.diff(baseCols)
      val missingLookupCols = baseCols.diff(lookupCols)
      val df1Complete = missingBaseCols.foldLeft(baseDF) {
        case (df, c) =>
          df.withColumn(c, lit(null))
      }
      val df2Complete = missingLookupCols.foldLeft(lookupDF) {
        case (df, c) =>
          df.withColumn(c, lit(null))
      }

      df1Complete.unionByName(df2Complete)
    }

    def fillFromLookupsByTS(primaryDF: DataFrame, primaryOnlyNoNulls: String,
                            columnsToLookup: Array[String], w: WindowSpec,
                            lookupDF: DataFrame*): DataFrame = {
      val finalDFWNulls = lookupDF.foldLeft(primaryDF) {
        case (primaryDF, lookup) =>
          unionWithMissingAsNull(primaryDF, lookup)
      }

      columnsToLookup.foldLeft((finalDFWNulls)) {
        case (df, c) =>
          val dt = df.schema.fields.filter(_.name == c).head.dataType
          df.withColumn(c, coalesce(last(col(c), ignoreNulls = true).over(w), lit(0).cast(dt)))
      }.filter(col(primaryOnlyNoNulls).isNotNull)

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

    def structFromJson(df: DataFrame, c: String): Column = {
      require(df.schema.fields.map(_.name).contains(c), s"The dataframe does not contain col ${c}")
      require(df.schema.fields.filter(_.name == c).head.dataType.isInstanceOf[StringType], "Column must be a json formatted string")
      val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
      if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
        println(s"WARNING: The json schema for column ${c} was not parsed correctly, please review.")
      }
      from_json(col(c), jsonSchema).alias(c)
    }

    // TODO -- Add validation that all desired power property columns exist
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
//          $"Properties.sparkdatabricksclusterUsageTagsclusterInstancePoolId".alias("InstancePoolID"),
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

  protected def sparkJobs(clusterLookupDF: DataFrame)(df: DataFrame): DataFrame = {
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

  protected def sparkStages()(df: DataFrame): DataFrame = {
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
  protected def sparkTasks()(df: DataFrame): DataFrame = {
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
      'actionName.isin("login", "tokenLogin", "samlLogin", "jwtLogin") &&
        $"userIdentity.email" =!= "dbadmin")
      .select('timestamp, 'date, 'serviceName, 'actionName,
        $"requestParams.user".alias("login_user"), $"requestParams.userName".alias("ssh_user_name"),
        $"requestParams.userName".alias("groups_user_name"),
        $"requestParams.userID".alias("account_admin_userID"),
        $"userIdentity.email".alias("userEmail"), 'sourceIPAddress, 'userAgent)
  }

  protected def newAccounts()(df: DataFrame): DataFrame = {
    df.filter('serviceName === "accounts" && 'actionName.isin("add"))
      .select('date, 'timestamp, 'serviceName, 'actionName, $"userIdentity.email".alias("userEmail"),
        $"requestParams.targetUserName", 'sourceIPAddress, 'userAgent)
  }

  private val auditBaseCols: Array[Column] = Array(
    'timestamp, 'serviceName, 'actionName, $"userIdentity.email".alias("userEmail"), 'requestId, 'response)

  private def clusterBase(auditRawDF: DataFrame): DataFrame = {
    val cluster_id_gen_w = Window.partitionBy('cluster_name).orderBy('timestamp).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val cluster_name_gen_w = Window.partitionBy('cluster_id).orderBy('timestamp).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    val cluster_state_gen_w = Window.partitionBy('cluster_id).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val cluster_id_gen = first('cluster_id, true).over(cluster_id_gen_w)
    val cluster_name_gen = first('cluster_name, true).over(cluster_name_gen_w)
    val cluster_state_gen = last('cluster_state, true).over(cluster_state_gen_w)

    val clusterSummaryCols = auditBaseCols ++ Array[Column](
      when('cluster_id.isNull, 'clusterId).otherwise('cluster_id).alias("cluster_id"),
      when('cluster_name.isNull, 'clusterName).otherwise('cluster_name).alias("cluster_name"),
      'clusterState.alias("cluster_state"), 'driver_node_type_id, 'node_type_id, 'num_workers, 'autoscale,
      'clusterWorkers.alias("actual_workers"), 'autotermination_minutes, 'enable_elastic_disk, 'start_cluster,
      'aws_attributes, 'clusterOwnerUserId, 'cluster_log_conf, 'init_scripts, 'custom_tags,
      'cluster_source, 'spark_env_vars, 'spark_conf,
      when('ssh_public_keys.isNotNull, true).otherwise(false).alias("has_ssh_keys"),
      'acl_path_prefix, 'instance_pool_id, 'spark_version, 'cluster_creator, 'idempotency_token,
      'organization_id, 'user_id, 'sourceIPAddress, 'ssh_public_keys)

    auditRawDF
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .select(clusterSummaryCols: _*)
      .withColumn("cluster_id", cluster_id_gen)
      .withColumn("cluster_name", cluster_name_gen)
      .withColumn("cluster_state", cluster_state_gen)
  }

  protected def buildClusterSpec(bronze_cluster_snap: PipelineTable)(df: DataFrame): DataFrame = {
    val lastClusterSnap = Window.partitionBy('cluster_id).orderBy('Pipeline_SnapTS.desc)
    val clusterBefore = Window.partitionBy('cluster_id)
      .orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val clusterBaseDF = clusterBase(df)
    val clusterSpecBaseCols = Array[Column]('serviceName, 'actionName,
      'cluster_id, 'cluster_name, 'cluster_state, 'driver_node_type_id, 'node_type_id,
      'num_workers, 'autoscale, 'autotermination_minutes, 'enable_elastic_disk, 'start_cluster, 'cluster_log_conf,
      'aws_attributes, 'init_scripts, 'custom_tags, 'cluster_source, 'spark_env_vars, 'spark_conf,
      'has_ssh_keys, 'acl_path_prefix, 'instance_pool_id, 'spark_version,
      'idempotency_token, 'organization_id, 'timestamp, 'userEmail)

    val clustersRemoved = clusterBaseDF
      .filter($"response.statusCode" === 200)
      .filter('actionName.isin("permanentDelete"))
      .select('cluster_id, 'userEmail.alias("deleted_by"))

    val creatorLookup = bronze_cluster_snap.asDF
      .withColumn("rnk", rank().over(lastClusterSnap))
      .withColumn("rn", row_number().over(lastClusterSnap))
      .filter('rnk === 1 && 'rn === 1)
      .select('cluster_id, $"default_tags.Creator".alias("cluster_creator_lookup"))

    clusterBaseDF
      .filter('actionName.isin("create", "edit"))
      .filter($"response.statusCode" === 200)
      .select(clusterSpecBaseCols: _*)
      .join(creatorLookup, Seq("cluster_id"), "left")
      .join(clustersRemoved, Seq("cluster_id"), "left")
      .withColumn("createdBy",
        when(isAutomatedCluster && 'actionName === "create", lit("JobsService"))
          .when(!isAutomatedCluster && 'actionName === "create", 'userEmail))
      .withColumn("createdBy", when(!isAutomatedCluster && 'createdBy.isNull, last('createdBy, true).over(clusterBefore)).otherwise('createdBy))
      .withColumn("createdBy", when('createdBy.isNull && 'cluster_creator_lookup.isNotNull, 'cluster_creator_lookup).otherwise('createdBy))
      .withColumn("lastEditedBy", when(!isAutomatedCluster && 'actionName === "edit", 'userEmail))
      .withColumn("lastEditedBy", when('lastEditedBy.isNull, last('lastEditedBy, true).over(clusterBefore)).otherwise('lastEditedBy))
      .drop("userEmail", "cluster_creator_lookup")
  }

  protected def buildClusterStatus(clusterSpec: PipelineTable,
                                   clusterSnapshot: PipelineTable,
                                   cloudMachineDetail: PipelineTable
                                  )(df: DataFrame): DataFrame = {
    val lastClusterSnap = Window.partitionBy('cluster_id).orderBy('Pipeline_SnapTS.desc)
    val clusterBefore = Window.partitionBy('cluster_id).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val reset = Window.partitionBy('cluster_id).orderBy('timestamp)
    val cumsum = Window.partitionBy('cluster_id, 'reset).orderBy('timestamp)

    val clusterBaseDF = clusterBase(df)

    // TODO -- Lookup missing cluster driver/worker by JobID/RunID cluster spec
    //  May not help if all cluster specs from jobs properly get recorded in cluster_silver in the first place
    val nodeTypeLookup = clusterSpec.asDF
      .withColumn("driver_node_type_id",
        when('driver_node_type_id.isNull, last('driver_node_type_id, true).over(clusterBefore))
          .otherwise('driver_node_type_id))
      .withColumn("node_type_id",
        when('node_type_id.isNull, last('node_type_id, true).over(clusterBefore))
          .otherwise('node_type_id))
      .select('cluster_id, 'timestamp, 'driver_node_type_id, 'node_type_id)

    val nodeTypeLookup2 = clusterSnapshot.asDF
      .select('cluster_id, 'terminated_time, 'driver_node_type_id, 'node_type_id)

    val creatorLookup = clusterSnapshot.asDF
      .withColumn("rnk", rank().over(lastClusterSnap))
      .withColumn("rn", row_number().over(lastClusterSnap))
      .filter('rnk === 1 && 'rn === 1)
      .select('cluster_id, $"default_tags.Creator".alias("cluster_creator_lookup"))

    val clusterSize = clusterBaseDF
      .filter($"response.statusCode" === 200)
      .filter('actionName.like("%esult"))
      .join(creatorLookup, Seq("cluster_id"), "left")
      .withColumn("date", from_unixtime('timestamp.cast("double") / 1000).cast("timestamp"))
      .withColumn("tsS", ('timestamp / 1000).cast("long"))
      .withColumn("reset",
        sum(when('actionName.isin("startResult", "restartResult", "createResult"), lit(1))
          .otherwise(lit(0))).over(reset))
      .withColumn("runtime_curr_stateS",
        coalesce(
          when(!'actionName.isin("startResult", "createResult"), ('tsS - lag('tsS, 1).over(reset)))
          .otherwise(lit(0)), lit(0)
        ))
      .withColumn("cumulative_uptimeS", sum('runtime_curr_stateS).over(cumsum))
      .withColumn("created_by",
        when('actionName === "createResult", 'userEmail).otherwise(lit(null)))
      .withColumn("created_by",
        coalesce(
          when('created_by.isNull, last('created_by, true).over(clusterBefore))
            .otherwise('created_by)
        ))
      .withColumn("created_by",
        when('created_by.isNull && 'cluster_creator_lookup.isNotNull, 'cluster_creator_lookup)
          .otherwise(lit("Unknown")))
      .withColumn("last_started_by",
        when('actionName === "startResult", 'userEmail)
          .otherwise(lit(null)))
      .withColumn("last_started_by",
        coalesce(
          when('last_started_by.isNull, last('last_started_by, true).over(clusterBefore))
            .otherwise('last_started_by), lit("Unknown")
        ))
      .withColumn("last_restarted_by",
        when('actionName === "restartResult", 'userEmail)
          .otherwise(lit(null)))
      .withColumn("last_restarted_by",
        coalesce(
          when('last_restarted_by.isNull, last('last_restarted_by, true).over(clusterBefore))
            .otherwise('last_restarted_by), lit("Unknown")
        ))
      .withColumn("last_resized_by",
        when('actionName === "resizeResult", 'userEmail)
          .otherwise(lit(null)))
      .withColumn("last_resized_by",
        coalesce(
          when('last_resized_by.isNull, last('last_resized_by, true).over(clusterBefore))
            .otherwise('last_resized_by), lit("Unknown")
        ))
      .withColumn("last_terminated_by",
        when('actionName === "deleteResult", 'userEmail)
          .otherwise(lit(null)))
      .withColumn("last_terminated_by",
        coalesce(
          when('last_terminated_by.isNull, last('last_terminated_by, true).over(clusterBefore))
            .otherwise('last_terminated_by), lit("Unknown")
        ))

    val clusterStatusCols = Array[Column]('date, 'timestamp, 'serviceName, 'actionName, 'cluster_id,
      'cluster_name, 'cluster_state, 'actual_workers, 'DriverNodeType, 'driverVCPUs, 'driverMemoryGB,
      'WorkerNodeType, 'workerVCPUs, 'workerMemoryGB, 'cluster_worker_memory, 'cpu_time_curr_state,
      'cpu_time_cumulative, 'runtime_curr_stateS, 'cumulative_uptimeS, 'driverHourlyCostOnDemand,
      'driverHourlyCostReserved, 'driver_onDemand_cost_curr_state, 'driver_onDemand_cost_cumulative,
      'driver_reserve_cost_curr_state, 'driver_reserve_cost_cumulative, 'workerHourlyCostOnDemand,
      'workerHourlyCostReserved, 'worker_onDemand_cost_curr_state, 'worker_onDemand_cost_cumulative,
      'worker_reserve_cost_curr_state, 'worker_reserve_cost_cumulative, 'created_by, 'last_started_by,
      'last_resized_by, 'last_restarted_by, 'last_terminated_by)

    val nodeTypeLookups = Array(nodeTypeLookup, nodeTypeLookup2)

    // TODO -- NEED TO ADD AZURE SUPPORT
    //  When azure is ready -- add if cloud type...
    val ec2Lookup = cloudMachineDetail.asDF
      .withColumn("DriverNodeType", 'API_Name)
      .withColumn("WorkerNodeType", 'API_Name)

    val driverLookup = ec2Lookup
      .withColumnRenamed("On_Demand_Cost_Hourly", "driverHourlyCostOnDemand")
      .withColumnRenamed("Linux_Reserved_Cost_Hourly", "driverHourlyCostReserved")
      .withColumnRenamed("vCPUs", "driverVCPUs")
      .withColumnRenamed("Memory_GB", "driverMemoryGB")
      .drop("WorkerNodeType")

    val workerLookup = ec2Lookup
      .withColumnRenamed("On_Demand_Cost_Hourly", "workerHourlyCostOnDemand")
      .withColumnRenamed("Linux_Reserved_Cost_Hourly", "workerHourlyCostReserved")
      .withColumnRenamed("vCPUs", "workerVCPUs")
      .withColumnRenamed("Memory_GB", "workerMemoryGB")
      .drop("DriverNodeType")

    UDF.fillFromLookupsByTS(
      clusterSize, "serviceName",
      Array("driver_node_type_id", "node_type_id"), clusterBefore, nodeTypeLookups: _*)
      .withColumnRenamed("driver_node_type_id", "DriverNodeType")
      .withColumnRenamed("node_type_id", "WorkerNodeType")
      .join(workerLookup, Seq("WorkerNodeType"), "left")
      .join(driverLookup, Seq("DriverNodeType"), "left")
      .withColumn("cpu_time_curr_state", (('runtime_curr_stateS * 'actual_workers * 'workerVCPUs) + ('runtime_curr_stateS * 'driverVCPUs)))
      .withColumn("cpu_time_cumulative", (('cumulative_uptimeS * 'actual_workers * 'workerVCPUs) + ('cumulative_uptimeS * 'driverVCPUs)))
      .withColumn("driver_onDemand_cost_curr_state",
        round('driverHourlyCostOnDemand * ('runtime_curr_stateS / 60 / 60), 2))
      .withColumn("driver_onDemand_cost_cumulative",
        round('driverHourlyCostOnDemand * ('cumulative_uptimeS / 60 / 60), 2))
      .withColumn("driver_reserve_cost_curr_state",
        round('driverHourlyCostReserved * ('runtime_curr_stateS / 60 / 60), 2))
      .withColumn("driver_reserve_cost_cumulative",
        round('driverHourlyCostReserved * ('cumulative_uptimeS / 60 / 60), 2))
      .withColumn("worker_onDemand_cost_curr_state",
        round('workerHourlyCostOnDemand * 'actual_workers * ('runtime_curr_stateS / 60 / 60), 2))
      .withColumn("worker_onDemand_cost_cumulative",
        round('workerHourlyCostOnDemand * 'actual_workers * ('cumulative_uptimeS / 60 / 60), 2))
      .withColumn("worker_reserve_cost_curr_state",
        round('workerHourlyCostReserved * 'actual_workers * ('runtime_curr_stateS / 60 / 60), 2))
      .withColumn("worker_reserve_cost_cumulative",
        round('workerHourlyCostReserved * 'actual_workers * ('cumulative_uptimeS / 60 / 60), 2))
      .withColumn("cluster_worker_memory", 'actual_workers * 'workerMemoryGB)
      .select(clusterStatusCols: _*)

  }

  /**
   * First shot At Silver Job Runs
   * @param df
   * @return
   */

    // TODO -- Job Runs Silver
//  // Review new_cluster/existingCluster from runs...is it needed?
//  val jobRunCompleteCols: Array[Column] = Array(
//    'timestamp.alias("completeTimeStamp"), 'jobId.alias("jobIdCompletion"), 'runId, 'idInJob.alias("idInJobCompletion"), 'jobClusterType, 'userAgent.alias("userAgentCompletion"), 'jobTerminalState, 'jobTriggerType, 'jobTaskType, $"response.errorMessage".alias("errorMessage"), when($"userIdentity.email" === "unknown", lit(null)).otherwise($"userIdentity.email").alias("userEmailCompletion"))
//
//  val jobRunStartCols: Array[Column] = Array(
//    'serviceName, 'actionName, 'timestamp.alias("startTimestamp"), 'job_id, when('runId.isNull, get_json_object($"response.result", "$.run_id")).otherwise('runId).alias("runId"),
//    get_json_object($"response.result", "$.number_in_job").alias("idInJob"), 'run_name,
//    $"userIdentity.email".alias("userEmail"), 'userAgent, get_json_object('notebook_task, "$.notebook_path").alias("notebook_path"), 'timeout_seconds, 'sessionId, 'requestId, 'response, 'sourceIPAddress, 'version)
//
//  val jobRunsCols: Array[Column] = Array(
//    'serviceName, 'actionName, when('jobIdCompletion.isNull, 'job_id).otherwise('jobIdCompletion).alias("jobId"), 'runId, 'jobClusterType, 'jobTerminalState, 'jobTriggerType, 'jobTaskType, 'errorMessage,
//    'run_name, 'notebook_path, when('userAgent.isNull, 'userAgentCompletion).otherwise('userAgent).alias("userAgent"), 'timeout_seconds,
//    when('idInJobCompletion.isNull, 'idInJob).otherwise('idInJobCompletion).alias("idInJob"),
//    subtractTime('startTimestamp, 'completeTimeStamp).alias("JobRunTime"),
//    when('userEmail.isNull, 'userEmailCompletion).otherwise('userEmail).alias("userEmail"), 'sessionId, 'requestId, 'response, 'sourceIPAddress, 'version
//  )
//
//  val jobRunsStartDF = auditJobs
//    .filter('actionName.isin("runNow", "submitRun"))
//    .select(jobRunStartCols: _*)
//    .filter('runId.isNotNull)
//
//  val jobRunsCompleteDF = auditJobs
//    .filter('actionName.isin("runSucceeded", "cancel", "runFailed"))
//    .select(jobRunCompleteCols: _*)
//    .filter('runId.isNotNull)
//
//  val jobRunsSilver = jobRunsStartDF
//    .join(jobRunsCompleteDF, Seq("runId"))
//    .select(jobRunsCols: _*)

  private def getJobsBase(df: DataFrame): DataFrame = {
      df
        .filter('serviceName === "jobs")
        .selectExpr("*", "requestParams.*").drop("requestParams")
    }

  protected def dbJobsStatusSummary()(df: DataFrame): DataFrame = {

    val jobsBase = getJobsBase(df)

    val jobs_statusCols: Array[Column] = Array(
      'serviceName, 'actionName, 'timestamp, when('job_id.isNull, get_json_object($"response.result", "$.job_id")).otherwise('job_id).alias("jobId"), 'job_type, 'name.alias("jobName"), 'timeout_seconds, 'schedule,
      get_json_object('notebook_task, "$.notebook_path").alias("notebook_path"), 'new_settings, 'existing_cluster_id, 'new_cluster,
      'sessionId, 'requestId, 'userAgent, 'response, 'sourceIPAddress, 'version
    )

    val lastJobStatus = Window.partitionBy('jobId).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val jobCluster = struct(
      struct('existing_cluster_id.alias("existing_cluster_id")).alias("existing_cluster"),
      struct('new_cluster.alias("new_cluster_spec")).alias("new_cluster")
    )

    val jobStatusBase = jobsBase
      .filter('actionName.isin("create", "reset", "update"))
      .select(jobs_statusCols: _*)
      .withColumn("existing_cluster_id",
        when('existing_cluster_id.isNull &&
          get_json_object('new_settings, "$.existing_cluster_id").isNotNull, get_json_object('new_settings, "$.existing_cluster_id"))
          .otherwise('existing_cluster_id))
      .withColumn("new_cluster", when('new_cluster.isNull, get_json_object('new_settings, "$.new_cluster")).otherwise('new_cluster))
      .withColumn("job_type", when('job_type.isNull, last('job_type, true).over(lastJobStatus)).otherwise('job_type))
      .withColumn("schedule", when('schedule.isNull, last('schedule, true).over(lastJobStatus)).otherwise('schedule))
      .withColumn("timeout_seconds", when('timeout_seconds.isNull, last('timeout_seconds, true).over(lastJobStatus)).otherwise('timeout_seconds))
      .withColumn("notebook_path", when('notebook_path.isNull, last('notebook_path, true).over(lastJobStatus)).otherwise('notebook_path))
      .withColumn("jobName", when('jobName.isNull, last('jobName, true).over(lastJobStatus)).otherwise('jobName))

    jobStatusBase
      .withColumn("new_cluster", when('new_cluster.isNotNull,
        UDF.structFromJson(jobStatusBase, "new_cluster")).otherwise(lit(null)))
      .withColumn("jobCluster",
        last(
          when('existing_cluster_id.isNull && 'new_cluster.isNull, lit(null))
            .otherwise(jobCluster),
          true
        ).over(lastJobStatus)
      )
  }

  protected def dbJobRunsSummary(clusterSpec: PipelineTable,
                                 jobsStatus: PipelineTable,
                                 jobsSnapshot: PipelineTable)(df: DataFrame): DataFrame = {
    // Review new_cluster/existingCluster from runs...is it needed?

    val jobsBase = getJobsBase(df)

    val jobRunCompleteCols: Array[Column] = Array(
      'timestamp.alias("completeTimestamp"), 'jobId.alias("jobIdCompletion"), 'runId,
      'idInJob.alias("idInJobCompletion"), 'jobClusterType, 'userAgent.alias("userAgentCompletion"),
      'jobTerminalState, 'jobTriggerType, 'jobTaskType, $"response.errorMessage".alias("errorMessage"),
      when($"userIdentity.email" === "unknown", lit(null)).otherwise($"userIdentity.email")
        .alias("userEmailCompletion"))

    val jobRunStartCols: Array[Column] = Array(
      'serviceName, 'actionName, 'timestamp.alias("startTimestamp"), 'job_id, when('runId.isNull,
        get_json_object($"response.result", "$.run_id")).otherwise('runId).alias("runId"),
      get_json_object($"response.result", "$.number_in_job").alias("idInJob"), 'run_name,
      $"userIdentity.email".alias("userEmail"), 'userAgent,
      get_json_object('notebook_task, "$.notebook_path").alias("notebook_path"),
      'timeout_seconds, 'sessionId, 'requestId, 'response, 'sourceIPAddress, 'version)

    val jobRunsCols: Array[Column] = Array(
      'serviceName, 'actionName,
      when('jobIdCompletion.isNull, 'job_id).otherwise('jobIdCompletion).alias("jobId"),
      'runId, 'jobClusterType, 'jobTerminalState, 'jobTriggerType, 'jobTaskType, 'errorMessage,
      'run_name, 'notebook_path, when('userAgent.isNull, 'userAgentCompletion)
        .otherwise('userAgent).alias("userAgent"), 'timeout_seconds,
      when('idInJobCompletion.isNull, 'idInJob).otherwise('idInJobCompletion).alias("idInJob"),
      UDF.subtractTime('startTimestamp, 'completeTimeStamp).alias("JobRunTime"),
      when('userEmail.isNull, 'userEmailCompletion).otherwise('userEmail).alias("userEmail"),
      'sessionId, 'requestId, 'response, 'sourceIPAddress, 'version
    )

    val jobRunsColsFinalOrder: Array[Column] = Array(
      'serviceName, 'actionName, 'startTimestamp, 'jobId, 'runId, 'run_name, 'cluster_id, 'notebook_path,
      'jobClusterType, 'jobTerminalState, 'jobTriggerType, 'jobTaskType, 'errorMessage, 'userAgent, 'timeout_seconds,
      'idInJob, 'JobRunTime, 'userEmail, 'sessionId, 'requestId, 'response, 'sourceIPAddress, 'version
    )

    val lastJobStatus = Window.partitionBy('jobId).orderBy('timestamp)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val ephemeralClusterLookup = clusterSpec.asDF
      .filter(isAutomatedCluster).select('cluster_id.alias("ephemeralClusterId"), 'cluster_name).distinct
    val existingClusterLookup = jobsStatus.asDF
      .select('jobId, 'existing_cluster_id, 'timestamp)
    val existingClusterLookup2 = jobsSnapshot.asDF
      .select('job_id.alias("jobId"), $"settings.existing_cluster_id", 'created_time.alias("timestamp"))

    val jobRunsStartDF = jobsBase
      .filter('actionName.isin("runNow", "submitRun"))
      .select(jobRunStartCols: _*)
      .filter('runId.isNotNull)
      .dropDuplicates("runId", "startTimestamp")

    val jobRunsCompleteDF = jobsBase
      .filter('actionName.isin("runSucceeded", "cancel", "runFailed"))
      .select(jobRunCompleteCols: _*)
      .filter('runId.isNotNull)
      .dropDuplicates("runId", "completeTimestamp")

    val jobRunsSilverBase = jobRunsStartDF
      .join(jobRunsCompleteDF, Seq("runId"))
      .select(jobRunsCols: _*)
      .withColumn("cluster_name", when('jobClusterType === "new",
        concat(lit("job-"),'jobId,lit("-run-"),'idInJob)).otherwise(lit(null)))
      .join(ephemeralClusterLookup, Seq("cluster_name"), "left")
      .withColumn("timestamp", $"JobRunTime.startEpochMS")

    UDF.fillFromLookupsByTS(jobRunsSilverBase, "runId", Array("existing_cluster_id"),
      lastJobStatus, Array(existingClusterLookup, existingClusterLookup2): _*)
      .withColumn("cluster_id", when('jobClusterType === "new", 'ephemeralClusterId)
        .otherwise('existing_cluster_id))
      .withColumnRenamed("timestamp", "startTimestamp")
      .select(jobRunsColsFinalOrder: _*)

  }

  protected def notebookSummary()(df: DataFrame): DataFrame = {
    val notebookCols = auditBaseCols ++ Array[Column]('notebookId, 'notebookName, 'path, 'oldName, 'oldPath, 'newName, 'newPath, 'parentPath, 'clusterId)

    df.filter('serviceName === "notebook")
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .withColumn("pathLength", when('notebookName.isNull, size(split('path, "/"))))
      .withColumn("notebookName", when('notebookName.isNull, split('path, "/")('pathLength - 1)).otherwise('notebookName))
      .select(notebookCols: _*)
  }

}
