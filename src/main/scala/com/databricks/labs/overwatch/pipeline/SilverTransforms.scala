package com.databricks.labs.overwatch.pipeline

import java.util.UUID

import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import TransformFunctions._

import scala.collection.mutable.ArrayBuffer
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

trait SilverTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val isAutomatedCluster = 'cluster_name.like("job-%-run-%")

  //  final private val orgId = dbutils.notebook.getContext.tags("orgId")

  object UDF {

    def structFromJson(df: DataFrame, c: String): Column = {
      require(df.schema.fields.map(_.name).contains(c), s"The dataframe does not contain col ${c}")
      require(df.schema.fields.filter(_.name == c).head.dataType.isInstanceOf[StringType], "Column must be a json formatted string")
      val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
      if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
        println(s"WARNING: The json schema for column ${c} was not parsed correctly, please review.")
      }
      from_json(col(c), jsonSchema).alias(c)
    }

    def structFromJson(df: DataFrame, cs: String*): Column = {
      val dfFields = df.schema.fields
      cs.foreach(c => {
        require(dfFields.map(_.name).contains(c), s"The dataframe does not contain col ${c}")
        require(dfFields.filter(_.name == c).head.dataType.isInstanceOf[StringType], "Column must be a json formatted string")
      })
      array(
        cs.map(c => {
          val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
          if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
            println(s"WARNING: The json schema for column ${c} was not parsed correctly, please review.")
          }
          from_json(col(c), jsonSchema).alias(c)
        }): _*
      )
    }

    def appendPowerProperties: Column = {
      struct(
        element_at('Properties, "sparkappname").alias("AppName"),
        element_at('Properties, "sparkdatabricksapiurl").alias("WorkspaceURL"),
        element_at('Properties, "sparkjobGroupid").alias("JobGroupID"),
        element_at('Properties, "sparkdatabrickscloudProvider").alias("CloudProvider"),
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
  }

  // Additional events for future research
  // 'Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveSQLMetricUpdates"
  //    --> select sqlPlanMetrics, ExecutionID
  // 'Event === "SparkListenerEnvironmentUpdate"
  //    --> select JVMInformation
  // 'Event.isin("SparkListenerBlockManagerAdded", "SparkListenerBlockManagerRemoved")
  //    --> select BlockManagerID, MaximumMemory, MaximumOnheapMemory, Timestamp

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

  protected def executor()(df: DataFrame): DataFrame = {


    val executorAdded = simplifyExecutorAdded(df).alias("executorAdded")
    val executorRemoved = simplifyExecutorRemoved(df).alias("executorRemoved")
    val joinKeys = Seq("organization_id", "fileCreateDate", "clusterId", "SparkContextID", "ExecutorID")

    executorAdded
      .joinWithLag(executorRemoved, joinKeys, "fileCreateDate")
      .withColumn("TaskRunTime",
        TransformFunctions.subtractTime('executorAddedTS, 'executorRemovedTS))
      .drop("executorAddedTS", "executorRemovedTS", "fileCreateDate")
      .withColumn("ExecutorAliveTime", struct(
        $"TaskRunTime.startEpochMS".alias("AddedEpochMS"),
        $"TaskRunTime.startTS".alias("AddedTS"),
        $"TaskRunTime.endEpochMS".alias("RemovedEpochMS"),
        $"TaskRunTime.endTS".alias("RemovedTS"),
        $"TaskRunTime.runTimeMS".alias("uptimeMS"),
        $"TaskRunTime.runTimeS".alias("uptimeS"),
        $"TaskRunTime.runTimeM".alias("uptimeM"),
        $"TaskRunTime.runTimeH".alias("uptimeH")
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

  protected def sqlExecutions()(df: DataFrame): DataFrame = {
    val executionsStart = simplifyExecutionsStart(df).alias("executionsStart")
    val executionsEnd = simplifyExecutionsEnd(df).alias("executionsEnd")
    val joinKeys = Seq("organization_id", "fileCreateDate", "clusterId", "SparkContextID", "ExecutionID")

    //TODO -- review if skew is necessary -- on all DFs
    executionsStart
      .joinWithLag(executionsEnd, joinKeys, "fileCreateDate")
      .withColumn("SqlExecutionRunTime",
        TransformFunctions.subtractTime('SqlExecStartTime, 'SqlExecEndTime))
      .drop("SqlExecStartTime", "SqlExecEndTime", "fileCreateDate")
      .withColumn("startTimestamp", $"SqlExecutionRunTime.startEpochMS")
  }


  protected def simplifyJobStart(sparkEventsBronze: DataFrame): DataFrame = {
    sparkEventsBronze.filter('Event.isin("SparkListenerJobStart"))
      .withColumn("PowerProperties", UDF.appendPowerProperties)
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

  protected def sparkJobs()(df: DataFrame): DataFrame = {
    val jobStart = simplifyJobStart(df).alias("jobStart")
    val jobEnd = simplifyJobEnd(df).alias("jobEnd")
    val joinKeys = Seq("organization_id", "fileCreateDate", "clusterId", "SparkContextID", "JobID")

    jobStart
      .joinWithLag(jobEnd, joinKeys, "fileCreateDate")
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

  protected def sparkStages()(df: DataFrame): DataFrame = {
    val stageStart = simplifyStageStart(df).alias("stageStart")
    val stageEnd = simplifyStageEnd(df).alias("stageEnd")
    val joinKeys = Seq("organization_id", "fileCreateDate", "clusterId", "SparkContextID", "StageID", "StageAttemptID")

    stageStart
      .joinWithLag(stageEnd, joinKeys, "fileCreateDate")
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
  protected def sparkTasks()(df: DataFrame): DataFrame = {
    val taskStart = simplifyTaskStart(df).alias("taskStart")
    val taskEnd = simplifyTaskEnd(df).alias("taskEnd")
    val joinKeys = Seq(
      "organization_id", "fileCreateDate", "clusterId", "SparkContextID",
      "StageID", "StageAttemptID", "TaskID", "TaskAttempt", "ExecutorID", "Host"
    )

    taskStart.joinWithLag(taskEnd, joinKeys, "fileCreateDate")
      .withColumn("TaskInfo", struct(
        $"TaskEndInfo.Accumulables", $"TaskEndInfo.Failed", $"TaskEndInfo.FinishTime",
        $"TaskEndInfo.GettingResultTime",
        $"TaskEndInfo.Host", $"TaskEndInfo.Index", $"TaskEndInfo.Killed", $"TaskStartInfo.LaunchTime",
        $"TaskEndInfo.Locality", $"TaskEndInfo.Speculative"
      )).drop("TaskStartInfo", "TaskEndInfo", "fileCreateDate")
      .withColumn("TaskRunTime", TransformFunctions.subtractTime('LaunchTime, 'FinishTime)).drop("LaunchTime", "FinishTime")
      .withColumn("startTimestamp", $"TaskRunTime.startEpochMS")
      .withColumn("startDate", $"TaskRunTime.startTS".cast("date"))
  }

  // TODO -- Azure Review
  protected def accountLogins()(df: DataFrame): DataFrame = {
    val endpointLogins = df.filter(
      'serviceName === "accounts" &&
        lower('actionName).like("%login%") &&
        $"userIdentity.email" =!= "dbadmin")

    val sshLoginRaw = df.filter('serviceName === "ssh" && 'actionName === "login")
      .withColumn("actionName", lit("ssh"))

    val accountLogins = endpointLogins.unionByName(sshLoginRaw)

    val sshDetails = sshLoginRaw.select(
      'timestamp,
      'organization_id,
      'date,
      'requestId,
      lit("ssh").alias("login_type"),
      struct(
        $"requestParams.instanceId".alias("instance_id"),
        $"requestParams.port".alias("login_port"),
        'sessionId.alias("session_id"),
        $"requestParams.publicKey".alias("login_public_key"),
        $"requestParams.containerId".alias("container_id"),
        $"requestParams.userName".alias("container_user_name")
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
          $"requestParams.user".alias("login_user"),
          //        $"requestParams.userName".alias("ssh_user_name"), TODO -- these are null in Azure - verify on AWS
          //        $"requestParams.user_name".alias("groups_user_name"),
          //        $"requestParams.userID".alias("account_admin_userID"),
          $"userIdentity.email".alias("user_email"),
          'sourceIPAddress, 'userAgent, 'requestId, 'response
        )
        .join(sshDetails, Seq("organization_id", "date", "timestamp", "login_type", "requestId"), "left")
        .drop("date")
    } else
      Seq("No New Records").toDF("__OVERWATCHEMPTY")
  }

  // TODO -- Azure Review
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
      Seq("No New Records").toDF("__OVERWATCHEMPTY")
  }

  private val auditBaseCols: Array[Column] = Array(
    'timestamp, 'date, 'organization_id, 'serviceName, 'actionName,
    $"userIdentity.email".alias("userEmail"), 'requestId, 'response)


  private def clusterBase(auditRawDF: DataFrame): DataFrame = {
    val cluster_id_gen_w = Window.partitionBy('organization_id, 'cluster_name).orderBy('timestamp).rowsBetween(Window.currentRow, 1000)
    val cluster_name_gen_w = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp).rowsBetween(Window.currentRow, 1000)
    val cluster_state_gen_w = Window.partitionBy('organization_id, 'cluster_id).orderBy('timestamp).rowsBetween(-1000, Window.currentRow)
    val cluster_id_gen = first('cluster_id, true).over(cluster_id_gen_w)
    val cluster_name_gen = first('cluster_name, true).over(cluster_name_gen_w)
    val cluster_state_gen = last('cluster_state, true).over(cluster_state_gen_w)

    val clusterSummaryCols = auditBaseCols ++ Array[Column](
      when('actionName === "create", get_json_object($"response.result", "$.cluster_id"))
        .when('actionName =!= "create" && 'cluster_id.isNull, 'clusterId)
        .otherwise('cluster_id).alias("cluster_id"),
      when('cluster_name.isNull, 'clusterName).otherwise('cluster_name).alias("cluster_name"),
      'clusterState.alias("cluster_state"), 'driver_node_type_id, 'node_type_id, 'num_workers, 'autoscale,
      'clusterWorkers.alias("actual_workers"), 'autotermination_minutes, 'enable_elastic_disk, 'start_cluster,
      'clusterOwnerUserId, 'cluster_log_conf, 'init_scripts, 'custom_tags, 'ssh_public_keys,
      'cluster_source, 'spark_env_vars, 'spark_conf,
      when('ssh_public_keys.isNotNull, true).otherwise(false).alias("has_ssh_keys"),
      'acl_path_prefix, 'instance_pool_id, 'instance_pool_name, 'spark_version, 'cluster_creator, 'idempotency_token,
      'user_id, 'sourceIPAddress, 'single_user_name)


    auditRawDF
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .select(clusterSummaryCols: _*)
      .withColumn("cluster_id", cluster_id_gen)
      .withColumn("cluster_name", cluster_name_gen)
      .withColumn("cluster_state", cluster_state_gen)
  }

  protected def buildClusterSpec(
                                  bronze_cluster_snap: PipelineTable,
                                  auditRawTable: PipelineTable
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


    val clusterBaseDF = clusterBase(df)

    // TODO instance pool lookup pass in from bronze
    val instancePoolLookup = auditRawTable.asDF
      .filter('serviceName === "instancePools" && 'actionName === "create")
      .select(
        'organization_id, get_json_object($"response.result", "$.instance_pool_id").alias("instance_pool_id"),
        $"requestParams.node_type_id".alias("pool_node_type")
      )

    val filledDriverType = when('cluster_name.like("job-%-run-%"), coalesce('driver_node_type_id, 'node_type_id))
      .when('instance_pool_id.isNotNull, 'pool_node_type)
      .when(isSingleNode, 'node_type_id)
      .otherwise(coalesce('driver_node_type_id, first('driver_node_type_id, true).over(clusterBefore), 'node_type_id))

    val filledWorkerType = when('instance_pool_id.isNotNull, 'pool_node_type)
      .when(isSingleNode, lit(null).cast("string")) // singleNode clusters don't have worker nodes
      .otherwise('node_type_id)

    val numWorkers = when(isSingleNode, lit(0).cast("int")).otherwise('num_workers.cast("int"))
    val startCluster = when('start_cluster === "false", lit(false))
      .otherwise(lit(true))
    val enableElasticDisk = when('enable_elastic_disk === "false", lit(false))
      .otherwise(lit(true))

    val clusterSpecBaseCols = Array[Column](
      'serviceName,
      'actionName,
      'organization_id,
      'cluster_id,
      when(isSQLAnalytics, lit("SQLAnalytics")).otherwise('cluster_name).alias("cluster_name"),
      'cluster_state,
      filledDriverType.alias("driver_node_type_id"),
      filledWorkerType.alias("node_type_id"),
      numWorkers.alias("num_workers"),
      'autoscale,
      'autotermination_minutes.cast("int").alias("autotermination_minutes"),
      enableElasticDisk.alias("enable_elastic_disk"),
      isAutomatedCluster.alias("is_automated"),
      when(isSingleNode, lit("Single Node"))
        .when(isServerless, lit("Serverless"))
        .when(isSQLAnalytics, lit("SQL Analytics"))
        .otherwise("Standard").alias("cluster_type"),
      'single_user_name,
      startCluster.alias("start_cluster"),
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'cluster_source,
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'instance_pool_id,
      'instance_pool_name,
      'spark_version,
      'idempotency_token,
      'timestamp,
      'userEmail)

    val clustersRemoved = clusterBaseDF
      .filter($"response.statusCode" === 200) //?
      .filter('actionName.isin("permanentDelete"))
      .select('organization_id, 'cluster_id, 'userEmail.alias("deleted_by"))

    val creatorLookup = bronze_cluster_snap.asDF
      .withColumn("rnk", rank().over(lastClusterSnap))
      .withColumn("rn", row_number().over(lastClusterSnap))
      .filter('rnk === 1 && 'rn === 1)
      .select('organization_id, 'cluster_id, $"default_tags.Creator".alias("cluster_creator_lookup"))

    clusterBaseDF
      .filter('actionName.isin("create", "edit"))
      .filter($"response.statusCode" === 200) //?
      .join(instancePoolLookup, Seq("organization_id", "instance_pool_id"), "left") //node_type must be derived from pool when cluster is pooled
      // TODO -- this will need to be reviewed for mixed pools (i.e. driver of different type)
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
          ).alias("single_user_profile")
        )
      )
      .withColumn("createdBy",
        when(isAutomatedCluster && 'actionName === "create", lit("JobsService"))
          .when(!isAutomatedCluster && 'actionName === "create", 'userEmail))
      .withColumn("createdBy", when(!isAutomatedCluster && 'createdBy.isNull, last('createdBy, true).over(clusterBefore)).otherwise('createdBy))
      .withColumn("createdBy", when('createdBy.isNull && 'cluster_creator_lookup.isNotNull, 'cluster_creator_lookup).otherwise('createdBy))
      .withColumn("lastEditedBy", when(!isAutomatedCluster && 'actionName === "edit", 'userEmail))
      .withColumn("lastEditedBy", when('lastEditedBy.isNull, last('lastEditedBy, true).over(clusterBefore)).otherwise('lastEditedBy))
      .drop("userEmail", "cluster_creator_lookup", "single_user_name")
  }

  /**
    * First shot At Silver Job Runs
    *
    * @param df
    * @return
    */

  //TODO -- jobs snapshot api seems to be returning more data than audit logs, review
  //  example = maxRetries, max_concurrent_runs, jar_task, python_task
  //  audit also seems to be missing many job_names (if true, retrieve from snap)
  private def getJobsBase(df: DataFrame): DataFrame = {
    df.filter(col("serviceName") === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
  }

  // TODO -- Add union lookup to jobs snapshot to fill in holes
  // TODO -- schedule is missing -- create function to check for missing columns and set them to null
  //  schema validator
  // TODO -- Remove the window lookups from here -- this should be in gold model
  protected def dbJobsStatusSummary()(df: DataFrame): DataFrame = {

    val jobsBase = getJobsBase(df)

    val jobs_statusCols: Array[Column] = Array(
      'organization_id, 'serviceName, 'actionName, 'timestamp,
      when('actionName === "create", get_json_object($"response.result", "$.job_id").cast("long"))
        .otherwise('job_id).cast("long").alias("jobId"),
      'job_type,
      'name.alias("jobName"),
      'timeout_seconds.cast("long").alias("timeout_seconds"),
      'schedule,
      get_json_object('notebook_task, "$.notebook_path").alias("notebook_path"),
      'new_settings, 'existing_cluster_id, 'new_cluster, 'aclPermissionSet, 'grants, 'targetUserId,
      'sessionId, 'requestId, 'userAgent, 'userIdentity, 'response, 'sourceIPAddress, 'version
    )

    val lastJobStatus = Window.partitionBy('organization_id, 'jobId).orderBy('timestamp).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val lastJobStatusUnbound = Window.partitionBy('organization_id, 'jobId).orderBy('timestamp)
    val jobCluster = struct(
      'existing_cluster_id.alias("existing_cluster_id"),
      'new_cluster.alias("new_cluster")
    )

    val jobStatusBase = jobsBase
      .filter('actionName.isin("create", "delete", "reset", "update", "resetJobAcl", "changeJobAcl"))
      .withColumn("jobId",
        when('actionName === "changeJobAcl", 'resourceId.cast("long"))
          .otherwise('jobId.cast("long"))
      )
      .select(jobs_statusCols: _*)
      .withColumn("existing_cluster_id", coalesce('existing_cluster_id, get_json_object('new_settings, "$.existing_cluster_id")))
      .withColumn("new_cluster",
        when('actionName.isin("reset", "update"), get_json_object('new_settings, "$.new_cluster"))
          .otherwise('new_cluster)
      )
      .withColumn("job_type", when('job_type.isNull, last('job_type, true).over(lastJobStatus)).otherwise('job_type))
      .withColumn("schedule", when('schedule.isNull, last('schedule, true).over(lastJobStatus)).otherwise('schedule))
      .withColumn("timeout_seconds", when('timeout_seconds.isNull, last('timeout_seconds, true).over(lastJobStatus)).otherwise('timeout_seconds))
      .withColumn("notebook_path", when('notebook_path.isNull, last('notebook_path, true).over(lastJobStatus)).otherwise('notebook_path))
      .withColumn("jobName", when('jobName.isNull, last('jobName, true).over(lastJobStatus)).otherwise('jobName))
      .withColumn("created_by", when('actionName === "create", $"userIdentity.email"))
      .withColumn("created_by", last('created_by, true).over(lastJobStatus))
      .withColumn("created_ts", when('actionName === "create", 'timestamp))
      .withColumn("created_ts", last('created_ts, true).over(lastJobStatus))
      .withColumn("deleted_by", when('actionName === "delete", $"userIdentity.email"))
      .withColumn("deleted_by", last('deleted_by, true).over(lastJobStatusUnbound))
      .withColumn("deleted_ts", when('actionName === "delete", 'timestamp))
      .withColumn("deleted_ts", last('deleted_ts, true).over(lastJobStatusUnbound))
      .withColumn("last_edited_by", when('actionName.isin("update", "reset"), $"userIdentity.email"))
      .withColumn("last_edited_by", last('last_edited_by, true).over(lastJobStatus))
      .withColumn("last_edited_ts", when('actionName.isin("update", "reset"), 'timestamp))
      .withColumn("last_edited_ts", last('last_edited_ts, true).over(lastJobStatus))
      .drop("userIdentity")

    // TODO -- during gold build, new cluster needs to use modifyStruct and structToMap funcs to cleanse
    //    val baseJobStatus = SchemaTools.scrubSchema(spark.table("overwatch.job_status_silver"))
    //    private val changeInventory = Map[String, Column](
    //      "new_cluster.custom_tags" -> SchemaTools.structToMap(baseJobStatus, "new_cluster.custom_tags"),
    //      "new_cluster.spark_conf" -> SchemaTools.structToMap(baseJobStatus, "new_cluster.spark_conf"),
    //      "new_cluster.spark_env_vars" -> SchemaTools.structToMap(baseJobStatus, "new_cluster.spark_env_vars")
    //    )

    jobStatusBase
      .withColumn("jobCluster",
        last(
          when('existing_cluster_id.isNull && 'new_cluster.isNull, lit(null))
            .otherwise(jobCluster),
          true
        ).over(lastJobStatus)
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
    * @param df
    * @return
    */
  protected def dbJobRunsSummary(completeAuditTable: DataFrame,
                                 clusterSpec: PipelineTable,
                                 clusterSnapshot: PipelineTable,
                                 jobsStatus: PipelineTable,
                                 jobsSnapshot: PipelineTable,
                                 etlStartTime: Column,
                                 etlStartEpochMS: Long)(df: DataFrame): DataFrame = {

    // TODO -- limit the lookback period to about 7 days -- no job should run for more than 7 days except
    //  streaming jobs. This is only needed to improve performance if needed.
    val repartitionCount = spark.conf.get("spark.sql.shuffle.partitions").toInt * 2

    val rawAuditExclusions = StructType(Seq(
      StructField("requestParams", StructType(Seq(
        StructField("organization_id", NullType, nullable = true),
        StructField("orgId", NullType, nullable = true)
      )), nullable = true)
    ))

    val jobsAuditComplete = completeAuditTable
      .verifyMinimumSchema(rawAuditExclusions) // remove interference from source
      .filter('serviceName === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .repartition(repartitionCount)
      .cache()
    jobsAuditComplete.count()

    val jobsAuditIncremental = getJobsBase(df)

    // Completes must be >= etlStartTime as it is the driver endpoint
    // All joiners to Completes may be from the past up to N days as defined in the incremental df
    // Identify all completed jobs in scope for this overwatch run
    val allCompletes = jobsAuditIncremental
      .filter('date >= etlStartTime.cast("date") && 'timestamp >= lit(etlStartEpochMS))
      .filter('actionName.isin("runSucceeded", "runFailed"))
      .select(
        'serviceName, 'actionName,
        'organization_id,
        'date,
        'runId.cast("int"),
        'jobId.cast("int"),
        'idInJob, 'jobClusterType, 'jobTaskType, 'jobTerminalState,
        'jobTriggerType,
        'requestId.alias("completionRequestID"),
        'response.alias("completionResponse"),
        'timestamp.alias("completionTime")
      )

    // CancelRequests are still lookups from the driver "complete" as a cancel request is a request and still
    // results in a runFailed after the cancellation
    // Identify all cancelled jobs in scope for this overwatch run
    val allCancellations = jobsAuditIncremental
      .filter('actionName.isin("cancel"))
      .select(
        'organization_id, 'date,
        'run_id.cast("int").alias("runId"),
        'requestId.alias("cancellationRequestId"),
        'response.alias("cancellationResponse"),
        'sessionId.alias("cancellationSessionId"),
        'sourceIPAddress.alias("cancellationSourceIP"),
        'timestamp.alias("cancellationTime"),
        'userAgent.alias("cancelledUserAgent"),
        'userIdentity.alias("cancelledBy")
      ).filter('runId.isNotNull)

    // DF for jobs launched with actionName == "runNow"
    val runNowStart = jobsAuditIncremental
      .filter('actionName.isin("runNow"))
      .select(
        'organization_id, 'date,
        get_json_object($"response.result", "$.run_id").cast("int").alias("runId"),
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
        lit(null).cast("long").alias("timeout_seconds"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      ).filter('runId.isNotNull)

    // DF for jobs launched with actionName == "submitRun"
    val runSubmitStart = jobsAuditIncremental
      .filter('actionName.isin("submitRun"))
      .select(
        'organization_id, 'date,
        get_json_object($"response.result", "$.run_id").cast("int").alias("runId"),
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
        'timeout_seconds.cast("long").alias("timeout_seconds"),
        'sourceIPAddress.alias("submitSourceIP"),
        'sessionId.alias("submitSessionId"),
        'requestId.alias("submitRequestID"),
        'response.alias("submitResponse"),
        'userAgent.alias("submitUserAgent"),
        'userIdentity.alias("submittedBy")
      ).filter('runId.isNotNull)

    // DF to pull unify differing schemas from runNow and submitRun and pull all job launches into one DF
    val allSubmissions = runNowStart
      .unionByName(runSubmitStart)

    // Find the corresponding runStart action for the completed jobs
    val runStarts = jobsAuditIncremental
      .filter('actionName.isin("runStart"))
      .select(
        'organization_id, 'date,
        'runId,
        'timestamp.alias("startTime"),
        'requestId.alias("startRequestID")
      )

    // Lookup to populate the clusterID/clusterName where missing from jobs
    lazy val clusterSpecLookup = clusterSpec.asDF
      .select('organization_id, 'timestamp, 'cluster_name, 'cluster_id.alias("clusterId"))
      .filter('clusterId.isNotNull && 'cluster_name.isNotNull)

    // Lookup to populate the clusterID/clusterName where missing from jobs
    lazy val clusterSnapLookup = clusterSnapshot.asDF
      .withColumn("timestamp", unix_timestamp('Pipeline_SnapTS))
      .select('organization_id, 'timestamp, 'cluster_name, 'cluster_id.alias("clusterId"))
      .filter('clusterId.isNotNull && 'cluster_name.isNotNull)

    // Lookup to populate the existing_cluster_id where missing from jobs -- it can be derived from name
    lazy val existingClusterLookup = jobsStatus.asDF
      .select('organization_id, 'timestamp, 'jobId, 'existing_cluster_id)
      .filter('existing_cluster_id.isNotNull)

    // tmpFix -- TODO -- build dfFunc df.selectWNulls
    // Lookup to populate the existing_cluster_id where missing from jobs -- it can be derived from name
    lazy val existingClusterLookup2 = if (SchemaTools.nestedColExists(jobsSnapshot.asDF, "settings.existing_cluster_id")) {
      jobsSnapshot.asDF
      .select('organization_id, 'job_id.alias("jobId"), $"settings.existing_cluster_id",
        'created_time.alias("timestamp"))
    } else {
      jobsSnapshot.asDF
        .select('organization_id, 'job_id.alias("jobId"),
          'created_time.alias("timestamp"))
    }.filter('existing_cluster_id.isNotNull)

    // Ensure the lookups have data -- this will be improved when the module unification occurs during the next
    // scheduled refactor
    val clusterLookups = ArrayBuffer[DataFrame]()
    if (clusterSpec.exists) clusterLookups.append(clusterSpecLookup)
    if (clusterSnapshot.exists) clusterLookups.append(clusterSnapLookup)
    val jobStatusClusterLookups = ArrayBuffer[DataFrame]()
    if (jobsStatus.exists) jobStatusClusterLookups.append(existingClusterLookup)
    if (jobsSnapshot.exists) jobStatusClusterLookups.append(existingClusterLookup2)

    // TODO -- add the lag back but there's an error in the joinWithLag func atm
    // Unify each run onto a single record and structure the df
    //    val jobRunsBase = allCompletes.alias("completes")
    //      .joinWithLag(allSubmissions, Seq("runId", "organization_id", "date"), "date", joinType = "left")
    //      .joinWithLag(allCancellations, Seq("runId", "organization_id", "date"), "date", joinType = "left")
    //      .joinWithLag(runStarts, Seq("runId", "organization_id", "date"), "date", joinType = "left")

    val jobRunsBase = allCompletes
      .join(allSubmissions, Seq("runId", "organization_id"), "left")
      .join(allCancellations, Seq("runId", "organization_id"), "left")
      .join(runStarts, Seq("runId", "organization_id"), "left")
      .withColumn("jobTerminalState", when('cancellationRequestId.isNotNull, "Cancelled").otherwise('jobTerminalState)) //.columns.sorted
      .withColumn("jobRunTime", TransformFunctions.subtractTime(array_min(array('startTime, 'submissionTime)), array_max(array('completionTime, 'cancellationTime))))
      .withColumn("cluster_name", when('jobClusterType === "new", concat(lit("job-"), 'jobId, lit("-run-"), 'idInJob)).otherwise(lit(null)))
      .select(
        'runId, 'jobId, 'idInJob, 'jobRunTime, 'run_name, 'jobClusterType, 'jobTaskType, 'jobTerminalState,
        'jobTriggerType, 'new_cluster,
        'existing_cluster_id, //.alias("clusterId"),
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
      .withColumn("timestamp", $"jobRunTime.endEpochMS")

    val automatedJobRunsBase = jobRunsBase
      .filter('jobClusterType === "new")
      .withColumnRenamed("existing_cluster_id", "clusterId")

    // JobRuns with interactive clusters
    // If the lookups are present (likely will be unless is first run or modules are turned off somehow)
    // use the lookups to populate the null clusterIds and cluster_names
    val interactiveRunsWID = if (jobStatusClusterLookups.nonEmpty) {
      jobStatusClusterLookups.foldLeft(jobRunsBase.filter('jobClusterType === "existing")) {
        case (dfBuilder, lookupDF) => {
          dfBuilder
            .joinAsOf(
              lookupDF,
              Seq("organization_id", "jobId"),
              Seq('timestamp),
              Seq("existing_cluster_id"),
              rowsBefore = -1000
            )
        }
      }.withColumnRenamed("existing_cluster_id", "clusterId")
    } else jobRunsBase.withColumnRenamed("existing_cluster_id", "clusterId")

    // JobRuns using Automated clusters -- get clusterID by derived name
    val (interactiveRunsWIDAndName, automatedRunsWIDAndName) = if (clusterLookups.nonEmpty) {
      val interactiveWMeta = clusterLookups.foldLeft(interactiveRunsWID) {
        case (dfBuilder, lookupDF) => {
          dfBuilder
            .joinAsOf(
              lookupDF,
              Seq("organization_id", "clusterId"),
              Seq('timestamp),
              Seq("cluster_name"),
              rowsBefore = -1000
            )
        }
      }

      val automatedWMeta = clusterLookups.foldLeft(automatedJobRunsBase) {
        case (dfBuilder, lookupDF) => {
          dfBuilder
            .joinAsOf(
              lookupDF,
              Seq("organization_id", "cluster_name"),
              Seq('timestamp),
              Seq("clusterId"),
              rowsBefore = -1000
            )
        }
      }
      (interactiveWMeta, automatedWMeta)
    } else (interactiveRunsWID, automatedJobRunsBase)

    // Union the interactive and automated back together with the cluster ids and names
    val jobRunsWClusterIDAndName = interactiveRunsWIDAndName.unionByName(automatedRunsWIDAndName)


    val newJobsNameLookup = jobsAuditComplete
      .select(
        'organization_id, 'timestamp,
        get_json_object($"response.result", "$.job_id").alias("jobId"),
        'name.alias("jobName")
      )

    val editJobsNameLookup = jobsAuditComplete
      .filter('actionName.isin("reset", "update"))
      .select('organization_id, 'timestamp, 'job_id.alias("jobId"), get_json_object('new_settings, "$.name").alias("jobName"))

    val jobNameLookup = newJobsNameLookup.unionByName(editJobsNameLookup)

    val jobNameLookup2 = jobsSnapshot.asDF
      .select('organization_id, unix_timestamp('Pipeline_SnapTS).alias("timestamp"),
        'job_id.alias("jobId"),
        $"settings.name".alias("jobName")
      )

    val jobNameLookups = Array(jobNameLookup, jobNameLookup2)

    // Backfill the jobNames
    val jobRunsFinal = jobNameLookups.foldLeft(jobRunsWClusterIDAndName) {
      case (dfBuilder, lookupDF) => {
        dfBuilder
          .joinAsOf(
            lookupDF,
            Seq("organization_id", "jobId"),
            Seq('timestamp),
            Seq("jobName"),
            rowsBefore = -1000
          )
      }
    }.drop("timestamp") // duplicated to enable asOf Lookups, dropping to clean up
      .withColumn("endEpochMS", $"jobRunTime.endEpochMS") // for incremental downstream after job termination
      .withColumn("runId", 'runId.cast("long"))
      .withColumn("jobId", 'jobId.cast("long"))
      .withColumn("idInJob", 'idInJob.cast("long"))
      .repartition(repartitionCount) // very large skew in some cases

    jobsAuditComplete.unpersist()

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
