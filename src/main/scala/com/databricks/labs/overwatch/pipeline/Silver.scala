package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.databricks.labs.overwatch.utils.Helpers._
import com.databricks.labs.overwatch.utils.SchemaTools

class Silver extends Pipeline with Transforms with SparkSessionWrapper{
  import spark.implicits._



//  val eventsDFRaw = spark.read.json("/cluster-logs/*/eventlog_example/*/*/eventlog_example") //.drop("Classpath Entries", "Properties", "Spark Properties", "System Properties")
//  val eventsDF = spark.createDataFrame(eventsDFRaw.rdd, SchemaTools.sanitizeSchema(eventsDFRaw.schema).asInstanceOf[StructType]).withColumn("filename", input_file_name).repartition(32).cache
//  eventsDF.count

  // Top Down

  // Builders
  val serverSessionStartDF = sparkEventsDF.filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionCreated")
    .select('ip, 'sessionId, 'startTime, 'userName, 'filename)

  val serverSessionEndDF = sparkEventsDF.filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionClosed")
    .select('sessionId, 'finishTime, 'filename)

  // Server Operation --> start -> parsed -> finish -> closed
  // Parsed adds string execution plan
  // Finish adds finishTime
  val serverOperationStartDF = sparkEventsDF.filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationStart")
    .select('groupId, 'id, 'sessionId, 'startTime, 'statement, 'userName, 'filename)

  val serverOperationEndDF = sparkEventsDF.filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationClosed")
    .select('id, 'closeTime, 'filename)


  // Finals
  val serverSessionDF = serverSessionStartDF.join(serverSessionEndDF, Seq("sessionId", "filename"))
    .withColumn("ServerSessionRunTime", SubtractTime('startTime, 'finishTime)).drop("startTime", "finishTime")

  val serverOperationDF = serverOperationStartDF.join(serverOperationEndDF, Seq("id"))
    .withColumn("ServerOperationRunTime", SubtractTime('startTime, 'closeTime)).drop("startTime", "finishTime")


  def buildMasterEvents(): Unit = {

    // Builders

    val sqlExecStartDF = sparkEventsDF.filter('Event.isin("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"))
      .select('description, 'details, 'executionId, 'physicalPlanDescription, 'sparkPlanInfo, 'time.alias("SqlExecStartTime"), 'filename)

    val sqlExecEndDF = sparkEventsDF.filter('Event.isin("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"))
      .select('executionId, 'time.alias("SqlExecEndTime"), 'filename)

    val stageStartDF = sparkEventsDF.filter('Event === "SparkListenerStageSubmitted")
      .select(
        $"Properties.sparkappid".alias("AppID"), $"Properties.sparksqlexecutionid".alias("StageExecutionID"),
        $"StageInfo.StageID", $"StageInfo.SubmissionTime", $"StageInfo.StageAttemptID", 'StageInfo.alias("StageStartInfo"),
        'Properties.alias("PropertiesAtStageStart"), 'filename
      )

    val stageEndDF = sparkEventsDF.filter('Event === "SparkListenerStageCompleted")
      .select(
        $"StageInfo.StageID", $"StageInfo.StageAttemptID", $"StageInfo.SubmissionTime", $"StageInfo.CompletionTime",
        'StageInfo.alias("StageEndInfo"), 'filename
      )

    val taskStartEvents = sparkEventsDF.filter('Event === "SparkListenerTaskStart")
      .select(
        'StageID, 'StageAttemptID, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID", $"TaskInfo.Attempt".alias("TaskAttempt"),
        $"TaskInfo.Host", $"TaskInfo.LaunchTime", 'TaskInfo.alias("TaskStartInfo"), 'filename
      )

    val taskEndEvents = sparkEventsDF.filter('Event === "SparkListenerTaskEnd")
      .select(
        'StageID, 'StageAttemptID, 'TaskEndReason, $"TaskInfo.TaskID", $"TaskInfo.ExecutorID", $"TaskInfo.Attempt".alias("TaskAttempt"),
        $"TaskInfo.Host", $"TaskInfo.LaunchTime", $"TaskInfo.FinishTime", 'TaskInfo.alias("TaskEndInfo"), 'TaskMetrics, 'TaskType, 'filename
      )

    // Finals
    val appDF = sparkEventsDF.filter('Event === "SparkListenerApplicationStart")
      .select('AppID, 'AppName, from_unixtime('Timestamp / 1000).alias("AppStartTime"), 'filename)

    val sqlExecDF = sqlExecStartDF.join(sqlExecEndDF, Seq("ExecutionID", "filename"))
      .withColumn("SqlExecutionRunTime", SubtractTime('SqlExecStartTime, 'SqlExecEndTime)).drop("SqlExecStartTime", "SqlExecEndTime")

    val stagesDF = stageStartDF.join(stageEndDF, Seq("StageID", "SubmissionTime", "StageAttemptID", "filename"))
      .withColumn("StageInfo", struct(
        $"StageEndInfo.Accumulables", $"StageEndInfo.CompletionTime", $"StageStartInfo.Details",
        $"StageStartInfo.FailureReason", $"StageEndInfo.NumberofTasks", $"StageStartInfo.ParentIDs", $"StageStartInfo.SubmissionTime"
      )).drop("StageEndInfo", "StageStartInfo")
      .withColumn("StageRunTime", SubtractTime('SubmissionTime, 'CompletionTime)).drop("SubmissionTime", "CompletionTime")

    val accumUpdatesDF = sparkEventsDF.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates")
      .select('executionId, 'accumUpdates, 'filename)

    val tasksDF = taskStartEvents.join(
      taskEndEvents, Seq(
        "StageID", "StageAttemptID", "TaskID", "TaskAttempt", "ExecutorID", "Host", "LaunchTime", "filename"
      ))
      .withColumn("TaskInfo", struct(
        $"TaskEndInfo.Accumulables", $"TaskEndInfo.Failed", $"TaskEndInfo.FinishTime", $"TaskEndInfo.GettingResultTime",
        $"TaskEndInfo.Host", $"TaskEndInfo.Index", $"TaskEndInfo.Killed", $"TaskStartInfo.LaunchTime", $"TaskEndInfo.Locality", $"TaskEndInfo.Speculative"
      )).drop("TaskStartInfo", "TaskEndInfo")
      .withColumn("TaskRunTime", SubtractTime('LaunchTime, 'FinishTime)).drop("LaunchTime", "FinishTime")

    val eventsMasterDF = appDF
      .join(jobsDF, Seq("AppID", "filename"))
      //     .join(sqlExecDF, Seq("ExecutionID", "filename"))
      .join(stagesDF, Seq("AppID", "StageID", "filename"))
      //     .join(accumUpdatesDF, Seq(""))
      .join(tasksDF, Seq("StageID", "StageAttemptID", "filename"))
      .withColumn("PowerProperties", struct(
        $"PropertiesAtStageStart.sparkappname".alias("AppName"),
        $"PropertiesAtStageStart.sparkdatabricksapiurl".alias("WorkspaceURL"),
        $"PropertiesAtStageStart.sparkdatabrickscloudProvider".alias("CloudProvider"),
        struct(
          $"PropertiesAtStageStart.sparkdatabricksclusterSource".alias("ClusterSource"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsautoTerminationMinutes".alias("AutoTerminationMinutes"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterAllTags".alias("ClusterTags"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterAvailability".alias("CluserAvailability"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterId".alias("ClusterID"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterInstancePoolId".alias("InstancePoolID"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterMaxWorkers".alias("MaxWorkers"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterMinWorkers".alias("MinWorkers"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterName".alias("Name"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterOwnerUserId".alias("OwnerUserID"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterScalingType".alias("ScalingType"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterSpotBidPricePercent".alias("SpotBidPricePercent"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterTargetWorkers".alias("TargetWorkers"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsclusterWorkers".alias("ActualWorkers"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagscontainerZoneId".alias("ZoneID"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsdataPlaneRegion".alias("Region"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagsdriverNodeType".alias("DriverNodeType"),
          $"PropertiesAtStageStart.sparkdatabricksworkerNodeTypeId".alias("WorkerNodeType"),
          $"PropertiesAtStageStart.sparkdatabricksclusterUsageTagssparkVersion".alias("SparkVersion")
        ).alias("ClusterDetails"),
        $"PropertiesAtStageStart.sparkdatabricksnotebookid".alias("NotebookID"),
        $"PropertiesAtStageStart.sparkdatabricksnotebookpath".alias("NotebookPath"),
        $"PropertiesAtStageStart.sparkdatabrickssparkContextId".alias("SparkContextID"),
        $"PropertiesAtStageStart.sparkdriverhost".alias("DriverHostIP"),
        $"PropertiesAtStageStart.sparkdrivermaxResultSize".alias("DriverMaxResults"),
        $"PropertiesAtStageStart.sparkexecutorid".alias("ExecutorID"),
        $"PropertiesAtStageStart.sparkexecutormemory".alias("ExecutorMemory"),
        $"PropertiesAtStageStart.sparksqlexecutionid".alias("ExecutionID"),
        $"PropertiesAtStageStart.sparksqlexecutionparent".alias("ExecutionParent"),
        $"PropertiesAtStageStart.sparksqlshufflepartitions".alias("ShufflePartitions"),
        $"PropertiesAtStageStart.user".alias("UserEmail"),
        $"PropertiesAtStageStart.userID".alias("UserID")
      ))

  }

}

object Silver {
  def apply(workspace: Workspace, database: Database): Silver = new Silver()
    .setWorkspace(workspace).setDatabase(database)

}
