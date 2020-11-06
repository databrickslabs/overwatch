package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Helpers

import scala.collection.mutable.ArrayBuffer

class PostProcessor {

  private val tablesToOptimize: ArrayBuffer[PipelineTable] = ArrayBuffer[PipelineTable]()

  private[overwatch] def markOptimize(table: PipelineTable): Unit = {
    tablesToOptimize.append(table)
  }

  // Todo -- Add these optimization columns to the abstract class def of Table

  def optimize(): Unit = {
//    tablesInScope.map(tbl => )
//    val zordersByTable: Map[String, Array[String]] = Map(
//      "spark_events_bronze" -> "SparkContextID, ClusterID, JobGroupID".split(", "),
//      "jobruns_bronze" -> ("job_id, run_id, start_time").split(", "),
//      "cluster_events_bronze" -> "cluster_id, timestamp".split(", ")
//    )
    // TODO -- spark_events_bronze -- put in proper rules -- hot fix due to optimization issues
    Helpers.parOptimize(tablesToOptimize.toArray.filterNot(_.name == "spark_events_bronze"), maxFileSizeMB = 128)
    Helpers.parOptimize(tablesToOptimize.toArray.filter(_.name == "spark_events_bronze"), maxFileSizeMB = 32)
  }

  // TODO -- add for columns -- might not be necessary with delta
  def analyze(): Unit = {
//    Helpers.parOptimize(tablesInScope.toArray)
//    val forColumnsByTable: Map[String, Array[String]] = Map(
//      "spark_events_bronze" -> "SparkContextID, ClusterID, JobGroupID, ExecutionID".split(", "),
//      "audit_log_bronze" -> ("actionName, requestId, serviceName, sessionId, " +
//        "timestamp, date, Pipeline_SnapTS, Overwatch_RunID").split(", "),
//      "jobs_bronze" -> ("created_time, creator_user_name, job_id, " +
//        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
//      "clusters_bronze" -> ("cluster_id, driver_node_type_id, instance_pool_id, node_type_id, " +
//        "start_time, terminated_time, Overwatch_RunID").split(", "),
//      "jobruns_bronze" -> ("job_id, original_attempt_run_id, run_id, start_time, " +
//        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
//      "pipeline_report" -> ("moduleID, moduleName, runStartTS, runEndTS, fromTS, " +
//        "untilTS, status, Pipeline_SnapTS, Overwatch_RunID").split(", "),
//      "pools_bronze" -> ("instance_pool_id, node_type_id, " +
//        "Pipeline_SnapTS, Overwatch_RunID").split(", "),
//      "cluster_events_bronze" -> ("cluster_id, timestamp, type, " +
//        "Pipeline_SnapTS, Overwatch_RunID").split(", ")
//    )
//    Helpers.computeStats(Config.databaseName, forColumnsByTable = forColumnsByTable)
  }


  // TODO - Cleanup outstanding cached objects
  def cleanupAnyDupsByKeys = ???

}
