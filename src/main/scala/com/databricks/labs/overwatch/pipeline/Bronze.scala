package com.databricks.labs.overwatch.pipeline

import java.io.{File, PrintWriter, StringWriter}

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, ModuleStatusReport, OverwatchScope, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


class Bronze(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SparkSessionWrapper with BronzeTransforms {

  envInit()

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  lazy private val appendJobsProcess = EtlDefinition(
    workspace.getJobsDF.cache(),
    Some(Seq(collectJobsIDs())),
    append(BronzeTargets.jobsTarget),
    Module(1001, "Bronze_Jobs_Snapshot")
  )

  lazy private val appendJobRunsSnapshotProcess = EtlDefinition(
    prepJobRunsDF(config.apiEnv, config.isFirstRun),
    None,
    append(BronzeTargets.jobRunsSnapshotTarget, newDataOnly = true),
    Module(1007, "Bronze_JobRuns_Snapshot")
  )

  private val jobRunsModule = Module(1008, "Bronze_JobRuns")
  lazy private val appendJobRunsAuditFilteredProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asDF,
    Some(Seq(getNewJobRuns(
      config.apiEnv,
      config.fromTime(jobRunsModule.moduleID),
      config.pipelineSnapTime,
      config.isFirstRun
    ))),
    append(BronzeTargets.jobRunsTarget, newDataOnly = true),
    jobRunsModule
  )

  private val appendClustersModule = Module(1002, "Bronze_Clusters_Snapshot")
  lazy private val appendClustersAPIProcess = EtlDefinition(
    workspace.getClustersDF.cache(),
    Some(Seq(collectClusterIDs())),
    append(BronzeTargets.clustersSnapshotTarget),
    appendClustersModule
  )

  lazy private val appendPoolsProcess = EtlDefinition(
    workspace.getPoolsDF,
    None,
    append(BronzeTargets.poolsTarget),
    Module(1003, "Bronze_Pools")
  )

  lazy private val appendAuditLogsProcess = EtlDefinition(
    getAuditLogsDF(config.auditLogPath.get),
    Some(Seq(
      collectClusterIDs(
        config.fromTime(sparkEventLogsModule.moduleID).asColumnTS
      ))),
    append(BronzeTargets.auditLogsTarget, newDataOnly = true),
    Module(1004, "Bronze_AuditLogs")
  )

  private val appendClusterEventLogsModule = Module(1005, "Bronze_ClusterEventLogs")
  lazy private val appendClusterEventLogsProcess = EtlDefinition(
    prepClusterEventLogs(
      config.fromTime(appendClusterEventLogsModule.moduleID).asUnixTimeMilli,
      config.pipelineSnapTime.asUnixTimeMilli,
      config.apiEnv
    ),
    None,
    append(BronzeTargets.clusterEventsTarget, newDataOnly = true),
    appendClusterEventLogsModule
  )

  private def getEventLogPathsSourceDF: DataFrame = {
    if (config.overwatchScope.contains(OverwatchScope.audit)) BronzeTargets.auditLogsTarget.asDF
    else BronzeTargets.clustersSnapshotTarget.asDF.filter('Pipeline_SnapTS === config.pipelineSnapTime.asColumnTS)
  }

  private val sparkEventLogsModule = Module(1006, "Bronze_EventLogs")
  lazy private val appendSparkEventLogsProcess = EtlDefinition(
    getEventLogPathsSourceDF,
    Some(Seq(
      collectEventLogPaths(
        config.fromTime(sparkEventLogsModule.moduleID).asColumnTS,
        config.isFirstRun,
        config.overwatchScope
      ),
      generateEventLogsDF(config.badRecordsPath, BronzeTargets.sparkEventLogsTarget)
    )),
    append(BronzeTargets.sparkEventLogsTarget),
    sparkEventLogsModule
  )

  // TODO -- Is there a better way to run this? .map case...does not preserve necessary ordering of events
  def run(): Unit = {

//    try {
//      if (config.isFirstRun || !spark.catalog.tableExists(config.databaseName, BronzeTargets.cloudMachineDetail.name)) {
//        // TODO -- Get data from local resources folder
//        val cloudProviderInstanceLookup = if (config.isDBConnect) {
//          spark.read.format("parquet").load("/tmp/tomes/overwatch/ec2_details_tbl")
//            .withColumn("DriverNodeType", 'API_Name)
//            .withColumn("WorkerNodeType", 'API_Name)
//        } else {
//          if (config.cloudProvider == "aws") {
//            val ec2LookupPath = getClass.getResource("/ec2_details_tbl").toString
//            val x = spark.read.format("parquet").load(ec2LookupPath)
//            x.show(20, false)
//            spark.read.format("parquet").load("ec2_details_tbl")
//              .withColumn("DriverNodeType", 'API_Name)
//              .withColumn("WorkerNodeType", 'API_Name)
//          } else {
//            // TODO -- Build this lookup table for azure
//            Seq("TO BUILD OUT").toDF("NotImplemented")
//            //        spark.read.format("parquet").load("azure_instance_details_tbl")
//          }
//        }
//        database.write(cloudProviderInstanceLookup, BronzeTargets.cloudMachineDetail)
//      }
//    } catch {
//      case e: Throwable => {
//        println("FAILED: Could not load cloud provider's instance metadata. This will need to be created before " +
//          "running silver.")
//        logger.log(Level.ERROR, "Failed to create cloudMachineDetail Target", e)
//      }
//    }

    val reports = ArrayBuffer[ModuleStatusReport]()

    if (config.overwatchScope.contains(OverwatchScope.audit)) {
      reports.append(appendAuditLogsProcess.process())

      /** Current cluster snapshot is important because cluster spec details are only available from audit logs
       * during create/edit events. Thus all existing clusters created/edited last before the audit logs were
       * enabled will be missing all info. This is especially important for overwatch early stages
       */
      if (config.overwatchScope.contains(OverwatchScope.clusters)) reports.append(appendClustersAPIProcess.process())
      // TODO -- keeping these api events with audit since there appears to be more granular data available
      //  from the api than from audit -- VERIFY
      if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) reports.append(appendClusterEventLogsProcess.process())
      if (config.overwatchScope.contains(OverwatchScope.jobs)) reports.append(appendJobsProcess.process())
      if (config.overwatchScope.contains(OverwatchScope.jobRuns)) reports.append(appendJobRunsAuditFilteredProcess.process())

      if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
        reports.append(appendSparkEventLogsProcess.process())
      }
      //      if (config.overwatchScope.contains(OverwatchScope.pools)) reports.append(appendPoolsProcess.process())
    } else {
      if (config.overwatchScope.contains(OverwatchScope.jobs)) {
        reports.append(appendJobsProcess.process())
      }
      // TODO -- determine if jobRuns API pull has diff/more data than audit -- perhaps create runs from API
      if (config.overwatchScope.contains(OverwatchScope.jobRuns)) {
        reports.append(appendJobRunsSnapshotProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.clusters)) {
        reports.append(appendClustersAPIProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.clusterEvents)) {
        reports.append(appendClusterEventLogsProcess.process())
      }
      if (config.overwatchScope.contains(OverwatchScope.sparkEvents)) {
        reports.append(appendSparkEventLogsProcess.process())
      }
      //      if (config.overwatchScope.contains(OverwatchScope.pools)) {
      //        reports.append(appendPoolsProcess.process())
      //      }
    }

    //    DOES NOT PRESERVER NECESSARY ORDERING
    //    val reports = Config.overwatchScope.map {
    //      case OverwatchScope.jobs => appendJobs
    //      case OverwatchScope.jobRuns => appendJobRuns(jobIDs)
    //      case OverwatchScope.clusters => appendClusters
    //      case OverwatchScope.clusterEvents => appendClusterEventLogs(clusterIDs)
    //      case OverwatchScope.pools => appendPools
    //      case OverwatchScope.audit => appendAuditLogs
    //      case OverwatchScope.sparkEvents => appendEventLogs
    //    }

    finalizeRun(reports.toArray)


  }

}

object Bronze {
  def apply(workspace: Workspace): Bronze = new Bronze(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}