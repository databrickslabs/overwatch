package com.databricks.labs.overwatch.pipeline

import java.io.StringWriter

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, ModuleStatusReport, OverwatchScope, SparkSessionWrapper}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class Silver(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SilverTransforms with SparkSessionWrapper {

  envInit()
  setCloudProvider(config.cloudProvider)

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sw = new StringWriter

  /**
   * Module sparkEvents
   * Bronze sources for spark events
   */

  // TODO -- Compare all configurations against defaults and notate non-default configs

  //  According to Michael -- don't use rdd cache
  //  private def cacheAuditLogs(auditModuleIDs: Array[Int]): Unit = {
  //    val minAuditTS = config.lastRunDetail.filter(run => auditModuleIDs.contains(run.moduleID)).map(_.untilTS).min
  //    val minAuditColTS = config.createTimeDetail(minAuditTS).asColumnTS
  //    newAuditLogsDF = newAuditLogsDF
  //      .filter('date >= minAuditColTS.cast("date"))
  //      .repartition(getTotalCores).cache
  //    newAuditLogsDF.count()
  //  }

  // Slack Chat
  // https://databricks.slack.com/archives/C04SZU99Q/p1588959876188200
  // Todo -- Only return filenameGroup with specific request
  // Todo -- ODBC/JDBC
  /**
   * ODBC/JDBC Sessions
   */

  //    val serverSessionStartDF: DataFrame = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionCreated")
  //      .select('SparkContextID, 'ip, 'sessionId, 'startTime, 'userName, 'filenameGroup.alias("startFilenameGroup"))
  //
  //    val serverSessionEndDF: DataFrame = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionClosed")
  //      .select('SparkContextID, 'sessionId, 'finishTime, 'filenameGroup.alias("endFilenameGroup"))
  //
  //    val serverOperationStartDF: DataFrame = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationStart")
  //      .select('SparkContextID, 'groupId, 'id, 'sessionId, 'startTime, 'statement, 'userName,
  //        'filenameGroup.alias("startFilenameGroup"))
  //
  //    val serverOperationEndDF: DataFrame = sparkEventsDF
  //      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationClosed")
  //      .select('SparkContextID, 'id, 'closeTime, 'filenameGroup.alias("endFilenameGroup"))

  //    private val serverSessionDF: DataFrame = serverSessionStartDF
  //      .join(serverSessionEndDF, Seq("SparkContextID", "sessionId"))
  //      .withColumn("ServerSessionRunTime", subtractTime('startTime, 'finishTime))
  //      .drop("startTime", "finishTime")
  //
  //    private val serverOperationDF: DataFrame = serverOperationStartDF
  //      .join(serverOperationEndDF, Seq("SparkContextID", "id"))
  //      .withColumn("ServerOperationRunTime", subtractTime('startTime, 'closeTime))
  //      .drop("startTime", "finishTime")

  /**
   * Executor
   */

//  lazy private val executorAddedDF: DataFrame = sparkEventsDF
//    .filter('Event === "SparkListenerExecutorAdded")
//    .select('SparkContextID, 'ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"),
//      'filenameGroup.alias("startFilenameGroup"))
//
//  lazy private val executorRemovedDF: DataFrame = sparkEventsDF
//    .filter('Event === "SparkListenerExecutorRemoved")
//    .select('SparkContextID, 'ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"),
//      'filenameGroup.alias("endFilenameGroup"))


  /**
   * Module SparkEvents
   */

  // TODO - -replace lazy val back to lazy private val when done testing
  // Todo -- no data to test yet
  //  lazy val appendJDBCSessionsProcess = EtlDefinition(
  //    getJDBCSession(Sources.serverSessionStartDF, Sources.serverSessionEndDF),
  //    None,
  //    append(Silver.jdbcSessionsTarget),
  //    Module(2001, "SPARK_JDBC_Sessions_Raw")
  //  )
  //
  //  lazy val appendJDBCOperationsProcess = EtlDefinition(
  //    getJDBCOperation(Sources.serverOperationStartDF, Sources.serverOperationEndDF),
  //    None,
  //    append(Silver.jdbcOperationsTarget),
  //    Module(2002, "SPARK_JDBC_Operations_Raw")
  //  )

  private val executorsModule = Module(2003, "SPARK_Executors_Raw")
  lazy private val appendExecutorsProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget.asIncrementalDF(executorsModule.moduleID),
    Some(Seq(executor())),
    append(SilverTargets.executorsTarget),
    executorsModule
  )

  // TODO -- Build Bronze
  //  lazy val appendApplicationsProcess = EtlDefinition(
  //    sparkEventsDF,
  //    Some(Seq(enhanceApplication())),
  //    append(Silver.),
  //    Module(2004, "SPARK_Applications_Raw")
  //  )

  private val executionsModule = Module(2005, "SPARK_Executions_Raw")
  lazy private val appendExecutionsProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget.asIncrementalDF(executionsModule.moduleID),
    Some(Seq(sqlExecutions())),
    append(SilverTargets.executionsTarget),
    executionsModule
  )

  private val jobsModule = Module(2006, "SPARK_Jobs_Raw")
  lazy private val appendJobsProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget.asIncrementalDF(jobsModule.moduleID),
    Some(Seq(sparkJobs())),
    append(SilverTargets.jobsTarget),
    jobsModule
  )

  private val stagesModule = Module(2007, "SPARK_Stages_Raw")
  lazy private val appendStagesProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget.asIncrementalDF(stagesModule.moduleID),
    Some(Seq(sparkStages())),
    append(SilverTargets.stagesTarget),
    stagesModule
  )

  private val tasksModule = Module(2008, "SPARK_Tasks_Raw")
  lazy private val appendTasksProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget.asIncrementalDF(tasksModule.moduleID),
    Some(Seq(sparkTasks())),
    append(SilverTargets.tasksTarget),
    tasksModule
  )

  private val jobStatusModule = Module(2010, "Silver_JobsStatus")
  lazy private val appendJobStatusProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobStatusModule.moduleID),
    Some(Seq(dbJobsStatusSummary())),
    append(SilverTargets.dbJobsStatusTarget),
    jobStatusModule
  )

  private val jobRunsModule = Module(2011, "Silver_JobsRuns")
  lazy private val appendJobRunsProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobRunsModule.moduleID),
    Some(Seq(
      dbJobRunsSummary(
        SilverTargets.clustersSpecTarget,
        SilverTargets.dbJobsStatusTarget,
        BronzeTargets.jobsSnapshotTarget
      )
    )),
    append(SilverTargets.dbJobRunsTarget),
    jobRunsModule
  )

  private val clusterSpecModule = Module(2014, "Silver_ClusterSpec")
  lazy private val appendClusterSpecProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(clusterSpecModule.moduleID),
    Some(Seq(
      buildClusterSpec(
        BronzeTargets.clustersSnapshotTarget
      ))),
    append(SilverTargets.clustersSpecTarget),
    clusterSpecModule
  )


  private val clusterStatusModule = Module(2015, "Silver_ClusterStatus")
  lazy private val appendClusterStatusProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(clusterStatusModule.moduleID),
    Some(Seq(
      buildClusterStatus(
        SilverTargets.clustersSpecTarget,
        BronzeTargets.clustersSnapshotTarget,
        BronzeTargets.cloudMachineDetail
      )
    )),
    append(SilverTargets.clustersStatusTarget),
    clusterStatusModule
  )

  private val userLoginsModule = Module(2016, "Silver_UserLogins")
  lazy private val appendUserLoginsProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(userLoginsModule.moduleID),
    Some(Seq(userLogins())),
    append(SilverTargets.userLoginsTarget),
    userLoginsModule
  )

  private val newAccountsModule = Module(2017, "Silver_NewAccounts")
  lazy private val appendNewAccountsProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(newAccountsModule.moduleID),
    Some(Seq(newAccounts())),
    append(SilverTargets.newAccountsTarget),
    newAccountsModule
  )

  private val notebookSummaryModule = Module(2018, "Silver_Notebooks")
  lazy private val appendNotebookSummaryProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(notebookSummaryModule.moduleID),
    Some(Seq(notebookSummary())),
    append(SilverTargets.notebookStatusTarget),
    notebookSummaryModule
  )

  // TODO -- temp until refactor
  private def updateSparkEventsPipelineState(eventLogsBronze: PipelineTable): Unit = {
    val updateSql =
      s"""
         |update ${eventLogsBronze.tableFullName}
         |set Downstream_Processed = true
         |where Downstream_Processed = false
         |""".stripMargin
    spark.sql(updateSql)
  }

  private def processSparkEvents(): Unit = {

    try {
      //      appendJDBCSessionsProcess.process(),
      //      appendJDBCOperationsProcess.process(),
      appendExecutorsProcess.process()
      //      appendApplicationsProcess.process(),
      appendExecutionsProcess.process()
      appendJobsProcess.process()
      appendStagesProcess.process()
      appendTasksProcess.process()
      updateSparkEventsPipelineState(BronzeTargets.sparkEventLogsTarget)
    } catch{
      case e: Throwable => {
        println(s"Failures detected in spark events processing. Failing and rolling back spark events Silver. $e")
        logger.log(Level.ERROR, s"Failed Spark Events Silver", e)
      }
    }

  }

  def run(): Boolean = {

    // TODO -- see which transforms are possible without audit and rebuild for no-audit
    //  CURRENTLY -- audit is required for silver
    val scope = config.overwatchScope

    if (scope.contains(OverwatchScope.accounts)) {
      appendUserLoginsProcess.process()
      appendNewAccountsProcess.process()
    }

    if (scope.contains(OverwatchScope.sparkEvents))
      processSparkEvents()
    if (scope.contains(OverwatchScope.clusters)) {
      appendClusterSpecProcess.process()
      appendClusterStatusProcess.process()
    }
    if (scope.contains(OverwatchScope.jobs)) {
      appendJobStatusProcess.process()
    }

    if (scope.contains(OverwatchScope.jobs)) {
      appendJobRunsProcess.process()
    }

    if (scope.contains(OverwatchScope.notebooks))
      appendNotebookSummaryProcess.process()

    initiatePostProcessing()
    true
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}
