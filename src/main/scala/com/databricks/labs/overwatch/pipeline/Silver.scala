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
  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sw = new StringWriter

  /**
   * Module sparkEvents
   * Bronze sources for spark events
   */

  // TODO -- Compare all configurations against defaults and notate non-default configs
  private lazy val sparkEventsDF: DataFrame = BronzeTargets.sparkEventLogsTarget.asDF
    .filter('Downstream_Processed === lit(false))
    .withColumn("filenameGroup", UDF.groupFilename('filename))

  private var newAuditLogsDF: DataFrame = if (config.overwatchScope.contains(OverwatchScope.audit))
    BronzeTargets.auditLogsTarget.asDF
  else null

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

  lazy private val executorAddedDF: DataFrame = sparkEventsDF
    .filter('Event === "SparkListenerExecutorAdded")
    .select('SparkContextID, 'ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"),
      'filenameGroup.alias("startFilenameGroup"))

  lazy private val executorRemovedDF: DataFrame = sparkEventsDF
    .filter('Event === "SparkListenerExecutorRemoved")
    .select('SparkContextID, 'ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"),
      'filenameGroup.alias("endFilenameGroup"))


  /**
   * Module SparkEvents
   */

  // TODO - -replace lazy val back to lazy private val when done testing
  // Todo -- no data to test yet
  //  lazy val appendJDBCSessionsProcess = EtlDefinition(
  //    getJDBCSession(Sources.serverSessionStartDF, Sources.serverSessionEndDF),
  //    None,
  //    append(Silver.jdbcSessionsTarget, newDataOnly = true),
  //    Module(2001, "SPARK_JDBC_Sessions_Raw")
  //  )
  //
  //  lazy val appendJDBCOperationsProcess = EtlDefinition(
  //    getJDBCOperation(Sources.serverOperationStartDF, Sources.serverOperationEndDF),
  //    None,
  //    append(Silver.jdbcOperationsTarget, newDataOnly = true),
  //    Module(2002, "SPARK_JDBC_Operations_Raw")
  //  )

  lazy private val appendExecutorsProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(executor())),
    append(SilverTargets.executorsTarget, newDataOnly = true),
    Module(2003, "SPARK_Executors_Raw")
  )

  // TODO -- Build Bronze
//  lazy val appendApplicationsProcess = EtlDefinition(
//    sparkEventsDF,
//    Some(Seq(enhanceApplication())),
//    append(Silver., newDataOnly = true),
//    Module(2004, "SPARK_Applications_Raw")
//  )

  lazy private val appendExecutionsProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(sqlExecutions())),
    append(SilverTargets.executionsTarget, newDataOnly = true),
    Module(2005, "SPARK_Executions_Raw")
  )

  lazy private val appendJobsProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(sparkJobs())),
    append(SilverTargets.jobsTarget, newDataOnly = true),
    Module(2006, "SPARK_Jobs_Raw")
  )

  lazy private val appendStagesProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(sparkStages())),
    append(SilverTargets.stagesTarget, newDataOnly = true),
    Module(2007, "SPARK_Stages_Raw")
  )

  lazy private val appendTasksProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(sparkTasks())),
    append(SilverTargets.tasksTarget, newDataOnly = true),
    Module(2008, "SPARK_Tasks_Raw")
  )

  lazy private val appendJobStatusProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(dbJobsStatusSummary())),
    append(SilverTargets.dbJobsStatusTarget, newDataOnly = true),
    Module(2010, "Silver_JobsStatus")
  )

  lazy private val appendJobRunsProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(
      dbJobRunsSummary(
        SilverTargets.clustersSpecTarget,
        SilverTargets.dbJobsStatusTarget,
        BronzeTargets.jobsSnapshotTarget
      )
    )),
    append(SilverTargets.dbJobRunsTarget, newDataOnly = true),
    Module(2011, "Silver_JobsRuns")
  )

  lazy private val appendClusterSpecProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(buildClusterSpec(BronzeTargets.clustersSnapshotTarget))),
    append(SilverTargets.clustersSpecTarget, newDataOnly = true),
    Module(2014, "Silver_ClusterSpec")
  )


  lazy private val appendClusterStatusProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(
      buildClusterStatus(
        SilverTargets.clustersSpecTarget,
        BronzeTargets.clustersSnapshotTarget,
        BronzeTargets.cloudMachineDetail
      )
    )),
    append(SilverTargets.clustersStatusTarget, newDataOnly = true),
    Module(2015, "Silver_ClusterStatus")
  )

  lazy private val appendUserLoginsProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(userLogins())),
    append(SilverTargets.userLoginsTarget, newDataOnly = true),
    Module(2016, "Silver_UserLogins")
  )

  lazy private val appendNewAccountsProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(newAccounts())),
    append(SilverTargets.newAccountsTarget, newDataOnly = true),
    Module(2017, "Silver_NewAccounts")
  )

  lazy private val appendNotebookSummaryProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(notebookSummary())),
    append(SilverTargets.notebookStatusTarget, newDataOnly = true),
    Module(2018, "Silver_Notebooks")
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

  private def processSparkEvents: Array[ModuleStatusReport] = {

    val sparkEventsSilverReports = Array(
//      appendJDBCSessionsProcess.process(),
//      appendJDBCOperationsProcess.process(),
      appendExecutorsProcess.process(),
//      appendApplicationsProcess.process(),
      appendExecutionsProcess.process(),
      appendJobsProcess.process(),
      appendStagesProcess.process(),
      appendTasksProcess.process()
    )
    updateSparkEventsPipelineState(BronzeTargets.sparkEventLogsTarget)

    sparkEventsSilverReports
  }

  def run(): Boolean = {

    // TODO -- see which transforms are possible without audit and rebuild for no-audit
    //  CURRENTLY -- audit is required for silver
    val reports = ArrayBuffer[ModuleStatusReport]()
    val scope = config.overwatchScope

    if (scope.contains(OverwatchScope.audit)) {

      reports.append(appendUserLoginsProcess.process())
      reports.append(appendNewAccountsProcess.process())

      if (scope.contains(OverwatchScope.sparkEvents))
        processSparkEvents.foreach(sparkReport => reports.append(sparkReport))
      if (scope.contains(OverwatchScope.clusters))
        reports.append(appendClusterSpecProcess.process())
      reports.append(appendClusterStatusProcess.process())
      if (scope.contains(OverwatchScope.jobs))
        reports.append(appendJobStatusProcess.process())
      if (scope.contains(OverwatchScope.jobRuns))
        reports.append(appendJobRunsProcess.process())
      if (scope.contains(OverwatchScope.notebooks))
        reports.append(appendNotebookSummaryProcess.process())
    } else {
      println(s"ERROR: Currently Silver is only supported with audit logs. Please enable audit logs and " +
        s"ensure the initial run has completed in Bronze")
    }

//    config.overwatchScope.foreach {
//      case OverwatchScope.sparkEvents =>
//        processSparkEvents.foreach(sparkReport => reports.append(sparkReport))
//      case OverwatchScope.jobs => reports.append(appendJobStatusProcess.process())
//      case OverwatchScope.clusters => {
//        reports.append(appendClusterSpecProcess.process())
//        reports.append(appendClusterStatusProcess.process())
//      }
//      case OverwatchScope.notebooks => reports.append(appendNotebookSummaryProcess.process())
////      case OverwatchScope.pools =>
//      case OverwatchScope.audit =>
////        val auditModuleIDs = Array(2010, 2014, 2016, 2017)
////        cacheAuditLogs(auditModuleIDs)
//        reports.append(appendUserLoginsProcess.process())
//        reports.append(appendNewAccountsProcess.process())
//        // todo -- create notebook ETL
//        // todo -- create pools history from audit log
//      case _ => ""
//    }
    finalizeRun(reports.toArray)
    true
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}
