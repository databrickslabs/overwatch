package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.{Level, Logger}

class Silver(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SilverTransforms {

  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

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

  private val executorsModule = Module(2003, "Silver_SPARK_Executors", this, Array(1006))
  lazy private val appendExecutorsProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(executorsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(executor())),
    append(SilverTargets.executorsTarget)
  )

  // TODO -- Build Bronze
  //  lazy val appendApplicationsProcess = EtlDefinition(
  //    sparkEventsDF,
  //    Some(Seq(enhanceApplication())),
  //    append(Silver.),
  //    Module(2004, "SPARK_Applications_Raw")
  //  )

  private val executionsModule = Module(2005, "Silver_SPARK_Executions", this, Array(1006))
  lazy private val appendExecutionsProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(executionsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sqlExecutions())),
    append(SilverTargets.executionsTarget)
  )

  private val sparkJobsModule = Module(2006, "Silver_SPARK_Jobs", this, Array(1006))
  lazy private val appendSparkJobsProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(sparkJobsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sparkJobs())),
    append(SilverTargets.jobsTarget)
  )

  private val sparkStagesModule = Module(2007, "Silver_SPARK_Stages", this, Array(1006))
  lazy private val appendSparkStagesProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(sparkStagesModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sparkStages())),
    append(SilverTargets.stagesTarget)
  )

  private val sparkTasksModule = Module(2008, "Silver_SPARK_Tasks", this, Array(1006))
  lazy private val appendSparkTasksProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(sparkTasksModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sparkTasks())),
    append(SilverTargets.tasksTarget)
  )

  private val jobStatusModule = Module(2010, "Silver_JobsStatus", this, Array(1004))
  lazy private val appendJobStatusProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobStatusModule, auditLogsIncrementalCols),
    Some(Seq(dbJobsStatusSummary())),
    append(SilverTargets.dbJobsStatusTarget)
  )

  private val jobRunsModule = Module(2011, "Silver_JobsRuns", this, Array(1004, 2010, 2014))
  lazy private val appendJobRunsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobRunsModule, auditLogsIncrementalCols, 2),
    Some(Seq(
      dbJobRunsSummary(
        BronzeTargets.auditLogsTarget.withMinimumSchemaEnforcement.asDF,
        SilverTargets.clustersSpecTarget,
        BronzeTargets.clustersSnapshotTarget,
        SilverTargets.dbJobsStatusTarget,
        BronzeTargets.jobsSnapshotTarget,
        jobRunsModule.fromTime.asColumnTS,
        jobRunsModule.fromTime.asUnixTimeMilli
      )
    )),
    append(SilverTargets.dbJobRunsTarget)
  )

  private val clusterSpecModule = Module(2014, "Silver_ClusterSpec", this, Array(1004))
  lazy private val appendClusterSpecProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(clusterSpecModule, auditLogsIncrementalCols),
    Some(Seq(
      buildClusterSpec(
        BronzeTargets.clustersSnapshotTarget,
        BronzeTargets.auditLogsTarget
      ))),
    append(SilverTargets.clustersSpecTarget)
  )

  private val accountLoginsModule = Module(2016, "Silver_AccountLogins", this, Array(1004))
  lazy private val appendAccountLoginsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(accountLoginsModule, auditLogsIncrementalCols),
    Some(Seq(accountLogins())),
    append(SilverTargets.accountLoginTarget)
  )

  private val modifiedAccountsModule = Module(2017, "Silver_ModifiedAccounts", this, Array(1004))
  lazy private val appendModifiedAccountsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(modifiedAccountsModule, auditLogsIncrementalCols),
    Some(Seq(accountMods())),
    append(SilverTargets.accountModTarget)
  )

  private val notebookSummaryModule = Module(2018, "Silver_Notebooks", this, Array(1004))
  lazy private val appendNotebookSummaryProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(notebookSummaryModule, auditLogsIncrementalCols),
    Some(Seq(notebookSummary())),
    append(SilverTargets.notebookStatusTarget)
  )

  private def processSparkEvents(): Unit = {

    executorsModule.execute(appendExecutorsProcess)
    executionsModule.execute(appendExecutionsProcess)
    sparkJobsModule.execute(appendSparkJobsProcess)
    sparkStagesModule.execute(appendSparkStagesProcess)
    sparkTasksModule.execute(appendSparkTasksProcess)

  }

  private def executeModules(): Unit = {
    config.overwatchScope.foreach {
      case OverwatchScope.accounts => {
        accountLoginsModule.execute(appendAccountLoginsProcess)
        modifiedAccountsModule.execute(appendModifiedAccountsProcess)
      }
      case OverwatchScope.notebooks => notebookSummaryModule.execute(appendNotebookSummaryProcess)
      case OverwatchScope.clusters => clusterSpecModule.execute(appendClusterSpecProcess)
      case OverwatchScope.sparkEvents => {
        processSparkEvents()
      }
      case OverwatchScope.jobs => {
        jobStatusModule.execute(appendJobStatusProcess)
        jobRunsModule.execute(appendJobRunsProcess)
      }
    }
  }

  def run(): Pipeline = {

    restoreSparkConf()
    executeModules()
    // TODO -- see which transforms are possible without audit and rebuild for no-audit
    //  CURRENTLY -- audit is required for silver
//    val scope = config.overwatchScope
//
//    if (scope.contains(OverwatchScope.accounts)) {
//      appendAccountLoginsProcess.process()
//      appendModifiedAccountsProcess.process()
//    }
//
//    if (scope.contains(OverwatchScope.clusters)) {
//      appendClusterSpecProcess.process()
//    }
//
//    if (scope.contains(OverwatchScope.jobs)) {
//      appendJobStatusProcess.process()
//      appendJobRunsProcess.process()
//    }
//
//    if (scope.contains(OverwatchScope.notebooks)) {
//      appendNotebookSummaryProcess.process()
//    }
//
//    if (scope.contains(OverwatchScope.sparkEvents)) {
//      processSparkEvents()
//    }
//
//    initiatePostProcessing()
    this // to be used as fail switch later if necessary
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}
