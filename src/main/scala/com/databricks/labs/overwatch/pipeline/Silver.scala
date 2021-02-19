package com.databricks.labs.overwatch.pipeline

import java.io.StringWriter
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, IncrementalFilter, Module, ModuleStatusReport, NoNewDataException, OverwatchScope, SparkSessionWrapper}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import TransformFunctions._

import scala.collection.mutable.ArrayBuffer

class Silver(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SilverTransforms {

  envInit()

  import spark.implicits._

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

  private val executorsModule = Module(2003, "SPARK_Executors_Raw")
  lazy private val appendExecutorsProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(executorsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
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
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(executionsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sqlExecutions())),
    append(SilverTargets.executionsTarget),
    executionsModule
  )

  private val jobsModule = Module(2006, "SPARK_Jobs_Raw")
  lazy private val appendJobsProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(jobsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sparkJobs())),
    append(SilverTargets.jobsTarget),
    jobsModule
  )

  private val stagesModule = Module(2007, "SPARK_Stages_Raw")
  lazy private val appendStagesProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(stagesModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sparkStages())),
    append(SilverTargets.stagesTarget),
    stagesModule
  )

  private val tasksModule = Module(2008, "SPARK_Tasks_Raw")
  lazy private val appendTasksProcess = EtlDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(tasksModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Some(Seq(sparkTasks())),
    append(SilverTargets.tasksTarget),
    tasksModule
  )

  private val jobStatusModule = Module(2010, "Silver_JobsStatus")
  lazy private val appendJobStatusProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobStatusModule, auditLogsIncrementalCols),
    Some(Seq(dbJobsStatusSummary())),
    append(SilverTargets.dbJobsStatusTarget),
    jobStatusModule
  )

  private val jobRunsModule = Module(2011, "Silver_JobsRuns")
  lazy private val appendJobRunsProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobRunsModule, auditLogsIncrementalCols, 2),
    Some(Seq(
      dbJobRunsSummary(
        BronzeTargets.auditLogsTarget.withMinimumSchemaEnforcement.asDF,
        SilverTargets.clustersSpecTarget,
        BronzeTargets.clustersSnapshotTarget,
        SilverTargets.dbJobsStatusTarget,
        BronzeTargets.jobsSnapshotTarget,
        config.fromTime(jobRunsModule.moduleID).asColumnTS,
        config.fromTime(jobRunsModule.moduleID).asUnixTimeMilli
      )
    )),
    append(SilverTargets.dbJobRunsTarget),
    jobRunsModule
  )

  private val clusterSpecModule = Module(2014, "Silver_ClusterSpec")
  lazy private val appendClusterSpecProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(clusterSpecModule, auditLogsIncrementalCols),
    Some(Seq(
      buildClusterSpec(
        BronzeTargets.clustersSnapshotTarget,
        BronzeTargets.auditLogsTarget
      ))),
    append(SilverTargets.clustersSpecTarget),
    clusterSpecModule
  )

  private val accountLoginsModule = Module(2016, "Silver_AccountLogins")
  lazy private val appendAccountLoginsProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(accountLoginsModule, auditLogsIncrementalCols),
    Some(Seq(accountLogins())),
    append(SilverTargets.accountLoginTarget),
    accountLoginsModule
  )

  private val modifiedAccountsModule = Module(2017, "Silver_ModifiedAccounts")
  lazy private val appendModifiedAccountsProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(modifiedAccountsModule, auditLogsIncrementalCols),
    Some(Seq(accountMods())),
    append(SilverTargets.accountModTarget),
    modifiedAccountsModule
  )

  private val notebookSummaryModule = Module(2018, "Silver_Notebooks")
  lazy private val appendNotebookSummaryProcess = EtlDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(notebookSummaryModule, auditLogsIncrementalCols),
    Some(Seq(notebookSummary())),
    append(SilverTargets.notebookStatusTarget),
    notebookSummaryModule
  )

  private def processSparkEvents(): Unit = {

    //      appendJDBCSessionsProcess.process(),
    //      appendJDBCOperationsProcess.process(),
    appendExecutorsProcess.process()
    //      appendApplicationsProcess.process(),
    appendExecutionsProcess.process()
    appendJobsProcess.process()
    appendStagesProcess.process()
    appendTasksProcess.process()

  }

  def run(): Boolean = {

    restoreSparkConf()
    // TODO -- see which transforms are possible without audit and rebuild for no-audit
    //  CURRENTLY -- audit is required for silver
    val scope = config.overwatchScope

    if (scope.contains(OverwatchScope.accounts)) {
      try {
        appendAccountLoginsProcess.process()
        appendModifiedAccountsProcess.process()
      } catch {
        case _: FailedModuleException =>
          logger.log(Level.ERROR, "FAILED: Accounts Module")
      }
    }

    if (scope.contains(OverwatchScope.clusters)) {
      try {
        appendClusterSpecProcess.process()
      } catch {
        case _: FailedModuleException =>
          logger.log(Level.ERROR, "FAILED: Clusters Module")
      }
    }

    if (scope.contains(OverwatchScope.jobs)) {
      try {
        appendJobStatusProcess.process()
        appendJobRunsProcess.process()
      } catch {
        case _: FailedModuleException =>
          logger.log(Level.ERROR, "FAILED: Jobs Module")
      }
    }

    if (scope.contains(OverwatchScope.notebooks))
      try {
        appendNotebookSummaryProcess.process()
      } catch {
        case _: FailedModuleException =>
          logger.log(Level.ERROR, "FAILED: Notebooks Module")
      }

    if (scope.contains(OverwatchScope.sparkEvents))
      try {
        if (BronzeTargets.sparkEventLogsTarget.exists) processSparkEvents()
        else {
          val msg = s"The source data for sparkEvents module does not exist or is inaccessible. " +
            s"Please ensure cluster logging is enabled " +
            s"on at least a few clusters and you have appropriate access to the log path prefixes. Otherwise " +
            s"please disable the sparkEvents module."
          throw new NoNewDataException(msg)
        }
      } catch {
        case e: FailedModuleException =>
          logger.log(Level.ERROR, "FAILED: SparkEvents Silver Module", e)
        case e: NoNewDataException =>
          logger.log(Level.ERROR, e)
          if (config.debugFlag) println(e.getMessage)
      }

    initiatePostProcessing()
    true // to be used as fail switch later if necessary
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}
