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

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sw = new StringWriter

  /**
   * Module sparkEvents
   * Bronze sources for spark events
   */

  // TODO -- Compare all configurations against defaults and notate non-default configs
  private lazy val sparkEventsDF: DataFrame = BronzeTargets.sparkEventLogsTarget.asDF
    .drop("ClasspathEntries", "HadoopProperties", "SparkProperties", "SystemProperties", "SparkPlanInfo") // TODO - TEMP
    .withColumn("filenameGroup", UDF.groupFilename('filename))

  private var newAuditLogsDF: DataFrame = BronzeTargets.auditLogsTarget.asDF

  private def cacheAuditLogs(auditModuleIDs: Array[Int]): Unit = {
    val minAuditTS = config.lastRunDetail.filter(run => auditModuleIDs.contains(run.moduleID)).map(_.untilTS).min
    val minAuditColTS = config.createTimeDetail(minAuditTS).asColumnTS
    newAuditLogsDF = newAuditLogsDF
      .filter('date >= minAuditColTS.cast("date"))
      .repartition(getTotalCores).cache
    newAuditLogsDF.count()
  }

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

  /**
   * Executor
   */

  lazy val executorAddedDF: DataFrame = sparkEventsDF
    .filter('Event === "SparkListenerExecutorAdded")
    .select('SparkContextID, 'ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"),
      'filenameGroup.alias("startFilenameGroup"))

  lazy val executorRemovedDF: DataFrame = sparkEventsDF
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

  lazy val appendExecutorsProcess = EtlDefinition(
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

  lazy val appendExecutionsProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(sqlExecutions())),
    append(SilverTargets.executionsTarget, newDataOnly = true),
    Module(2005, "SPARK_Executions_Raw")
  )

  lazy val appendJobsProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(jobs(sparkEventsDF))),
    append(SilverTargets.jobsTarget, newDataOnly = true),
    Module(2006, "SPARK_Jobs_Raw")
  )

  lazy val appendStagesProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(stages())),
    append(SilverTargets.stagesTarget, newDataOnly = true),
    Module(2007, "SPARK_Stages_Raw")
  )

  lazy val appendTasksProcess = EtlDefinition(
    sparkEventsDF,
    Some(Seq(tasks())),
    append(SilverTargets.tasksTarget, newDataOnly = true),
    Module(2008, "SPARK_Tasks_Raw")
  )

  lazy val appendJobStatusProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(jobsStatusSummary())),
    append(SilverTargets.secJobsStatusTarget, newDataOnly = true),
    Module(2010, "SEC_JobsStatus")
  )

  lazy val appendJobRunsProcess = EtlDefinition(
    BronzeTargets.jobRunsTarget.asDF,
    None,
    append(SilverTargets.dbJobRunsTarget, newDataOnly = true),
    Module(2011, "DB_JobRuns")
  )

  lazy val appendClusterAuditSummaryProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(clustersStatusSummary())),
    append(SilverTargets.secClustersStatusTarget, newDataOnly = true),
    Module(2014, "SEC_ClusterStatusChange")
  )

  lazy val appendClusterEventsProcess = EtlDefinition(
    BronzeTargets.clusterEventsTarget.asDF,
    None,
    append(SilverTargets.dbClustersEventsTarget, newDataOnly = true),
    Module(2015, "DB_ClusterEvents")
  )

  lazy val appendUserLoginsProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(userLogins())),
    append(SilverTargets.secUserLoginsTarget, newDataOnly = true),
    Module(2016, "SEC_UserLogins")
  )

  lazy val appendNewAccountsProcess = EtlDefinition(
    newAuditLogsDF,
    Some(Seq(newAccounts())),
    append(SilverTargets.secAccountsTarget, newDataOnly = true),
    Module(2017, "SEC_UserAccounts")
  )


  def processSparkEvents: Array[ModuleStatusReport] = {
    Array(
//      appendJDBCSessionsProcess.process(),
//      appendJDBCOperationsProcess.process(),
      appendExecutorsProcess.process(),
//      appendApplicationsProcess.process(),
      appendExecutionsProcess.process(),
      appendJobsProcess.process(),
      appendStagesProcess.process(),
      appendTasksProcess.process()
    )
  }

  def run(): Boolean = {
    val reports = ArrayBuffer[ModuleStatusReport]()
    config.overwatchScope.foreach {
      case OverwatchScope.sparkEvents =>
        processSparkEvents.foreach(sparkReport => reports.append(sparkReport))
      case OverwatchScope.jobs =>
      case OverwatchScope.jobRuns =>
        reports.append(appendJobRunsProcess.process())
      case OverwatchScope.clusters =>
      case OverwatchScope.clusterEvents =>
        reports.append(appendClusterEventsProcess.process())
      case OverwatchScope.pools =>
      case OverwatchScope.audit =>
        val auditModuleIDs = Array(2010, 2014, 2016, 2017)
        cacheAuditLogs(auditModuleIDs)
        reports.append(appendJobStatusProcess.process())
        reports.append(appendClusterAuditSummaryProcess.process())
        reports.append(appendUserLoginsProcess.process())
        reports.append(appendNewAccountsProcess.process())
        // todo -- create notebook ETL
        // todo -- create pools history from audit log
        newAuditLogsDF.unpersist()
    }

    finalizeRun(reports.toArray)
    true
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}
