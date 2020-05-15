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

  object UDFs {


  }

  object Sources {

    /**
     * Module sparkEvents
     * Bronze sources for spark events
     */

    // TODO -- Compare all configurations against defaults and notate non-default configs
    val sparkEventsDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .drop("ClasspathEntries", "HadoopProperties", "SparkProperties", "SystemProperties", "SparkPlanInfo") // TODO - TEMP
      .withColumn("filenameGroup", UDF.groupFilename('filename))

    // Slack Chat
    // https://databricks.slack.com/archives/C04SZU99Q/p1588959876188200
    // Todo -- Only return filenameGroup with specific request
    // Todo -- ODBC/JDBC
    /**
     * ODBC/JDBC Sessions
     */

    val serverSessionStartDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionCreated")
      .select('SparkContextID, 'ip, 'sessionId, 'startTime, 'userName, 'filenameGroup.alias("startFilenameGroup"))

    val serverSessionEndDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerSessionClosed")
      .select('SparkContextID, 'sessionId, 'finishTime, 'filenameGroup.alias("endFilenameGroup"))

    val serverOperationStartDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationStart")
      .select('SparkContextID, 'groupId, 'id, 'sessionId, 'startTime, 'statement, 'userName,
        'filenameGroup.alias("startFilenameGroup"))

    val serverOperationEndDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "org.apache.spark.sql.hive.thriftserver.ui.SparkListenerThriftServerOperationClosed")
      .select('SparkContextID, 'id, 'closeTime, 'filenameGroup.alias("endFilenameGroup"))

    /**
     * Executor
     */

    val executorAddedDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "SparkListenerExecutorAdded")
      .select('SparkContextID, 'ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"),
        'filenameGroup.alias("startFilenameGroup"))

    val executorRemovedDF: DataFrame = Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "SparkListenerExecutorRemoved")
      .select('SparkContextID, 'ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"),
        'filenameGroup.alias("endFilenameGroup"))


  }

  /**
   * Module SparkEvents
   */

  // Todo -- no data to test yet
  lazy private val appendJDBCSessionsProcess = EtlDefinition(
    getJDBCSession(Sources.serverSessionStartDF, Sources.serverSessionEndDF),
    None,
    append(Silver.jdbcSessionsTarget, newDataOnly = true),
    Module(2001, "SPARK_JDBC_Sessions_Raw")
  )

  lazy private val appendJDBCOperationsProcess = EtlDefinition(
    getJDBCOperation(Sources.serverOperationStartDF, Sources.serverOperationEndDF),
    None,
    append(Silver.jdbcOperationsTarget, newDataOnly = true),
    Module(2002, "SPARK_JDBC_Operations_Raw")
  )

  lazy private val appendExecutorsProcess = EtlDefinition(
    getExecutor(Sources.executorAddedDF, Sources.executorRemovedDF),
    None,
    append(Silver.executorsTarget, newDataOnly = true),
    Module(2003, "SPARK_Executors_Raw")
  )

  lazy private val appendApplicationsProcess = EtlDefinition(
    getApplication(Bronze.sparkEventLogsTarget.asDF
      .filter('Event === "SparkListenerApplicationStart")),
    None,
    append(Silver.executorsTarget, newDataOnly = true),
    Module(2004, "SPARK_Applications_Raw")
  )

  lazy private val appendExecutionsProcess = EtlDefinition(
    Bronze.sparkEventLogsTarget.asDF,
    Some(Seq(sqlExecutions())),
    append(Silver.executionsTarget, newDataOnly = true),
    Module(2005, "SPARK_Executions_Raw")
  )

  lazy private val appendJobsProcess = EtlDefinition(
    Bronze.sparkEventLogsTarget.asDF,
    Some(Seq(jobs())),
    append(Silver.jobsTarget, newDataOnly = true),
    Module(2006, "SPARK_Jobs_Raw")
  )

  lazy private val appendStagesProcess = EtlDefinition(
    Bronze.sparkEventLogsTarget.asDF,
    Some(Seq(stages())),
    append(Silver.stagesTarget, newDataOnly = true),
    Module(2007, "SPARK_Stages_Raw")
  )

  lazy private val appendTasksProcess = EtlDefinition(
    Bronze.sparkEventLogsTarget.asDF,
    Some(Seq(tasks())),
    append(Silver.tasksTarget, newDataOnly = true),
    Module(2008, "SPARK_Tasks_Raw")
  )

  lazy private val appendJobsHistoricalProcess = EtlDefinition(
    Bronze.jobsTarget.asDF.coalesce(1),
    None,
    append(Silver.dbJobsHistoricalTarget, newDataOnly = true),
    Module(2009, "DB_Jobs_Historical")
  )

  lazy private val overwriteJobsCurrentSnapshotProcess = EtlDefinition(
    Bronze.jobsTarget.asDF,
    None,
    append(Silver.dbJobsCurrentTarget, newDataOnly = true),
    Module(2010, "DB_Jobs_Current")
  )

  lazy private val appendJobRunsProcess = EtlDefinition(
    Bronze.jobRunsTarget.asDF,
    None,
    append(Silver.dbJobRunsTarget, newDataOnly = true),
    Module(2011, "DB_JobRuns")
  )

  lazy private val appendClustersHistoricalProcess = EtlDefinition(
    Bronze.clustersTarget.asDF,
    None,
    append(Silver.dbClustersHistoricalTarget, newDataOnly = true),
    Module(2013, "DB_Clusters_Historical")
  )

  lazy private val overwriteClustersCurrentProcess = EtlDefinition(
    Bronze.clustersTarget.asDF,
    None,
    append(Silver.dbClustersCurrentTarget, newDataOnly = true),
    Module(2014, "DB_Clusters_Current")
  )

  lazy private val appendClusterEventsProcess = EtlDefinition(
    Bronze.clusterEventsTarget.asDF,
    None,
    append(Silver.dbClustersEventsTarget, newDataOnly = true),
    Module(2015, "DB_ClusterEvents")
  )

  lazy private val appendPoolsHistoricalProcess = EtlDefinition(
    Bronze.poolsTarget.asDF,
    None,
    append(Silver.dbPoolsHistoricalTarget, newDataOnly = true),
    Module(2016, "DB_Pools_Historical")
  )

  lazy private val overwritePoolsCurrentProcess = EtlDefinition(
    Bronze.poolsTarget.asDF,
    None,
    append(Silver.dbPoolsCurrentTarget, newDataOnly = true),
    Module(2017, "DB_Pools_Current")
  )

  lazy private val appendDBAuditLogsProcess = EtlDefinition(
    Bronze.auditLogsTarget.asDF,
    None,
    append(Silver.secAuditLogs, newDataOnly = true),
    Module(2018, "DB_Audit_Logs")
  )

  private def processSparkEvents: Array[ModuleStatusReport] = {
    Array(
      appendJDBCSessionsProcess.process(),
      appendJDBCOperationsProcess.process(),
      appendExecutorsProcess.process(),
      appendApplicationsProcess.process(),
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
        reports.append(appendJobsHistoricalProcess.process())
        reports.append(overwriteJobsCurrentSnapshotProcess.process())
      case OverwatchScope.jobRuns =>
        reports.append(appendJobRunsProcess.process())
      case OverwatchScope.clusters =>
        reports.append(appendClustersHistoricalProcess.process())
        reports.append(overwriteClustersCurrentProcess.process())
      case OverwatchScope.clusterEvents =>
        reports.append(appendClusterEventsProcess.process())
      case OverwatchScope.pools =>
        reports.append(appendPoolsHistoricalProcess.process())
        reports.append(overwritePoolsCurrentProcess.process())
      case OverwatchScope.audit =>
        reports.append(appendDBAuditLogsProcess.process())
        
    }

    true
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)

  //    .setWorkspace(workspace).setDatabase(workspace.database)

}
