package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.Logger

class Silver(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SilverTransforms {

  /**
   * Enable access to Silver pipeline tables externally.
   * @return
   */
  def getAllTargets: Array[PipelineTable] = {
    Array(
      SilverTargets.executorsTarget,
      SilverTargets.executionsTarget,
      SilverTargets.jobsTarget,
      SilverTargets.stagesTarget,
      SilverTargets.tasksTarget,
      SilverTargets.dbJobRunsTarget,
      SilverTargets.accountLoginTarget,
      SilverTargets.accountModTarget,
      SilverTargets.clustersSpecTarget,
      SilverTargets.dbJobsStatusTarget,
      SilverTargets.notebookStatusTarget
    )
  }

  envInit()

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Module sparkEvents
   * Bronze sources for spark events
   */

  // TODO -- Compare all configurations against defaults and notate non-default configs

  // TODO -- Issue_50
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

  // Todo -- Issue_81
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

  lazy private[overwatch] val executorsModule = Module(2003, "Silver_SPARK_Executors", this, Array(1006))
  lazy private val appendExecutorsProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(executorsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Seq(executor()),
    append(SilverTargets.executorsTarget)
  )

  // TODO -- Build Bronze
  //  lazy val appendApplicationsProcess = EtlDefinition(
  //    sparkEventsDF,
  //    Some(Seq(enhanceApplication())),
  //    append(Silver.),
  //    Module(2004, "SPARK_Applications_Raw")
  //  )

  lazy private[overwatch] val executionsModule = Module(2005, "Silver_SPARK_Executions", this, Array(1006))
  lazy private val appendExecutionsProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(executionsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Seq(sqlExecutions()),
    append(SilverTargets.executionsTarget)
  )

  lazy private[overwatch] val sparkJobsModule = Module(2006, "Silver_SPARK_Jobs", this, Array(1006))
  lazy private val appendSparkJobsProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(sparkJobsModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Seq(sparkJobs()),
    append(SilverTargets.jobsTarget)
  )

  lazy private[overwatch] val sparkStagesModule = Module(2007, "Silver_SPARK_Stages", this, Array(1006))
  lazy private val appendSparkStagesProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(sparkStagesModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Seq(sparkStages()),
    append(SilverTargets.stagesTarget)
  )

  lazy private[overwatch] val sparkTasksModule = Module(2008, "Silver_SPARK_Tasks", this, Array(1006))
  lazy private val appendSparkTasksProcess = ETLDefinition(
    BronzeTargets.sparkEventLogsTarget
      .asIncrementalDF(sparkTasksModule, 2, "fileCreateDate", "fileCreateEpochMS"),
    Seq(sparkTasks()),
    append(SilverTargets.tasksTarget)
  )

  lazy private[overwatch] val jobStatusModule = Module(2010, "Silver_JobsStatus", this, Array(1004))
  lazy private val appendJobStatusProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobStatusModule, auditLogsIncrementalCols),
    Seq(dbJobsStatusSummary()),
    append(SilverTargets.dbJobsStatusTarget)
  )

  lazy private[overwatch] val jobRunsModule = Module(2011, "Silver_JobsRuns", this, Array(1004, 2010, 2014))
  lazy private val appendJobRunsProcess = ETLDefinition(
    // TODO -- TEST NEEDED, jobs running longer than "additionalLagDays" of 2 below, are they able to get joined up
    //  with their jobStart events properly? If not, add logic to identify long running jobs and go get them
    //  from historical
    BronzeTargets.auditLogsTarget.asIncrementalDF(jobRunsModule, auditLogsIncrementalCols, 2),
    Seq(
      dbJobRunsSummary(
        BronzeTargets.auditLogsTarget.withMinimumSchemaEnforcement.asDF,
        SilverTargets.clustersSpecTarget,
        BronzeTargets.clustersSnapshotTarget,
        SilverTargets.dbJobsStatusTarget,
        BronzeTargets.jobsSnapshotTarget,
        config.apiEnv,
        jobRunsModule.fromTime.asColumnTS,
        jobRunsModule.fromTime.asUnixTimeMilli
      )
    ),
    append(SilverTargets.dbJobRunsTarget)
  )

  lazy private[overwatch] val clusterSpecModule = Module(2014, "Silver_ClusterSpec", this, Array(1004))
  lazy private val appendClusterSpecProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(clusterSpecModule, auditLogsIncrementalCols),
    Seq(
      buildClusterSpec(
        BronzeTargets.clustersSnapshotTarget,
        BronzeTargets.auditLogsTarget
      )),
    append(SilverTargets.clustersSpecTarget)
  )

  lazy private[overwatch] val accountLoginsModule = Module(2016, "Silver_AccountLogins", this, Array(1004))
  lazy private val appendAccountLoginsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(accountLoginsModule, auditLogsIncrementalCols),
    Seq(accountLogins()),
    append(SilverTargets.accountLoginTarget)
  )

  lazy private[overwatch] val modifiedAccountsModule = Module(2017, "Silver_ModifiedAccounts", this, Array(1004))
  lazy private val appendModifiedAccountsProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(modifiedAccountsModule, auditLogsIncrementalCols),
    Seq(accountMods()),
    append(SilverTargets.accountModTarget)
  )

  lazy private[overwatch] val notebookSummaryModule = Module(2018, "Silver_Notebooks", this, Array(1004))
  lazy private val appendNotebookSummaryProcess = ETLDefinition(
    BronzeTargets.auditLogsTarget.asIncrementalDF(notebookSummaryModule, auditLogsIncrementalCols),
    Seq(notebookSummary()),
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
      case _ =>
    }
  }

  def run(): Pipeline = {

    restoreSparkConf()
    executeModules()
    initiatePostProcessing()
    this // to be used as fail switch later if necessary
  }

}

object Silver {
  def apply(workspace: Workspace): Silver = {
    new Silver(workspace, workspace.database, workspace.getConfig)
      .initPipelineRun()
      .loadStaticDatasets()
  }

  def apply(workspace: Workspace, readOnly: Boolean): Silver = {
    apply(workspace).setReadOnly
  }

}
