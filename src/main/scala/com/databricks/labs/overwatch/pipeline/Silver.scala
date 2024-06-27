package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.Helpers.{deriveApiTempDir, deriveApiTempErrDir}
import com.databricks.labs.overwatch.utils.{Config, OverwatchScope}
import org.apache.log4j.Logger

class Silver(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
    with SilverTransforms {

  /**
   * Enable access to Silver pipeline tables externally.
   *
   * @return
   */
  def getAllTargets: Array[PipelineTable] = {
    Array(
      SilverTargets.executorsTarget,
      SilverTargets.executionsTarget,
      SilverTargets.jobsTarget,
      SilverTargets.stagesTarget,
      SilverTargets.tasksTarget,
      SilverTargets.poolsSpecTarget,
      SilverTargets.clusterStateDetailTarget,
      SilverTargets.dbJobRunsTarget,
      SilverTargets.accountLoginTarget,
      SilverTargets.accountModTarget,
      SilverTargets.clustersSpecTarget,
      SilverTargets.dbJobsStatusTarget,
      SilverTargets.notebookStatusTarget,
      SilverTargets.sqlQueryHistoryTarget,
      SilverTargets.warehousesSpecTarget,
      SilverTargets.warehousesStateDetailTarget
    )
  }

  def getAllModules: Seq[Module] = {
    config.overwatchScope.flatMap {
      case OverwatchScope.accounts => {
        Array(accountLoginsModule, modifiedAccountsModule)
      }
      case OverwatchScope.notebooks => Array(notebookSummaryModule)
      case OverwatchScope.pools => Array(poolsSpecModule)
      case OverwatchScope.clusters => Array(clusterSpecModule)
      case OverwatchScope.clusterEvents => Array(clusterStateDetailModule)
      case OverwatchScope.sparkEvents => {
        Array(executorsModule,
          executionsModule,
          sparkJobsModule,
          sparkStagesModule,
          sparkTasksModule)
      }
      case OverwatchScope.jobs => {
        Array(jobStatusModule, jobRunsModule)
      }
      case OverwatchScope.dbsql => {
        Array(sqlQueryHistoryModule,
          warehouseSpecModule)
      }
      case OverwatchScope.warehouseEvents => Array(warehouseStateDetailModule)
      case _ => Array[Module]()
    }
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

  private val sparkExecutorsSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048"
  )
  private val sparkEventsTarget = BronzeTargets.sparkEventLogsTarget
  lazy private[overwatch] val executorsModule = Module(2003, "Silver_SPARK_Executors", this, Array(1006))
    .withSparkOverrides(sparkExecutorsSparkOverrides)
  lazy private val appendExecutorsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        sparkEventsTarget.asIncrementalDF(executorsModule, sparkEventsTarget.incrementalColumns: _*),
        Seq(executor(
          sparkEventsTarget.asIncrementalDF(executorsModule, 30, sparkEventsTarget.incrementalColumns: _*),
          executorsModule.fromTime
        )),
        append(SilverTargets.executorsTarget)
      )
  }

  // TODO -- Build Bronze
  //  lazy val appendApplicationsProcess = EtlDefinition(
  //    sparkEventsDF,
  //    Some(Seq(enhanceApplication())),
  //    append(Silver.),
  //    Module(2004, "SPARK_Applications_Raw")
  //  )

  private val sparkExecutionsSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
    "spark.sql.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString),
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString)
  )
  lazy private[overwatch] val executionsModule = Module(2005, "Silver_SPARK_Executions", this, Array(1006), 8.0, shuffleFactor = 2.0)
    .withSparkOverrides(sparkExecutionsSparkOverrides)
  lazy private val appendExecutionsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        sparkEventsTarget.asIncrementalDF(executionsModule, sparkEventsTarget.incrementalColumns: _*),
        Seq(sqlExecutions(
          sparkEventsTarget.asIncrementalDF(executionsModule, 3, sparkEventsTarget.incrementalColumns: _*),
          executionsModule.fromTime, executionsModule.untilTime
        )),
        append(SilverTargets.executionsTarget)
      )
  }

  private val sparkJobsSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
    "spark.sql.files.maxPartitionBytes" -> (1024 * 1024 * 64).toString,
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> (1024 * 1024 * 32).toString,
    "spark.sql.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString),
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString)
  )
  lazy private[overwatch] val sparkJobsModule = Module(2006, "Silver_SPARK_Jobs", this, Array(1006), 8.0, shuffleFactor = 2.0)
    .withSparkOverrides(sparkJobsSparkOverrides)
  lazy private val appendSparkJobsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        sparkEventsTarget.asIncrementalDF(sparkJobsModule, sparkEventsTarget.incrementalColumns: _*),
        Seq(sparkJobs(
          sparkEventsTarget.asIncrementalDF(sparkJobsModule, 3, sparkEventsTarget.incrementalColumns: _*),
          sparkJobsModule.fromTime, sparkJobsModule.untilTime
        )),
        append(SilverTargets.jobsTarget)
      )
  }

  private val sparkStagesSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048",
    "spark.sql.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString),
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString)
  )
  lazy private[overwatch] val sparkStagesModule = Module(2007, "Silver_SPARK_Stages", this, Array(1006), 8.0, shuffleFactor = 4.0)
    .withSparkOverrides(sparkStagesSparkOverrides)
  lazy private val appendSparkStagesProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        sparkEventsTarget.asIncrementalDF(sparkStagesModule, sparkEventsTarget.incrementalColumns: _*),
        Seq(sparkStages(
          sparkEventsTarget.asIncrementalDF(sparkStagesModule, 3, sparkEventsTarget.incrementalColumns: _*),
          sparkStagesModule.fromTime, sparkStagesModule.untilTime
        )),
        append(SilverTargets.stagesTarget)
      )
  }

  private val sparkTasksSparkOverrides = Map(
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "500000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "2048", // output is very dense, shrink output file size
    "spark.sql.files.maxPartitionBytes" -> (1024 * 1024 * 64).toString,
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> (1024 * 1024 * 8).toString,
    "spark.sql.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString),
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> ((1024 * 1024 * 2).toString)
  )
  lazy private[overwatch] val sparkTasksModule = Module(2008, "Silver_SPARK_Tasks", this, Array(1006), 8.0, shuffleFactor = 8.0)
    .withSparkOverrides(sparkTasksSparkOverrides)
  lazy private val appendSparkTasksProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        sparkEventsTarget.asIncrementalDF(sparkTasksModule, sparkEventsTarget.incrementalColumns: _*),
        Seq(sparkTasks(
          sparkEventsTarget.asIncrementalDF(sparkTasksModule, 3, sparkEventsTarget.incrementalColumns: _*),
          sparkTasksModule.fromTime
        )),
        append(SilverTargets.tasksTarget)
      )
  }

  lazy private[overwatch] val jobStatusModule = Module(2010, "Silver_JobsStatus", this, Array(1001, 1004))
  lazy private val appendJobStatusProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(jobStatusModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(
          dbJobsStatusSummary(
            BronzeTargets.jobsSnapshotTarget,
            jobStatusModule.isFirstRun,
            SilverTargets.dbJobsStatusTarget.keys,
            jobStatusModule.fromTime,
            config.tempWorkingDir,
            jobStatusModule.daysToProcess
          )),
        append(SilverTargets.dbJobsStatusTarget)
      )
  }

  lazy private[overwatch] val jobRunsModule = Module(2011, "Silver_JobsRuns", this, Array(1004, 2010, 2014), shuffleFactor = 12.0)
  lazy private val appendJobRunsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(jobRunsModule, BronzeTargets.auditLogsTarget.incrementalColumns, 30),
        Seq(
          dbJobRunsSummary(
            SilverTargets.clustersSpecTarget,
            BronzeTargets.clustersSnapshotTarget,
            SilverTargets.dbJobsStatusTarget,
            BronzeTargets.jobsSnapshotTarget,
            jobRunsModule.fromTime, jobRunsModule.untilTime,
            SilverTargets.dbJobRunsTarget.keys,
            jobRunsModule.daysToProcess
          )
        ),
        append(SilverTargets.dbJobRunsTarget)
      )
  }

  lazy private[overwatch] val poolsSpecModule = Module(2009, "Silver_PoolsSpec", this, Array(1003, 1004))
  lazy private val appendPoolsSpecProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(poolsSpecModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(buildPoolsSpec(
          BronzeTargets.poolsSnapshotTarget.asDF,
          poolsSpecModule.isFirstRun,
          poolsSpecModule.fromTime
        )),
        append(SilverTargets.poolsSpecTarget)
      )
  }

  lazy private[overwatch] val clusterSpecModule = Module(2014, "Silver_ClusterSpec", this, Array(1002, 1004))
  lazy private val appendClusterSpecProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(clusterSpecModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(
          buildClusterSpec(
            BronzeTargets.clustersSnapshotTarget,
            BronzeTargets.poolsSnapshotTarget,
            BronzeTargets.auditLogsTarget,
            clusterSpecModule.isFirstRun,
            clusterSpecModule.untilTime
          )),
        append(SilverTargets.clustersSpecTarget)
      )
  }

  lazy private[overwatch] val clusterStateDetailModule = Module(2019, "Silver_ClusterStateDetail", this, Array(1005))
  lazy private val appendClusterStateDetailProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.clusterEventsTarget.asIncrementalDF(
          clusterStateDetailModule,
          BronzeTargets.clusterEventsTarget.incrementalColumns,
          SilverTargets.clusterStateDetailTarget.maxMergeScanDates // pick up last state up to 30 days ago
        ),
        Seq(buildClusterStateDetail(
          clusterStateDetailModule.untilTime,
          BronzeTargets.auditLogsTarget.asIncrementalDF(clusterSpecModule, BronzeTargets.auditLogsTarget.incrementalColumns,1), //Added to get the Removed Cluster,
          SilverTargets.dbJobRunsTarget.asIncrementalDF(clusterStateDetailModule, SilverTargets.dbJobRunsTarget.incrementalColumns, 30),
          SilverTargets.clustersSpecTarget
        )),
        append(SilverTargets.clusterStateDetailTarget)
      )
  }

  lazy private[overwatch] val accountLoginsModule = Module(2016, "Silver_AccountLogins", this, Array(1004))
  lazy private val appendAccountLoginsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(accountLoginsModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(accountLogins()),
        append(SilverTargets.accountLoginTarget)
      )
  }

  lazy private[overwatch] val modifiedAccountsModule = Module(2017, "Silver_ModifiedAccounts", this, Array(1004))
  lazy private val appendModifiedAccountsProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(modifiedAccountsModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(accountMods()),
        append(SilverTargets.accountModTarget)
      )
  }

  lazy private[overwatch] val notebookSummaryModule = Module(2018, "Silver_Notebooks", this, Array(1004))
  lazy private val appendNotebookSummaryProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        BronzeTargets.auditLogsTarget.asIncrementalDF(notebookSummaryModule, BronzeTargets.auditLogsTarget.incrementalColumns),
        Seq(notebookSummary()),
        append(SilverTargets.notebookStatusTarget)
      )
  }

  lazy private[overwatch] val sqlQueryHistoryModule = Module(2020, "Silver_SQLQueryHistory", this, Array(1004))
  lazy private val appendSqlQueryHistoryProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getSqlQueryHistoryParallelDF(
          sqlQueryHistoryModule.fromTime,
          sqlQueryHistoryModule.untilTime,
          sqlQueryHistoryModule.pipeline.pipelineSnapTime,
          deriveApiTempDir(config.tempWorkingDir, sqlQueryHistoryModule.moduleName, pipelineSnapTime),
          deriveApiTempErrDir(config.tempWorkingDir, sqlQueryHistoryModule.moduleName, pipelineSnapTime)),
        Seq(enhanceSqlQueryHistory),
        append(SilverTargets.sqlQueryHistoryTarget)
      )
  }

  lazy private[overwatch] val warehouseSpecModule = Module(2021, "Silver_WarehouseSpec", this, Array(1004, 1013))
  lazy private val appendWarehouseSpecProcess: () => ETLDefinition = {
    () =>
    ETLDefinition(
      BronzeTargets.auditLogsTarget.asIncrementalDF(warehouseSpecModule, BronzeTargets.auditLogsTarget.incrementalColumns),
      Seq(
        buildWarehouseSpec(
          BronzeTargets.warehousesSnapshotTarget,
          warehouseSpecModule.isFirstRun,
          SilverTargets.warehousesSpecTarget
        )),
      append(SilverTargets.warehousesSpecTarget)
    )
  }

  lazy private[overwatch] val warehouseStateDetailModule = Module(2022, "Silver_WarehouseStateDetail", this, Array(1004, 1013))
  lazy private val appendWarehouseStateDetailProcess: () => ETLDefinition = {
    () =>
      ETLDefinition(
        workspace.getWarehousesEventDF(warehouseStateDetailModule.fromTime,warehouseStateDetailModule.untilTime),
        Seq(buildWarehouseStateDetail(
          warehouseStateDetailModule.untilTime,
          BronzeTargets.auditLogsTarget.asIncrementalDF(warehouseSpecModule, BronzeTargets.auditLogsTarget.incrementalColumns,1), //Added to get the Removed Cluster,
          SilverTargets.dbJobRunsTarget.asIncrementalDF(warehouseStateDetailModule, SilverTargets.dbJobRunsTarget.incrementalColumns, 30),
          SilverTargets.warehousesSpecTarget
        )),
        append(SilverTargets.warehousesStateDetailTarget)
      )
  }

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
      case OverwatchScope.pools => poolsSpecModule.execute(appendPoolsSpecProcess)
      case OverwatchScope.clusters => clusterSpecModule.execute(appendClusterSpecProcess)
      case OverwatchScope.clusterEvents => clusterStateDetailModule.execute(appendClusterStateDetailProcess)
      case OverwatchScope.dbsql => {
        sqlQueryHistoryModule.execute(appendSqlQueryHistoryProcess)
        warehouseSpecModule.execute(appendWarehouseSpecProcess)
      }
      case OverwatchScope.sparkEvents => {
        processSparkEvents()
      }
      case OverwatchScope.jobs => {
        jobStatusModule.execute(appendJobStatusProcess)
        jobRunsModule.execute(appendJobRunsProcess)
      }
      case OverwatchScope.warehouseEvents => {
        warehouseStateDetailModule.execute(appendWarehouseStateDetailProcess)
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
    apply(
      workspace,
      readOnly = false,
      suppressReport = false,
      suppressStaticDatasets = false
    )
  }

  private[overwatch] def apply(
                                workspace: Workspace,
                                readOnly: Boolean = false,
                                suppressReport: Boolean = false,
                                suppressStaticDatasets: Boolean = false
                              ): Silver = {
    val silverPipeline = new Silver(workspace, workspace.database, workspace.getConfig)
      .setReadOnly(if (workspace.isValidated) readOnly else true) // if workspace is not validated set it read only
      .suppressRangeReport(suppressReport)
      .initPipelineRun()

    if (suppressStaticDatasets) {
      silverPipeline
    } else {
      silverPipeline.loadStaticDatasets()
    }
  }

}
