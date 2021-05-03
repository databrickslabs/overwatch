package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Module, Pipeline, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils.Layer.bronze
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{abs, count, lit}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.config

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

case class SnapReport(tableFullName: String,
                      from: java.sql.Timestamp,
                      until: java.sql.Timestamp,
                      totalCount: Long,
                      errorMessage: String)

case class ValidationReport(tableSourceName: String,
                            tableSnapName: String,
                            tableSourceCount: Long,
                            tableSnapCount: Long,
                            tableCountDiff: Long,
                            totalDiscrepancies: Long,
                            from: java.sql.Timestamp,
                            until: java.sql.Timestamp,
                            message: String)

case class SnapValidationParams(snapDatabaseName: String,
                                sourceDatabaseName: String,
                                scopes: Option[Seq[String]],
                                primordialDateString: String,
                                maxDaysToLoad: Int,
                                parallelism: Int)

class SnapValidation(workspace: Workspace, sourceDB: String)
    extends ValidationUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private val parallelism = getDriverCores - 1
  val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

  /**
   * Start and end time calculations
   * TODO: check addOneTick, why are calculations taking place after startcompare + 1 day?
   */
//  private val startCompareMillis = TimeTypesConstants.dtFormat.parse(config.primordialDateString.get).getTime
//  private val endCompareMillis = startCompareMillis + config.maxDays.toLong * 86400000L
//
//  private val startSnap = Pipeline.createTimeDetail(startCompareMillis).asColumnTS
//  private val startCompare = startSnap //Pipeline.createTimeDetail(startCompareMillis + 86400000L).asColumnTS
//  private val endCompare = Pipeline.createTimeDetail(endCompareMillis).asColumnTS
//  println(s"DEBUG: Creating interval variables: ${startCompareMillis} to ${endCompareMillis}, ${config.maxDays} days to load")

  /**
   * Compares data between original source tables and tables recreates from snapshot
   *
   * @param target
   * @param snapDatabaseName
   * @param sourceDatabaseName
   * @return ValidateReport
   */
//  protected def compareTable(target: PipelineTable): ValidationReport = {
//    val databaseTable = s"${params.snapDatabaseName}.${target.name}"
//    val tableName = s"${params.sourceDatabaseName}.${target.name}"
//
//    // drops columns not to be checked - some columns/structs are not always present, so we might want to avoid
//    // errors with field ordering, etc. others are random columns, so should be always dropped in all targets.
//    def dropUnecessaryCols(df: DataFrame, targetName: String) = {
//      val commonColumnsToDrop = Array("__overwatch_ctrl_noise", "Pipeline_SnapTS", "Overwatch_RunID", "__overwatch_incremental_col")
//
//      (targetName match {
//        case "spark_tasks_silver" => df.drop('TaskEndReason) // variable schema
//        case "sparkTask_gold" => df.drop('task_end_reason)  // variable schema
//        case "clusterStateFact_gold" => df.drop('driverSpecs).drop('workerSpecs) // api call or window func
//        case "jobrun_silver" |
//             "notebook_silver" |
//             "cluster_gold" |
//             "jobRun_gold" |
//             "notebook_gold" => df.drop('clusterId).drop('cluster_id) // api call or window func
//        case "job_status_silver" => df.drop('jobCluster) // api call or window func
//        case "sparkExecution_gold" => df.drop('event_log_start).drop('event_log_end) // filename can change to .gz after archival
//        case _ => df
//      }).drop(commonColumnsToDrop: _*)
//    }
//
//    try {
//      // take df1 from target (recalculated from snap), df2 from original table
//      val df1 = dropUnecessaryCols(getFilteredDF(target, tableName, startCompare, endCompare), target.name)
//      val df2 = dropUnecessaryCols(spark.table(databaseTable), target.name)
//
//      val count1 = df1.cache.count
//      val count2 = df2.cache.count
//
//      val returndf = df2.except(df1)
//        .union(df1.except(df2))
//        .dropDuplicates()
//        .select(
//          lit(tableName).alias("tableSourceName"),
//          lit(databaseTable).alias("tableSnapName"),
//          lit(count1).alias("tableSourceCount"),
//          lit(count2).alias("tableSnapCount"),
//          startCompare.alias("from"),
//          endCompare.alias("until"),
//          lit(count2-count1).alias("tableCountDiff"),
//          abs(count("*")-abs(lit(count2)-lit(count1))).alias("totalDiscrepancies"),
//          lit("processing ok").cast("string").alias("message")
//        ).as[ValidationReport]
//        .first()
//
//      df1.unpersist()
//      df2.unpersist()
//
//      returndf
//    } catch {
//      case e: Throwable =>
//        println(s"FAILED: ${databaseTable} --> ${e.getMessage}")
//        ValidationReport(tableName, databaseTable, 0L, 0L, 0L, 0L, tsTojsql(startCompare), tsTojsql(endCompare), e.getMessage)
//    }
//
//  }

  /**
   * Snapshots table
   *
   * @param target
   * @param params
   * @return
   */
  protected def snapTable(bronzeModule: Module, target: PipelineTable): SnapReport = {

    val module = bronzeModule.copy(_moduleDependencies = Array[Int]())
    val pipeline = module.pipeline

    try {
      val finalDFToClone = target.copy(_databaseName = sourceDB).asIncrementalDF(module, target.incrementalColumns)
      pipeline.database.write(finalDFToClone, target, pipeline.pipelineSnapTime.asColumnTS)


      println(s"SNAPPED: ${target.tableFullName}")
      val snapCount = spark.table(target.tableFullName).count()

      println(s"UPDATING Module State")
      pipeline.updateModuleState(module.moduleState.copy(
        fromTS = module.fromTime.asUnixTimeMilli,
        untilTS = module.untilTime.asUnixTimeMilli,
        recordsAppended = snapCount
      ))

      target.asDF()
        .select(
          lit(target.tableFullName).alias("tableFullName"),
          module.fromTime.asColumnTS.alias("from"),
          module.untilTime.asColumnTS.alias("until"),
          lit(snapCount).alias("totalCount"),
          lit(null).cast("string").alias("errorMessage")
        ).as[SnapReport]
        .first()

    } catch {
      case e: Throwable =>
        val errMsg = s"FAILED SNAP: ${target.tableFullName} --> ${e.getMessage}"
        println(errMsg)
        logger.log(Level.ERROR, errMsg, e)
        SnapReport(
          target.tableFullName,
          new java.sql.Timestamp(module.fromTime.asUnixTimeMilli),
          new java.sql.Timestamp(module.untilTime.asUnixTimeMilli),
          0L,
          errMsg)
    }
  }

  protected def snapStateTables(bronzePipeline: Pipeline): Unit = {

    val tableLoc = bronzePipeline.BronzeTargets.cloudMachineDetail.tableLocation
    val dropInstanceDetailsSql = s"drop table if exists ${bronzePipeline.BronzeTargets.cloudMachineDetail.tableFullName}"
    println(s"Dropping instanceDetails in SnapDB created from instantiation\n${dropInstanceDetailsSql}")
    spark.sql(dropInstanceDetailsSql)
    dbutils.fs.rm(tableLoc, true)

    val uniqueTablesToClone = Array(
      bronzePipeline.BronzeTargets.processedEventLogs,
      bronzePipeline.BronzeTargets.cloudMachineDetail.copy(mode = "overwrite"),
      bronzePipeline.pipelineStateTarget
    ).par
    uniqueTablesToClone.tasksupport = taskSupport

    uniqueTablesToClone.foreach(target => {
      try {
        val stmt = s"CREATE TABLE ${target.tableFullName} DEEP CLONE ${sourceDB}.${target.name}"
        println(s"CLONING TABLE ${target.tableFullName}.\nSTATEMENT: ${stmt}")
        spark.sql(stmt)
      } catch {
        case e: Throwable => {
          val errMsg = s"FAILED TO CLONE: ${target.tableFullName}\nERROR: ${e.getMessage}"
          println(errMsg)
          logger.log(Level.ERROR, errMsg, e)
        }
      }
    })
  }

  /**
   * Execute snapshots
   * TODO: fix, brittle.
   * TODO: pipeline incremental processing not supported as state tables are replaced.
   * @return
   */
  def executeSnapshots(): Dataset[SnapReport] = {
    // state tables clone and reset for silver and gold modules
    case class ModuleTarget(module: Module, target: PipelineTable)

    val bronzePipeline = Bronze(workspace)
    val bronzeTargets = bronzePipeline.BronzeTargets
    snapStateTables(bronzePipeline)
    resetPipelineReportState(bronzePipeline.pipelineStateTarget)

    // snapshots bronze tables, returns ds with report
    val bronzeTargetsWModule = bronzePipeline.config.overwatchScope.map {
      case OverwatchScope.audit => ModuleTarget(bronzePipeline.auditLogsModule, bronzeTargets.auditLogsTarget)
      case OverwatchScope.clusters => ModuleTarget(bronzePipeline.clustersSnapshotModule, bronzeTargets.clustersSnapshotTarget)
      case OverwatchScope.clusterEvents => ModuleTarget(bronzePipeline.clusterEventLogsModule, bronzeTargets.clusterEventsTarget)
      case OverwatchScope.jobs => ModuleTarget(bronzePipeline.jobsSnapshotModule, bronzeTargets.jobsSnapshotTarget)
      case OverwatchScope.pools => ModuleTarget(bronzePipeline.poolsSnapshotModule, bronzeTargets.poolsTarget)
      case OverwatchScope.sparkEvents => ModuleTarget(bronzePipeline.sparkEventLogsModule, bronzeTargets.sparkEventLogsTarget)
    }.par

    bronzeTargetsWModule.tasksupport = taskSupport

    bronzeTargetsWModule
      .map(targetDetail => snapTable(targetDetail.module, targetDetail.target))
      .toArray.toSeq.toDS
  }

  /**
   * Execute recalculations for silver and gold
   * TODO: fix, brittle.
   * @return
   */
  def executeRecalculations() = {
    Silver(workspace).run()
    Gold(workspace).run()
  }

  /**
   * Execute equality checks for silver and gold
   * TODO: fix, brittle.
   * @return
   */
//  def equalityReport() = {
//    val targets = (Silver(workspace).getAllTargets ++ Gold(workspace).getAllTargets).par
//    targets.tasksupport = taskSupport
//
//    targets.map(t => compareTable(t)).toArray.toSeq.toDS
//  }
}

object SnapValidation {
  def apply(params: SnapValidationParams): SnapValidation = {

    /**
     * create config environment for overwatch
     * TODO: review datatarget
     * TODO: hardcoded for azure!
     */

    require(params.sourceDatabaseName != params.snapDatabaseName, "Source Overwatch database cannot be the same as " +
      "the snapshot target.")
    val dataTarget = DataTarget(
      Some(params.snapDatabaseName), Some(s"dbfs:/user/hive/warehouse/${params.snapDatabaseName}.db"), None
    )

    val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = "", eventHubName = "", auditRawEventsPrefix = "")

    val overwatchParams = OverwatchParams(
      auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
      dataTarget = Some(dataTarget),
      badRecordsPath = None,
//      overwatchScope = Some("audit,accounts,jobs,sparkEvents,clusters,clusterEvents,notebooks,pools".split(",")),
      overwatchScope = params.scopes,
      maxDaysToLoad = params.maxDaysToLoad,
      primordialDateString = Some(params.primordialDateString)
    )

    val args = objToJson(overwatchParams).compactString

    val workspace = if (args.nonEmpty) {
      Initializer(Array(args), debugFlag = true)
    } else {
      Initializer(Array())
    }

    // creates object
//    new SnapValidation(params, overwatchParams, workspace)

    new SnapValidation(workspace, params.sourceDatabaseName)
  }
}

