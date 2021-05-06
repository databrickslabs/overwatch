package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Module, Pipeline, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{abs, count, lit, rank, row_number}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window

import java.sql.Timestamp
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

case class SnapReport(tableFullName: String,
                      from: java.sql.Timestamp,
                      until: java.sql.Timestamp,
                      totalCount: Long,
                      errorMessage: String)

case class ValidationReport(tableSourceName: Option[String],
                            tableSnapName: Option[String],
                            tableSourceCount: Option[Long],
                            tableSnapCount: Option[Long],
                            totalDiscrepancies: Option[Long],
                            from: Option[java.sql.Timestamp],
                            until: Option[java.sql.Timestamp],
                            message: Option[String])

case class SnapValidationParams(snapDatabaseName: String,
                                sourceDatabaseName: String,
                                scopes: Option[Seq[String]],
                                primordialDateString: String,
                                maxDaysToLoad: Int,
                                parallelism: Int)

case class ModuleTarget(module: Module, target: PipelineTable)

class SnapValidation(workspace: Workspace, sourceDB: String)
    extends ValidationUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private val parallelism = getDriverCores - 1
  val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

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
   * @return
   */
  def executeBronzeSnapshot(): Dataset[SnapReport] = {
    // state tables clone and reset for silver and gold modules

    val bronzePipeline = Bronze(workspace, readOnly = true)
    val bronzeTargets = bronzePipeline.BronzeTargets
    snapStateTables(bronzePipeline)
    resetPipelineReportState(bronzePipeline.pipelineStateTarget)

    // snapshots bronze tables, returns ds with report
    val bronzeTargetsWModule = bronzePipeline.config.overwatchScope.flatMap {
      case OverwatchScope.audit => Seq(ModuleTarget(bronzePipeline.auditLogsModule, bronzeTargets.auditLogsTarget))
      case OverwatchScope.clusters => Seq(ModuleTarget(bronzePipeline.clustersSnapshotModule, bronzeTargets.clustersSnapshotTarget))
      case OverwatchScope.clusterEvents => Seq(ModuleTarget(bronzePipeline.clusterEventLogsModule, bronzeTargets.clusterEventsTarget))
      case OverwatchScope.jobs => Seq(ModuleTarget(bronzePipeline.jobsSnapshotModule, bronzeTargets.jobsSnapshotTarget))
      case OverwatchScope.pools => Seq(ModuleTarget(bronzePipeline.poolsSnapshotModule, bronzeTargets.poolsTarget))
      case OverwatchScope.sparkEvents => Seq(ModuleTarget(bronzePipeline.sparkEventLogsModule, bronzeTargets.sparkEventLogsTarget))
      case _ => Seq[ModuleTarget]()
    }.par

    bronzeTargetsWModule.tasksupport = taskSupport

    bronzeTargetsWModule
      .map(targetDetail => snapTable(targetDetail.module, targetDetail.target))
      .toArray.toSeq.toDS
  }

  /**
   * Execute recalculations for silver and gold with code from this package
   * @return
   */
  def executeRecalculations() = {
    Silver(workspace).run()
    Gold(workspace).run()
  }

  /**
   * TODO - generalize this function
   *  Probably can add an overload to pipeline instantiation initialize pipeline to a specific state
   *  Issue for Kitana is that the bronze state needs to be current but silver/gold must be
   *  current - 1.
   * @param pipeline
   * @param versionsAgo
   */
  def rollbackPipelineState(pipeline: Pipeline, versionsAgo: Int = 2): Unit = {
    // snapshot current state for bronze
    val initialBronzeState = pipeline.getPipelineState.filter(_._1 < 2000)
    // clear the state
    pipeline.clearPipelineState()
    if (spark.catalog.databaseExists(pipeline.config.databaseName) &&
      spark.catalog.tableExists(pipeline.config.databaseName, "pipeline_report")) {
      val w = Window.partitionBy('organization_id, 'moduleID).orderBy('Pipeline_SnapTS.desc)
      val wRank = Window.partitionBy('organization_id, 'moduleID, 'rnk).orderBy('Pipeline_SnapTS.desc)
      spark.table(s"${pipeline.config.databaseName}.pipeline_report")
        .filter('Status === "SUCCESS" || 'Status.startsWith("EMPTY"))
        .filter('organization_id === pipeline.config.organizationId)
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(wRank))
        .filter('moduleID >= 2000) // only retrieves states for silver / gold
        .filter('rnk === versionsAgo && 'rn === 1)
        .drop("inputConfig", "parsedConfig")
        .as[SimplifiedModuleStatusReport]
        .collect()
        .foreach(pipeline.updateModuleState)
      // reapply the initial bronze state
      initialBronzeState.foreach(state => pipeline.updateModuleState(state._2))
    } else {
      pipeline.config.setIsFirstRun(true)
      Array[SimplifiedModuleStatusReport]()
    }
    println(s"Rolled pipeline back $versionsAgo versions. RESULT:\n")
    pipeline.showRangeReport()
  }

  /**
   *
   * @param incrementalTest
   * @return
   */
  def equalityReport(incrementalTest: Boolean): Dataset[ValidationReport] = {

    val silverPipeline = Silver(workspace, readOnly = true)
    val silverTargets = silverPipeline.SilverTargets
    val goldPipeline = Gold(workspace, readOnly = true)
    val goldTargets = goldPipeline.GoldTargets
    rollbackPipelineState(silverPipeline)
    rollbackPipelineState(goldPipeline)

    val silverTargetsWModule = silverPipeline.config.overwatchScope.flatMap {
      case OverwatchScope.accounts => {
        Seq(
        ModuleTarget(silverPipeline.accountLoginsModule, silverTargets.accountLoginTarget),
        ModuleTarget(silverPipeline.modifiedAccountsModule, silverTargets.accountModTarget)
        )
      }
      case OverwatchScope.notebooks => Seq(ModuleTarget(silverPipeline.notebookSummaryModule, silverTargets.notebookStatusTarget))
      case OverwatchScope.clusters => Seq(ModuleTarget(silverPipeline.clusterSpecModule, silverTargets.clustersSpecTarget))
      case OverwatchScope.sparkEvents => {
        Seq(
          ModuleTarget(silverPipeline.executorsModule, silverTargets.executorsTarget),
          ModuleTarget(silverPipeline.executionsModule, silverTargets.executionsTarget),
          ModuleTarget(silverPipeline.sparkJobsModule, silverTargets.jobsTarget),
          ModuleTarget(silverPipeline.sparkStagesModule, silverTargets.stagesTarget),
          ModuleTarget(silverPipeline.sparkTasksModule, silverTargets.tasksTarget)
        )
      }
      case OverwatchScope.jobs => {
        Seq(
          ModuleTarget(silverPipeline.jobStatusModule, silverTargets.dbJobsStatusTarget),
          ModuleTarget(silverPipeline.jobRunsModule, silverTargets.dbJobRunsTarget)
        )
      }
      case _ => Seq[ModuleTarget]()
    }.toArray

    val goldTargetsWModule = goldPipeline.config.overwatchScope.flatMap {
      case OverwatchScope.accounts => {
        Seq(
          ModuleTarget(goldPipeline.accountModModule, goldTargets.accountModsTarget),
          ModuleTarget(goldPipeline.accountLoginModule, goldTargets.accountLoginTarget)
        )
      }
      case OverwatchScope.notebooks => {
        Seq(ModuleTarget(goldPipeline.notebookModule, goldTargets.notebookTarget))
      }
      case OverwatchScope.clusters => {
        Seq(
          ModuleTarget(goldPipeline.clusterModule, goldTargets.clusterTarget),
          ModuleTarget(goldPipeline.clusterStateFactModule, goldTargets.clusterStateFactTarget)
        )
      }
      case OverwatchScope.sparkEvents => {
        Seq(
          ModuleTarget(goldPipeline.sparkExecutorModule, goldTargets.sparkExecutorTarget),
          ModuleTarget(goldPipeline.sparkExecutionModule, goldTargets.sparkExecutionTarget),
          ModuleTarget(goldPipeline.sparkJobModule, goldTargets.sparkJobTarget),
          ModuleTarget(goldPipeline.sparkStageModule, goldTargets.sparkStageTarget),
          ModuleTarget(goldPipeline.sparkTaskModule, goldTargets.sparkTaskTarget)
        )
      }
      case OverwatchScope.jobs => {
        Seq(
          ModuleTarget(goldPipeline.jobsModule, goldTargets.jobTarget),
          ModuleTarget(goldPipeline.jobRunsModule, goldTargets.jobRunTarget),
          ModuleTarget(goldPipeline.jobRunCostPotentialFactModule, goldTargets.jobRunCostPotentialFactTarget)
        )
      }
      case _ => Seq[ModuleTarget]()
    }.toArray

    val targetsToValidate = (silverTargetsWModule ++ goldTargetsWModule).par
    targetsToValidate.tasksupport = taskSupport

    targetsToValidate.map(targetDetail => {
//      try {
        assertDataFrameDataEquals(targetDetail, sourceDB, incrementalTest)
//      } catch {
//        case e: Throwable => validationFailureReport(e)
//      }
    }).toArray.toSeq.toDS()

  }
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
      auditLogConfig = AuditLogConfig(rawAuditPath = Some(""), azureAuditLogEventhubConfig = Some(azureLogConfig)),
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

