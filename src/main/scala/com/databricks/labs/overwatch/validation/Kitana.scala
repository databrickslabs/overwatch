package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Module, Pipeline, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Duration, LocalDate}
import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool

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

case class ValidationParams(snapDatabaseName: String,
                            sourceDatabaseName: String,
                            primordialDateString: String,
                            scopes: Option[Seq[String]] = None,
                            maxDaysToLoad: Int,
                            parallelism: Int)

case class NullKey(k: String, nullCount: Long)

case class KeyReport(tableName: String,
                     keys: Array[String],
                     baseCount: Long,
                     keyCount: Long,
                     nullKeys: Array[NullKey],
                     cols: Array[String],
                     msg: String)

case class TargetDupDetail(target: PipelineTable, df: Option[DataFrame]) extends SparkSessionWrapper {
  def getDuplicatesDFForTable(keysOnly: Boolean = false): DataFrame = {
    //    getDuplicatesDFsByTable
    val selects = (target.keys(true) ++ target.incrementalColumns).distinct
    if (df.isEmpty) {
      spark.emptyDataFrame
    } else {
      if (keysOnly) {
        df.get.select(selects map col: _*)
          .orderBy(selects map col: _*)
      } else {
        df.get
          .orderBy(selects map col: _*)
      }
    }
  }
}

case class DupReport(tableName: String,
                     keys: Array[String],
                     incrementalColumns: Array[String],
                     dupCount: Long,
                     keysWithDups: Long,
                     totalRecords: Long,
                     pctKeysWithDups: Double,
                     pctDuplicateRecords: Double,
                     msg: String
                    )

case class ModuleTarget(module: Module, target: PipelineTable)

class Kitana(sourceWorkspace: Workspace, val snapWorkspace: Workspace, sourceDBName: String)
  extends ValidationUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private var _parallelism: Int = getDriverCores - 1
  private var _isRefresh: Boolean = false

  private def setRefresh(value: Boolean): this.type = {
    _isRefresh = value
    this
  }
  private def setParallelism(value: Option[Int]): this.type = {
    _parallelism = value.getOrElse(getDriverCores - 1)
    this
  }

  private def parallelism: Int = _parallelism

  private val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

  /**
   *
   * @param startDateString yyyy-MM-dd
   * @param endDateString   yyyy-MM-dd
   * @return
   */
  def executeBronzeSnapshot(
                             startDateString: String,
                             endDateString: String,
                             dtFormat: Option[String] = None
                           ): Dataset[SnapReport] = {
    val dtFormatFinal = getDateFormat(dtFormat)
    val startDate = Pipeline.deriveLocalDate(startDateString, dtFormatFinal)
    val endDate = Pipeline.deriveLocalDate(endDateString, dtFormatFinal)
    val fromTime = Pipeline.createTimeDetail(startDate.atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
    val untilTime = Pipeline.createTimeDetail(endDate.atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
    val daysToTest = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays.toInt
    _executeBronzeSnapshot(fromTime, untilTime, daysToTest)
  }

  /**
   * Execute snapshots
   *
   * @return
   */
  private def _executeBronzeSnapshot(
                                      snapFromTime: TimeTypes,
                                      snapUntilTime: TimeTypes,
                                      daysToSnap: Int
                                    ): Dataset[SnapReport] = {
    // state tables clone and reset for silver and gold modules

    val bronzeConfig = snapWorkspace.getConfig
    bronzeConfig.setPrimordialDateString(Some(snapFromTime.asDTString))
    bronzeConfig.setMaxDays(daysToSnap)

    logger.log(Level.INFO, s"BRONZE Snap: Primordial Date Overridden: ${bronzeConfig.primordialDateString}")
    logger.log(Level.INFO, s"BRONZE Snap: Max Days Overridden: ${bronzeConfig.maxDays}")
    val bronzeSnapWorkspace = snapWorkspace.copy(_config = bronzeConfig)
    val bronzePipeline = Bronze(bronzeSnapWorkspace, readOnly = true)
    val bronzeTargets = bronzePipeline.BronzeTargets

    validateSnapPipeline(sourceWorkspace, bronzePipeline, snapFromTime, snapUntilTime)

    snapStateTables(sourceDBName, taskSupport, bronzePipeline)
    resetPipelineReportState(bronzePipeline.pipelineStateTarget, bronzeConfig.primordialDateString.get, _isRefresh)

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
      .map(targetDetail => snapTable(sourceDBName, targetDetail.module, targetDetail.target))
      .toArray.toSeq.toDS
  }

  /**
   * Execute recalculations for silver and gold with code from this package
   *
   * @return
   */
  def executeSilverGoldRebuild(primordialPadding: Int = 7, maxDays: Int = 2): (Pipeline, Pipeline) = {

    val snapLookupPipeline = Bronze(snapWorkspace)
    val recalcConfig = snapWorkspace.getConfig
    recalcConfig.setMaxDays(maxDays)
    recalcConfig.setPrimordialDateString(Some(deriveRecalcPrimordial(snapLookupPipeline, primordialPadding)))

    val recalcWorkspace = snapWorkspace.copy(_config = recalcConfig)
    (Silver(recalcWorkspace, readOnly = true).run(), Gold(recalcWorkspace, readOnly = true).run())
  }

  /**
   * TODO - create generic version of this function
   * Probably can add an overload to pipeline instantiation initialize pipeline to a specific state
   * Issue for Kitana is that the bronze state needs to be current but silver/gold must be
   * current - 1.
   *
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
  def validateEquality(incrementalTest: Boolean): Dataset[ValidationReport] = {

    val silverPipeline = Silver(snapWorkspace, readOnly = true)
    val silverTargets = silverPipeline.SilverTargets
    val goldPipeline = Gold(snapWorkspace, readOnly = true)
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
      assertDataFrameDataEquals(targetDetail, sourceDBName, incrementalTest)
    }).toArray.toSeq.toDS()

  }

  def validateKeys(workspace: Workspace = snapWorkspace): Dataset[KeyReport] = {
    val targetsToValidate = getAllPipelineTargets(workspace).par
    targetsToValidate.tasksupport = taskSupport
    val keyValidationMessage = s"The targets that will be validated are:\n${targetsToValidate.map(_.tableFullName).mkString(", ")}"
    if (workspace.getConfig.debugFlag || snapWorkspace.getConfig.debugFlag) println(keyValidationMessage)
    logger.log(Level.INFO, keyValidationMessage)
    validateTargetKeys(targetsToValidate)
  }

  def identifyDups(
                    workspace: Workspace = snapWorkspace,
                    dfsByKeysOnly: Boolean = true
                  ): (Dataset[DupReport], Map[String, DataFrame]) = {
    val targetsToHunt = getAllPipelineTargets(workspace).par
    targetsToHunt.tasksupport = taskSupport
    val (dupReport, targetDupDetails) = dupHunter(targetsToHunt)

    val dupsDFByTarget = targetDupDetails.map(td => {
      td.target.name -> td.getDuplicatesDFForTable(keysOnly = dfsByKeysOnly)
    }).toMap

    (dupReport, dupsDFByTarget)
  }

  def refreshSnapshot(
                       startDateString: String,
                       endDateString: String,
                       dtFormat: Option[String] = None): Dataset[SnapReport] = {
    val sourceConfig = sourceWorkspace.getConfig // prod source
    val snapConfig = snapWorkspace.getConfig // existing bronze configs
    val sourceDBName = sourceConfig.databaseName
    val snapDBName = snapWorkspace.getConfig.databaseName
    val cloudProvider = snapWorkspace.getConfig.cloudProvider
    require(sourceDBName != snapDBName, "The source and snapshot databases cannot be the same. This function " +
      "will completely remove the bronze tables from the snapshot database. Be careful and ensure you have " +
      "identified the proper snapshot database name.\nEXITING")

//    val bronzePipeline = Bronze(snapWorkspace)
//    val bronzeTargets = bronzePipeline.getAllTargets.par
//    bronzeTargets.tasksupport = taskSupport
//    bronzeTargets.foreach(t => Helpers.fastDrop(t, cloudProvider))

//    val dropBronzeStateSQL = s"""delete from ${bronzePipeline.pipelineStateTarget.tableFullName} where moduleID < 2000"""
//    logger.log(Level.INFO, s"Dropping bronze states in snap db for refresh: $dropBronzeStateSQL")
//    spark.sql(dropBronzeStateSQL)

//    val pipelineStateTargetWithOverwrite = bronzePipeline.pipelineStateTarget
//      .copy(mode = "overwrite", withCreateDate = false, withOverwatchRunID = false)
//    val pipStatesLessBronze = bronzePipeline.getVerbosePipelineState.filterNot(_.moduleID < 2000)
//
//    bronzePipeline.database.write(
//      pipStatesLessBronze.toSeq.toDS.toDF(),
//      pipelineStateTargetWithOverwrite,
//      bronzePipeline.pipelineSnapTime.asColumnTS
//    )

    Kitana(snapDBName, sourceWorkspace)
      .setRefresh(true)
      .executeBronzeSnapshot(startDateString, endDateString, dtFormat)

  }

  def fullResetSnapDB(): Unit = {
    validateTargetDestruction(snapWorkspace.getConfig.databaseName)
    val targetsToDrop = getAllPipelineTargets(snapWorkspace).par
    targetsToDrop.tasksupport = taskSupport
    targetsToDrop.foreach(t => {
      Helpers.fastDrop(t, snapWorkspace.getConfig.cloudProvider)
    })
  }

  def validateSchemas = ???
  def validatePartCols = ???
  //TODO -- add spark.conf protection such that for resets the value of k/v has to be the target database name
  // the user wants to blow up.

}

object Kitana {

  def apply(
             snapDBName: String,
             workspace: Workspace,
             snapDBLocation: Option[String] = None,
             snapDBDataLocation: Option[String] = None,
             snapTokenSecret: Option[TokenSecret] = None,
             parallelism: Option[Int] = None
           ): Kitana = {
    val origConfig = workspace.getConfig
    val origWorkspaceParams = origConfig.inputConfig
    val origDataTarget = origWorkspaceParams.dataTarget
    val sourceDBName = origDataTarget.get.databaseName.get

    require(sourceDBName != snapDBName, "Source Overwatch database cannot be the same as " +
      "the snapshot target.")

    val snapDataTarget = DataTarget(
      databaseName = Some(snapDBName),
      databaseLocation = snapDBLocation,
      etlDataPathPrefix = snapDBDataLocation,
      consumerDatabaseName = None,
      consumerDatabaseLocation = None
    )

    val snapWorkspaceParams = origWorkspaceParams.copy(
      dataTarget = Some(snapDataTarget),
      tokenSecret = if (snapTokenSecret.nonEmpty) snapTokenSecret else origWorkspaceParams.tokenSecret
    )

    val snapArgs = objToJson(snapWorkspaceParams).compactString
    val snapWorkspace = Initializer(Array(snapArgs), debugFlag = origConfig.debugFlag, isSnap = true)
    new Kitana(workspace, snapWorkspace, sourceDBName)
      .setParallelism(parallelism)

  }
}

