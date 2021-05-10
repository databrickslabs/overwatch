package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Module, Pipeline, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
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

case class SchemaValidationReport(
                                   tableName: String,
                                   schemaTestType: String,
                                   requiredColumns: Seq[String],
                                   actualColumns: Seq[String],
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
  private var isRefresh: Boolean = false

  private def setRefresh(value: Boolean): this.type = {
    isRefresh = value
    this
  }

  private def setParallelism(value: Option[Int]): this.type = {
    _parallelism = value.getOrElse(getDriverCores - 1)
    this
  }

  private def parallelism: Int = _parallelism

  private val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

  def getBronzePipeline(workspace: Workspace = snapWorkspace, readOnly: Boolean = true): Bronze = Bronze(workspace, readOnly)

  def getSilverPipeline(workspace: Workspace = snapWorkspace, readOnly: Boolean = true): Silver = Silver(workspace, readOnly)

  def getGoldPipeline(workspace: Workspace = snapWorkspace, readOnly: Boolean = true): Gold = Gold(workspace, readOnly)

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
    try {
      _executeBronzeSnapshot(fromTime, untilTime, daysToTest)
    } catch {
      case e: BronzeSnapException =>
        Seq(e.snapReport).toDS()
    }
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

    validateSnapPipeline(sourceWorkspace, bronzePipeline, snapFromTime, snapUntilTime, isRefresh)

    val statefulSnapsReport = snapStateTables(sourceDBName, taskSupport, bronzePipeline)
    resetPipelineReportState(bronzePipeline.pipelineStateTarget, snapFromTime.asDTString, isRefresh)
    bronzePipeline.clearPipelineState() // clears states such that module fromTimes == primordial date

    // snapshots bronze tables, returns ds with report
    val bronzeTargetsWModule = getLinkedModuleTarget(bronzePipeline).par

    bronzeTargetsWModule.tasksupport = taskSupport

    val scopeSnaps = bronzeTargetsWModule
      .map(targetDetail => snapTable(sourceDBName, targetDetail.module, targetDetail.target, snapFromTime, snapUntilTime))
      .toArray.toSeq

    (statefulSnapsReport ++ scopeSnaps).toDS()
  }

  /**
   * Execute recalculations for silver and gold with code from this package
   *
   * @return
   */
  def executeSilverGoldRebuild(primordialPadding: Int = 7, maxDays: Int = 2): (Pipeline, Pipeline) = {

    val snapLookupPipeline = getBronzePipeline()

    val silverPipeline = getSilverPipeline(readOnly = false)
    val goldPipeline = getGoldPipeline(readOnly = false)

    // set primordial padding n days ahead of bronze primordial as per configured padding
    silverPipeline.config
      .setPrimordialDateString(Some(getPaddedPrimoridal(snapLookupPipeline, primordialPadding)))
      .setMaxDays(maxDays)
    goldPipeline.config
      .setPrimordialDateString(Some(getPaddedPrimoridal(snapLookupPipeline, primordialPadding)))
      .setMaxDays(maxDays)

    (silverPipeline.run(), goldPipeline.run())
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
    val goldPipeline = Gold(snapWorkspace, readOnly = true)
    rollbackPipelineState(silverPipeline)
    rollbackPipelineState(goldPipeline)

    val silverTargetsWModule = getLinkedModuleTarget(silverPipeline)

    val goldTargetsWModule = getLinkedModuleTarget(goldPipeline)

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
    val sourceDBName = sourceConfig.databaseName
    val snapDBName = snapWorkspace.getConfig.databaseName
    require(sourceDBName != snapDBName, "The source and snapshot databases cannot be the same. This function " +
      "will completely remove the bronze tables from the snapshot database. Be careful and ensure you have " +
      "identified the proper snapshot database name.\nEXITING")

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

  private def validatePartitionCols(target: PipelineTable): (PipelineTable, SchemaValidationReport) = {
    val catPartCols = target.catalogTable.partitionColumnNames.map(_.toLowerCase)
    val msg = if (catPartCols == target.partitionBy) "PASS" else s"FAIL: Partition Columns do not match"
    val report = SchemaValidationReport(
      target.tableFullName,
      "Partition Columns",
      catPartCols,
      target.partitionBy,
      msg
    )
    (target, report)
  }

  private def validateRequiredColumns(target: PipelineTable, schemaValidationReport: SchemaValidationReport): SchemaValidationReport = {
    val existingColumns = target.asDF.columns
    val requiredColumns = target.keys(true) ++ target.incrementalColumns ++ target.partitionBy
    val caseSensitive = spark.conf.getOption("spark.sql.caseSensitive").getOrElse("false").toBoolean
    val isError = !requiredColumns.forall(f => target.asDF.hasFieldNamed(f, caseSensitive))
    val msg = if (isError) "FAIL: Missing required columns" else "PASS"
    SchemaValidationReport(
      target.tableFullName,
      "Required Columns",
      requiredColumns,
      existingColumns,
      msg
    )

  }

  def validateSchemas(workspace: Workspace = snapWorkspace): Dataset[SchemaValidationReport] = {

    val bronzeModuleTargets = getLinkedModuleTarget(getBronzePipeline(workspace))
    val silverModuleTargets = getLinkedModuleTarget(getSilverPipeline(workspace))
    val goldModuleTargets = getLinkedModuleTarget(getGoldPipeline(workspace))
    val targetsInScope = (bronzeModuleTargets ++ silverModuleTargets ++ goldModuleTargets).map(_.target).par
    targetsInScope.tasksupport = taskSupport

    targetsInScope
      .map(validatePartitionCols)
      .map(x => validateRequiredColumns(x._1, x._2))
      .toArray.toSeq.toDS()
  }

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

