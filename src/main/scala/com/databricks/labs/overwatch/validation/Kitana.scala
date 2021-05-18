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

class Kitana(sourceWorkspace: Workspace, val snapWorkspace: Workspace, sourceDBName: String, parallelism: Option[Int])
  extends ValidationUtils(sourceDBName, snapWorkspace, parallelism) {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private var isRefresh: Boolean = false
  private var isCompleteRefresh: Boolean = false

  private def setRefresh(value: Boolean): this.type = {
    isRefresh = value
    this
  }

  private def setCompleteRefresh(value: Boolean): this.type = {
    isCompleteRefresh = value
    this
  }

  //  /**
  //   *
  //   * @param startDateString yyyy-MM-dd
  //   * @param endDateString   yyyy-MM-dd
  //   * @return
  //   */
  //  def executeBronzeSnapshot(
  //                             startDateString: String,
  //                             endDateString: String,
  //                             dtFormat: Option[String] = None
  //                           ): Dataset[SnapReport] = {
  //    val dtFormatFinal = getDateFormat(dtFormat)
  //    val startDate = Pipeline.deriveLocalDate(startDateString, dtFormatFinal)
  //    val endDate = Pipeline.deriveLocalDate(endDateString, dtFormatFinal)
  //    val fromTime = Pipeline.createTimeDetail(startDate.atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
  //    val untilTime = Pipeline.createTimeDetail(endDate.atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
  //    val daysToTest = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays.toInt
  //    try {
  //      _executeBronzeSnapshot(fromTime, untilTime, daysToTest)
  //    } catch {
  //      case e: BronzeSnapException =>
  //        Seq(e.snapReport).toDS()
  //    }
  //  }

  /**
   * Execute snapshots
   *
   * @return
   */
  def executeBronzeSnapshot(): Dataset[SnapReport] = {

    val bronzeLookupPipeline = if (isRefresh) getBronzePipeline(workspace = snapWorkspace)
    else getBronzePipeline(workspace = sourceWorkspace)

    val snapPipeline = getBronzePipeline(readOnly = false)
    val primordialDateString = bronzeLookupPipeline.config.primordialDateString.get
    val fromDate = Pipeline.deriveLocalDate(primordialDateString, getDateFormat)
    val fromTime = Pipeline.createTimeDetail(fromDate.atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
    val BREAK1 = bronzeLookupPipeline.getPipelineState.values.toArray
    val BREAK2 = bronzeLookupPipeline.getPipelineState.values.toArray.maxBy(_.fromTS).fromTS
    val untilTime = Pipeline.createTimeDetail(bronzeLookupPipeline.getPipelineState.values.toArray.maxBy(_.fromTS).fromTS)


    validateSnapPipeline(bronzeLookupPipeline, snapPipeline, fromTime, untilTime, isRefresh)

    snapPipeline.clearPipelineState() // clears states such that module fromTimes == primordial date

    val bronzeTargetsWModule = getLinkedModuleTarget(snapPipeline).par
    bronzeTargetsWModule.tasksupport = taskSupport

    val statefulSnapsReport = snapStateTables(snapPipeline, untilTime)
    resetPipelineReportState(
      snapPipeline, bronzeTargetsWModule.toArray, fromTime, untilTime, isRefresh, isCompleteRefresh
    )

    // snapshots bronze tables, returns ds with report
    val scopeSnaps = bronzeTargetsWModule
      .map(targetDetail => snapTable(targetDetail.module, targetDetail.target, untilTime))
      .toArray.toSeq

    (statefulSnapsReport ++ scopeSnaps).toDS()
  }

  /**
   * Execute recalculations for silver and gold with code from this package
   *
   * @return
   */
  def executeSilverGoldRebuild(primordialPadding: Int = 7, maxDays: Option[Int] = None): Unit = {

    // set primordial padding n days ahead of bronze primordial as per configured padding
    val snapLookupPipeline = getBronzePipeline(snapWorkspace)


    if (primordialPadding < 7) {
      val msg = s"WARNING!!  The padding has been set to less than 7 days. This can misrepresent the accuracy of the " +
        s"pipeline as the pipeline often requires >= 7 days to become accurate. The stateful calculations prior to " +
        s"the primordial date are not available during the comparison.\nFor best results please use padding of at " +
        s"least 7 days --> RECOMMENDED 30 days."
      println(msg)
      logger.log(Level.WARN, msg)
    }
    val paddedWorkspace = padPrimordialAndSetMaxDays(snapLookupPipeline, primordialPadding, maxDays)

    val silverPipeline = getSilverPipeline(paddedWorkspace, readOnly = false)
    val BREAKState1 = silverPipeline.getConfig
    silverPipeline.run()
    val goldPipeline = getGoldPipeline(paddedWorkspace, readOnly = false)
    val BREAKState2 = goldPipeline.getConfig
    goldPipeline.run()

  }

  /**
   *
   * @param incrementalTest
   * @return
   */
  def validateEquality(
                        primordialPadding: Int = 7,
                        tol: Double = 0D,
                        maxDays: Option[Int] = None
                      ): Dataset[ValidationReport] = {

    val snapLookupPipeline = getBronzePipeline(snapWorkspace)
    val paddedWorkspace = padPrimordialAndSetMaxDays(snapLookupPipeline, primordialPadding, maxDays)

    val silverPipeline = getSilverPipeline(paddedWorkspace)
      .clearPipelineState()

    val BREAKState1 = silverPipeline.getPipelineState

    val goldPipeline = getGoldPipeline(paddedWorkspace)
      .clearPipelineState()
    //    rollbackPipelineState(silverPipeline)

    val BREAKState2 = silverPipeline.getPipelineState
    //    rollbackPipelineState(goldPipeline)

    val silverTargetsWModule = getLinkedModuleTarget(silverPipeline)

    val goldTargetsWModule = getLinkedModuleTarget(goldPipeline)

    val targetsToValidate = (silverTargetsWModule ++ goldTargetsWModule).par
      .filter(_.target.exists)
    targetsToValidate.tasksupport = taskSupport

    targetsToValidate.map(targetDetail => {
      assertDataFrameDataEquals(targetDetail, sourceDBName, tol)
    }).toArray.toSeq.toDS()

  }

  def validateKeys(workspace: Workspace = snapWorkspace): Dataset[KeyReport] = {
    val targetsToValidate = getAllKitanaTargets(workspace).par
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
    val targetsToIgnore = Array("audit_log_bronze", "spark_events_bronze")
    val targetsToHunt = getAllKitanaTargets(workspace).filterNot(t => targetsToIgnore.contains(t.name)).par
    targetsToHunt.tasksupport = taskSupport
    val (dupReport, targetDupDetails) = dupHunter(targetsToHunt)

    val dupsDFByTarget = targetDupDetails.map(td => {
      td.target.name -> td.getDuplicatesDFForTable(keysOnly = dfsByKeysOnly)
    }).toMap

    (dupReport, dupsDFByTarget)
  }

  def refreshSnapshot(completeRefresh: Boolean = false): Dataset[SnapReport] = {
    val sourceConfig = sourceWorkspace.getConfig // prod source
    val sourceDBName = sourceConfig.databaseName
    val snapDBName = snapWorkspace.getConfig.databaseName
    require(sourceDBName != snapDBName, "The source and snapshot databases cannot be the same. This function " +
      "will completely remove the bronze tables from the snapshot database. Be careful and ensure you have " +
      "identified the proper snapshot database name.\nEXITING")

    Kitana(snapDBName, sourceWorkspace)
      .setRefresh(true)
      .setCompleteRefresh(completeRefresh)
      .executeBronzeSnapshot()


  }

  def fullResetSnapDB(): Unit = {
    validateTargetDestruction(snapWorkspace.getConfig.databaseName)
    fastDropTargets(getAllKitanaTargets(snapWorkspace, includePipelineStateTable = true).par)
  }

  private def validatePartitionCols(target: PipelineTable): (PipelineTable, SchemaValidationReport) = {
    try {
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
    } catch {
      case e: Throwable =>
        val report = SchemaValidationReport(
          target.tableFullName,
          "Required Columns",
          Array[String](),
          Array[String](),
          e.getMessage
        )
        (target, report)
    }
  }

  private def validateRequiredColumns(target: PipelineTable, schemaValidationReport: SchemaValidationReport): SchemaValidationReport = {
    try {
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
    } catch {
      case e: Throwable =>
        SchemaValidationReport(
          target.tableFullName,
          "Required Columns",
          Array[String](),
          Array[String](),
          e.getMessage
        )
    }

  }

  def validateSchemas(workspace: Workspace = snapWorkspace): Dataset[SchemaValidationReport] = {

    val targetsInScope = getAllKitanaTargets(workspace, includePipelineStateTable = true).par
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
    new Kitana(workspace, snapWorkspace, sourceDBName, parallelism)

  }
}

