package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.{Initializer, Module, Pipeline, PipelineTable}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

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

  /**
   * Creates databases for snapshot and creates shallow clones for bronze in the snapshot database.
   * These snapshots are complete and rely on downstream filters to limit the data from the source table snap version.
   *
   * @return snap report dataset for summary of events
   */
  def executeBronzeSnapshot(): Dataset[SnapReport] = {

    val bronzeLookupPipeline = getBronzePipeline(workspace = sourceWorkspace)

    val snapPipeline = getBronzePipeline(readOnly = false)
    val primordialDateString = bronzeLookupPipeline.config.primordialDateString.get
    val fromDate = Pipeline.deriveLocalDate(primordialDateString, getDateFormat)
    val fromTime = Pipeline.createTimeDetail(fromDate.atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
//    val untilTime = Pipeline.createTimeDetail(fromTime.asLocalDateTime.plusDays(bronzeLookupPipeline.config.maxDays).toLocalDate
//      .atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli)
//    val untilTime = Pipeline.createTimeDetail(bronzeLookupPipeline.getPipelineState.values.toArray.maxBy(_.fromTS).fromTS)

    validateSnapDatabase(snapPipeline)
//    validateSnapPipeline(bronzeLookupPipeline, snapPipeline, fromTime, untilTime)

    snapPipeline.clearPipelineState() // clears states such that module fromTimes == primordial date

    val bronzeTargetsWModule = getLinkedModuleTarget(snapPipeline).par
    bronzeTargetsWModule.tasksupport = taskSupport

    val untilTime = bronzeTargetsWModule.toArray.maxBy(_.module.untilTime.asUnixTimeMilli).module.untilTime

    val statefulSnapsReport = snapStateTables(snapPipeline, untilTime)
    resetPipelineReportState(snapPipeline, bronzeTargetsWModule.toArray, fromTime, untilTime)

    // snapshots bronze tables, returns ds with report
    val scopeSnaps = bronzeTargetsWModule
      .map(targetDetail => snapTable(targetDetail.module, targetDetail.target, untilTime))
      .toArray.toSeq

    // set snapshot schema version == source schema version regardless of JAR
    val sourceSchemaVersion = SchemaTools.getSchemaVersion(sourceWorkspace.getConfig.databaseName)
    if (sourceSchemaVersion != snapWorkspace.getConfig.overwatchSchemaVersion) {
      val schemaVersionRollbackMsg = s"Rolling snapshot workspace back from version ${snapWorkspace.getConfig.overwatchSchemaVersion} " +
        s"to $sourceSchemaVersion"
      logger.log(Level.INFO, schemaVersionRollbackMsg)
      if (snapWorkspace.getConfig.debugFlag) println(schemaVersionRollbackMsg)
      SchemaTools.modifySchemaVersion(snapWorkspace.getConfig.databaseName, sourceSchemaVersion)
    }

    (statefulSnapsReport ++ scopeSnaps).toDS()
  }

  /**
   * Execute recalculations for silver and gold with code from this package
   * This function will build all target tables for active scopes commensurate with pipeline state table
   * "pipeline_report". On first run it will load from primordial date, subsequent runs will execute from
   * point left off allowing for multi-run validations.
   *
   * @param primordialPadding Days between bronze primordial date and silver primordial. This padding is strongly
   *                          recommended as there are several multi-dag lagging lookups in the codebase that
   *                          can result in incorrect data in the first few days of logs.
   * @param maxDays maximum number of days for which to load data
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

    val silverPipeline = getSilverPipeline(paddedWorkspace, readOnly = false, suppressStaticDatasets = false)
    silverPipeline.run()
    val goldPipeline = getGoldPipeline(paddedWorkspace, readOnly = false, suppressStaticDatasets = false)
    goldPipeline.run()

  }

  /**
   * Completes comparison between snapped and rebuilt silver/gold compared to the source/prod silver/gold to ensure
   * that current code base generates the same output given equal input.
   * @param primordialPadding Days to add to the silver/gold primordial dates from which to begin comparison between
   *                          snapped/derived and production silver/gold
   * @param tol allowable tolerance of misses as a percentage represented as a double between 0.0 and 1.0
   * @param maxDays Maximum number of days to validate. If null, all available data will be valiated
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

    val goldPipeline = getGoldPipeline(paddedWorkspace)
      .clearPipelineState()

    val silverTargetsWModule = getLinkedModuleTarget(silverPipeline)

    val goldTargetsWModule = getLinkedModuleTarget(goldPipeline)

    val targetsToValidate = (silverTargetsWModule ++ goldTargetsWModule).par
      .filter(_.target.exists)
    targetsToValidate.tasksupport = taskSupport

    targetsToValidate.map(targetDetail => {
      assertDataFrameDataEquals(targetDetail, sourceDBName, tol)
    }).toArray.toSeq.toDS()

  }

  /**
   * Validates keys through several tests and provides a report to show the status of each test
   * Test 1: are keys unique
   * Test 2: do any keys contain null values
   * @param workspace snapshot workspace, can be overridden
   * @return KeyReport -- results from the tests
   */
  def validateKeys(workspace: Workspace = snapWorkspace): Dataset[KeyReport] = {
    val nonDistinctTargets = Array("audit_log_bronze", "spark_events_bronze")
    val targetsToValidate = getAllKitanaTargets(workspace)
      .filterNot(t => nonDistinctTargets.contains(t.name))
      .par
    targetsToValidate.tasksupport = taskSupport
    val keyValidationMessage = s"The targets that will be validated are:\n${targetsToValidate.map(_.tableFullName).mkString(", ")}"
    if (workspace.getConfig.debugFlag || snapWorkspace.getConfig.debugFlag) println(keyValidationMessage)
    logger.log(Level.INFO, keyValidationMessage)
    validateTargetKeys(targetsToValidate)
  }

  /**
   * Searches all data for duplicate values by keys only or by entire record.
   * Keys are a composite of keys AND incremental columns search for true duplicates
   *
   * @param workspace snapshot workspace, can be overridden
   * @param dfsByKeysOnly whether or not to validate uniqueness by keys only (true) or entire record
   * @return
   */
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

  /**
   * CAUTION: Very dangerous function. Permanently deletes all tables and underlying data in snapshot database.
   * To protect user from accidentally deleting data from undesired targets, an external check is made to ensure a
   * spark config is set properly to allow removal of data from target database.
   * spark conf overwatch.permit.db.destruction MUST equal target database name where target database is the
   * kitana snapshot database
   */
  def fullResetSnapDB(): Unit = {
    validateTargetDestruction(snapWorkspace.getConfig.databaseName)
    fastDropTargets(getAllKitanaTargets(snapWorkspace, includePipelineStateTable = true).par)
  }

  private def validatePartitionCols(target: PipelineTable): SchemaValidationReport = {
    try {
      val catPartCols = target.catalogTable.partitionColumnNames
      val msg = if (catPartCols == target.partitionBy) "PASS" else s"FAIL: Partition Columns do not match"
      val report = SchemaValidationReport(
        target.tableFullName,
        "Partition Columns",
        catPartCols,
        target.partitionBy,
        msg
      )
      report
    } catch {
      case e: Throwable =>
        val report = SchemaValidationReport(
          target.tableFullName,
          "Required Columns",
          Array[String](),
          Array[String](),
          e.getMessage
        )
        report
    }
  }

  /**
   * Tests to ensure that all partition, incremental, and key columns and present in the schema
   * @param target
   * @return
   */
  private def validateRequiredColumns(target: PipelineTable): SchemaValidationReport = {
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

  /**
   * Validates key components of the schema such as:
   * presence of all partitions columns incremental columns, and key columns
   * @param workspace snapshot workspace, can be overridden
   * @return SchemaValidationReport
   */
  def validateSchemas(workspace: Workspace = snapWorkspace): Dataset[SchemaValidationReport] = {

    val targetsInScope = getAllKitanaTargets(workspace, includePipelineStateTable = true).par
    targetsInScope.tasksupport = taskSupport

    val partitionReport = targetsInScope
      .map(validatePartitionCols)
      .toArray

    val requiredColumnsReport = targetsInScope
      .map(validateRequiredColumns)
      .toArray

    (partitionReport ++ requiredColumnsReport).toSeq.toDS()
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
    val snapWorkspace = Initializer(snapArgs, debugFlag = origConfig.debugFlag, isSnap = true, disableValidations = false)

    new Kitana(workspace, snapWorkspace, sourceDBName, parallelism)

  }
}

