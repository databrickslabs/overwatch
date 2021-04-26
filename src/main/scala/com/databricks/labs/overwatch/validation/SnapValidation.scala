package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Pipeline, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{count, lit}

case class SnapReport(tableFullName: String,
                      from: java.sql.Timestamp,
                      until: java.sql.Timestamp,
                      totalCount: Long,
                      errorMessage: String)

case class ValidationReport(tableSourceName: String,
                            tableSnapName: String,
                            from: java.sql.Timestamp,
                            until: java.sql.Timestamp,
                            totalDiscrepancies: Long,
                            errorMessage: String)

case class SnapValidationParams(snapDatabaseName: String,
                                sourceDatabaseName: String,
                                primordialDateString: String,
                                maxDaysToLoad: Int)

class SnapValidation(params: SnapValidationParams,
                     config: OverwatchParams,
                     workspace: Workspace) extends ValidationUtils {

  import spark.implicits._

  /**
   * Start and end time calculations
   * TODO: check addOneTick, why are calculations taking place after startcompare + 1 day?
   */
  val startCompareMillis = TimeTypesConstants.dtFormat.parse(config.primordialDateString.get).getTime()
  val endCompareMillis = startCompareMillis + config.maxDaysToLoad * 86400000

  val startCompare = Pipeline.createTimeDetail(startCompareMillis + 86400000).asColumnTS
  val endCompare = Pipeline.createTimeDetail(endCompareMillis).asColumnTS

  /**
   * Compares data between original source tables and tables recreates from snapshot
   *
   * @param target
   * @param snapDatabaseName
   * @param sourceDatabaseName
   * @return ValidateReport
   */
  protected def compareTable(target: PipelineTable): ValidationReport = {
    val databaseTable = s"${params.snapDatabaseName}.${target.name}"
    val tableName = s"${params.sourceDatabaseName}.${target.name}"

    try {
      val baseDF = spark.table(tableName)
      val filteredDF = if (target.incrementalColumns.isEmpty) {
        baseDF
      } else {
        target.incrementalColumns.foldLeft(baseDF)((df, incColName) =>
          df.filter(tsFilter(colToTS(df, incColName), startCompare, endCompare))
        )
      }

      val df1: DataFrame = filteredDF
        .drop('__overwatch_ctrl_noise)
        .drop('Pipeline_SnapTS)
        .drop('Overwatch_RunID)

      val df2: DataFrame = spark.table(databaseTable)
        .drop('__overwatch_ctrl_noise)
        .drop('Pipeline_SnapTS)
        .drop('Overwatch_RunID)

      df1.except(df2)
        .union(df2.except(df1))
        .dropDuplicates()
        .select(
          lit(tableName).alias("tableSourceName"),
          lit(databaseTable).alias("tableSnapName"),
          startCompare.alias("from"),
          endCompare.alias("until"),
          count("*").alias("totalDiscrepancies"),
          lit(null).cast("string").alias("errorMessage")
        ).as[ValidationReport]
        .first()

    } catch {
      case e: Throwable =>
        println(s"FAILED: ${databaseTable} --> ${e.getMessage}")
        ValidationReport(tableName, databaseTable, tsTojsql(startCompare), tsTojsql(endCompare), 0L, e.getMessage)
    }

  }

  /**
   * Snapshots table
   *
   * @param target
   * @param params
   * @return
   */
  protected def snapTable(target: PipelineTable): SnapReport = {
    val databaseTable = s"${params.snapDatabaseName}.${target.name}"
    val tableName = s"${params.sourceDatabaseName}.${target.name}"

    try {
      val baseDF = spark.table(tableName)
      val filteredDF = if (target.incrementalColumns.isEmpty) {
        baseDF
      } else {
        target.incrementalColumns.foldLeft(baseDF)((df, incColName) =>
          df.filter(tsFilter(colToTS(df, incColName), startCompare, endCompare))
        )
      }

      filteredDF
        .repartition()
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(databaseTable)

      println(s"SNAPPED: ${target.tableFullName}")

      val snappedDF = spark.read.table(databaseTable)
      snappedDF
        .select(
          lit(target.tableFullName).alias("tableFullName"),
          startCompare.alias("from"),
          endCompare.alias("until"),
          count("*").alias("totalCount"),
          lit(null).cast("string").alias("errorMessage")
        ).as[SnapReport]
        .first()
    } catch {
      case e: Throwable =>
        println(s"FAILED: ${target.tableFullName} --> ${e.getMessage}")
        SnapReport(target.tableFullName, tsTojsql(startCompare), tsTojsql(endCompare), 0L, e.getMessage)
    }
  }

  protected def snapStateTables() = {
    val uniqueTablesToClone =
      Array("pipeline_report","spark_events_processedfiles").map(t => TableIdentifier(t, Some(params.sourceDatabaseName)))

    uniqueTablesToClone.foreach(tbli => {
      if (spark.sessionState.catalog.tableExists(tbli)) {
        val stmt = s"CREATE TABLE ${params.snapDatabaseName}.${tbli.table} DEEP CLONE ${tbli.database.get}.${tbli.table}"
        println(stmt)
        spark.sql(stmt)
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
    snapStateTables()
    resetPipelineReportState(params.snapDatabaseName)

    // snapshots bronze tables, returns ds with report
    val bronzeTargets = Bronze(workspace).getAllTargets
    bronzeTargets
      .map(bronzeTarget => snapTable(bronzeTarget))
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
  def equalityReport() = {
    val targets = Silver(workspace).getAllTargets ++ Gold(workspace).getAllTargets

    targets.map(t => compareTable(t)).toArray.toSeq.toDS
  }
}

object SnapValidation {
  def apply(params: SnapValidationParams): SnapValidation = {

    /**
     * create config environment for overwatch
     * TODO: review datatarget
     * TODO: hardcoded for azure!
     */
    val dataTarget = DataTarget(
      Some(params.snapDatabaseName), Some(s"dbfs:/user/hive/warehouse/${params.snapDatabaseName}.db"),
      Some(params.snapDatabaseName), Some(s"dbfs:/user/hive/warehouse/${params.snapDatabaseName}.db")
    )

    val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = "", eventHubName = "", auditRawEventsPrefix = "")

    val overwatchParams = OverwatchParams(
      auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
      dataTarget = Some(dataTarget),
      badRecordsPath = None,
      overwatchScope = Some("audit,accounts,jobs,sparkEvents,clusters,clusterEvents,notebooks,pools".split(",")),
      maxDaysToLoad = params.maxDaysToLoad,
      primordialDateString = Some(params.primordialDateString)
    )

    val args = objToJson(overwatchParams).compactString

    val workspace = if (args.length != 0) {
      Initializer(Array(args), debugFlag = true)
    } else {
      Initializer(Array())
    }

    // creates object
    new SnapValidation(params, overwatchParams, workspace)
  }
}

