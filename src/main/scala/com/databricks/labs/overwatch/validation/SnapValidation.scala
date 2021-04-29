package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Pipeline, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.JsonUtils.objToJson
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{count, lit}
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

case class SnapReport(tableFullName: String,
                      from: java.sql.Timestamp,
                      until: java.sql.Timestamp,
                      totalCount: Long,
                      errorMessage: String)

case class ValidationReport(tableSourceName: String,
                            tableSourceCount: Long,
                            tableSnapName: String,
                            tableSnapCount: Long,
                            from: java.sql.Timestamp,
                            until: java.sql.Timestamp,
                            totalDiscrepancies: Long,
                            message: String)

case class SnapValidationParams(snapDatabaseName: String,
                                sourceDatabaseName: String,
                                primordialDateString: String,
                                maxDaysToLoad: Int,
                                parallelism: Int)

class SnapValidation(params: SnapValidationParams,
                     config: OverwatchParams,
                     workspace: Workspace) extends ValidationUtils {

  import spark.implicits._

  val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(params.parallelism))

  /**
   * Start and end time calculations
   * TODO: check addOneTick, why are calculations taking place after startcompare + 1 day?
   */
  var startCompareMillis = TimeTypesConstants.dtFormat.parse(config.primordialDateString.get).getTime()
  var endCompareMillis = startCompareMillis + config.maxDaysToLoad.toLong * 86400000L

  var startSnap = Pipeline.createTimeDetail(startCompareMillis).asColumnTS
  var startCompare = startSnap //Pipeline.createTimeDetail(startCompareMillis + 86400000L).asColumnTS
  var endCompare = Pipeline.createTimeDetail(endCompareMillis).asColumnTS
  println(s"DEBUG: Creating interval variables: ${startCompareMillis} to ${endCompareMillis}, ${config.maxDaysToLoad} days to load")

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

    // drops columns not to be checked - some columns/structs are not always present, so we might want to avoid
    // errors with field ordering, etc. others are random columns, so should be always dropped in all targets.
    def dropUnecessaryCols(df: DataFrame, targetName: String) = {
      val commonColumnsToDrop = Array("__overwatch_ctrl_noise", "Pipeline_SnapTS", "Overwatch_RunID", "__overwatch_incremental_col")

      (targetName match {
        case "spark_tasks_silver" => df.drop('TaskEndReason)
        case "sparkTask_gold" => df.drop('task_end_reason)
        case "clusterStateFact_gold" => df.drop('driverSpecs).drop('workerSpecs)
        case _ => df
      }).drop(commonColumnsToDrop: _*)
    }

    try {
      // take df1 from target (recalculated from snap), df2 from original table
      val df1 = dropUnecessaryCols(getFilteredDF(target, tableName, startCompare, endCompare), target.name)
      val df2 = dropUnecessaryCols(spark.table(databaseTable), target.name)

      val count1 = df1.cache.count
      val count2 = df2.cache.count

      val returndf = df1.except(df2)
        .union(df2.except(df1))
        .dropDuplicates()
        .select(
          lit(tableName).alias("tableSourceName"),
          lit(count1).alias("tableSourceCount"),
          lit(databaseTable).alias("tableSnapName"),
          lit(count2).alias("tableSnapCount"),
          startCompare.alias("from"),
          endCompare.alias("until"),
          count("*").alias("totalDiscrepancies"),
          lit("processing ok").cast("string").alias("message")
        ).as[ValidationReport]
        .first()

      df1.unpersist()
      df2.unpersist()

      returndf
    } catch {
      case e: Throwable =>
        println(s"FAILED: ${databaseTable} --> ${e.getMessage}")
        ValidationReport(tableName, 0L, databaseTable, 0L, tsTojsql(startCompare), tsTojsql(endCompare), 0L, e.getMessage)
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
      getFilteredDF(target, tableName, startSnap, endCompare)
        .repartition()
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(databaseTable)

      println(s"SNAPPED: ${target.tableFullName}")

      spark.read.table(databaseTable)
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
    val bronzeTargets = (Bronze(workspace).getAllTargets).par
    bronzeTargets.tasksupport = taskSupport

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
    val targets = (Silver(workspace).getAllTargets ++ Gold(workspace).getAllTargets).par
    targets.tasksupport = taskSupport

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

