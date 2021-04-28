package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, PipelineTable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions.{col, lit, rand}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object Upgrade extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  case class UpgradeStatus(db: String, tbl: String, actionTake: Option[String], errorMsg: Option[String])

  def bronzeTo021(workspace: Workspace, dryRun: Boolean = true): DataFrame = {
    logger.log(Level.INFO, "Beginning Bronze Upgrade")
    spark.conf.set("spark.sql.shuffle.partitions", 600)

    val parallelism = 12
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

    case class TargetVActual(target: PipelineTable, status: CatalogTable)
    val upgradeReport: ArrayBuffer[UpgradeStatus] = ArrayBuffer()
    val pipeline = Bronze(workspace)

    val pipelineReportTarget = PipelineTable(
      name = "pipeline_report",
      keys = Array("organization_id", "Overwatch_RunID"),
      config = pipeline.config,
      incrementalColumns = Array("Pipeline_SnapTS")
    )

    val bronzeTargets = (pipeline.getAllTargets.filter(_.exists) :+ pipelineReportTarget).par
    bronzeTargets.tasksupport = taskSupport
    val targetsWActuals = bronzeTargets.map(target => {
      val tbli = TableIdentifier(target.name, Some(target.databaseName))
      val tblMeta = spark.sessionState.catalog.getTableMetadata(tbli)
      TargetVActual(target, tblMeta)
    })


    targetsWActuals.foreach(toValidate => {
      val target = toValidate.target
      val status = toValidate.status
      try {
        val requiredPartitionColumns = target.partitionBy
        val actualPartitionColumns = status.partitionColumnNames
        var dfToWrite = spark.table(target.tableFullName)
        val actionsTaken: ArrayBuffer[String] = ArrayBuffer()
        var overwriteFlag = false

        dfToWrite = if (!status.schema.fieldNames.map(_.toLowerCase).contains("organization_id")) {
          actionsTaken.append("Added organization_id")
          overwriteFlag = true
          dfToWrite.withColumn("organization_id", lit(pipeline.config.organizationId))
        } else dfToWrite

        dfToWrite = if (target.partitionBy.contains("__overwatch_ctrl_noise") &&
          !status.schema.fieldNames.contains("__overwatch_ctrl_noise")
        ) {
          actionsTaken.append("Added __overwatch_ctrl_noise")
          overwriteFlag = true
          dfToWrite.withColumn("__overwatch_ctrl_noise", (rand() * lit(32)).cast("int"))
        } else dfToWrite

        dfToWrite = if (!requiredPartitionColumns.sameElements(actualPartitionColumns) && requiredPartitionColumns.nonEmpty) {
          actionsTaken.append(s"Added Missing Partitions: ${requiredPartitionColumns.diff(actualPartitionColumns).mkString(", ")}")
          overwriteFlag = true
          dfToWrite
            .repartition(requiredPartitionColumns map col: _*)
        } else dfToWrite

        if (overwriteFlag & target.partitionBy.nonEmpty) {
          if (!dryRun) {
            dfToWrite
              .repartition(requiredPartitionColumns map col: _*)
              .write.format("delta")
              .partitionBy(requiredPartitionColumns: _*)
              .mode("overwrite").option("overwriteSchema", "true")
              .saveAsTable(target.tableFullName)

            spark.sql(s"""optimize ${target.tableFullName}""")
          }
        } else {
          if (overwriteFlag && !dryRun) {
            dfToWrite
              .write.format("delta")
              .mode("overwrite").option("overwriteSchema", "true")
              .saveAsTable(target.tableFullName)

            spark.sql(s"""optimize ${target.tableFullName}""")
          }

        }

        val actionsTakenString = if (actionsTaken.toArray.isEmpty) "None" else (actionsTaken.toArray.mkString("\n"))
        upgradeReport.append(UpgradeStatus(target.databaseName, target.name, Some(actionsTakenString), None))
      } catch {
        case e: Throwable =>
          upgradeReport.append(UpgradeStatus(target.databaseName, target.name, None, Some(e.getMessage)))
      }
    })

    import spark.implicits._
    upgradeReport.toArray.toSeq.toDF()
  }

}
