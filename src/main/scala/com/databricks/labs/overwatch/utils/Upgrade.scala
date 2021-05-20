package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, PipelineTable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions.{col, lit, rand}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object Upgrade extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._

  def upgrade0412(prodWorkspace: Workspace): Dataset[UpgradeReport] = {
    val bronzePipeline = Bronze(prodWorkspace)
    val jobsSnapshotTarget = bronzePipeline.getAllTargets.filter(_.name == "jobs_snapshot_bronze").head

    try {
    if (!jobsSnapshotTarget.exists) {
      val errMsg = s"The jobs_snapshot_bronze source does not exist in the workspace's configured database: " +
        s"${bronzePipeline.config.databaseName}. This upgrade is only necessary if this source was created by " +
        s"a version < 0.4.2"
      logger.log(Level.ERROR, errMsg)
      throw new UpgradeException(errMsg, jobsSnapshotTarget)
    } else {
      val jobSnapshotDF = jobsSnapshotTarget.asDF(withGlobalFilters = false)
      val cloudProvider = bronzePipeline.config.cloudProvider
      val outputDF = SchemaTools.scrubSchema(jobSnapshotDF)

      val changeInventory = Map[String, Column](
        "settings.new_cluster.custom_tags" -> SchemaTools.structToMap(outputDF, "settings.new_cluster.custom_tags"),
        "settings.new_cluster.spark_conf" -> SchemaTools.structToMap(outputDF, "settings.new_cluster.spark_conf"),
        "settings.new_cluster.spark_env_vars" -> SchemaTools.structToMap(outputDF, "settings.new_cluster.spark_env_vars"),
        s"settings.new_cluster.${cloudProvider}_attributes" -> SchemaTools.structToMap(outputDF, s"settings.new_cluster.${cloudProvider}_attributes"),
        "settings.notebook_task.base_parameters" -> SchemaTools.structToMap(outputDF, "settings.notebook_task.base_parameters")
      )

      val upgradedDF = outputDF.select(SchemaTools.modifyStruct(outputDF.schema, changeInventory): _*)
      val upgradedJobsSnapTarget = jobsSnapshotTarget.copy(
        mode = "overwrite",
        withCreateDate = false,
        withOverwatchRunID = false
      )
      prodWorkspace.database.write(upgradedDF, upgradedJobsSnapTarget, lit(null))

      Seq(UpgradeReport(jobsSnapshotTarget.databaseName, jobsSnapshotTarget.name, Some("SUCCESS"))).toDS()
    }

    } catch {
      case e: UpgradeException => Seq(e.getUpgradeReport).toDS()
      case e: Throwable =>
        logger.log(Level.ERROR, e.getMessage, e)
        Seq(UpgradeReport(jobsSnapshotTarget.databaseName, jobsSnapshotTarget.name, Some(e.getMessage))).toDS()
    }



  }

  // Placeholder for schema upgrade logic

}
