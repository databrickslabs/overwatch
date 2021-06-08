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

  private def getSchemaVersion(dbName: String): String = {
    val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
    dbMeta.properties.getOrElse("SCHEMA", "UNKNOWN")
  }

  private def upgradeSchema(dbName: String, upgradeToVersion: String): Unit = {
    val existingSchemaVersion = getSchemaVersion(dbName)
    val upgradeStatement =
      s"""ALTER DATABASE $dbName SET DBPROPERTIES
         |(SCHEMA=$upgradeToVersion)""".stripMargin

    val upgradeMsg = s"upgrading schema from $existingSchemaVersion --> $upgradeToVersion with STATEMENT:\n " +
      s"$upgradeStatement"
    logger.log(Level.INFO, upgradeMsg)
    spark.sql(upgradeStatement)
    val newSchemaVersion = getSchemaVersion(dbName)
    assert(newSchemaVersion == upgradeToVersion)
    logger.log(Level.INFO, s"UPGRADE SUCCEEDED")

  }

  def upgrade0412(prodWorkspace: Workspace): Dataset[UpgradeReport] = {

    prodWorkspace.getConfig.setOverwatchSchemaVersion(getSchemaVersion(prodWorkspace.getConfig.databaseName))
    val bronzePipeline = Bronze(prodWorkspace)
    val jobsSnapshotTarget = bronzePipeline.getAllTargets.filter(_.name == "jobs_snapshot_bronze").head
    val clusterSnapshotTarget = bronzePipeline.getAllTargets.filter(_.name == "clusters_snapshot_bronze").head
    val cloudProvider = bronzePipeline.config.cloudProvider

    try {
      // Jobs Table
    if (!jobsSnapshotTarget.exists && !clusterSnapshotTarget.exists) {
      val errMsg = s"Neither the jobs_snapshot_bronze nor the clusters_snapshot_bronze sources exist in the workspace's configured database: " +
        s"${bronzePipeline.config.databaseName}. This upgrade is only necessary if this source was created by " +
        s"a version < 0.4.12"
      logger.log(Level.ERROR, errMsg)
      throw new UpgradeException(errMsg, jobsSnapshotTarget)
    } else {
      val jobSnapshotDF = jobsSnapshotTarget.asDF(withGlobalFilters = false)
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

      // clusters table
      if (cloudProvider != "azure") {
        val errMsg = s"cluster_snapshot schema changes not present. Only present for Azure workspaces. Skipping."
        logger.log(Level.ERROR, errMsg)
      } else {
        val clusterSnapshotDF = SchemaTools.scrubSchema(clusterSnapshotTarget.asDF(withGlobalFilters = false))
        val newClusterSnap = clusterSnapshotDF
          .withColumn("azure_attributes", SchemaTools.structToMap(clusterSnapshotDF, "azure_attributes"))
        val upgradeClusterSnapTarget = clusterSnapshotTarget.copy(
          mode = "overwrite",
          withCreateDate = false,
          withOverwatchRunID = false
        )
        prodWorkspace.database.write(newClusterSnap, upgradeClusterSnapTarget, lit(null))
      }

      upgradeSchema(jobsSnapshotTarget.databaseName, "0.412")
      Seq(
        UpgradeReport(jobsSnapshotTarget.databaseName, jobsSnapshotTarget.name, Some("SUCCESS"))
      ).toDS()
    }

    } catch {
      case e: UpgradeException => Seq(e.getUpgradeReport).toDS()
      case e: Throwable =>
        logger.log(Level.ERROR, e.getMessage, e)
        Seq(UpgradeReport(jobsSnapshotTarget.databaseName, jobsSnapshotTarget.name, Some(e.getMessage))).toDS()
    }



  }

  def upgrade042(workspace: Workspace): Dataset[UpgradeReport] = ???

    // call previous upgrades if not coming from 0412
    // upgrade schema for jrcp_gold
    // drop instanceDetails

  // Placeholder for schema upgrade logic

}
