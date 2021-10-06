package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.PipelineFunctions.getPipelineTarget
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Silver}
import com.databricks.labs.overwatch.utils.Helpers.fastDrop
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}

import scala.collection.mutable.ArrayBuffer

object Upgrade extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private def persistPipelineStateChange(db: String, moduleIds: Array[Int], asOf: Option[java.sql.Timestamp] = None): Unit = {
    val logMsg = s"ROLLING BACK STATE: Modules ${moduleIds.mkString(",")} are rolling back"
    println(logMsg)
    logger.log(Level.INFO, logMsg)

    val baseUpdateSQL = s"update ${db}.pipeline_report set status = 'UPGRADED' " +
      s"where moduleID in (${moduleIds.mkString(", ")}) " +
      s"and (status = 'SUCCESS' or status like 'EMPT%') "
    val updateStatement = if (asOf.nonEmpty) {
      baseUpdateSQL + s"and Pipeline_SnapTS > '${asOf.get.toString}' "
    } else baseUpdateSQL

    spark.sql(updateStatement)
  }

  private def getNumericalSchemaVersion(version: String): Int = {
    version.split("\\.").reverse.head.toInt
  }

  @throws(classOf[UpgradeException])
  private def validateSchemaUpgradeEligibility(currentVersion: String, targetVersion: String): Unit = {
    var valid = true
    val currentNumericalVersion = getNumericalSchemaVersion(currentVersion)
    val targetNumericalVersion = getNumericalSchemaVersion(targetVersion)
    valid = targetNumericalVersion > currentNumericalVersion
    require(valid,
      s"This binary produces schema version $currentVersion. The Overwatch assets are registered as " +
        s"$targetVersion. This upgrade is meant to upgrade schemas below $targetVersion to $targetVersion schema. This is not a " +
        s"valid upgrade."
    )
  }

  /**
   * Upgrade to 0412 schema workspace from prior version for tables:
   * jobs_snapshot_bronze & clusters_snapshot_bronze
   * @param prodWorkspace production workspace defined in production pipeline run configuration
   * @return
   */
  def upgradeTo0412(prodWorkspace: Workspace): Dataset[UpgradeReport] = {
    val currentSchemaVersion = SchemaTools.getSchemaVersion(prodWorkspace.getConfig.databaseName)
    val targetSchemaVersion = "0.412"
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)
    val bronzePipeline = Bronze(prodWorkspace, readOnly = true, suppressStaticDatasets = true, suppressReport = true)
    val jobsSnapshotTarget = bronzePipeline.getAllTargets.filter(_.name == "jobs_snapshot_bronze").head
    val clusterSnapshotTarget = bronzePipeline.getAllTargets.filter(_.name == "clusters_snapshot_bronze").head
    val cloudProvider = bronzePipeline.config.cloudProvider
    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()

    try {
      // Jobs Table
      if (!jobsSnapshotTarget.exists && !clusterSnapshotTarget.exists) {
        val errMsg = s"Neither the jobs_snapshot_bronze nor the clusters_snapshot_bronze sources exist in the workspace's configured database: " +
          s"${bronzePipeline.config.databaseName}. This upgrade is only necessary if this source was created by " +
          s"a version < 0.4.12"
        logger.log(Level.ERROR, errMsg)
        upgradeStatus.append(UpgradeReport(jobsSnapshotTarget.databaseName, jobsSnapshotTarget.name, Some(errMsg)))
        upgradeStatus.toDS()
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
          _mode = WriteMode.overwrite,
          withCreateDate = false,
          withOverwatchRunID = false
        )
        prodWorkspace.database.write(upgradedDF, upgradedJobsSnapTarget, lit(null).cast("timestamp"))
        upgradeStatus.append(UpgradeReport(jobsSnapshotTarget.databaseName, jobsSnapshotTarget.name, Some("SUCCESS")))

        // clusters table
        if (cloudProvider != "azure") {
          val errMsg = s"cluster_snapshot schema changes not present. Only present for Azure workspaces. Skipping."
          logger.log(Level.ERROR, errMsg)
          upgradeStatus.append(UpgradeReport(clusterSnapshotTarget.databaseName, clusterSnapshotTarget.name, Some(errMsg)))
        } else {
          val clusterSnapshotDF = SchemaTools.scrubSchema(clusterSnapshotTarget.asDF(withGlobalFilters = false))
          val newClusterSnap = clusterSnapshotDF
            .withColumn("azure_attributes", SchemaTools.structToMap(clusterSnapshotDF, "azure_attributes"))
          val upgradeClusterSnapTarget = clusterSnapshotTarget.copy(
            _mode = WriteMode.overwrite,
            withCreateDate = false,
            withOverwatchRunID = false
          )
          prodWorkspace.database.write(newClusterSnap, upgradeClusterSnapTarget, lit(null).cast("timestamp"))
          upgradeStatus.append(UpgradeReport(clusterSnapshotTarget.databaseName, clusterSnapshotTarget.name, Some("SUCCESS")))
        }

        SchemaTools.modifySchemaVersion(jobsSnapshotTarget.databaseName, targetSchemaVersion)
        upgradeStatus.toDS()
      }

    } catch {
      case e: UpgradeException => Seq(e.getUpgradeReport).toDS()
      case e: Throwable =>
        logger.log(Level.ERROR, e.getMessage, e)
        Seq(UpgradeReport("", "", Some(e.getMessage))).toDS()
    }


  }

  /**
   * Upgrade to 0420 schema workspace from prior version for tables:
   * instanceDetails & jobruncostpotentialfact
   * @param prodWorkspace production workspace defined in production pipeline run configuration
   * @param isLocalUpgrade
   * @return
   */
  def upgradeTo042(prodWorkspace: Workspace, isLocalUpgrade: Boolean = false): Dataset[UpgradeReport] = {
    val currentSchemaVersion = SchemaTools.getSchemaVersion(prodWorkspace.getConfig.databaseName)
    val targetSchemaVersion = "0.420"

    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    if (getNumericalSchemaVersion(currentSchemaVersion) < 412) {
      val logMsg = s"SCHEMA UPGRADE: Current Schema is < 0.412, attempting to step-upgrade the Schema"
      println(logMsg)
      logger.log(Level.INFO, logMsg)
      upgradeStatus.appendAll(upgradeTo0412(prodWorkspace).collect())
    }
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)

    try {
      val bronzePipeline = Bronze(prodWorkspace, readOnly = true, suppressStaticDatasets = true, suppressReport = true)
      val snapDate = bronzePipeline.pipelineSnapTime.asDTString
      val w = Window.partitionBy(lower(trim('API_name))).orderBy('activeFrom)
      val wKeyCheck = Window.partitionBy(lower(trim('API_Name)), 'activeFrom, 'activeUntil).orderBy('activeFrom, 'activeUntil)

      // UPGRADE InstanceDetails
      val instanceDetailsTarget = bronzePipeline.getAllTargets.filter(_.name == "instanceDetails").head
      logger.log(Level.INFO, s"Upgrading ${instanceDetailsTarget.tableFullName}")
      if (instanceDetailsTarget.exists) {
        val upgradedInstanceDetails = instanceDetailsTarget.asDF(withGlobalFilters = isLocalUpgrade)
          .withColumn("activeFrom", lit(prodWorkspace.getConfig.primordialDateString.get).cast("date"))
          .withColumn("activeUntil", lit(null).cast("date"))
          .withColumn("sqlComputeDBUPrice", lit(prodWorkspace.getConfig.contractSQLComputeDBUPrice))
          .withColumn("jobsLightDBUPrice", lit(prodWorkspace.getConfig.contractJobsLightDBUPrice))
          .withColumn("activeUntil", coalesce('activeUntil, lit(snapDate)))
          .withColumn("previousUntil", lag('activeUntil, 1).over(w))
          .withColumn("rnk", rank().over(wKeyCheck))
          .withColumn("rn", row_number().over(wKeyCheck))
          .withColumn("isValid", when('previousUntil.isNull, lit(true)).otherwise(
            'activeFrom === 'previousUntil
          ))
          .filter('isValid && 'rnk === 1 && 'rn === 1)
          .withColumn("activeUntil", lit(null).cast("date"))
          .drop("previousUntil", "isValid", "rnk", "rn")

        val upgradeTarget = instanceDetailsTarget.copy(
          _mode = WriteMode.overwrite,
          withCreateDate = false,
          withOverwatchRunID = false
        )

        bronzePipeline.database.write(upgradedInstanceDetails, upgradeTarget, lit(null).cast("timestamp"))
        upgradeStatus.append(UpgradeReport(prodWorkspace.getConfig.databaseName, upgradeTarget.name, Some("SUCCESS")))
      } else {
        val errMsg = s"${instanceDetailsTarget.tableFullName} does not exist. There's no need to upgrade this table " +
          s"since it didn't already exist. It will be created on the next run. Validate that the underlying ETL " +
          s"Data Path is empty and re-run your pipeline."
        logger.log(Level.WARN, errMsg)
        upgradeStatus.append(UpgradeReport(prodWorkspace.getConfig.databaseName, instanceDetailsTarget.name, Some(errMsg)))
      }

      // UPGRADE jrcp_gold
      val goldPipeline = Gold(prodWorkspace, readOnly = true, suppressStaticDatasets = true, suppressReport = true)
      val jrcpGold = goldPipeline.getAllTargets.filter(_.name == "jobRunCostPotentialFact_gold").head
      logger.log(Level.INFO, s"Upgrading ${jrcpGold.tableFullName}")
      if (jrcpGold.exists) {
        val upgradedJRCP = jrcpGold.asDF(withGlobalFilters = isLocalUpgrade)
          .withColumn("endEpochMS", $"job_runtime.endEpochMS")

        val upgradeTarget = jrcpGold.copy(
          _mode = WriteMode.overwrite,
          withCreateDate = false,
          withOverwatchRunID = false
        )

        goldPipeline.database.write(upgradedJRCP, upgradeTarget, lit(null).cast("timestamp"))
        upgradeStatus.append(UpgradeReport(prodWorkspace.getConfig.databaseName, jrcpGold.name, Some("SUCCESS")))
      } else {
        val errMsg = s"${jrcpGold.name} doesn't exist in the current schema and will not be upgraded. This is not " +
          s"an error, simply a notification."
        logger.log(Level.WARN, errMsg)
        upgradeStatus.append(UpgradeReport(prodWorkspace.getConfig.databaseName, jrcpGold.name, Some(errMsg)))
      }

      logger.log(Level.INFO, "All upgrades Complete")
      logger.log(Level.INFO, s"Upgrading registered schema version to $targetSchemaVersion")
      SchemaTools.modifySchemaVersion(prodWorkspace.getConfig.databaseName, targetSchemaVersion)
      upgradeStatus.toDS()
    } catch {
      case e: UpgradeException => Seq(e.getUpgradeReport).toDS()
      case e: Throwable =>
        logger.log(Level.ERROR, e.getMessage, e)
        Seq(UpgradeReport("", "", Some(e.getMessage))).toDS()
    }
  }

  def upgradeTo043(prodWorkspace: Workspace, isLocalUpgrade: Boolean = false): Dataset[UpgradeReport] = {
    val currentSchemaVersion = SchemaTools.getSchemaVersion(prodWorkspace.getConfig.databaseName)
    val targetSchemaVersion = "0.430"

    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    if (getNumericalSchemaVersion(currentSchemaVersion) < 420) {
      val logMsg = s"SCHEMA UPGRADE: Current Schema is < 0.420, attempting to step-upgrade the Schema"
      println(logMsg)
      logger.log(Level.INFO, logMsg)
      upgradeStatus.appendAll(upgradeTo042(prodWorkspace).collect())
    }
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)

    val cloudProvider = prodWorkspace.getConfig.cloudProvider
    val silverPipeline = Silver(prodWorkspace, readOnly = true, suppressStaticDatasets = true, suppressReport = true)

    try {
      // job_status_silver -- small dim - easy drop and rebuild from bronze
      fastDrop(getPipelineTarget(silverPipeline, "job_status_silver"), cloudProvider)

      val moduleIDsToRollback = Array(2010)
      persistPipelineStateChange(prodWorkspace.getConfig.databaseName, moduleIDsToRollback)
      // other silver drops/rollbacks

      val upgradeConfig = prodWorkspace.getConfig.inputConfig.copy(overwatchScope = Some(Array("audit", "clusters", "jobs")))
      val upgradeArgs = JsonUtils.objToJson(upgradeConfig).compactString
      val upgradeWorkspace = Initializer(upgradeArgs)
      Silver(upgradeWorkspace).run()
      // run Silver for proper scopes

      // TODO - if count >= original count, success
      upgradeStatus.append(UpgradeReport(prodWorkspace.getConfig.databaseName, "job_status_silver", Some("SUCCESS")))

      upgradeStatus.toDS()
    } catch {
      case e: UpgradeException => Seq(e.getUpgradeReport).toDS()
      case e: Throwable =>
        logger.log(Level.ERROR, e.getMessage, e)
        Seq(UpgradeReport("", "", Some(e.getMessage))).toDS()
    }
  }

}
