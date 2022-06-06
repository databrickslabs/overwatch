package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.Helpers.fastDrop
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import TransformFunctions._

import java.util.concurrent.{ConcurrentHashMap, ForkJoinPool}
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport

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

  private def finalizeUpgrade(
                               overwatchETLDBName: String,
                               tempDir: String,
                               targetSchemaVersion: String
                             ): Unit = {
    val cleanupMsg = s"Cleaning up all upgrade backups and temporary reports from the upgrade located " +
      s"within path $tempDir"
    logger.log(Level.INFO, cleanupMsg)
    println(cleanupMsg)
    Helpers.fastrm(Array(tempDir))
    val cleanupCompleteMsg = "UPGRADE - Cleanup complete"
    logger.log(Level.INFO, cleanupCompleteMsg)
    println(cleanupMsg)
    SchemaTools.modifySchemaVersion(overwatchETLDBName, targetSchemaVersion)
    val upgradeFinalizedMsg = "Upgrade Complete & Finalized"
    logger.log(Level.INFO, upgradeFinalizedMsg)
    println(upgradeFinalizedMsg)
  }

  /**
   * Upgrade to 0412 schema workspace from prior version for tables:
   * jobs_snapshot_bronze & clusters_snapshot_bronze
   *
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
        val outputDF = SchemaScrubber.scrubSchema(jobSnapshotDF)

        val changeInventory = Map[String, Column](
          "settings.new_cluster.custom_tags" -> SchemaTools.structToMap(outputDF, "settings.new_cluster.custom_tags"),
          "settings.new_cluster.spark_conf" -> SchemaTools.structToMap(outputDF, "settings.new_cluster.spark_conf"),
          "settings.new_cluster.spark_env_vars" -> SchemaTools.structToMap(outputDF, "settings.new_cluster.spark_env_vars"),
          s"settings.new_cluster.aws_attributes" -> SchemaTools.structToMap(outputDF, s"settings.new_cluster.aws_attributes"),
          s"settings.new_cluster.azure_attributes" -> SchemaTools.structToMap(outputDF, s"settings.new_cluster.azure_attributes"),
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
          val clusterSnapshotDF = SchemaScrubber.scrubSchema(clusterSnapshotTarget.asDF(withGlobalFilters = false))
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
   *
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

  private def verifyUpgradeStatus(
                                   upgradeStatus: Array[UpgradeReport],
                                   initialTableVersions: Map[String, Long],
                                   snapDir: String): Unit = {
    if (upgradeStatus.exists(_.failUpgrade)) { // if upgrade report contains fail upgrade
      val reportTime = System.currentTimeMillis()
      val upgradeReportPath = s"$snapDir/upgradeReport_$reportTime"
      val initialTableVersionsPath = s"$snapDir/initialTableVersions_$reportTime"

      val failMsg = s"UPGRADE FAILED:\nUpgrade Report saved as Dataframe to $upgradeReportPath\nTable Versions " +
        s"prior to upgrade stored as Dataframe at $initialTableVersionsPath"

      logger.log(Level.INFO, failMsg)
      println(failMsg)

      upgradeStatus.toSeq.toDF().write.format("delta").mode("overwrite")
        .save(upgradeReportPath)

      initialTableVersions.toSeq.toDF().write.format("delta").mode("overwrite")
        .save(initialTableVersionsPath)

      throw new Exception(failMsg)
    }
  }

  private def getWorkspaceUpdateLogic(workspaceMap: Map[String, String]): String = {
    if (workspaceMap.nonEmpty) { // user provided workspace nam to org id map
      "CASE " + workspaceMap.map(wm => s"WHEN (organization_id = '${wm._1}') THEN '${wm._2}'").mkString(" ") + " ELSE organization_id END"
    } else { // no workpace name to map provided -- using org_id
      "organization_id"
    }
  }

  private def appendWorkspaceName(target: PipelineTable, workspaceMap: Map[String, String]): Unit = {
    if (target.exists) { // ensure target exists
      val alterTableStmt = s"alter table delta.`${target.tableLocation}` add columns (workspace_name string)"
      val workspaceColumnUpdateLogic = getWorkspaceUpdateLogic(workspaceMap)
      val updateWorkspaceNameStmt = s"update delta.`${target.tableLocation}` set workspace_name = $workspaceColumnUpdateLogic"

      if (target.asDF.schema.map(_.name).contains("workspace_name")) { // column already exists fail upgrade for table
        val columnExistsMsg = s"Column 'workspace_name' already exists for ths table. Aborting upgrade for " +
          s"${target.name}"
        throw new UpgradeException(columnExistsMsg, target)
      } else { // workspace_name column does not exist -- continue
        // upgrade target DDL (add col)
        logger.log(Level.INFO, s"UPGRADE: Alter table ${target.name}\nSTATEMENT: $alterTableStmt")
        try {
          spark.sql(alterTableStmt)
        } catch {
          case _: NoSuchObjectException =>
          case e: Throwable =>
            throw new UpgradeException(e.getMessage, target, failUpgrade = true)
        }

        // backload workspace_name
        logger.log(Level.INFO, s"UPGRADE: Backfill workspace_name for ${target.name}\nSTATEMENT: $updateWorkspaceNameStmt")
        try {
          spark.sql(updateWorkspaceNameStmt)
        } catch {
          case _: NoSuchObjectException =>
          case e: Throwable =>
            throw new UpgradeException(e.getMessage, target, failUpgrade = true)
        }
      }

    } else { // target does not exist
      val nonExistsMsg = s"TABLE DOES NOT EXIST: ${target.name} skipping upgrade"
      logger.log(Level.WARN, nonExistsMsg)
      throw new UpgradeException(nonExistsMsg, target)
      // log message here
    }
  }

  /**
   * when instantiating the workspace for orgs outside the current workspace the workspace config must be built
   * using the pipeline_report's latest configs for the org
   *
   * @param pipelineReportPath pass in database name
   * @return
   */
  def getLatestWorkspaceByOrg(pipelineReportPath: String): Array[OrgConfigDetail] = {
    val oldestDateByModuleW = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)

    val overwatchParamsCols = Array("auditLogConfig", "tokenSecret", "dataTarget", "badRecordsPath", "overwatchScope",
      "maxDaysToLoad", "databricksContractPrices", "primordialDateString", "intelligentScaling", "workspace_name",
      "externalizeOptimize")

    // Necessary because if recovering pipeline upgrade the columns may or may not exist so both scenarios
    // must be handled. This ensures both columns are present and present only once
    val orgRunDetailsBase = spark.read.format("delta").load(pipelineReportPath)
      .select('organization_id, 'Pipeline_SnapTS, $"inputConfig.*")
      .withColumn("workspace_name", lit("placeholder"))
      .withColumn("externalizeOptimize", lit(true))
      .select('organization_id, 'Pipeline_SnapTS, struct(overwatchParamsCols map col: _*).alias("inputConfig"))

    val ehConfigFields = orgRunDetailsBase.selectExpr("inputConfig.auditLogConfig.azureAuditLogEventhubConfig.*")
      .schema.fields

    val ehConfigExistingCols = ehConfigFields.map(f => col("inputConfig.auditLogConfig.azureAuditLogEventhubConfig." + f.name).alias(f.name))

    val ehConfigCols = if (ehConfigFields.map(_.name).contains("minEventsPerTrigger")) {
      ehConfigExistingCols
    } else {
      ehConfigExistingCols :+ lit(10).alias("minEventsPerTrigger")
    }

    // handle optional nulls error when building OverwatchParams
    val addNewConfigs = Map(
      "inputConfig.auditLogConfig.azureAuditLogEventhubConfig" ->
        when($"inputConfig.auditLogConfig.rawAuditPath".isNotNull, lit(null))
          .otherwise(struct(ehConfigCols: _*))
          .alias("azureAuditLogEventhubConfig")
    )

    // get latest workspace config by org_id
    orgRunDetailsBase
      .select(SchemaTools.modifyStruct(orgRunDetailsBase.schema, addNewConfigs): _*)
      .withColumn("rnk", rank().over(oldestDateByModuleW))
      .withColumn("rn", row_number().over(oldestDateByModuleW))
      .filter('rnk === 1 && 'rn === 1)
      .select(
        'organization_id,
        'inputConfig.alias("latestParams")
      )
      .distinct
      .as[OrgConfigDetail]
      .collect
  }

  def upgradeTo060(
                    workspace: Workspace,
                    workspaceNameMap: Map[String, String] = Map(),
                    maxDays: Int = 9999,
                    rebuildSparkTables: Boolean = true,
                    startStep: Double = 1.0, // TODO -- Reorder steps and switch back to Int
                    snapDir: String = "/tmp/overwatch/060_upgrade_snapsot__ctrl_0x110"
                  ): DataFrame = {

    // TODO - ADD steps as an array to allow to resume upgrades at various steps
    val currentSchemaVersion = SchemaTools.getSchemaVersion(workspace.getConfig.databaseName)
    val targetSchemaVersion = "0.600"
    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    val initialSourceVersions: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)
    require(
      getNumericalSchemaVersion(currentSchemaVersion) <= 430,
      "This upgrade function is only for upgrading schema version 042+ to new version 060. " +
        "Please first upgrade to at least schema version 0.4.2 before proceeding."
    )
    val config = workspace.getConfig
    config.setMaxDays(maxDays)
      .setExternalizeOptimize(true) // complete optimize at end
      .setOverwatchSchemaVersion("0.420") // temporarily set the config schema to the upgradeFrom version
      .setDebugFlag(false) // disabling due to reduce output noise
    val cloudProvider = config.cloudProvider
    val pipReportPath = s"${config.etlDataPathPrefix}/pipeline_report"
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(getDriverCores * 2))

    // Step 1 snap backup to enable fast recovery
    if (startStep <= 1) {
      val stepMsg = Some("Step 1: Snapshot Overwatch for quick recovery")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      require(
        !Helpers.pathExists(snapDir),
        s"UPGRADE ERROR: It sseems there is already an upgrade in progress since $snapDir already exists. " +
          s"This is where a temporary backup is stored during the upgrade. " +
          s"Either complete the upgrade or delete this directory. WARNING: " +
          s"If you delete the directory and the Overwatch dataset becomes " +
          s"corrupted during upgrade there will be no backup."
      )
      println(stepMsg)
      logger.log(Level.INFO, s"UPGRADE: BEGINNING BACKUP - This may take some time")
      try {
        workspace.snap(snapDir)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "VARIOUS", Some("SUCCESS"), stepMsg)
        )
      } catch {
        case e: Throwable =>
          val failMsg = s"UPGRADE FAILED: Backup could not complete as expected!\n${e.getMessage}"
          logger.log(Level.ERROR, failMsg)
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "VARIOUS", Some(PipelineFunctions.appendStackStrace(e, failMsg)), stepMsg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // upgrade pipReport table
    // it must be upgraded first as a pipeline cannot be instantiated without the upgrade
    // step 2 Upgrade pipeline_report
    if (startStep <= 2) {
      val stepMsg = Some("Step 2: Upgrade pipeline_report")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg)
      val pipReportLatestVersion = Helpers.getLatestTableVersionByPath(pipReportPath)
      initialSourceVersions.put("pipeline_report", pipReportLatestVersion)

      try {
        spark.read.format("delta").load(pipReportPath)
          .withColumn("workspace_name", expr(getWorkspaceUpdateLogic(workspaceNameMap)))
          .withColumn("writeOpsMetrics", lit(map(lit("numOutputRows"), 'recordsAppended.cast("string"))))
          .withColumn("externalizeOptimize", lit(false))
          .drop("recordsAppended")
          .repartition('organization_id)
          .select("organization_id", "workspace_name", "moduleID", "moduleName", "primordialDateString",
            "runStartTS", "runEndTS", "fromTS", "untilTS", "status", "writeOpsMetrics",
            "lastOptimizedTS", "vacuumRetentionHours", "inputConfig", "parsedConfig", "Pipeline_SnapTS",
            "Overwatch_RunID", "externalizeOptimize")
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("organization_id")
          .save(pipReportPath)

        upgradeStatus.append(
          UpgradeReport(config.databaseName, "pipeline_report", Some("SUCCESS"), stepMsg)
        )
      } catch {
        case e: Throwable =>
          val errorMsg = s"UPGRADE FAILED: pipeline_report upgrade failed.\nAttempting to restore " +
            s"to version $pipReportLatestVersion\nError message below\n${e.getMessage}"
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "pipeline_report", Some(PipelineFunctions.appendStackStrace(e, errorMsg)),
              Some("Step 1: Upgrade pipeline_report"), failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 3: Update costing table structures -- reset numbers after all is working
    if (startStep <= 3) {
      val stepMsg = Some("Step 3: Update costing table structures")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      val idLocation = s"${config.etlDataPathPrefix}/instancedetails"
      val dbuCostLocation = s"${config.etlDataPathPrefix}/dbucostdetails"

      try {
        // rebuild dbuCostDetails and retain custom costs through time
        logger.log(Level.INFO, "REBUILDING dbuCostDetails table for 0.6.0")
        spark.read.format("delta").load(idLocation)
          .select(
            'organization_id,
            explode(array(lit("interactive"), lit("automated"), lit("sqlCompute"), lit("jobsLight"))).alias("sku"),
            when('sku === "interactive", 'interactiveDBUPrice)
              .when('sku === "automated", 'automatedDBUPrice)
              .when('sku === "sqlCompute", 'sqlComputeDBUPrice)
              .when('sku === "jobsLight", 'jobsLightDBUPrice)
              .alias("contract_price"),
            'activeFrom,
            'activeUntil,
            when('activeUntil.isNull, lit(true)).otherwise(lit(false)).alias("isActive"),
            'Pipeline_SnapTS,
            'Overwatch_RunID
          )
          .distinct
          .repartition('organization_id)
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("organization_id")
          .save(dbuCostLocation)

        // create the metastore entry
        val b = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
        b.database.registerTarget(b.BronzeTargets.dbuCostDetail)

        upgradeStatus.append(
          UpgradeReport(config.databaseName, "dbuCostDetails", Some("SUCCESS"), stepMsg)
        )
        logger.log(Level.INFO, "dbuCostDetails rebuild complete")

        // rebuild instanceDetails and retain custom costs through time
        logger.log(Level.INFO, "REBUILDING instanceDetails table for 0.6.0")
        spark.read.format("delta").load(idLocation)
          .drop("interactiveDBUPrice", "automatedDBUPrice", "sqlComputeDBUPrice", "jobsLightDBUPrice")
          .withColumn("isActive", when('activeUntil.isNull, lit(true)).otherwise(lit(false)))
          .withColumnRenamed("API_name", "API_Name") // correct case from previous versions
          .repartition('organization_id)
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("organization_id")
          .save(idLocation)
        logger.log(Level.INFO, "instanceDetails rebuild complete")
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "instanceDetails", Some("SUCCESS"), stepMsg)
        )
      } catch {
        case e: Throwable =>
          val errorMsg = s"ERROR upgrading costing tables: ${e.getMessage}"
          logger.log(Level.INFO, errorMsg)
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "instanceDetails", Some(PipelineFunctions.appendStackStrace(e, errorMsg)), stepMsg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 4 - Rebuild Audit Raw Land to reduce partition Counts
    if (startStep <= 4) {
      val stepMsg = Some("Step 4: Rebuild Audit Raw Land to reduce partition Counts")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)

      val b = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
      val auditLogRawTarget = PipelineFunctions.getPipelineTarget(b, "audit_log_raw_events")
      logger.log(Level.INFO, "Rebuilding audit_log_raw_events to reduce partition count")
      if (auditLogRawTarget.exists) {
        try {
          spark.read.format("delta").load(auditLogRawTarget.tableLocation)
            .repartition('organization_id, 'Overwatch_RunID)
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .partitionBy("organization_id")
            .save(auditLogRawTarget.tableLocation)
          logger.log(Level.INFO, "audit_log_raw_events Rebuild Complete")
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "audit_log_raw_events", Some("SUCCESS"), stepMsg)
          )
        } catch {
          case e: Throwable =>
            val errMsg = s"audit_log_raw_events table was found at ${auditLogRawTarget.tableLocation} but the " +
              s"table upgrade failed. CAUSE: ${e.getMessage}"
            upgradeStatus.append(
              UpgradeReport(config.databaseName, "audit_log_raw_events", Some(PipelineFunctions.appendStackStrace(e, errMsg)), stepMsg, failUpgrade = true)
            )
        }
      } else { // either not azure and/or table does not exist
        val skippedMsg = s"audit_log_raw_events data could not be found at the proper location. If any of your " +
          s"workspaces are on Azure, this table must " +
          s"be upgraded to continue. audit_log_raw_events NOT FOUND AT ${auditLogRawTarget.tableLocation}"
        logger.log(Level.WARN, skippedMsg)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "audit_log_raw_events", Some(skippedMsg), stepMsg)
        )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 5 - Append workspace_name to all bronze targets
    if (startStep <= 5) {
      val stepMsg = Some("Step 5: Upgrade bronze targets")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      val b = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
      val bronzeTargets = b.getAllTargets.par
      bronzeTargets.tasksupport = taskSupport

      // Get start version of all bronze targets
      bronzeTargets.filter(_.exists).foreach(t => initialSourceVersions.put(t.name, Helpers.getLatestTableVersionByPath(t.tableLocation)))

      bronzeTargets.filter(_.exists).foreach(target => {
        try {
          appendWorkspaceName(target, workspaceNameMap)
          upgradeStatus.append(
            UpgradeReport(config.databaseName, target.name, Some("SUCCESS"), stepMsg)
          )
        } catch {
          case e: UpgradeException =>
            val upgradeReport = e.getUpgradeReport.copy(step = stepMsg)
            upgradeStatus.append(upgradeReport)
          case e: Throwable =>
            upgradeStatus.append(
              UpgradeReport(config.databaseName, target.name, Some(PipelineFunctions.appendStackStrace(e, e.getMessage)), stepMsg)
            )
        }
      })
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 6: upgrade default_tags from struct to map in bronze tables
    if (startStep <= 6) {
      val stepMsg = Some("Step 6: upgrade default_tags from struct to map in bronze tables")
      logger.log(Level.INFO, stepMsg.get)
      println(stepMsg.get)

      val b = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
      val clustersSnapTarget = PipelineFunctions.getPipelineTarget(b, "clusters_snapshot_bronze")
      val poolsSnapTarget = PipelineFunctions.getPipelineTarget(b, "pools_snapshot_bronze")

      try {
        if (clustersSnapTarget.exists) {
          val clustersSnapDF = spark.read.format("delta").load(clustersSnapTarget.tableLocation)

          clustersSnapDF
            .withColumn("default_tags", SchemaTools.structToMap(clustersSnapDF, "default_tags"))
            .repartition('organization_id)
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .partitionBy("organization_id")
            .save(clustersSnapTarget.tableLocation)

          upgradeStatus.append(
            UpgradeReport(config.databaseName, "clusters_snapshot_bronze", Some("SUCCESS"), stepMsg)
          )
        }

        if (poolsSnapTarget.exists) {
          val poolsSnapDF = spark.read.format("delta").load(poolsSnapTarget.tableLocation)
          poolsSnapDF
            .withColumn("default_tags", SchemaTools.structToMap(poolsSnapDF, "default_tags"))
            .withColumn(s"aws_attributes", SchemaTools.structToMap(poolsSnapDF, s"aws_attributes"))
            .withColumn(s"azure_attributes", SchemaTools.structToMap(poolsSnapDF, s"azure_attributes"))
            .repartition('organization_id)
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .partitionBy("organization_id")
            .save(poolsSnapTarget.tableLocation)

          upgradeStatus.append(
            UpgradeReport(config.databaseName, "pools_snapshot_bronze", Some("SUCCESS"), stepMsg)
          )
        }
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "*_snapshot_bronze", Some(PipelineFunctions.appendStackStrace(e)), stepMsg, failUpgrade = true)
          )
      }


      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 7: Backload workspace name for spark tables if aren't to be rebuilt
    if (startStep <= 7) {
      val stepMsg = Some("Step 7: Backload workspace name for spark tables if they weren't rebuilt")
      if (!rebuildSparkTables) {
        logger.log(Level.INFO, stepMsg.get)
        println(stepMsg.get)
        val s = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
        val g = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)
        val allSilverGoldTargets = s.getAllTargets ++ g.getAllTargets
        val targetsToBackload = allSilverGoldTargets.filter(_.name.toLowerCase.contains("spark")).filter(_.exists).par

        targetsToBackload
          .foreach(target => {
            try {
              appendWorkspaceName(target, workspaceNameMap)
              upgradeStatus.append(
                UpgradeReport(config.databaseName, target.name, Some("SUCCESS"), stepMsg)
              )
            } catch {
              case e: UpgradeException =>
                val upgradeReport = e.getUpgradeReport.copy(step = stepMsg)
                upgradeStatus.append(upgradeReport)
              case e: Throwable =>
                upgradeStatus.append(
                  UpgradeReport(config.databaseName, target.name, Some(PipelineFunctions.appendStackStrace(e)), stepMsg)
                )
            }
          })
      }
    }

    // Step 8: drop silver / gold targets
    if (startStep <= 8) {
      val stepMsg = Some("Step 8: Drop original silver / golds for rebuild")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      val s = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
      val g = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)
      val sparkExecutorTargetsToRebuild = Array("sparkExecutor_gold", "spark_executors_silver")
      val allSilverGoldTargets = s.getAllTargets ++ g.getAllTargets
      val targetsToRebuild = if (rebuildSparkTables) { // rebuild all spark modules
        allSilverGoldTargets.par
      } else { // don't rebuild spark modules (except executors)
        // executor tables had schema upgrade, they are small so just rebuild them
        allSilverGoldTargets.filter(
          t => !t.name.toLowerCase.contains("spark") ||
            sparkExecutorTargetsToRebuild.contains(t.name)).par
      }
      targetsToRebuild.tasksupport = taskSupport

      // Get start version of all targets to be rebuilt
      targetsToRebuild.filter(_.exists).foreach(t => initialSourceVersions.put(t.name, Helpers.getLatestTableVersionByPath(t.tableLocation)))

      targetsToRebuild.foreach(target => {
        try {
          logger.log(Level.INFO, s"UPGRADE: Beginning Step6, ${target.name}")
          val dropMsg = fastDrop(target, cloudProvider)
          println(dropMsg)
          logger.log(Level.INFO, dropMsg)
          upgradeStatus.append(
            UpgradeReport(target.databaseName, target.name, Some("SUCCESS"), stepMsg)
          )
        } catch {
          case e: Throwable => {
            upgradeStatus.append(
              UpgradeReport(target.databaseName, target.name, Some(PipelineFunctions.appendStackStrace(e)), stepMsg)
            )
          }
        }
      })
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 9: update pipReport to reflect rolled back silver/gold
    if (startStep <= 9) {
      val stepMsg = Some("Step 9: update pipReport to reflect rolled back silver/gold")
      logger.log(Level.INFO, stepMsg.get)
      println(stepMsg.get)
      val s = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
      val g = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)
      val sparkExecutorModulesToRebuild = Array(2003, 3014)
      val allSilverGoldModules = s.getAllModules ++ g.getAllModules
      val moduleIDsToRollback = if (rebuildSparkTables) {
        allSilverGoldModules.map(_.moduleId)
      } else { // don't rebuild spark modules (except executors)
        // executor tables had schema upgrade, they are small so just rebuild them
        allSilverGoldModules.filter(m =>
          !m.moduleName.toLowerCase.contains("spark") ||
            sparkExecutorModulesToRebuild.contains(m.moduleId)
        ).map(_.moduleId)
      }
      val rollBackSilverGoldModulesStmt =
        s"""
           |update delta.`$pipReportPath`
           |set status = concat('ROLLED BACK FOR UPGRADE: Original Status - ', status)
           |where moduleID in (${moduleIDsToRollback.mkString(", ")})
           |""".stripMargin

      val updateMsg = s"UPGRADE - Step 9 - Rolling back modules to be rebuilt\nSTATEMENT: $rollBackSilverGoldModulesStmt"
      logger.log(Level.INFO, updateMsg)
      try {
        spark.sql(rollBackSilverGoldModulesStmt)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "pipeline_report", Some("SUCCESS"), stepMsg)
        )
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "pipeline_report", Some(PipelineFunctions.appendStackStrace(e)), stepMsg, failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 10: rebuild silver targets
    if (startStep <= 10) {
      val stepMsg = Some("Step 10: rebuild silver targets")
      logger.log(Level.INFO, stepMsg.get)
      println(stepMsg.get)

      try {
        // get latest workspace config by org id
        val orgRunDetails = getLatestWorkspaceByOrg(pipReportPath)

        logger.log(Level.INFO, s"REBUILDING SILVER for ORG_IDs ${orgRunDetails.map(_.organization_id).mkString(",")}")
        orgRunDetails.foreach(org => {
          val launchSilverPipelineForOrgMsg = s"BEGINNING SILVER REBUILD FOR ORG_ID: ${org.organization_id}"
          logger.log(Level.INFO, launchSilverPipelineForOrgMsg)
          println(launchSilverPipelineForOrgMsg)
          // get params for org's workspace
          val orgParams = JsonUtils.objToJson(org.latestParams).compactString
          // initialize org specific workspace
          val orgWorkspace = Initializer(orgParams, debugFlag = false, isSnap = false, disableValidations = true)

          val orgCloudProvider = if (orgWorkspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.isEmpty) "aws" else "azure"
          logger.info(s"CLOUD PROVIDER SET: $orgCloudProvider for ORGID: ${org.organization_id}")
          // set org-specific workspace name as per upgrade config name map
          // downgrade jar schema version to pass checks
          // one upgrade per data-target -- so from current workspace use local workspace dataTarget
          orgWorkspace
            .getConfig
            .setOrganizationId(org.organization_id)
            .setMaxDays(maxDays)
            .setWorkspaceName(workspaceNameMap.getOrElse(org.organization_id, org.organization_id))
            .setOverwatchSchemaVersion("0.420")
            .setDatabaseNameAndLoc(config.databaseName, config.databaseLocation, config.etlDataPathPrefix)
            .setConsumerDatabaseNameandLoc(config.consumerDatabaseName, config.consumerDatabaseLocation)
            .setCloudProvider(orgCloudProvider)

          // initiate silver rebuild
          Silver(orgWorkspace, suppressReport = true, suppressStaticDatasets = true).run()
          logger.log(Level.INFO, s"COMPLETED SILVER REBUILD FOR ORG_ID: ${org.organization_id}")
          upgradeStatus.append(
            UpgradeReport(
              config.databaseName, "SILVER TARGETS", Some(s"SUCCESS: ORG_ID = ${org.organization_id}"),
              stepMsg
            )
          )
        })
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "SILVER TARGETS", Some(PipelineFunctions.appendStackStrace(e)), stepMsg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 11: rebuild gold targets
    if (startStep <= 11) {
      val stepMsg = Some("Step 11: rebuild gold targets")
      logger.log(Level.INFO, stepMsg.get)
      println(stepMsg.get)

      try {
        // get latest workspace config by org id
        val orgRunDetails = getLatestWorkspaceByOrg(pipReportPath)

        logger.log(Level.INFO, s"REBUILDING GOLD for ORG_IDs ${orgRunDetails.map(_.organization_id).mkString(",")}")
        orgRunDetails.foreach(org => {
          val launchGoldPipelineForOrgMsg = s"BEGINNING GOLD REBUILD FOR ORG_ID: ${org.organization_id}"
          logger.log(Level.INFO, launchGoldPipelineForOrgMsg)
          println(launchGoldPipelineForOrgMsg)
          // get params for org's workspace
          val orgParams = JsonUtils.objToJson(org.latestParams).compactString
          // initialize org specific workspace
          val orgWorkspace = Initializer(orgParams, debugFlag = false, isSnap = false, disableValidations = true)

          val orgCloudProvider = if (orgWorkspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.isEmpty) "aws" else "azure"
          // set org-specific workspace name as per upgrade config name map
          orgWorkspace
            .getConfig
            .setOrganizationId(org.organization_id)
            .setMaxDays(maxDays)
            .setWorkspaceName(workspaceNameMap.getOrElse(org.organization_id, org.organization_id))
            .setOverwatchSchemaVersion("0.420")
            .setDatabaseNameAndLoc(config.databaseName, config.databaseLocation, config.etlDataPathPrefix)
            .setConsumerDatabaseNameandLoc(config.consumerDatabaseName, config.consumerDatabaseLocation)
            .setCloudProvider(orgCloudProvider)

          // initiate silver rebuild
          Gold(orgWorkspace, suppressReport = true, suppressStaticDatasets = true).run()
          logger.log(Level.INFO, s"COMPLETED GOLD REBUILD FOR ORG_ID: ${org.organization_id}")
          upgradeStatus.append(
            UpgradeReport(
              config.databaseName, "GOLD TARGETS", Some(s"SUCCESS: ORG_ID = ${org.organization_id}"),
              stepMsg
            )
          )
        })
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "GOLD TARGETS", Some(PipelineFunctions.appendStackStrace(e)), stepMsg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }
    upgradeStatus.toArray.toSeq.toDF
  }

  /**
   * Call this function after the entire upgrade has been confirmed to cleanup the backups and temporary files
   */
  def finalize060Upgrade(
                          overwatchETLDBName: String,
                          snapDir: String = "/tmp/overwatch/060_upgrade_snapsot__ctrl_0x110"
                        ): Unit = {
    finalizeUpgrade(overwatchETLDBName, snapDir, "0.600")
  }

  private def upgradeDeltaTable(qualifiedName: String): Unit = {
    try {
      val tblPropertiesUpgradeStmt =
        s"""ALTER TABLE $qualifiedName SET TBLPROPERTIES (
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5',
      'delta.columnMapping.mode' = 'name'
    )
    """
      logger.info(s"UPGRADE STATEMENT for $qualifiedName: $tblPropertiesUpgradeStmt")
      spark.sql(tblPropertiesUpgradeStmt)
    } catch {
      case e: Throwable =>
        logger.error(s"FAILED $qualifiedName ->", e)
        println(s"FAILED UPGRADE FOR $qualifiedName")
    }
  }

  /**
   * Upgrades Overwatch schema to version 0610
   * @param etlDatabaseName overwatch_etl database name
   * @param startStep step from which to start (defaults to step 1)
   * @param enableUpgradeBelowDBR104 required boolean to allow user to upgrade using DBR < 10.4LTS
   * @param tempDir temporary dir in which to store metadata and upgrade status results
   * @return
   */
  def upgradeTo0610(
                     etlDatabaseName: String,
                     startStep: Int = 1,
                     enableUpgradeBelowDBR104: Boolean = false,
                     tempDir: String = s"/tmp/overwatch/upgrade0610_status__ctrl_0x111/${System.currentTimeMillis()}"
                   ): DataFrame = {
    dbutils.fs.mkdirs(tempDir) // init tempDir -- if no errors it wouldn't be created
    val blankConfig = new Config()
    val currentSchemaVersion = SchemaTools.getSchemaVersion(etlDatabaseName)
    val numericalSchemaVersion = getNumericalSchemaVersion(currentSchemaVersion)
    val targetSchemaVersion = "0.610"
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)
    require(
      numericalSchemaVersion >= 600 && numericalSchemaVersion < 610,
      "This upgrade function is only for upgrading schema version 0600+ to new version 0610 " +
        "Please first upgrade to at least schema version 0600 before proceeding. " +
        "Upgrade documentation can be found in the change log."
    )
    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    val dbrVersion = spark.conf.get("spark.databricks.clusterUsageTags.effectiveSparkVersion")
    val dbrMajorV = dbrVersion.split("\\.").head
    val dbrMinorV = dbrVersion.split("\\.")(1)
    val dbrVersionNumerical = s"$dbrMajorV.$dbrMinorV".toDouble
    val initialSourceVersions: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala
    val packageVersion = blankConfig.getClass.getPackage.getImplementationVersion.replaceAll("\\.", "").tail.toInt
    val startingSchemaVersion = SchemaTools.getSchemaVersion(etlDatabaseName).split("\\.").takeRight(1).head.toInt
    assert(startingSchemaVersion >= 600 && packageVersion >= 610,
      s"""
         |This schema upgrade is only necessary when upgrading from < 0610 but > 05x.
         |If upgrading from 05x directly to 0610+ simply run the 'upgradeTo060' function.
         |""".stripMargin)
    if (startStep <= 1) {
      val stepMsg = Some("Step 1: Upgrade Schema - Job Status Silver")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      val targetName = "job_status_silver"
      try {
        if (!spark.catalog.tableExists(etlDatabaseName, targetName)) {
          throw new SimplifiedUpgradeException(
            s"""
               |$targetName cannot be found in db $etlDatabaseName, proceeding with upgrade assuming no jobs
               |have been recorded.
               |""".stripMargin,
            etlDatabaseName, targetName, Some("1"), failUpgrade = false
          )
        }
        initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
        val jobSilverDF = spark.table(s"${etlDatabaseName}.${targetName}")
        SchemaTools.cullNestedColumns(jobSilverDF, "new_settings", Array("tasks", "job_clusters"))
          .repartition(col("organization_id"), col("__overwatch_ctrl_noise"))
          .write
          .format("delta")
          .partitionBy("organization_id", "__overwatch_ctrl_noise")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(s"${etlDatabaseName}.${targetName}")
        upgradeStatus.append(UpgradeReport(etlDatabaseName, targetName, Some("SUCCESS"), stepMsg))
      } catch {
        case e: SimplifiedUpgradeException =>
          upgradeStatus.append(e.getUpgradeReport.copy(step = stepMsg))
        case e: Throwable =>
          val failMsg = s"UPGRADE FAILED"
          logger.log(Level.ERROR, failMsg)
          upgradeStatus.append(
            UpgradeReport(etlDatabaseName, targetName, Some(PipelineFunctions.appendStackStrace(e, failMsg)), stepMsg, failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)
    }
    if (startStep <= 2) {
      val stepMsg = Some("Step 2: Upgrade Schema - Job Gold")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      val targetName = "job_gold"
      try {
        if (!spark.catalog.tableExists(etlDatabaseName, targetName)) {
          throw new SimplifiedUpgradeException(
            s"$targetName cannot be found in db $etlDatabaseName, proceeding with upgrade assuming no jobs " +
              s"have been recorded.",
            etlDatabaseName, targetName, Some("1"), failUpgrade = false
          )
        }
        initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
        val jobGoldDF = spark.table(s"${etlDatabaseName}.${targetName}")
        SchemaTools.cullNestedColumns(jobGoldDF, "new_settings", Array("tasks", "job_clusters"))
          .repartition(col("organization_id"), col("__overwatch_ctrl_noise"))
          .write
          .format("delta")
          .partitionBy("organization_id", "__overwatch_ctrl_noise")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(s"${etlDatabaseName}.${targetName}")
        upgradeStatus.append(UpgradeReport(etlDatabaseName, targetName, Some("SUCCESS"), stepMsg))
      } catch {
        case e: SimplifiedUpgradeException =>
          upgradeStatus.append(e.getUpgradeReport.copy(step = stepMsg))
        case e: Throwable =>
          val failMsg = s"UPGRADE FAILED"
          logger.log(Level.ERROR, failMsg)
          upgradeStatus.append(
            UpgradeReport(etlDatabaseName, targetName, Some(PipelineFunctions.appendStackStrace(e, failMsg)), stepMsg, failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)
    }
    if (startStep <= 3) {
      val stepMsg = Some("Step 3: Upgrade Schema - Spark Events Bronze")
      println(stepMsg.get)
      logger.log(Level.INFO, stepMsg.get)
      val targetName = "spark_events_bronze"
      try {
        if (!spark.catalog.tableExists(etlDatabaseName, targetName)) {
          throw new SimplifiedUpgradeException(
            s"""
               |${targetName} cannot be found in db $etlDatabaseName, proceeding with upgrade assuming
               |sparkEvents module is disabled.
               |""".stripMargin,
            etlDatabaseName, targetName, Some("1"), failUpgrade = false
          )
        }
        initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
        spark.conf.set("spark.databricks.delta.optimizeWrite.numShuffleBlocks", "500000")
        spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "2048")
        spark.conf.set("spark.sql.files.maxPartitionBytes", (1024 * 1024 * 64).toString)
        spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")

        val sparkEventsBronzeDF = spark.table(s"${etlDatabaseName}.${targetName}")
        val sparkEventsSchema = sparkEventsBronzeDF.schema
        val fieldsRequiringRebuild = Array("modifiedConfigs", "extraTags")

        if (dbrVersionNumerical < 10.4) { // if dbr < 10.4LTS -- rebuild table without the column
          assert(enableUpgradeBelowDBR104, s"EXPLICIT force of parameter 'enableUpgradeBelowDBR104' is required " +
            s"as upgrading without DBR 10.4LTS+ requires a full rebuild of $targetName table which can be " +
            s"compute intensive for customers with large tables. Recommend upgrade to DBR 10.4LTS before " +
            s"continuing.")

          val partitionByCols = Seq("organization_id", "Event", "fileCreateDate")
          val statsColumns = ("organization_id, Event, clusterId, SparkContextId, JobID, StageID, " +
            "StageAttemptID, TaskType, ExecutorID, fileCreateDate, fileCreateEpochMS, fileCreateTS, filename, " +
            "Pipeline_SnapTS, Overwatch_RunID").split(", ")
          if (sparkEventsSchema.fields.exists(f => fieldsRequiringRebuild.contains(f.name))) {
            logger.info(s"Beginning full rebuild of $targetName table. This could take some time. Recommend " +
              s"monitoring of cluster size and ensure autoscaling enabled.")
            sparkEventsBronzeDF
              .drop(fieldsRequiringRebuild: _*)
              .moveColumnsToFront(statsColumns)
              .write.format("delta")
              .partitionBy(partitionByCols: _*)
              .mode("overwrite").option("overwriteSchema", "true")
              .saveAsTable(s"${etlDatabaseName}.${targetName}")
          }
        } else { // if dbr >= 10.4 -- rename and deprecate the column (perf savings)
          // spark_events_bronze must be upgraded to minimum delta reader/writer values
          upgradeDeltaTable(s"${etlDatabaseName}.${targetName}")

          val fieldsToRename = sparkEventsSchema.fieldNames.filter(f => fieldsRequiringRebuild.contains(f))
          fieldsToRename.foreach(f => {
            val modifyColStmt = s"alter table ${etlDatabaseName}.${targetName} rename " +
              s"column $f to ${f}_tobedeleted"
            logger.info(s"Beginning $targetName upgrade\nSTMT1: $modifyColStmt")
            spark.sql(modifyColStmt)
          })
        }
        upgradeStatus.append(UpgradeReport(etlDatabaseName, targetName, Some("SUCCESS"), stepMsg))
      } catch {
        case e: SimplifiedUpgradeException =>
          upgradeStatus.append(e.getUpgradeReport.copy(step = stepMsg))
        case e: Throwable =>
          val failMsg = s"UPGRADE FAILED"
          logger.log(Level.ERROR, failMsg)
          upgradeStatus.append(
            UpgradeReport(etlDatabaseName, targetName, Some(PipelineFunctions.appendStackStrace(e, failMsg)), stepMsg, failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)
    }
    upgradeStatus.toArray.toSeq.toDF
  }

  /**
   * Finalize the upgrade. This cleans up the temporary paths and upgrades the schema version
   * @param overwatchETLDBName overwatch_etl database name
   * @param tempDir temporary directory used for the 0610 upgrade
   */
  def finalize0610Upgrade(
                           overwatchETLDBName: String,
                           tempDir: String = "/tmp/overwatch/upgrade0610_status__ctrl_0x111/"
                         ): Unit = {
    require(Helpers.pathExists(tempDir), s"The default temporary directory $tempDir does not exist. If you used a " +
      s"custom temporary directory please put add that path to the 'tempDir' parameter of this function and try again. " +
      s"Otherwise, nothing can be found in the upgrade temp path, WILL NOT upgrading the schema.")
      finalizeUpgrade(overwatchETLDBName, tempDir, "0.610")
  }

}
