package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.PipelineFunctions.getPipelineTarget
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, PipelineFunctions, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.Helpers.fastDrop
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.collection.concurrent
import collection.JavaConverters._
import java.util.concurrent.{ConcurrentHashMap, ForkJoinPool}
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

  private def appendWorkspaceName(target: PipelineTable, workspaceMap: Map[String, String]): UpgradeReport = {
    if (target.exists) { // ensure target exists
      val alterTableStmt = s"alter table delta.`${target.tableLocation}` add columns (workspace_name string)"
      val workspaceColumnUpdateLogic = getWorkspaceUpdateLogic(workspaceMap)
      val updateWorkspaceNameStmt = s"update delta.`${target.tableLocation}` set workspace_name = $workspaceColumnUpdateLogic"

      if (target.asDF.schema.map(_.name).contains("workspace_name")) { // column already exists fail upgrade for table
        val columnExistsMsg = s"Column 'workspace_name' already exists for ths table. Aborting upgrade for " +
          s"${target.name}"
        throw new UpgradeException(columnExistsMsg, target)
      } else { // workspace_name column does not exist -- continue
        // upgrade target DDL
        logger.log(Level.INFO, s"UPGRADE: Alter table ${target.name}\nSTATEMENT: $alterTableStmt")
        try {
          spark.sql(alterTableStmt)
        } catch {
          case e: Throwable =>
            throw new UpgradeException(e.getMessage, target)
        }

        // backload workspace_name
        logger.log(Level.INFO, s"UPGRADE: Backfill workspace_name for ${target.name}\nSTATEMENT: $updateWorkspaceNameStmt")
        try {
          spark.sql(updateWorkspaceNameStmt)
        } catch {
          case e: Throwable =>
            throw new UpgradeException(e.getMessage, target)
        }

        UpgradeReport(target.databaseName, target.name, Some("SUCCESS"))
      }

    } else { // target does not exist
      val nonExistsMsg = s"TABLE DOES NOT EXIST: ${target.name} skipping upgrade"
      logger.log(Level.WARN, nonExistsMsg)
      throw new UpgradeException(nonExistsMsg, target)
      // log message here
    }
  }

  def upgradeTo060(
                    workspace: Workspace,
                    workspaceNameMap: Map[String, String] = Map(),
                    maxDays: Int = 9999,
                    rebuildSparkTables: Boolean = true,
                    startStep: Double = 1.0 // TODO -- Reorder steps and switch back to Int
                  ): DataFrame = {

    // TODO - ADD steps as an array to allow to resume upgrades at various steps
    val currentSchemaVersion = SchemaTools.getSchemaVersion(workspace.getConfig.databaseName)
    val targetSchemaVersion = "0.600"
    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    val initialSourceVersions: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)
    require(
      getNumericalSchemaVersion(currentSchemaVersion) <= 430,
      "This upgrade function is only for upgrading schema version 043+ to new version 060. " +
        "Please first upgrade to at least schema version 0.4.3 before proceeding."
    )
    val snapDir = "/tmp/overwatch/060_upgrade_snapsot__ctrl_0x110"
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
      val step1Msg = Some("Step 1: Snapshot Overwatch for quick recovery")
      println(step1Msg.get)
      logger.log(Level.INFO, step1Msg.get)
      require(
        !Helpers.pathExists(snapDir),
        s"UPGRADE ERROR: It sseems there is already an upgrade in progress since $snapDir already exists. " +
          s"This is where a temporary backup is stored during the upgrade. " +
          s"Either complete the upgrade or delete this directory. WARNING: " +
          s"If you delete the directory and the Overwatch dataset becomes " +
          s"corrupted during upgrade there will be no backup."
      )
      println(step1Msg)
      logger.log(Level.INFO, s"UPGRADE: BEGINNING BACKUP - This may take some time")
      try {
        workspace.snap(snapDir)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "VARIOUS", Some("SUCCESS"), step1Msg)
        )
      } catch {
        case e: Throwable =>
          val failMsg = s"UPGRADE FAILED: Backup could not complete as expected!\n${e.getMessage}"
          logger.log(Level.ERROR, failMsg)
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "VARIOUS", Some(failMsg), step1Msg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // upgrade pipReport table
    // it must be upgraded first as a pipeline cannot be instantiated without the upgrade
    // step 2 Upgrade pipeline_report
    if (startStep <= 2) {
      val step2Msg = Some("Step 2: Upgrade pipeline_report")
      println(step2Msg.get)
      logger.log(Level.INFO, step2Msg)
      val pipReportLatestVersion = Helpers.getLatestVersion(pipReportPath)
      initialSourceVersions.put("pipeline_report", pipReportLatestVersion)

      val alterPipReportStmt =
        s"""
           |alter table delta.`$pipReportPath`
           |add columns (externalizeOptimize boolean, workspace_name string)
           |""".stripMargin

      val backloadNewColsForPipReportStmt =
        s"""
           |update delta.`$pipReportPath`
           |set workspace_name = ${getWorkspaceUpdateLogic(workspaceNameMap)},
           |externalizeOptimize = false
           |""".stripMargin

      logger.log(Level.INFO, s"UPGRADE: Upgrading pipeline_report schema\nSTATEMENT:" +
        s"$alterPipReportStmt")

      logger.log(Level.INFO, s"UPGRADE: Backloading new pipeline_report columns schema\nSTATEMENT:" +
        s"$backloadNewColsForPipReportStmt")

      try {
        spark.sql(alterPipReportStmt)
        spark.sql(backloadNewColsForPipReportStmt)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "pipeline_report", Some("SUCCESS"), step2Msg)
        )
      } catch {
        case e: Throwable =>
          val errorMsg = s"UPGRADE FAILED: pipeline_report upgrade failed.\nAttempting to restore " +
            s"to version $pipReportLatestVersion\nError message below\n${e.getMessage}"
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "pipeline_report", Some(errorMsg),
              Some("Step 1: Upgrade pipeline_report"), failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 2.1: Update costing table structures -- reset numbers after all is working
    if (startStep <= 2.1) {
      val step21Msg = Some("Step 2.1: Update costing table structures")
      println(step21Msg.get)
      logger.log(Level.INFO, step21Msg.get)

      try {
        // rebuild dbuCostDetails and retain custom costs through time
        logger.log(Level.INFO, "REBUILDING dbuCostDetails table for 0.6.0")
        spark.table(s"${config.databaseName}.instanceDetails")
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
          .saveAsTable(s"${config.databaseName}.dbuCostDetails")

        upgradeStatus.append(
          UpgradeReport(config.databaseName, "dbuCostDetails", Some("SUCCESS"), step21Msg)
        )
        logger.log(Level.INFO, "dbuCostDetails rebuild complete")

        // rebuild instanceDetails and retain custom costs through time
        logger.log(Level.INFO, "REBUILDING instanceDetails table for 0.6.0")
        spark.table(s"${config.databaseName}.instanceDetails")
          .drop("interactiveDBUPrice", "automatedDBUPrice", "sqlComputeDBUPrice", "jobsLightDBUPrice")
          .withColumn("isActive", when('activeUntil.isNull, lit(true)).otherwise(lit(false)))
          .withColumnRenamed("API_name", "API_Name") // correct case from previous versions
          .repartition('organization_id)
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("organization_id")
          .saveAsTable(s"${config.databaseName}.instanceDetails")
        logger.log(Level.INFO, "instanceDetails rebuild complete")
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "instanceDetails", Some("SUCCESS"), step21Msg)
        )
      } catch {
        case e: Throwable =>
          val errorMsg = s"ERROR upgrading costing tables: ${e.getMessage}"
          logger.log(Level.INFO, errorMsg)
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "instanceDetails", Some(errorMsg), step21Msg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 2.2 - Rebuild Audit Raw Land to reduce partition Counts
    if (startStep <= 2.2) {
      val step22Msg = Some("Step 2.2: Rebuild Audit Raw Land to reduce partition Counts")
      println(step22Msg.get)
      logger.log(Level.INFO, step22Msg.get)

      val b = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
      val auditLogRawTarget = PipelineFunctions.getPipelineTarget(b, "audit_log_raw_events")
      logger.log(Level.INFO, "Rebuilding audit_log_raw_events to reduce partition count")
      if (auditLogRawTarget.exists) {
        try {
          spark.read.format("delta").load(auditLogRawTarget.tableLocation)
            .repartition('organization_id)
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .partitionBy("organization_id")
            .save(auditLogRawTarget.tableLocation)
          logger.log(Level.INFO, "audit_log_raw_events Rebuild Complete")
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "audit_log_raw_events", Some("SUCCESS"), step22Msg)
          )
        } catch {
          case e: Throwable =>
            val errMsg = s"audit_log_raw_events table was found at ${auditLogRawTarget.tableLocation} but the " +
              s"table upgrade failed. CAUSE: ${e.getMessage}"
            upgradeStatus.append(
              UpgradeReport(config.databaseName, "audit_log_raw_events", Some(errMsg), step22Msg, failUpgrade = true)
            )
        }
      } else { // either not azure and/or table does not exist
        val skippedMsg = s"audit_log_raw_events data could not be found at the proper location. If any of your " +
          s"workspaces are on Azure, this table must " +
          s"be upgraded to continue. audit_log_raw_events NOT FOUND AT ${auditLogRawTarget.tableLocation}"
        logger.log(Level.WARN, skippedMsg)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "audit_log_raw_events", Some(skippedMsg), step22Msg)
        )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 3 - Append workspace_name to all bronze targets
    if (startStep <= 3) {
      val step3Msg = Some("Step 3: Upgrade bronze targets")
      println(step3Msg.get)
      logger.log(Level.INFO, step3Msg.get)
      val b = Bronze(workspace, suppressReport = true, suppressStaticDatasets = true)
      val bronzeTargets = b.getAllTargets.par
      bronzeTargets.tasksupport = taskSupport

      // Get start version of all bronze targets
      bronzeTargets.filter(_.exists).foreach(t => initialSourceVersions.put(t.name, Helpers.getLatestVersion(t.tableLocation)))

      bronzeTargets.foreach(target => {
        try {
          appendWorkspaceName(target, workspaceNameMap)
          upgradeStatus.append(
            UpgradeReport(config.databaseName, target.name, Some("SUCCESS"), step3Msg)
          )
        } catch {
          case e: UpgradeException =>
            val upgradeReport = e.getUpgradeReport.copy(step = step3Msg)
            upgradeStatus.append(upgradeReport)
          case e: Throwable =>
            upgradeStatus.append(
              UpgradeReport(config.databaseName, target.name, Some(e.getMessage), step3Msg)
            )
        }
      })
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 4: drop silver / gold targets
    if (startStep <= 4) {
      val step4Msg = Some("Step 4: Drop original silver / golds for rebuild")
      println(step4Msg.get)
      logger.log(Level.INFO, step4Msg.get)
      val step4Silver = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
      val step4Gold = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)
      val allSilverGoldTargets = step4Silver.getAllTargets ++ step4Gold.getAllTargets
      val targetsToRebuild = if (rebuildSparkTables) {
        allSilverGoldTargets.par
      } else {
        allSilverGoldTargets.filterNot(_.name.toLowerCase.contains("spark")).par
      }
      targetsToRebuild.tasksupport = taskSupport

      // Get start version of all targets to be rebuilt
      targetsToRebuild.filter(_.exists).foreach(t => initialSourceVersions.put(t.name, Helpers.getLatestVersion(t.tableLocation)))

      targetsToRebuild.foreach(target => {
        try {
          logger.log(Level.INFO, s"UPGRADE: Beginning Step4, ${target.name}")
          fastDrop(target, cloudProvider)
          upgradeStatus.append(
            UpgradeReport(target.databaseName, target.name, Some("SUCCESS"), step4Msg)
          )
        } catch {
          case e: Throwable => {
            upgradeStatus.append(
              UpgradeReport(target.databaseName, target.name, Some(e.getMessage), step4Msg)
            )
          }
        }
      })
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 4.1: upgrade default_tags from struct to map in bronze tables
    if (startStep <= 4.1) {
      val step41Msg = Some("Step 4.1: upgrade default_tags from struct to map in bronze tables")
      logger.log(Level.INFO, step41Msg.get)
      println(step41Msg.get)

      val b = Bronze(workspace)
      val clustersSnapTarget = PipelineFunctions.getPipelineTarget(b, "clusters_snapshot_bronze")
      val poolsSnapTarget = PipelineFunctions.getPipelineTarget(b, "pools_snapshot_bronze")

      try {
        val clustersSnapDF = spark.read.format("delta").load(clustersSnapTarget.tableLocation)
        val poolsSnapDF = spark.read.format("delta").load(poolsSnapTarget.tableLocation)

        clustersSnapDF
          .withColumn("default_tags", SchemaTools.structToMap(clustersSnapDF, "default_tags"))
          .repartition('organization_id)
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("organization_id")
          .save(clustersSnapTarget.tableLocation)

        upgradeStatus.append(
          UpgradeReport(config.databaseName, "clusters_snapshot_bronze", Some("SUCCESS"), step41Msg)
        )

        poolsSnapDF
          .withColumn("default_tags", SchemaTools.structToMap(poolsSnapDF, "default_tags"))
          .repartition('organization_id)
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .partitionBy("organization_id")
          .save(clustersSnapTarget.tableLocation)

        upgradeStatus.append(
          UpgradeReport(config.databaseName, "pools_snapshot_bronze", Some("SUCCESS"), step41Msg)
        )
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "*_snapshot_bronze", Some(e.getMessage), step41Msg, failUpgrade = true)
          )
      }


      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 5: update pipReport to reflect rolled back silver/gold
    if (startStep <= 5) {
      val step5Msg = Some("Step 5: update pipReport to reflect rolled back silver/gold")
      logger.log(Level.INFO, step5Msg.get)
      println(step5Msg.get)
      val step5Silver = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
      val step5Gold = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)
      val allSilverGoldModules = step5Silver.getAllModules ++ step5Gold.getAllModules
      val moduleIDsToRollback = if (rebuildSparkTables) {
        allSilverGoldModules
      } else {
        allSilverGoldModules.filterNot(m => m.moduleName.toLowerCase.contains("spark"))
      }.map(_.moduleId)
      val rollBackSilverGoldModulesStmt =
        s"""
           |update delta.`$pipReportPath`
           |set status = concat('ROLLED BACK FOR UPGRADE: Original Status - ', status)
           |where moduleID in (${moduleIDsToRollback.mkString(", ")})
           |""".stripMargin

      val updateMsg = s"UPGRADE - Step 5 - Rolling back modules to be rebuilt\nSTATEMENT: $rollBackSilverGoldModulesStmt"
      logger.log(Level.INFO, updateMsg)
      try {
        spark.sql(rollBackSilverGoldModulesStmt)
        upgradeStatus.append(
          UpgradeReport(config.databaseName, "pipeline_report", Some("SUCCESS"), step5Msg)
        )
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "pipeline_report", Some(e.getMessage), step5Msg, failUpgrade = true)
          )
      }
      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

      // Step 6: rebuild silver targets
    if (startStep <= 6) {
      val step6Msg = Some("Step 6: rebuild silver targets")
      logger.log(Level.INFO, step6Msg.get)
      println(step6Msg.get)

      try {
        // get min primordialDate by OrgId to rebuild across all orgs
        val oldestDateByModuleW = Window.partitionBy('organization_id).orderBy('primordialDate)
        val orgRunDetails = spark.table("overwatch_dev_etl_060_upgrade.pipeline_report")
          .withColumn("primordialDate", 'primordialDateString.cast("date"))
          .select('organization_id, min('primordialDate).over(oldestDateByModuleW)
            .cast("string").alias("primordialDateString"))
          .distinct
          .as[OrgRunDetail]
          .collect

        logger.log(Level.INFO, s"REBUILDING SILVER for ORG_IDs ${orgRunDetails.map(_.organization_id).mkString(",")}")
        orgRunDetails.foreach(org => {
          logger.log(Level.INFO, s"BEGINNING SILVER REBUILD FOR ORG_ID: ${org.organization_id}")
          val orgSpecificWorkspace = workspace.copy(
            config.setOrganizationId(org.organization_id).setPrimordialDateString(Some(org.primordialDateString))
          )
          Silver(orgSpecificWorkspace, suppressReport = true, suppressStaticDatasets = true).run()
          logger.log(Level.INFO, s"COMPLETED SILVER REBUILD FOR ORG_ID: ${org.organization_id}")
          upgradeStatus.append(
            UpgradeReport(
              config.databaseName, "SILVER TARGETS", Some(s"SUCCESS: ORG_ID = ${org.organization_id}"),
              step6Msg
            )
          )
        })
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "SILVER TARGETS", Some(e.getMessage), step6Msg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 6.1: rebuild gold targets
    if (startStep <= 6.1) {
      val step61Msg = Some("Step 6: rebuild gold targets")
      logger.log(Level.INFO, step61Msg.get)
      println(step61Msg.get)

      try {
        // get min primordialDate by OrgId to rebuild across all orgs
        val oldestDateByModuleW = Window.partitionBy('organization_id).orderBy('primordialDate)
        val orgRunDetails = spark.table("overwatch_dev_etl_060_upgrade.pipeline_report")
          .withColumn("primordialDate", 'primordialDateString.cast("date"))
          .select('organization_id, min('primordialDate).over(oldestDateByModuleW)
            .cast("string").alias("primordialDateString"))
          .distinct
          .as[OrgRunDetail]
          .collect

        logger.log(Level.INFO, s"REBUILDING GOLD for ORG_IDs ${orgRunDetails.map(_.organization_id).mkString(",")}")
        orgRunDetails.foreach(org => {
          logger.log(Level.INFO, s"BEGINNING GOLD REBUILD FOR ORG_ID: ${org.organization_id}")
          val orgSpecificWorkspace = workspace.copy(
            config.setOrganizationId(org.organization_id).setPrimordialDateString(Some(org.primordialDateString))
          )
          Gold(orgSpecificWorkspace, suppressReport = true, suppressStaticDatasets = true).run()
          logger.log(Level.INFO, s"COMPLETED GOLD REBUILD FOR ORG_ID: ${org.organization_id}")
          upgradeStatus.append(
            UpgradeReport(
              config.databaseName, "GOLD TARGETS", Some(s"SUCCESS: ORG_ID = ${org.organization_id}"),
              step61Msg
            )
          )
        })
      } catch {
        case e: Throwable =>
          upgradeStatus.append(
            UpgradeReport(config.databaseName, "SILVER TARGETS", Some(e.getMessage), step61Msg, failUpgrade = true)
          )
      }

      verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, snapDir)
    }

    // Step 7: Backload workspace name for spark tables if they weren't rebuilt
    if (startStep <= 7) {
      val step7Msg = Some("Step 7: Backload workspace name for spark tables if they weren't rebuilt")
      if (!rebuildSparkTables) {
        logger.log(Level.INFO, step7Msg.get)
        println(step7Msg.get)
        val step7Silver = Silver(workspace, suppressReport = true, suppressStaticDatasets = true)
        val step7Gold = Gold(workspace, suppressReport = true, suppressStaticDatasets = true)
        val allSilverGoldTargets = step7Silver.getAllTargets ++ step7Gold.getAllTargets
        val targetsToBackload = allSilverGoldTargets.filter(_.name.toLowerCase.contains("spark")).filter(_.exists).par

        targetsToBackload
          .foreach(target => {
            try {
              appendWorkspaceName(target, workspaceNameMap)
              upgradeStatus.append(
                UpgradeReport(config.databaseName, target.name, Some("SUCCESS"), step7Msg)
              )
            } catch {
              case e: UpgradeException =>
                val upgradeReport = e.getUpgradeReport.copy(step = step7Msg)
                upgradeStatus.append(upgradeReport)
              case e: Throwable =>
                upgradeStatus.append(
                  UpgradeReport(config.databaseName, target.name, Some(e.getMessage), step7Msg)
                )
            }
          })
      }
    }

    upgradeStatus.toArray.toSeq.toDF
  }

  /**
   * Call this function after the entire upgrade has been confirmed to cleanup the backups and temporary files
   */
  def finalize060Upgrade(overwatchETLDBName: String): Unit = {
    val snapDir = "/tmp/overwatch/060_upgrade_snapsot__ctrl_0x110"
    val targetSchemaVersion = "0.600"
    val cleanupMsg = s"Cleaning up all upgrade backups and temporary reports from the upgraded to 060 located " +
      s"within path $snapDir"
    logger.log(Level.INFO, cleanupMsg)
    println(cleanupMsg)
    Helpers.fastrm(Array(snapDir))
    logger.log(Level.INFO, "UPGRADE - Cleanup complete")
    SchemaTools.modifySchemaVersion(overwatchETLDBName, targetSchemaVersion)

  }

}
