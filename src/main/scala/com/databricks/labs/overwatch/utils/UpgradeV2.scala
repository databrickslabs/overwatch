package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.Helpers.{fastDrop, parOptimize}
import com.databricks.labs.overwatch.utils.Upgrade.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import spark.implicits._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import scala.collection.concurrent
import scala.collection.mutable.ArrayBuffer

class UpgradeTo0610(prodWorkspace: Workspace, startStep: Int = 1, endStep: Int = 3, enableUpgradeBelowDBR104: Boolean = false) extends UpgradeHandler {


  def this(workspace: Workspace) {
    this(workspace, 1, 3)
  }


  override def upgrade(): DataFrame = {
    val tempDir = s"/tmp/overwatch/upgrade0610_status__ctrl_0x111/${System.currentTimeMillis()}"
    val etlDatabaseName = prodWorkspace.database.getDatabaseName
    dbutils.fs.mkdirs(tempDir) // init tempDir -- if no errors it wouldn't be created
    val blankConfig = new Config()
    val currentSchemaVersion = SchemaTools.getSchemaVersion(etlDatabaseName)
    val numericalSchemaVersion = getNumericalSchemaVersion(currentSchemaVersion)
    val targetSchemaVersion = "0.610"
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)
    validateNumericalSchemaVersion(numericalSchemaVersion, 600, 610)
    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    val dbrVersion = spark.conf.get("spark.databricks.clusterUsageTags.effectiveSparkVersion")
    val dbrMajorV = dbrVersion.split("\\.").head
    val dbrMinorV = dbrVersion.split("\\.")(1)
    val dbrVersionNumerical = s"$dbrMajorV.$dbrMinorV".toDouble
    val initialSourceVersions: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala
    val packageVersion = blankConfig.getClass.getPackage.getImplementationVersion.replaceAll("\\.", "").tail.toInt
    val startingSchemaVersion = SchemaTools.getSchemaVersion(etlDatabaseName).split("\\.").takeRight(1).head.toInt
    validateSchemaAndPackageVersion(startingSchemaVersion, packageVersion, 600, 610)

    (startStep to endStep).foreach {
      case 1 =>
        val stepMsg = Some("Step 1: Upgrade Schema - Job Status Silver")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        val targetName = "job_status_silver"
        try {
          checkIfTargetExists(etlDatabaseName, targetName)
          initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
          val jobSilverDF = spark.table(s"${etlDatabaseName}.${targetName}")
          removeNestedColumnsAndSaveAsTable(jobSilverDF,"new_settings", Array("tasks", "job_clusters"),etlDatabaseName,targetName)
          upgradeStatus.append(UpgradeReport(etlDatabaseName, targetName, Some("SUCCESS"), stepMsg))
        } catch {
          case e:Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, targetName)
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)
      case 2 =>

        val stepMsg = Some("Step 2: Upgrade Schema - Job Gold")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        val targetName = "job_gold"
        try {
          checkIfTargetExists(etlDatabaseName, targetName)
          initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
          val jobGoldDF = spark.table(s"${etlDatabaseName}.${targetName}")
          removeNestedColumnsAndSaveAsTable(jobGoldDF,"new_settings", Array("tasks", "job_clusters"),etlDatabaseName,targetName)
          upgradeStatus.append(UpgradeReport(etlDatabaseName, targetName, Some("SUCCESS"), stepMsg))
        } catch {

          case e:Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, targetName)
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 3 =>
        val stepMsg = Some("Step 3: Upgrade Schema - Spark Events Bronze")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        val targetName = "spark_events_bronze"
        try {
          checkIfTargetExists(etlDatabaseName, targetName)
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
          case e:Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, targetName)

        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)


    }
    upgradeStatus.toArray.toSeq.toDF()
  }

  override val logger: Logger = Logger.getLogger(this.getClass)
}

class UpgradeTo0700(prodWorkspace: Workspace, startStep: Int = 11, endStep: Int = 17) extends UpgradeHandler {

  def this(workspace: Workspace) {
    this(workspace, 11, 17)
  }


  override val logger: Logger = Logger.getLogger(this.getClass)


  override def upgrade(): DataFrame = {
    val prodConfig = prodWorkspace.getConfig
    val etlDatabaseName = prodConfig.databaseName
    val upgradeParamString = JsonUtils.objToJson(
      prodConfig.inputConfig.copy(
        overwatchScope = Some(Seq("audit", "clusters", "jobs")),
        maxDaysToLoad = 1000,
        externalizeOptimize = true)
    ).compactString
    val upgradeWorkspace = Initializer(upgradeParamString)
    val upgradeConfig = upgradeWorkspace.getConfig
    upgradeConfig
      .setExternalizeOptimize(true)
      .setOverwatchSchemaVersion("0.610") // override to pass schema check across versions
      .setDebugFlag(false)
    val tempDir = upgradeConfig.etlDataPathPrefix.split("/").dropRight(1).mkString("/") + s"/upgrade070_tempDir/${System.currentTimeMillis()}"
    val pipReportPath = s"${upgradeConfig.etlDataPathPrefix}/pipeline_report"
    val silverModulesToRebuild = Array(2010, 2011)
    val goldModulesToRebuild = Array(3002, 3003, 3015)
    dbutils.fs.mkdirs(tempDir) // init tempDir -- if no errors it wouldn't be created
    val currentSchemaVersion = SchemaTools.getSchemaVersion(etlDatabaseName)
    val numericalSchemaVersion = getNumericalSchemaVersion(currentSchemaVersion)
    val targetSchemaVersion = "0.700"
    validateSchemaUpgradeEligibility(currentSchemaVersion, targetSchemaVersion)
    validateNumericalSchemaVersion(numericalSchemaVersion, 610, 700)

    val upgradeStatus: ArrayBuffer[UpgradeReport] = ArrayBuffer()
    val initialSourceVersions: concurrent.Map[String, Long] = new ConcurrentHashMap[String, Long]().asScala
    val packageVersion = new Config().getClass.getPackage.getImplementationVersion.replaceAll("\\.", "").tail.toInt
    val startingSchemaVersion = SchemaTools.getSchemaVersion(etlDatabaseName).split("\\.").takeRight(1).head.toInt
    validateSchemaAndPackageVersion(startingSchemaVersion, packageVersion, 610, 700)

    //startStep =1 , resumeStep =  -1 negative
    (startStep to endStep).foreach {
      case 11 =>
        val stepMsg = Some("Step 1: Optimize Main Sources")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        try {
          val optBronze = Bronze(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
          val optSilver = Silver(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
          val targetsToOptimize = Array(
            optBronze.pipelineStateTarget,
            optBronze.BronzeTargets.auditLogsTarget,
            optSilver.SilverTargets.clustersSpecTarget
          )
          parOptimize(targetsToOptimize, 128, includeVacuum = false)
          upgradeStatus.append(UpgradeReport(etlDatabaseName, "Targets to Optimize", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "Targets to Optimize")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 12 =>
        val stepMsg = Some("Step 2: Snapshot Targets To Be Rebuilt")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        try {
          val tablesToUpgrade = Array("jobs_snapshot_bronze", "job_status_silver", "jobrun_silver", "job_gold", "jobrun_gold", "jobruncostpotentialfact_gold")
          val snapExcludes = upgradeWorkspace.getWorkspaceDatasets.filterNot(ds => tablesToUpgrade.contains(ds.name))
          val cloneReport = upgradeWorkspace.snap(tempDir, excludes = snapExcludes.map(_.name).toArray)
          cloneReport.toDS.write.format("delta").save(s"${tempDir}/clone_report")
          upgradeStatus.append(UpgradeReport(etlDatabaseName, "Tables to Upgrade", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "Tables to Upgrade")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 13 =>
        val stepMsg = Some("Step 3: Upgrade jobs_snapshot_bronze")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        try {
          val upgradeBronze = Bronze(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
          val jSnapBronzeTarget = PipelineFunctions.getPipelineTarget(upgradeBronze, "jobs_snapshot_bronze")
            .copy(_mode = WriteMode.overwrite, withCreateDate = false, withOverwatchRunID = false, masterSchema = None)
          val jSnapBronzeDF = jSnapBronzeTarget.asDF(withGlobalFilters = false)
            .scrubSchema

          // validation to ensure settings.tags exists and is a not already a map
          if (SchemaTools.nestedColExists(jSnapBronzeDF.schema, "settings.tags")) {
            val tagsTypename = jSnapBronzeDF.select($"settings.tags").schema.fields.find(_.name == "tags").get.dataType.typeName
            if (tagsTypename == "map") {
              throw new SimplifiedUpgradeException(
                s"settings.tags is already the proper type 'map'. Skipping Step",
                etlDatabaseName, jSnapBronzeTarget.name, Some("3"), failUpgrade = false
              )
            } else if (tagsTypename != "map" && tagsTypename != "struct") {
              throw new SimplifiedUpgradeException(
                s"settings.tags must either be a struct or a map to proceed. Failing Upgrade",
                etlDatabaseName, jSnapBronzeTarget.name, Some("3"), failUpgrade = true
              )
            }
          } else {
            throw new SimplifiedUpgradeException(
              "settings.tags does not exist in jobs_snapshot_bronze. Skipping Step",
              etlDatabaseName, jSnapBronzeTarget.name, Some("3"), failUpgrade = false
            )
          }
          val upgradedTags = NamedColumn("tags", SchemaTools.structToMap(jSnapBronzeDF, "settings.tags"))
          val upgradedJSnapBronze = jSnapBronzeDF
            .appendToStruct("settings", Array(upgradedTags), overrideExistingStructCols = true)

          upgradeBronze.database.write(upgradedJSnapBronze, jSnapBronzeTarget, upgradeBronze.pipelineSnapTime.asColumnTS)

          upgradeStatus.append(UpgradeReport(etlDatabaseName, "jobs_snapshot_bronze", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "jobs_snapshot_bronze")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 14 =>
        val stepMsg = Some("Step 4: Drop Targets to Rebuild")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)
        try {

          val silver = Silver(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
          val gold = Gold(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
          val targetsToRebuild = Array(
            PipelineFunctions.getPipelineTarget(silver, "job_status_silver"),
            PipelineFunctions.getPipelineTarget(silver, "jobrun_silver"),
            PipelineFunctions.getPipelineTarget(gold, "job_gold"),
            PipelineFunctions.getPipelineTarget(gold, "jobrun_gold"),
            PipelineFunctions.getPipelineTarget(gold, "jobRunCostPotentialFact_gold")
          )

          targetsToRebuild.filter(_.exists).map(t => fastDrop(t, upgradeConfig.cloudProvider))
          // ensure parent dirs are deleted
          targetsToRebuild.foreach(t => dbutils.fs.rm(t.tableLocation, true))
          upgradeStatus.append(UpgradeReport(etlDatabaseName, "Tables to Upgrade", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "Tables to Upgrade")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 15 => // TODO -- rebuild all workspaces val orgRunDetails = getLatestWorkspaceByOrg(pipReportPath)
        val stepMsg = Some("Step 5: Rollback State For Upgrade Modules")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)

        val rollbackPipReportSQL =
          s"""
             |update delta.`$pipReportPath`
             |set status = concat('ROLLED BACK FOR UPGRADE to 070: Original Status - ', status)
             |where moduleID in (${(silverModulesToRebuild ++ goldModulesToRebuild).mkString(", ")})
             |and (status = 'SUCCESS' or status like 'EMPT%')
             |""".stripMargin
        val updateMsg = s"UPGRADE - Step 5 - Rolling back modules to be rebuilt\nSTATEMENT: $rollbackPipReportSQL"
        logger.log(Level.INFO, updateMsg)

        try {
          spark.sql(rollbackPipReportSQL)

          upgradeStatus.append(UpgradeReport(etlDatabaseName, "pipeline_report", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "pipeline_report")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 16 =>
        val stepMsg = Some("Step 6: Rebuild Silver Targets")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)

        try {
          // get latest workspace config by org id
          val orgRunDetails = getWorkspaceByOrgNew(pipReportPath)
          logger.log(Level.INFO, s"\nREBUILDING SILVER for ORG_IDs ${orgRunDetails.map(_.organization_id).mkString(",")}\n")
          orgRunDetails.foreach(org => {
            val launchSilverPipelineForOrgMsg = s"BEGINNING SILVER REBUILD FOR ORG_ID: ${org.organization_id}"
            logger.log(Level.INFO, launchSilverPipelineForOrgMsg)
            println(launchSilverPipelineForOrgMsg)
            val orgWorkspace = org.workspace

            val orgCloudProvider = if (orgWorkspace.getConfig.auditLogConfig.rawAuditPath.isEmpty) "azure" else "aws"
            logger.info(s"CLOUD PROVIDER SET: $orgCloudProvider for ORGID: ${org.organization_id}")

            orgWorkspace
              .getConfig
              .setDebugFlag(false)
              .setOrganizationId(org.organization_id)
              .setMaxDays(1000)
              .setOverwatchSchemaVersion("0.700")
              .setOverwatchScope(Seq(OverwatchScope.jobs))
              .setDatabaseNameAndLoc(upgradeConfig.databaseName, upgradeConfig.databaseLocation, upgradeConfig.etlDataPathPrefix)
              .setConsumerDatabaseNameandLoc(upgradeConfig.consumerDatabaseName, upgradeConfig.consumerDatabaseLocation)
              .setCloudProvider(orgCloudProvider)
              .setTempWorkingDir(upgradeConfig.tempWorkingDir)

            val upgradeSilver = Silver(orgWorkspace, suppressReport = true, suppressStaticDatasets = true)
              .setReadOnly(false)
            // reset rebuild module states to ensure first run is detected
            if (startStep <= 4) silverModulesToRebuild.foreach(upgradeSilver.dropModuleState) // only drop the state if the Upgrade process dropped and reset the states
            upgradeSilver.run()
            logger.log(Level.INFO, s"COMPLETED SILVER REBUILD FOR ORG_ID: ${org.organization_id}")
          })
          upgradeStatus.append(UpgradeReport(etlDatabaseName, "Silver Targets to be Rebuilt", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "Silver Targets to be Rebuilt")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

      case 17 =>
        val stepMsg = Some("Step 7: Rebuild Gold Targets")
        println(stepMsg.get)
        logger.log(Level.INFO, stepMsg.get)

        try {
          // get latest workspace config by org id
          val orgRunDetails = getWorkspaceByOrgNew(pipReportPath)
          logger.log(Level.INFO, s"\nREBUILDING GOLD for ORG_IDs ${orgRunDetails.map(_.organization_id).mkString(",")}\n")
          orgRunDetails.foreach(org => {
            val launchSilverPipelineForOrgMsg = s"BEGINNING GOLD REBUILD FOR ORG_ID: ${org.organization_id}"
            logger.log(Level.INFO, launchSilverPipelineForOrgMsg)
            println(launchSilverPipelineForOrgMsg)
            val orgWorkspace = org.workspace

            val orgCloudProvider = if (orgWorkspace.getConfig.auditLogConfig.rawAuditPath.isEmpty) "azure" else "aws"
            logger.info(s"CLOUD PROVIDER SET: $orgCloudProvider for ORGID: ${org.organization_id}")

            orgWorkspace
              .getConfig
              .setDebugFlag(false)
              .setOrganizationId(org.organization_id)
              .setMaxDays(1000)
              .setOverwatchSchemaVersion("0.700")
              .setOverwatchScope(Seq(OverwatchScope.jobs))
              .setDatabaseNameAndLoc(upgradeConfig.databaseName, upgradeConfig.databaseLocation, upgradeConfig.etlDataPathPrefix)
              .setConsumerDatabaseNameandLoc(upgradeConfig.consumerDatabaseName, upgradeConfig.consumerDatabaseLocation)
              .setCloudProvider(orgCloudProvider)
              .setTempWorkingDir(upgradeConfig.tempWorkingDir)

            val upgradeGold = Gold(orgWorkspace, suppressReport = true, suppressStaticDatasets = true)
              .setReadOnly(false)
            // reset rebuild module states to ensure first run is detected
            if (startStep <= 4) goldModulesToRebuild.foreach(upgradeGold.dropModuleState) // only drop the state if the Upgrade process dropped and reset the states
            upgradeGold.run()
            logger.log(Level.INFO, s"COMPLETED GOLD REBUILD FOR ORG_ID: ${org.organization_id}")
          })
          upgradeStatus.append(UpgradeReport(etlDatabaseName, "Gold Targets to be Rebuilt", Some("SUCCESS"), stepMsg))
        } catch {
          case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, "Gold Targets to be Rebuilt")
        }
        verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir)

    }
    upgradeStatus.toArray.toSeq.toDF()
  }
}