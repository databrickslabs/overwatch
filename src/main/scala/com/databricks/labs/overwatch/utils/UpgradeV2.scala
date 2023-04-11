package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.Helpers.{fastDrop, parOptimize}
import com.databricks.labs.overwatch.utils.Upgrade.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import spark.implicits._

class UpgradeTo0610(prodWorkspace: Workspace, startStep: Int = 1, endStep: Int = 4, enableUpgradeBelowDBR104: Boolean = false) extends UpgradeHandler {


    def this(workspace: Workspace) {
    this(workspace, 1, 4, false)
  }

  var startSchemaVersion = 600
  val targetSchemaVersion = "0.610"
  val etlDatabaseName = prodWorkspace.database.getDatabaseName
  val etlPrefix = prodWorkspace.getConfig.etlDataPathPrefix


  def step1():Unit={
    val targetName = "job_status_silver"
    checkIfTargetExists(etlDatabaseName, targetName)
    initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
    val jobSilverDF = spark.table(s"${etlDatabaseName}.${targetName}")
    removeNestedColumnsAndSaveAsTable(jobSilverDF, "new_settings", Array("tasks", "job_clusters"), etlDatabaseName, targetName)
  }

  def step2():Unit={
    val targetName = "job_gold"
    checkIfTargetExists(etlDatabaseName, targetName)
    initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
    val jobGoldDF = spark.table(s"${etlDatabaseName}.${targetName}")
    removeNestedColumnsAndSaveAsTable(jobGoldDF, "new_settings", Array("tasks", "job_clusters"), etlDatabaseName, targetName)
  }
  def step3():Unit={
    val targetName = "spark_events_bronze"
    checkIfTargetExists(etlDatabaseName, targetName)
    initialSourceVersions.put(targetName, Helpers.getLatestTableVersionByName(s"${etlDatabaseName}.${targetName}"))
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
  }

  def step4():Unit={
    SchemaTools.modifySchemaVersion(etlDatabaseName, "0.610")

  }


  override def upgrade(): DataFrame = {

    initializeParamsAndValidations(etlDatabaseName, etlPrefix, startSchemaVersion, targetSchemaVersion)

    (startStep to endStep).foreach {
      case 1 =>
        UpgradeModuleExecutor(step1(),Some("Step 1: Upgrade Schema - Job Status Silver"),etlDatabaseName,"job_status_silver",tempDir,upgradeStatus,initialSourceVersions, logger)
      case 2 =>
        UpgradeModuleExecutor(step2(),Some("Step 2: Upgrade Schema - Job Gold"),etlDatabaseName,"job_gold",tempDir,upgradeStatus,initialSourceVersions, logger)
      case 3 =>
        UpgradeModuleExecutor(step3(),Some("Step 3: Upgrade Schema - Spark Events Bronze"),etlDatabaseName,"spark_events_bronze",tempDir,upgradeStatus,initialSourceVersions, logger)
      case 4 =>
        UpgradeModuleExecutor(step4(),Some("Step 4: Upgrade Schema - ETL DB"),etlDatabaseName,"Modify Schema Version",tempDir,upgradeStatus,initialSourceVersions, logger)
    }
    upgradeStatus.toArray.toSeq.toDF()
  }

  override val logger: Logger = Logger.getLogger(this.getClass)
}




class UpgradeTo0700(prodWorkspace: Workspace, startStep: Int = 11, endStep: Int = 18) extends UpgradeHandler {

  def this(workspace: Workspace) {
    this(workspace, 11, 18)
  }

  var startSchemaVersion = 610
  val targetSchemaVersion = "0.700"
  val etlDatabaseName = prodWorkspace.database.getDatabaseName
  val etlPrefix = prodWorkspace.getConfig.etlDataPathPrefix


  override val logger: Logger = Logger.getLogger(this.getClass)
  val prodConfig = prodWorkspace.getConfig
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
  val silverModulesToRebuild = Array(2010, 2011)
  val goldModulesToRebuild = Array(3002, 3003, 3015)


  def step1(): Unit = {
    val optBronze = Bronze(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
    val optSilver = Silver(upgradeWorkspace, suppressReport = true, suppressStaticDatasets = true)
    val targetsToOptimize = Array(
      optBronze.pipelineStateTarget,
      optBronze.BronzeTargets.auditLogsTarget,
      optSilver.SilverTargets.clustersSpecTarget
    )
    parOptimize(targetsToOptimize, 128, includeVacuum = false)
  }

  def step2(): Unit = {
    val tablesToUpgrade = Array("jobs_snapshot_bronze", "job_status_silver", "jobrun_silver", "job_gold", "jobrun_gold", "jobruncostpotentialfact_gold")
    val snapExcludes = upgradeWorkspace.getWorkspaceDatasets.filterNot(ds => tablesToUpgrade.contains(ds.name))
    val cloneReport = upgradeWorkspace.snap(tempDir, excludes = snapExcludes.map(_.name).toArray)
    cloneReport.toDS.write.format("delta").save(s"${tempDir}/clone_report")
  }

  def step3(): Unit = {
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
  }

  def step4(): Unit = {
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
  }

  def step5(): Unit = { // TODO -- rebuild all workspaces val orgRunDetails = getLatestWorkspaceByOrg(pipReportPath)
    val rollbackPipReportSQL =
      s"""
         |update delta.`$pipReportPath`
         |set status = concat('ROLLED BACK FOR UPGRADE to 070: Original Status - ', status)
         |where moduleID in (${(silverModulesToRebuild ++ goldModulesToRebuild).mkString(", ")})
         |and (status = 'SUCCESS' or status like 'EMPT%')
         |""".stripMargin
    val updateMsg = s"UPGRADE - Step 5 - Rolling back modules to be rebuilt\nSTATEMENT: $rollbackPipReportSQL"
    logger.log(Level.INFO, updateMsg)
    spark.sql(rollbackPipReportSQL)

  }

  def step6(): Unit = { // TODO -- rebuild all workspaces val orgRunDetails = getLatestWorkspaceByOrg(pipReportPath)
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

  }

  def step7(): Unit = { // TODO -- rebuild all workspaces val orgRunDetails = getLatestWorkspaceByOrg(pipReportPath)
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

  }
  def step8():Unit={
    SchemaTools.modifySchemaVersion(etlDatabaseName, "0.700")

  }

  override def upgrade(): DataFrame = {

    initializeParamsAndValidations(etlDatabaseName, etlPrefix, startSchemaVersion, targetSchemaVersion)


    //startStep =1 , resumeStep =  -1 negative
    (startStep to endStep).foreach {
      case 11 =>
        UpgradeModuleExecutor(step1(), Some("Step 1: Optimize Main Sources"), etlDatabaseName, "Targets to Optimize", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 12 =>
        UpgradeModuleExecutor(step2(), Some("Step 2: Snapshot Targets To Be Rebuilt"), etlDatabaseName, "Tables to Upgrade", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 13 =>
        UpgradeModuleExecutor(step3(), Some("Step 3: Upgrade jobs_snapshot_bronze"), etlDatabaseName, "jobs_snapshot_bronze", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 14 =>
        UpgradeModuleExecutor(step4(), Some("Step 4: Drop Targets to Rebuild"), etlDatabaseName, "Tables to Upgrade", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 15 =>
        UpgradeModuleExecutor(step5(), Some("Step 5: Rollback State For Upgrade Modules"), etlDatabaseName, "pipeline_report", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 16 =>
        UpgradeModuleExecutor(step6(), Some("Step 6: Rebuild Silver Targets"), etlDatabaseName, "Silver Targets to be Rebuilt", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 17 =>
        UpgradeModuleExecutor(step7(), Some("Step 7: Rebuild Gold Targets"), etlDatabaseName, "Gold Targets to be Rebuilt", tempDir, upgradeStatus, initialSourceVersions, logger)
      case 18 =>
        UpgradeModuleExecutor(step8(), Some("Step 8: Upgrade Schema - ETL DB"), etlDatabaseName, "Modify Schema Version", tempDir, upgradeStatus, initialSourceVersions, logger)

    }
    upgradeStatus.toArray.toSeq.toDF()

  }
}