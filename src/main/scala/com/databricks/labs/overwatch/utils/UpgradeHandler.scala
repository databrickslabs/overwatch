package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.pipeline.{Initializer, PipelineFunctions}
import com.databricks.labs.overwatch.utils.Upgrade.{logger, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, rank, row_number, to_json}

import scala.collection.mutable.ArrayBuffer
import spark.implicits._

abstract class UpgradeHandler extends  SparkSessionWrapper{
  val logger: Logger
  val failMsg = s"UPGRADE FAILED"

  def upgrade(): DataFrame

  protected def getNumericalSchemaVersion(version: String): Int = {
    version.split("\\.").reverse.head.toInt
  }

  @throws(classOf[UpgradeException])
  protected def validateSchemaUpgradeEligibility(currentVersion: String, targetVersion: String): Unit = {
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

  protected def validateNumericalSchemaVersion(schemaVersion:Int, startSchemaVersion:Int , endSchemaVersion:Int):Unit={
    require(
      schemaVersion >= startSchemaVersion && schemaVersion < endSchemaVersion,
      s"This upgrade function is only for upgrading schema version ${startSchemaVersion}+ to new version ${endSchemaVersion} " +
        "Please first upgrade to at least schema version 0600 before proceeding. " +
        "Upgrade documentation can be found in the change log."
    )
  }

  protected def validateSchemaAndPackageVersion(schemaVersion:Int, packageVersion:Int , startSchemaVersion:Int,startPackageVersion:Int):Unit ={
    assert(schemaVersion >= startSchemaVersion && packageVersion >= startPackageVersion,
      s"""
         |This schema upgrade is only necessary when upgrading from < ${startPackageVersion} but >= ${startSchemaVersion}.
         |If upgrading from a lower schema, please perform the necessary intermediate upgrades.
         |""".stripMargin)
 }

  protected def verifyUpgradeStatus(
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
  protected def handleUpgradeException(e:Throwable,upgradeStatus: ArrayBuffer[UpgradeReport],etlDatabaseName:String,stepMsg:Option[String],tbl:String)={

    e match {
      case e: SimplifiedUpgradeException =>
        upgradeStatus.append(e.getUpgradeReport.copy(step = stepMsg))
      case e: Throwable =>
        logger.log(Level.ERROR, failMsg)
        upgradeStatus.append(
          UpgradeReport(etlDatabaseName, tbl, Some(PipelineFunctions.appendStackStrace(e, failMsg)), stepMsg, failUpgrade = true)
        )

    }

  }
  protected def getWorkspaceByOrgNew(pipelineReportPath: String, statusFilter: Column = lit(true)): Array[OrgWorkspace] = {
    val latestConfigByOrg = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
    val compactStringByOrg = spark.read.format("delta").load(pipelineReportPath)
      .filter(statusFilter)
      .withColumn("rnk", rank().over(latestConfigByOrg))
      .withColumn("rn", row_number().over(latestConfigByOrg))
      .filter('rnk === 1 && 'rn === 1)
      .select('organization_id, to_json('inputConfig).alias("compactString"))
      .as[(String, String)].collect()

    compactStringByOrg.map(cso => {
      OrgWorkspace(cso._1, Initializer(cso._2, disableValidations = true))
    })
  }

  protected def upgradeDeltaTable(qualifiedName: String): Unit = {
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

  protected def checkIfTargetExists(etlDatabaseName:String,targetName:String):Unit ={
    if (!spark.catalog.tableExists(etlDatabaseName, targetName)) {
      throw new SimplifiedUpgradeException(
        s"""
           |$targetName cannot be found in db $etlDatabaseName, proceeding with upgrade assuming no jobs
           |have been recorded.
           |""".stripMargin,
        etlDatabaseName, targetName, Some("1"), failUpgrade = false
      )
    }
  }
}

