package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.pipeline.{ETLDefinition, Module, PipelineFunctions}
import com.databricks.labs.overwatch.utils.Upgrade.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import spark.implicits._

import scala.collection.concurrent
import scala.collection.mutable.ArrayBuffer

object UpgradeModuleExecutor {

  protected def handleUpgradeException(e:Throwable,upgradeStatus: ArrayBuffer[UpgradeReport],etlDatabaseName:String,stepMsg:Option[String],tbl:String,logger:Logger)={
    val failMsg = s"UPGRADE FAILED"

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

  protected def verifyUpgradeStatus(
                                     upgradeStatus: Array[UpgradeReport],
                                     initialTableVersions: Map[String, Long],
                                     snapDir: String,logger:Logger): Unit = {
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
  def apply(functionToExecute: => Unit,stepMsg:Some[String],etlDatabaseName:String,targetName:String,tempDir:String,upgradeStatus:ArrayBuffer[UpgradeReport],initialSourceVersions: concurrent.Map[String, Long],logger:Logger): Unit = {

    println(stepMsg.get)
    logger.log(Level.INFO, stepMsg.get)
    try {
      functionToExecute
      upgradeStatus.append(UpgradeReport(etlDatabaseName, targetName, Some("SUCCESS"), stepMsg))
    }
    catch {
      case e: Throwable => handleUpgradeException(e, upgradeStatus, etlDatabaseName, stepMsg, targetName,logger)
    }
    verifyUpgradeStatus(upgradeStatus.toArray, initialSourceVersions.toMap, tempDir,logger)

  }

}
