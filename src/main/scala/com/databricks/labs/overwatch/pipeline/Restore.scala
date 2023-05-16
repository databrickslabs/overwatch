package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.Helpers.{logger, removeTrailingSlashes}
import com.databricks.labs.overwatch.utils.{BadConfigException, CloneDetail, Config, Helpers, SparkSessionWrapper, WriteMode}
import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.pipeline.Snapshot

import java.io.FileNotFoundException


/**
 *
 * @param _sourceETLDB        ETL Database for Souce from where Restore need to be done.
 * @param _targetPrefix       Target Path for where Restore would be done
 * @param _workspace          Workspace from where Restore would be performed
 * @param _database           Workspace Database Name.
 * @param _config             Source Workspace Config.
 */

class Restore (_sourceETLDB: String, _targetPrefix: String, _workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config) {

  import spark.implicits._

  private val targetPrefix = removeTrailingSlashes(_targetPrefix)
  private val workSpace = _workspace
  private val sourceETLDB = _sourceETLDB
  private val bronze = Bronze(workSpace)
  private val silver = Silver(workSpace)
  private val gold = Gold(workSpace)

  private val logger: Logger = Logger.getLogger(this.getClass)
  val Config = _config

  private[overwatch] def restore(
                                  allTarget: Array[PipelineTable],
                                  cloneLevel: String = "DEEP",
                                ): Unit = {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    try {
      dbutils.fs.ls(targetPrefix)
    } catch {
      case e: Exception => dbutils.fs.mkdirs(targetPrefix)
    }

    try {
      if (!dbutils.fs.ls(targetPrefix).nonEmpty) {
        val cloneSpecs = new Snapshot(workSpace.getConfig.databaseName, targetPrefix, workspace, workspace.database, workspace.getConfig, "restore").buildCloneSpecs(cloneLevel, allTarget)
        val cloneReport = Helpers.parClone(cloneSpecs)
        val restoreReportPath = s"${targetPrefix}/restore_report/"
        cloneReport.toDS.write.format("delta").mode("append").save(restoreReportPath)
      } else {
        println("Target Path is not Empty...... Could not proceed with Restoration")
      }

    } catch {
      case e: Throwable =>
        val failMsg = PipelineFunctions.appendStackStrace(e)
        logger.log(Level.WARN, failMsg)
    }
  }
}

object Restore extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

    def apply(
               workSpace: Workspace,
               allTarget: Array[PipelineTable],
               targetPrefix : String,
               backupPath: String,
               CloneLevel: String
             ): Any = {

      val restoration = new Restore(workSpace.getConfig.databaseName, targetPrefix, workSpace, workSpace.database, workSpace.getConfig)
      restoration.restore(allTarget,CloneLevel)

    }

    /**
     * Create a backup of the Overwatch datasets
     *
     * @param arg(0)        Source ETL Path Prefix from where restore need to be performed
     * @param arg(1)        Target ETL Path Prefix to where restore data would be loaded.
     * @return
     */

    def main(args: Array[String]): Unit = {

      val sourcePrefix = args(0)
      val orgID = Initializer.getOrgId
      val targetPrefix = args(1)
      val cloneLevel = "Deep"
      //


      val backupPath = s"${sourcePrefix}/data"
      val pipReportPath = s"${backupPath}/pipeline_report"
      try{
        dbutils.fs.ls(s"$pipReportPath/_delta_log").nonEmpty
        logger.log(Level.INFO, s"Overwatch has being deployed with ${pipReportPath} location...proceed")
      }catch {
        case e: FileNotFoundException =>
          val msg = s"Overwatch has not been deployed with ${pipReportPath} location...can not proceed"
          logger.log(Level.ERROR, msg)
          throw new BadConfigException(msg)
      }
      val workSpace = Helpers.getRemoteWorkspaceByPath(pipReportPath,true,orgID)

      val bronze = Bronze(workSpace)
      val silver = Silver(workSpace)
      val gold = Gold(workSpace)
      val pipelineReport = bronze.pipelineStateTarget
      val allTarget = bronze.getAllTargets ++ silver.getAllTargets ++ gold.getAllTargets ++ Array(pipelineReport).filter(_.exists(dataValidation = true))

      // Step 1 : Restore Process Started
      Restore(workSpace,allTarget,targetPrefix,backupPath,cloneLevel)

      println("Restore Completed")
    }
}
