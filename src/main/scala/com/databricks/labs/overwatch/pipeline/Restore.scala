package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.Helpers.{logger, removeTrailingSlashes}
import com.databricks.labs.overwatch.utils.{CloneDetail, Config, Helpers, SparkSessionWrapper, WriteMode}
import org.apache.log4j.{Level, Logger}

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
                                  globalPath: String,
                                  cloneLevel: String = "DEEP",
                                  mode: String = "Overwrite",
                                ): Unit = {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    try {
      dbutils.fs.ls(targetPrefix)
    } catch {
      case e: Exception => dbutils.fs.mkdirs(targetPrefix)
    }

    if (mode.toLowerCase() == "overwrite") { // Restore in Overwrite Mode
      if (!dbutils.fs.ls(targetPrefix).nonEmpty) {
        val sourcesToSnap = workspace.getWorkspaceDatasets
        val cloneSpecs = sourcesToSnap.map(dataset => {
          val sourceName = dataset.name
          val sourcePath = dataset.path
          val targetPath = s"$targetPrefix/global_share/$sourceName"
          CloneDetail(sourcePath, targetPath, None, cloneLevel)
        }).toArray.toSeq
        val cloneReport = Helpers.parClone(cloneSpecs)
        val restoreReportPath = s"${targetPrefix}/restore_report/"
        cloneReport.toDS.write.format("delta").mode("overwrite").save(restoreReportPath)
      } else {
        println("Target Path is not Empty...... Could not proceed with OverWrite Mode. Please select Append Mode of Restoration")
      }
    } else { // Restore in Append Mode
      try {
        val allTargets = (bronze.getAllTargets ++ silver.getAllTargets ++ gold.getAllTargets).filter(_.exists(dataValidation = true))
        val newTargets = allTargets.map(_.copy(_mode = WriteMode.append, withCreateDate = false, withOverwatchRunID = false))
        newTargets.foreach(t => {
          val tableName = t.name.toLowerCase()
          println(s"Data is written from ${globalPath} to ${targetPrefix}/global_share/${tableName} with writeMode ${t.writeMode}")
          val inputDF = spark.read.format("delta").load(s"${globalPath}/${tableName}")
          inputDF.write.format("delta").mode(s"${t.writeMode}").option("mergeSchema", "true").save(s"${targetPrefix}/global_share/${tableName}")
        })

      } catch {
        case e: Throwable =>
          val failMsg = PipelineFunctions.appendStackStrace(e)
          logger.log(Level.WARN, failMsg)
      }
    }

  }

}

object Restore extends SparkSessionWrapper {

    def apply(workspace: Workspace,
              targetPrefix : String,
              backupPath: String,
              CloneLevel: String,
              modeOfRestoration: String
             ): Any = {

      val restoration = new Restore(workspace.getConfig.databaseName, targetPrefix, workspace, workspace.database, workspace.getConfig)
      restoration.restore(backupPath,"Deep",modeOfRestoration)

    }

    /**
     * Create a backup of the Overwatch datasets
     *
     * @param arg(0)        Source ETL Path Prefix
     * @param arg(1)        Source Organization ID
     * @param arg(2)        Target ETL Path Prefix
     * @param arg(3)        Mode of Restore. If "Overwrite" then purge the target and restore otherwise append on the existing one.
     * @return
     */

    def main(args: Array[String]): Unit = {

      val sourcePrefix = args(0)
      val orgID = args(1)
      val targetPrefix = args(2)
      val modeOfRestoration = args(3)

      val backupPath = s"${sourcePrefix}/data"  //As we are restoring from backup path  so path will be sourcePrefix/data/pipeline_report
      val pipReportPath = s"${backupPath}/pipeline_report"

      val workSpace = Helpers.getRemoteWorkspaceByPath(pipReportPath,true,orgID)

      Restore(workSpace,targetPrefix,backupPath,"Deep",modeOfRestoration)

      println("Restore Completed")
    }
}
