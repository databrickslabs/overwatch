package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.utils.{CloneDetail, Helpers, SparkSessionWrapper, WriteMode}
import org.apache.log4j.Logger


object Restore extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._

  private[overwatch] def restore(
                               workspace: Workspace,
                               targetPrefix: String,
                               globalPath :String,
                               cloneLevel: String = "DEEP",
                               mode :String = "Overwrite",
                             ): Unit = {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    try{
      dbutils.fs.ls(targetPrefix)
    }catch{
      case e:Exception => dbutils.fs.mkdirs(targetPrefix)
    }

    if (mode.toLowerCase() == "overwrite"){
      if (!dbutils.fs.ls(targetPrefix).nonEmpty){
        val sourcesToSnap = workspace.getWorkspaceDatasets
        val cloneSpecs = sourcesToSnap.map(dataset => {
          val sourceName = dataset.name
          val sourcePath = dataset.path
          val targetPath = s"$targetPrefix/global_share/$sourceName"
          CloneDetail(sourcePath, targetPath, None, cloneLevel)
        }).toArray.toSeq
        val cloneReport = Helpers.parClone(cloneSpecs)
        val cloneReportPath = s"${targetPrefix}/clone_report/"
        cloneReport.toDS.write.format("delta").save(cloneReportPath)
      }else{
        println("Target Path is not Empty...... Could not proceed with OverWrite Mode. Please select Append Mode of Restoration")
      }
  }else{ // Restore in Backup Mode
      val b = Bronze(workspace)
      val s = Silver(workspace)
      val g = Gold(workspace)
      val allTargets = (b.getAllTargets ++ s.getAllTargets ++ g.getAllTargets).filter(_.exists(dataValidation = true))
      val newTargets = allTargets.map(_.copy(_mode = WriteMode.append,withCreateDate = false, withOverwatchRunID = false))
      newTargets.foreach(t => {
        val tableName = t.name.toLowerCase()
        println(s"Data is written from ${globalPath} to ${targetPrefix}/global_share/${tableName}")
        val inputDF = spark.read.format("delta").load(s"${globalPath}/${tableName}")
        inputDF.write.format("delta").mode(s"${t.writeMode}").option("mergeSchema", "true").save(s"${targetPrefix}/global_share/${tableName}")
      })

    }
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
    val mode = args(3)

    val globalPath = s"${sourcePrefix}/global_share"
    val pipReportPath = s"${globalPath}/pipeline_report"

    val workspace = Helpers.getRemoteWorkspaceByPath(pipReportPath,true,orgID)

    restore(workspace,targetPrefix,globalPath,"Deep",mode)
    println("SnapShot Completed")
  }


}
