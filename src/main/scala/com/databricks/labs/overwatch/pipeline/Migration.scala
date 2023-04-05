package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{CloneDetail, CloneReport, Config, Helpers, OverwatchScope, SparkSessionWrapper, WorkspaceDataset}
import org.apache.log4j.Logger
import com.databricks.labs.overwatch.env.{Database, Workspace}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.log4j.{Level, Logger}


object Migration extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._

  private[overwatch] def snap(
                               bronze: Bronze,
                               workspace: Workspace,
                               snapshotRootPath: String,
                               cloneLevel: String = "DEEP",
                               asOfTS: Option[String] = None,
                               excludes: Array[String] = Array()
                             ): Unit = {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val bronzeTargets = bronze.targetToSnap
    val cleanExcludes = excludes.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })
    val targetsToSnap = bronzeTargets
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))
    val sourcesToSnap = workspace.getWorkspaceDatasets
      .filterNot(dataset => excludes.map(_.toLowerCase).contains(dataset.name.toLowerCase))
    val cloneSpecs  = buildCloneSpecs(targetsToSnap,snapshotRootPath,cloneLevel,asOfTS)
    val cloneReport = Helpers.parClone(cloneSpecs,snapshotRootPath)
    val cloneReportPath = if (snapshotRootPath.takeRight(1) == "/") s"${snapshotRootPath}report/" else s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").save(cloneReportPath)
  }

  private[overwatch] def buildCloneSpecs(
                                          targetsToSnap: Array[PipelineTable],
                                          snapshotRootPath: String,
                                          cloneLevel: String = "DEEP",
                                          asOfTS: Option[String] = None,
                                        ): Seq[CloneDetail] = {
    val cloneSpecs = targetsToSnap.map(dataset => {
      val sourceName = dataset.name.toLowerCase
      val sourcePath = dataset.tableLocation
      val targetPath = if (snapshotRootPath.takeRight(1) == "/") s"$snapshotRootPath$sourceName" else s"$snapshotRootPath/$sourceName"
      CloneDetail(sourcePath, targetPath, asOfTS, cloneLevel)
    }).toArray.toSeq
    cloneSpecs
  }
  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database Name
   * @param arg(1)        Target snapshotRootPath
   * @param arg(2)        Flag to Determine whether the snap is normal batch process or Incremental one.(if "true" then incremental else normal snap)
   * @param arg(3)        Optional Field for RemoteWorkSpaceID. Needed when arg(0) is Remote_OverwatchGlobalPath
   * @return
   */

  def main(args: Array[String]): Unit = {

    val workspace = if (args(0).contains("/")){
      val remote_workspace_id = args(3)
      val pathToOverwatchGlobalShareData = args(0)
      Helpers.getRemoteWorkspaceByPath(s"${pathToOverwatchGlobalShareData}/pipeline_report/",true,remote_workspace_id)
    }else {
      Helpers.getWorkspaceByDatabase(args(0))
    }
    val bronze = Bronze(workspace)
    if (args(2).toLowerCase() == "true"){
      incrementalSnap(bronze,workspace,args(1))
    }else{
      snap(bronze,workspace,args(1))
    }
    println("SnapShot Completed")
  }


}
