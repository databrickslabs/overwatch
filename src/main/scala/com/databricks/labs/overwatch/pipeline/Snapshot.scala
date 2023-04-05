package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{CloneDetail, CloneReport, Config, Helpers, OverwatchScope, SparkSessionWrapper, WorkspaceDataset}
import org.apache.log4j.Logger
import com.databricks.labs.overwatch.env.{Database, Workspace}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.log4j.{Level, Logger}


object Snapshot extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param snapshotRootPath prefix of path target to send the snap
   * @param cloneLevel       Deep or Shallow
   * @param asOfTS           appends asOfTimestamp option to Delta reader to limit data on clone. This will only go back as
   *                         far as the latest vacuum by design.
   * @param excludes         Array of table names to exclude from the snapshot
   *                         this is the table name only - without the database prefix
   * @return
   */
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

      val cloneSpecs  = buildCloneSpecs(targetsToSnap,snapshotRootPath,cloneLevel,asOfTS)
      val cloneReport = Helpers.parClone(cloneSpecs)
      val cloneReportPath = if (snapshotRootPath.takeRight(1) == "/") s"${snapshotRootPath}report/" else s"${snapshotRootPath}/clone_report/"
      cloneReport.toDS.write.format("delta").save(cloneReportPath)
    }

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param snapshotRootPath prefix of path target to send the snap
   * @param excludes         Array of table names to exclude from the snapshot
   *                         this is the table name only - without the database prefix
   * @return
   */

  private[overwatch] def incrementalSnap(
                       bronze: Bronze,
                       workspace: Workspace,
                       snapshotRootPath: String,
                       excludes: Array[String] = Array()
                     ): Unit = {

    val bronzeTargets = bronze.targetToSnap
    val cleanExcludes = excludes.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })
    val targetsToSnap = bronzeTargets
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))
    val finalSnapshotRootPath = s"${snapshotRootPath}/data"
    val cloneSpecs = buildCloneSpecs(targetsToSnap, finalSnapshotRootPath)
    Helpers.snapStream(cloneSpecs, snapshotRootPath)
  }

  private[overwatch] def buildCloneSpecs(
                       targetsToSnap: Array[PipelineTable],
//                       sourcesToSnap: Seq[WorkspaceDataset],
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
   * @param arg(0)        Source Database Name or Source Remote_OverwatchGlobalPath
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
