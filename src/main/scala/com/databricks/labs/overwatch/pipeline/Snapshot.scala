package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{CloneDetail, CloneReport, Config, Helpers, OverwatchScope, SparkSessionWrapper, WorkspaceDataset}
import org.apache.log4j.Logger
import com.databricks.labs.overwatch.env.{Database, Workspace}
import org.apache.log4j.{Level, Logger}

object Snapshot extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)


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
    def snap(
              bronze: Bronze,
              workspace: Workspace,
              snapshotRootPath: String,
              cloneLevel: String = "DEEP",
              asOfTS: Option[String] = None,
              excludes: Array[String] = Array()
            ): Seq[CloneReport] = {
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
      Helpers.parClone(cloneSpecs)
    }

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param snapshotRootPath prefix of path target to send the snap
   * @param excludes         Array of table names to exclude from the snapshot
   *                         this is the table name only - without the database prefix
   * @return
   */

  def incrementalSnap(
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

    val sourcesToSnap = workspace.getWorkspaceDatasets
      .filterNot(dataset => excludes.map(_.toLowerCase).contains(dataset.name.toLowerCase))
    val finalSnapshotRootPath = s"${snapshotRootPath}/data"

    val cloneSpecs = buildCloneSpecs(targetsToSnap, finalSnapshotRootPath)
//    val cloneSpecs = buildCloneSpecs(sourcesToSnap, finalSnapshotRootPath)
    // par clone
    Helpers.snapStream(cloneSpecs, snapshotRootPath)
  }

  def buildCloneSpecs(
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

  def main(args: Array[String]): Unit = {

    println(args(0), args(1),args(2))

    val workspace = if (args(0).contains("/")){
      val remote_workspace_id = args(3)
      val pathToOverwatchGlobalShareData = args(0)
      Helpers.getRemoteWorkspaceByPath(s"${pathToOverwatchGlobalShareData}/pipeline_report/",true,remote_workspace_id)
    }else {
      Helpers.getWorkspaceByDatabase(args(0))
    }
    val bronze = Bronze(workspace)

    println(workspace)

    if (args(2).toLowerCase() == "incremental"){
      incrementalSnap(bronze,workspace,args(1))
    }else{
      snap(bronze,workspace,args(1))
    }
    println("SnapShot Completed")


  }

}
