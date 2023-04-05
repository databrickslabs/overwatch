package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{CloneDetail, CloneReport, Config, Helpers, OverwatchScope, SparkSessionWrapper, WorkspaceDataset}
import org.apache.log4j.Logger
import com.databricks.labs.overwatch.env.{Database, Workspace}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.log4j.{Level, Logger}


object Migration extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._

  private[overwatch] def migrate(
                               bronze: Bronze,
                               workspace: Workspace,
                               scope: String,
                               targetPrefix: String,
                               cloneLevel: String = "DEEP",
                               asOfTS: Option[String] = None,
                               SOURCECONFIGPATH:String,
                               TARGETCONFIGPATH:String

                             ): Unit = {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val cloneSpecs = if (scope.toLowerCase() == "bronze"){
      val sourcesToSnap =  bronze.targetToSnap
      sourcesToSnap.map(dataset => {
        val sourceName = dataset.name.toLowerCase
        val sourcePath = dataset.tableLocation
        val targetPath = s"$targetPrefix/global_share/$sourceName"
        CloneDetail(sourcePath, targetPath, None, cloneLevel)
      }).toArray.toSeq
    }else{
      val sourcesToSnap = workspace.getWorkspaceDatasets
      sourcesToSnap.map(dataset => {
        val sourceName = dataset.name
        val sourcePath = dataset.path
        val targetPath = s"$targetPrefix/global_share/$sourceName"
        CloneDetail(sourcePath, targetPath, None, cloneLevel)
      }).toArray.toSeq
    }

//    val cloneSpecs  = buildCloneSpecs(sourcesToSnap,snapshotRootPath,cloneLevel,asOfTS)
    val cloneReport = Helpers.parClone(cloneSpecs,snapshotRootPath)
    val cloneReportPath = if (snapshotRootPath.takeRight(1) == "/") s"${snapshotRootPath}report/" else s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").save(cloneReportPath)
  }

//  private[overwatch] def buildCloneSpecs(
//                                          targetsToSnap: Array[PipelineTable],
//                                          snapshotRootPath: String,
//                                          cloneLevel: String = "DEEP",
//                                          asOfTS: Option[String] = None,
//                                        ): Seq[CloneDetail] = {
//    val cloneSpecs = targetsToSnap.map(dataset => {
//      val sourceName = dataset.name.toLowerCase
//      val sourcePath = dataset.tableLocation
//      val targetPath = if (snapshotRootPath.takeRight(1) == "/") s"$snapshotRootPath$sourceName" else s"$snapshotRootPath/$sourceName"
//      CloneDetail(sourcePath, targetPath, asOfTS, cloneLevel)
//    }).toArray.toSeq
//    cloneSpecs
//  }


  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database Name
   * @param arg(1)        Target snapshotRootPath
   * @param arg(2)        Scope for the tables to migrate. if 'Bronze' then Migrate Bronze Tables,if 'All' then migrate all the Tables.
   * @param arg(3)        Optional Field for RemoteWorkSpaceID. Needed when arg(0) is Remote_OverwatchGlobalPath
   * @return
   */

  def main(args: Array[String]): Unit = {

    val sourceETLDB = args(0)
    val TARGETETLDB = a

    val workspace = Helpers.getWorkspaceByDatabase(args(0))
    val bronze = Bronze(workspace)

    migrate(bronze,workspace,args(1),args(2))
    println("SnapShot Completed")
  }


}
