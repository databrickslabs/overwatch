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
                               targetPrefix: String,
                               scope: String = "All",
                               cloneLevel: String = "DEEP",
                               sourceConfigPath:String,
                               targetConfigPath:String

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

    val finalCloneSpecs = cloneSpecs :+ CloneDetail(sourceConfigPath, targetConfigPath, None, cloneLevel)

    val cloneReport = Helpers.parClone(cloneSpecs)
    val cloneReportPath = s"${targetPrefix}/clone_report/"
    cloneReport.toDS.write.format("delta").save(cloneReportPath)
  }

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
    val targetPrefix = args(1)
    val scope = args(2)
    val sourceConfigPath = args(3)
    val targetConfigPath = args(4)
    val targetETLDB = args(4)


    val workspace = Helpers.getWorkspaceByDatabase(sourceETLDB)
    val bronze = Bronze(workspace)

    migrate(bronze,workspace,targetPrefix,scope,scope,"Deep",sourceConfigPath,targetConfigPath)
    println("SnapShot Completed")
  }


}
