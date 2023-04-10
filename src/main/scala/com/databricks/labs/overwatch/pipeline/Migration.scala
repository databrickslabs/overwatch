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
      val bronze = Bronze(workspace)
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

    val cloneReport = Helpers.parClone(finalCloneSpecs)
    val cloneReportPath = s"${targetPrefix}/clone_report/"
    cloneReport.toDS.write.format("delta").save(cloneReportPath)
  }

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database Name
   * @param arg(1)        Target ETL Path Prefix
   * @param arg(2)        Scope for the tables to migrate. if 'Bronze' then Migrate Bronze Tables,if 'All' then migrate all the Tables.
   * @param arg(3)        Path for Source Config file.
   * @param arg(4)        Target ETl Config Path
   * @param arg(5)        Target Consumer Database Name
   * @return
   */

  def main(args: Array[String]): Unit = {

    val sourceETLDB = args(0)
    val targetPrefix = args(1)
    val scope = args(2)
    val sourceConfigPath = args(3)
    val targetConfigPath = args(4)
//    val targetETLDB = args(5)
//    val targetConsumerDB = args(6)


    val workspace = Helpers.getWorkspaceByDatabase(sourceETLDB)

    migrate(workspace,targetPrefix,scope,"Deep",sourceConfigPath,targetConfigPath)



    println("SnapShot Completed")
  }


}
