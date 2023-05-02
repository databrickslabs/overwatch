package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes


class Migration(_sourceETLDB: String, _targetPrefix: String, _workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config){


  import spark.implicits._
  private val migrateRootPath = removeTrailingSlashes(_targetPrefix)
  private val workSpace = _workspace
  private val targetPrefix = _targetPrefix
  private val sourceETLDB = _sourceETLDB
  private val bronze = Bronze(workSpace)
  private val silver = Silver(workSpace)
  private val gold = Gold(workSpace)

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()
  val Config = _config

  private[overwatch] def buildCloneSpecs(
                                          sourceToSnap: Array[PipelineTable]
                                        ): Seq[CloneDetail] = {

    val finalMigrateRootPath= s"${migrateRootPath}/data"
    val cloneSpecs = sourceToSnap.map(dataset => {
      val sourceName = dataset.name.toLowerCase
      val sourcePath = dataset.tableLocation
      val mode = dataset._mode
      val immutableColumns = (dataset.keys ++ dataset.incrementalColumns).distinct
      val targetPath = s"$finalMigrateRootPath/$sourceName"
      CloneDetail(sourcePath, targetPath, None, "Deep",immutableColumns,mode)
    }).toArray.toSeq
    cloneSpecs
  }
  private[overwatch] def migrate(
                               pipeline : String,
                               cloneLevel: String = "DEEP",
                               excludes: Option[String] = Some(""),
                               sourceConfigPath : String,
                               targetConfigPath : String
                             ): this.type= {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val sourceToSnap = {
      if (pipeline.toLowerCase() == "bronze") bronze.getAllTargets
      else if (pipeline.toLowerCase() == "silver") silver.getAllTargets
      else if (pipeline.toLowerCase() == "gold") gold.getAllTargets
      else Array(pipelineStateTarget)
    }

    val exclude = excludes match {
      case Some(s) if s.nonEmpty => s
      case _ => ""
    }
    val excludeList = exclude.split(":")

    val cleanExcludes = excludeList.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })
    cleanExcludes.foreach(x => println(x))


    val sourceToSnapFiltered = sourceToSnap
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))

    val cloneSpecs = buildCloneSpecs(sourceToSnapFiltered) :+ CloneDetail(sourceConfigPath, targetConfigPath, None, cloneLevel)
    val cloneReport = Helpers.parClone(cloneSpecs)
    val cloneReportPath = s"${migrateRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").mode("append").save(cloneReportPath)
    this
  }

  private[overwatch] def updateConfig(
                                       targetConfigPath: String,
                                       targetPrefix : String,
                                       targetETLDB: String,
                                       targetConsumerDB: String

                                     ): Unit = {
    val configUpdateStatement = s"""
      update delta.`$targetConfigPath`
      set
        storage_prefix = '$targetPrefix',
        etl_database_name = '$targetETLDB',
        consumer_database_name = '$targetConsumerDB'
      """
    spark.sql(configUpdateStatement)
  }

}

object Migration extends SparkSessionWrapper {


  def apply(workspace: Workspace,
            sourceETLDB : String,
            targetPrefix : String,
            pipeline : String,
            excludes: Option[String],
            CloneLevel: String,
            sourceConfigPath : String,
            targetConfigPath : String,
            targetETLDB : String,
            targetConsumerDB: String
           ): Any = {
      val migration = new Migration(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig)
      migration.migrate(pipeline,CloneLevel,excludes,sourceConfigPath,targetConfigPath)
      migration.updateConfig(targetConfigPath,targetPrefix,targetETLDB,targetConsumerDB)

  }


  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database Name.
   * @param arg(1)        Target MigrateRootPath
   * @param arg(2)        Define the Medallion Layers. Argumnent should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)
   * @param arg(3)        Type of Snapshot to be performed. Full for Full Snapshot , Incremental for Incremental Snapshot
   * @param arg(4)        Array of table names to exclude from the snapshot
   *                      this is the table name only - without the database prefix
   * @return
   */

  def main(args: Array[String]): Unit = {

    val sourceETLDB = args(0)
    val migrateRootPath = args(1)
    val pipeline = args(2)
    val sourceConfigPath = args(3)
    val targetConfigPath = args(4)
    val targetETLDB = args(5)
    val targetConsumerDB = args(6)
    val tablesToExclude = args.lift(7).getOrElse("")
    val cloneLevel = args.lift(8).getOrElse("Deep")

    val snapWorkSpace = Helpers.getWorkspaceByDatabase(sourceETLDB)

    val pipelineLower = pipeline.toLowerCase
    if (pipelineLower.contains("bronze")) Migration(snapWorkSpace,sourceETLDB,migrateRootPath,"Bronze",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)
    if (pipelineLower.contains("silver")) Migration(snapWorkSpace,sourceETLDB,migrateRootPath,"Silver",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)
    if (pipelineLower.contains("gold")) Migration(snapWorkSpace,sourceETLDB,migrateRootPath,"Gold",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)
    Migration(snapWorkSpace,sourceETLDB,migrateRootPath,"pipeline_report",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)

    println("Migration Completed")
  }







}


