package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes

import java.io.FileNotFoundException


class Migration_OLD(_sourceETLDB: String, _targetPrefix: String, _workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config){


  import spark.implicits._
  private val migrateRootPath = removeTrailingSlashes(_targetPrefix)
  private val workSpace = _workspace
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

    val finalMigrateRootPath= s"${migrateRootPath}/global_share"
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

object Migration_OLD extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)


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
      val migration = new Migration_OLD(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig)
      migration.migrate(pipeline,CloneLevel,excludes,sourceConfigPath,targetConfigPath)
      migration.updateConfig(targetConfigPath,targetPrefix,targetETLDB,targetConsumerDB)

  }




  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database name or ETlDataPathPrefix name.
   * @param arg(1)        Remote Source WorkspaceID(Needed when you provide ETlDataPathPrefix as arg(0)
   * @param arg(2)        Target MigrateRootPath
   * @param arg(3)        Define the Medallion Layers. Argumnent should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)
   * @param arg(4)        Configuration path for Source
   * @param arg(5)        Configuration path for Destination
   * @param arg(6)        Target ETlDB Name
   * @param arg(7)        Target Consumer DB Name
   * @param arg(8)        Array of table names to exclude from the snapshot.This is the table name only - without the database prefix
   * @param arg(9)        Type of Cloning to be performed - "Deep" or "Shallow" .Default is Deep cloning
   * @return
   */

  def main(args: Array[String]): Unit = {

    val sourceETLDBOrETlDataPathPrefix = args(0)
    val remoteSourceWorkSpaceID = args.lift(1).getOrElse("")
    val migrateRootPath = args(2)
    val pipeline = args(3)
    val sourceConfigPath = args(4)
    val targetConfigPath = args(5)
    val targetETLDB = args(6)
    val targetConsumerDB = args(7)
    val tablesToExclude = args.lift(8).getOrElse("")
    val cloneLevel = args.lift(9).getOrElse("Deep")

    //Build Workspace for the Source using the SourceETLDB or SourceRemoteETLDataPathPrefix
    val snapWorkSpace = if (sourceETLDBOrETlDataPathPrefix.contains("/")){ // Will be using this when want to migrate from Remote Workspace
      val RemoteETLDataPathPrefix =   removeTrailingSlashes(sourceETLDBOrETlDataPathPrefix) + "/global_share"
      val pipReportPath = RemoteETLDataPathPrefix+"/pipeline_report"
      try{
        dbutils.fs.ls(s"$pipReportPath/_delta_log").nonEmpty
        logger.log(Level.INFO, s"Overwatch has being deployed with ${pipReportPath} location...proceed")
      }catch {
        case e: FileNotFoundException =>
          val msg = s"Overwatch has not been deployed with ${pipReportPath} location...can not proceed"
          logger.log(Level.ERROR, msg)
          throw new BadConfigException(msg)
      }
      Helpers.getRemoteWorkspaceByPath(pipReportPath,successfulOnly= true,remoteSourceWorkSpaceID)
    }else{ // Will be using this when want to migrate from Same Workspace
      val sourceETLDB = sourceETLDBOrETlDataPathPrefix
      Helpers.getWorkspaceByDatabase(sourceETLDB)
    }
    val sourceDB = snapWorkSpace.getConfig.databaseName



    val pipelineLower = pipeline.toLowerCase
    if (pipelineLower.contains("bronze")) Migration_OLD(snapWorkSpace,sourceDB,migrateRootPath,"Bronze",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)
    if (pipelineLower.contains("silver")) Migration_OLD(snapWorkSpace,sourceDB,migrateRootPath,"Silver",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)
    if (pipelineLower.contains("gold")) Migration_OLD(snapWorkSpace,sourceDB,migrateRootPath,"Gold",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)
    Migration_OLD(snapWorkSpace,sourceDB,migrateRootPath,"pipeline_report",Some(tablesToExclude),cloneLevel,sourceConfigPath,targetConfigPath,targetETLDB,targetConsumerDB)

    println("Migration Completed")
  }
}


