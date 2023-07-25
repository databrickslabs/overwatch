package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row}


/**
 ** Class for Snapshot Process (Both Incremental or Full Snapshot)
 * @param _sourceETLDB          ETL Database for Souce from where Snapshot need to be done.
 * @param _targetPrefix         Target Path for where snapshot would be done
 * @param _workspace            Workspace from where snapshot would be performed
 * @param _database             Workspace Database Name.
 * @param _config               Source Workspace Config.
 * @param _processType          Process Type for Snapshot. Default is Snapshot, Otherwise it is "Restore" or "Migration"
 */
class Snapshot (_sourceETLDB: String, _targetPrefix: String, _workspace: Workspace, _database: Database, _config: Config,_processType: String)
  extends Pipeline(_workspace, _database, _config){


  import spark.implicits._
  private val snapshotRootPath = removeTrailingSlashes(_targetPrefix)
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()
  private val Config = _config
  private val processType = _processType

  private[overwatch] def streamWrite(sourceName:String,
                  checkPointLocation:String,
                  targetLocation:String,
                  rawStreamingDF:DataFrame,
                  cloneSpec:CloneDetail):StreamingQuery = {
    logger.log(Level.INFO, s"Beginning write to ${sourceName}")
    val msg = s"Checkpoint Path Set: ${checkPointLocation} - proceeding with streaming write with source as ${sourceName}"
    logger.log(Level.INFO, msg)

    var streamWriter = rawStreamingDF.writeStream.outputMode("append").trigger(Trigger.Once()).format("delta").option("checkpointLocation", checkPointLocation)
      .queryName(s"StreamTo_${sourceName}")

    streamWriter = if (cloneSpec.mode == WriteMode.overwrite) { // set overwrite && set overwriteSchema == true
      streamWriter.option("overwriteSchema", "true")
    } else { // append AND merge schema
      streamWriter
        .option("mergeSchema", "true")
    }
    streamWriter
      .asInstanceOf[DataStreamWriter[Row]]
      .option("path", targetLocation)
      .start()
  }

  private[overwatch] def upsertToDelta(microBatchOutputDF: DataFrame,
                    batchId: Long,
                    immutableColumns: Array[String],
                    sourceName:String,
                    deltaTarget:DeltaTable) : Unit = {
    val mergeCondition: String = immutableColumns.map(k => s"updates.$k = target.$k").mkString(" AND ")
    val mergeDetailMsg =
      s"""
         |Beginning upsert to ${sourceName}.
         |MERGE CONDITION: $mergeCondition
         |""".stripMargin
    logger.log(Level.INFO, mergeDetailMsg)
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", Config.runID)

    deltaTarget
      .merge(microBatchOutputDF.as("updates"), mergeCondition)
      .whenMatched
      .updateAll()
      .whenNotMatched
      .insertAll()
      .execute()

    spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
  }


  private[overwatch] def snapStream(cloneDetails: Seq[CloneDetail]): Unit = {

    val cloneDetailsPar = cloneDetails.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(1))
    cloneDetailsPar.tasksupport = taskSupport
    import spark.implicits._
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",true)

    logger.log(Level.INFO, "Streaming START:")
    val cloneReport = cloneDetailsPar.map(cloneSpec => {
      try {
        val rawStreamingDF = spark.readStream.format("delta").option("ignoreChanges", "true").load(s"${cloneSpec.source}")
        val sourceName = s"${cloneSpec.source}".split("/").takeRight(1).head
        val checkPointLocation = s"${snapshotRootPath}/checkpoint/${sourceName}"
        val targetLocation = s"${cloneSpec.target}"

        val streamWriter = if(Helpers.pathExists(targetLocation) && cloneSpec.mode == WriteMode.merge){
            val deltaTarget = DeltaTable.forPath(spark,targetLocation).alias("target")
            val updatesDF = rawStreamingDF
            val immutableColumns = cloneSpec.immutableColumns

            updatesDF
              .writeStream
              .format("delta")
              .foreachBatch { (df: DataFrame, batchId: Long) =>
                upsertToDelta(df, batchId, immutableColumns, sourceName,deltaTarget)
              }
              .trigger(Trigger.Once())
              .option("checkpointLocation", checkPointLocation)
              .queryName(s"Streaming_${sourceName}")
              .option("mergeSchema", "true")
              .option("path", targetLocation)
              .start()

        }else //First time Streaming
        {
          streamWrite(sourceName:String,checkPointLocation:String,targetLocation:String,rawStreamingDF:DataFrame,cloneSpec:CloneDetail)
        }

        val streamManager = Helpers.getQueryListener(streamWriter,workspace.getConfig, workspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.get.minEventsPerTrigger)
        spark.streams.addListener(streamManager)
        val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter.name}\nID: ${streamWriter.id}"
        logger.log(Level.INFO, listenerAddedMsg)

        streamWriter.awaitTermination()
        spark.streams.removeListener(streamManager)
        logger.log (Level.INFO, s"Streaming COMPLETE: ${cloneSpec.source} --> ${cloneSpec.target}")

        CloneReport(cloneSpec, s"Streaming For: ${cloneSpec.source} --> ${cloneSpec.target}", "SUCCESS")

      } catch {
        case e: Throwable if (e.getMessage.contains("is after the latest commit timestamp of")) => {
          val failMsg = PipelineFunctions.appendStackStrace(e)
          val msg = s"SUCCESS WITH WARNINGS: The timestamp provided, ${cloneSpec.asOfTS.get} " +
            s"resulted in a temporally unsafe exception. Cloned the source without the as of timestamp arg. " +
            s"\nDELTA ERROR MESSAGE: ${failMsg}"
          logger.log(Level.WARN, msg)
          CloneReport(cloneSpec, s"Streaming For: ${cloneSpec.source} --> ${cloneSpec.target}", msg)
        }
        case e: Throwable => {
          val failMsg = PipelineFunctions.appendStackStrace(e)
          CloneReport(cloneSpec, s"Streaming For: ${cloneSpec.source} --> ${cloneSpec.target}", failMsg)
        }
      }
    }).toArray.toSeq

    val cloneReportPath = s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.mode("append").option("mergeSchema", "true").format("delta").save(cloneReportPath)
    logger.log(Level.INFO, s"Clone report has been generated to this path: ${cloneReportPath}")
  }

  private[overwatch] def buildCloneSpecs(
                                          cloneLevel: String,
                                          sourcesToSnap: Array[PipelineTable]
                                        ): Seq[CloneDetail] = {

    val finalSnapshotRootPath  = if (Array("migration","restore").contains(processType.toLowerCase())){
      s"${snapshotRootPath}/global_share"
    }else{
      s"${snapshotRootPath}/data"
    }

    sourcesToSnap.map(dataset => {
      val sourceName = dataset.name.toLowerCase
      val sourcePath = dataset.tableLocation
      val mode = dataset._mode
      val immutableColumns = (dataset.keys ++ dataset.incrementalColumns).distinct
      val targetPath = s"$finalSnapshotRootPath/$sourceName"
      CloneDetail(sourcePath, targetPath, None, cloneLevel,immutableColumns,mode)
    }).toSeq
  }

  private[overwatch] def filteredTables(sourceToSnap : Array[PipelineTable], excludes: Option[String] = Some("")) : Array[PipelineTable] = {

    val exclude = excludes match {
      case Some(s) if s.nonEmpty => s
      case _ => ""
    }
    val excludeList = exclude.split(",").map(_.trim)

    val cleanExcludes = excludeList.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })

    sourceToSnap
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))
  }


  private[overwatch] def incrementalSnap(
                                          pipelineTables : Array[PipelineTable],
                                          excludes: Option[String] = Some("")
                                        ): Unit = {


    val sourceToSnapFiltered = filteredTables(pipelineTables,excludes)

    val cloneSpecs = buildCloneSpecs("DEEP",sourceToSnapFiltered)
    snapStream(cloneSpecs)
  }

  private[overwatch] def snap(
                               pipelineTables : Array[PipelineTable],
                               cloneLevel: String = "DEEP",
                               excludes: Option[String] = Some("")
                             ): Unit= {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val sourceToSnapFiltered = filteredTables(pipelineTables,excludes)

    val cloneSpecs = buildCloneSpecs(cloneLevel,sourceToSnapFiltered)
    val cloneReport = Helpers.parClone(cloneSpecs)
    val cloneReportPath = s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").mode("append").save(cloneReportPath)
    logger.log(Level.INFO, s"Clone report has been generated to this path: ${cloneReportPath}")
  }

}

object Snapshot extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def isValid(sourceETLDB : String,
               snapshotType : String,
               pipeline : String = "Bronze,Silver,Gold",
               cloneLevel: String = "DEEP",
               ): Boolean = {

    // Check whether sourceETLDB is Overwatch Database
    val isOverwatchDB = spark.sessionState.catalog.getDatabaseMetadata(sourceETLDB).properties.getOrElse("OVERWATCHDB", "FALSE").toBoolean
    if (isOverwatchDB){
      println(s"${sourceETLDB} is Overwatch Database and suitable for Snapshot")
    }else{
      val errMsg = s"${sourceETLDB} is Not Overwatch Database and not suitable for Snapshot"
      throw new BadConfigException(errMsg)
    }

    // Snapshot Type should be Incremental or Full
    if (snapshotType == "Incremental" || snapshotType == "Full"){
      println(s"Snapshot Type is Suitable for Snapshot Process. Provided SnapshotType value is ${snapshotType}")
    }else{
      val errMsg = s"Provided SnapshotType value is ${snapshotType}. SnapshotType value should be either Full or Incremental. Can Not Proceed with Snapshot"
      throw new BadConfigException(errMsg)
    }

    // Pipeline Should be Bronze, Sliver Or Gold

    val pipelineList = pipeline.split(",").map(_.toLowerCase).map(_.trim)

    for (layer <- pipelineList){
      if (layer == "bronze" || layer == "silver" || layer == "gold") {
        println(s"Zone should be either Bronze,Silver or Gold. Provied Zone value is ${layer}")
      }else
      {
        val errMsg = s"Unknown Zone found ${layer}, Zone should be either Bronze,Silver or Gold"
        throw new BadConfigException(errMsg)
      }}

    // Clone Level should be "Deep" or "Shallow"
    if (cloneLevel == "Deep" || cloneLevel == "SHALLOW"){
      println(s"cloneLevel Type is Suitable for Snapshot Process. Provided cloneLevel value is ${cloneLevel}")
    }else{
      val errMsg = s"Provided cloneLevel value is ${cloneLevel}. cloneLevel value should be Deep Full or SHALLOW. Can Not Proceed with Snapshot"
      throw new BadConfigException(errMsg)
    }
    println("Validation successful.You Can proceed with Snapshot process")
    true

  }

  def apply(
             sourceETLDB: String,
             targetPrefix : String,
             snapshotType : String): Unit = {
    apply(
      sourceETLDB,
      targetPrefix,
      snapshotType,
      pipeline = "Bronze,Silver,Gold",
      tablesToExclude = "",
      cloneLevel = "Deep",
      processType = "Snapshot"
    )
  }

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param sourceETLDB        Source Database Name.
   * @param targetPrefix       Target snapshotRootPath
   * @param snapshotType       Type of Snapshot to be performed. "Full" for Full Snapshot , "Incremental" for Incremental Snapshot
   * @param pipeline           Define the Medallion Layers. Argumnent should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)
   * @param tablesToExclude    Array of table names to exclude from the snapshot
   *                           this is the table name only - without the database prefix. By Default it is empty.
   * @param cloneLevel         Clone Level for Snapshot. By Default it is "Deep". You can also specify "Shallow" Clone.
   * @param processType        This argument specify the process type. Whether it is Restore, Migration or Snapshot. By Default it is Snapshot. This argument is
   *                           used internally by restore or Migration process by changing this argument.
   * @return
   */

  def apply(sourceETLDB : String,
            targetPrefix : String,
            snapshotType : String,
            pipeline : String = "Bronze,Silver,Gold",
            tablesToExclude : String = "",
            cloneLevel: String = "DEEP",
            processType : String = "Snapshot"
           ): Unit = {


    if (isValid(sourceETLDB, snapshotType, pipeline, cloneLevel)) {
      val snapWorkSpace = Helpers.getWorkspaceByDatabase(sourceETLDB)
      val bronze = Bronze(snapWorkSpace)
      val silver = Silver(snapWorkSpace)
      val gold = Gold(snapWorkSpace)
      val pipelineReport = bronze.pipelineStateTarget

      val snapshotObj = new Snapshot(sourceETLDB, targetPrefix, snapWorkSpace, snapWorkSpace.database, snapWorkSpace.getConfig, processType)

      val pipelineList = pipeline.split(",").map(_.toLowerCase)

      pipelineList.foreach(layer => {
        if (layer == "bronze" || layer == "silver" || layer == "gold") {
          //validated
        } else {
          val errMsg = s"Unknown Zone found ${layer}, Zone should be either Bronze,Silver or Gold"
          throw new BadConfigException(errMsg)
        }
      })
      try {
        for (layer <- pipelineList) {
          val pipelineTables = if (layer == "bronze") {
            bronze.getAllTargets
          } else if (layer == "silver") {
            silver.getAllTargets
          } else {
            gold.getAllTargets
          }

          if (snapshotType.toLowerCase() == "incremental") {
            snapshotObj.incrementalSnap(pipelineTables, Some(tablesToExclude))
          } else {
            snapshotObj.snap(pipelineTables, cloneLevel, Some(tablesToExclude))
          }
        }
        if (snapshotType.toLowerCase() == "incremental") {
          snapshotObj.incrementalSnap(Array(pipelineReport), Some(tablesToExclude))
        } else {
          snapshotObj.snap(Array(pipelineReport), cloneLevel, Some(tablesToExclude))
        }

      } catch {
        case e: Throwable =>
          val failMsg = PipelineFunctions.appendStackStrace(e, "Unable to proceed with Snapshot Process")
          logger.log(Level.ERROR, failMsg)
          throw e
      }
    } else {
      throw new BadConfigException("Validation Failed for Snapshot. Can not Proceed with Snapshot Process")
    }
  }
}


