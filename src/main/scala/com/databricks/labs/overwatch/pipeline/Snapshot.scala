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

  def writeStream(sourceName:String,
                  checkPointLocation:String,
                  targetLocation:String,
                  rawStreamingDF:DataFrame,
                  cloneSpec:CloneDetail):StreamingQuery = {
    logger.log(Level.INFO, s"Beginning write to ${sourceName}")
    val msg = s"Checkpoint Path Set: ${checkPointLocation} - proceeding with streaming write"
    logger.log(Level.INFO, msg)

    val beginMsg = s"Stream to ${sourceName} beginning."
    logger.log(Level.INFO, beginMsg)
    var streamWriter = rawStreamingDF.writeStream.outputMode("append").format("delta").option("checkpointLocation", checkPointLocation)
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

  def upsertToDelta(microBatchOutputDF: DataFrame,
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

        val streamWriter = if(Helpers.pathExists(targetLocation)){
          if (cloneSpec.mode == WriteMode.merge)
          { //If Table mode is Merge then do simple merge
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
          }
          else //Not Streaming Merge
          {
            writeStream(sourceName:String,checkPointLocation:String,targetLocation:String,rawStreamingDF:DataFrame,cloneSpec:CloneDetail)
          }
        }else //First time Streaming
        {
         writeStream(sourceName:String,checkPointLocation:String,targetLocation:String,rawStreamingDF:DataFrame,cloneSpec:CloneDetail)
        }

        val streamManager = Helpers.getQueryListener(streamWriter,workspace.getConfig, workspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.get.minEventsPerTrigger)
        spark.streams.addListener(streamManager)
        val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter.name}\nID: ${streamWriter.id}"
        logger.log(Level.INFO, listenerAddedMsg)

        streamWriter.awaitTermination()
        spark.streams.removeListener(streamManager)
        logger.log (Level.INFO, s"Streaming COMPLETE: ${cloneSpec.source} --> ${cloneSpec.target}")
        spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
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

  private[overwatch] def incrementalSnap(
                                          pipelineTables : Array[PipelineTable],
                                          excludes: Option[String] = Some("")
                                        ): this.type = {

    val sourceToSnap = pipelineTables

    val exclude = excludes match {
      case Some(s) if s.nonEmpty => s
      case _ => ""
    }
    val excludeList = exclude.split(",")

    val cleanExcludes = excludeList.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })

    val sourceToSnapFiltered = sourceToSnap
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))

    val cloneSpecs = buildCloneSpecs("Deep",sourceToSnapFiltered)
    snapStream(cloneSpecs)
    this
  }

  private[overwatch] def snap(
                               pipelineTables : Array[PipelineTable],
                               cloneLevel: String = "DEEP",
                               excludes: Option[String] = Some("")
                             ): this.type= {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val sourceToSnap = pipelineTables
    val exclude = excludes match {
      case Some(s) if s.nonEmpty => s
      case _ => ""
    }
    val excludeList = exclude.split(",")

    val cleanExcludes = excludeList.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })


    val sourceToSnapFiltered = sourceToSnap
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))

    val cloneSpecs = buildCloneSpecs(cloneLevel,sourceToSnapFiltered)
    val cloneReport = Helpers.parClone(cloneSpecs)
    val cloneReportPath = s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").mode("append").save(cloneReportPath)
    this
  }

}

object Snapshot extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def apply(workspace: Workspace,
            sourceETLDB : String,
            targetPrefix : String,
            snapshotType: String,
            excludes: Option[String],
            cloneLevel: String,
            pipelineTables : Array[PipelineTable],
            processType : String,
           ): Snapshot = {
    if (snapshotType.toLowerCase()== "incremental")
      new Snapshot(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig,processType).incrementalSnap(pipelineTables,excludes)
    else{
      new Snapshot(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig,processType).snap(pipelineTables,cloneLevel,excludes)
    }

  }




  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database Name.
   * @param arg(1)        Target snapshotRootPath
   * @param arg(2)        Define the Medallion Layers. Argumnent should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)
   * @param arg(3)        Type of Snapshot to be performed. "Full" for Full Snapshot , "Incremental" for Incremental Snapshot
   * @param arg(4)        Array of table names to exclude from the snapshot
   *                      this is the table name only - without the database prefix. By Default it is empty.
   * @param arg(5)        Clone Level for Snapshot. By Default it is "Deep". You can also specify "Shallow" Clone.
   *
   * @param arg(6)        This argument specify the process type. Whether it is Restore, Migration or Snapshot. By Default it is Snapshot. This argument is
   *                      used internally by restore or Migration process by changing this argument.
   * @return
   */

  def main(args: Array[String]): Any = {

    val sourceETLDB = args(0)
    val snapshotRootPath = args(1)
    val pipeline = args(2)
    val snapshotType = args(3)
    val tablesToExclude = args.lift(4).getOrElse("")
    val cloneLevel = args.lift(5).getOrElse("Deep")
    val processType = args.lift(6).getOrElse("Snapshot")

    val snapWorkSpace = Helpers.getWorkspaceByDatabase(sourceETLDB)
    val bronze = Bronze(snapWorkSpace)
    val silver = Silver(snapWorkSpace)
    val gold = Gold(snapWorkSpace)
    val pipelineReport = bronze.pipelineStateTarget

    try {
      val pipelineLower = pipeline.toLowerCase
      if (pipelineLower.contains("bronze")) Snapshot(snapWorkSpace, sourceETLDB, snapshotRootPath, snapshotType, Some(tablesToExclude), cloneLevel, bronze.getAllTargets, processType)
      if (pipelineLower.contains("silver")) Snapshot(snapWorkSpace, sourceETLDB, snapshotRootPath, snapshotType, Some(tablesToExclude), cloneLevel, silver.getAllTargets, processType)
      if (pipelineLower.contains("gold")) Snapshot(snapWorkSpace, sourceETLDB, snapshotRootPath, snapshotType, Some(tablesToExclude), cloneLevel, gold.getAllTargets, processType)
      Snapshot(snapWorkSpace, sourceETLDB, snapshotRootPath, snapshotType, Some(tablesToExclude), cloneLevel, Array(pipelineReport), processType)

    }catch{
      case e: Throwable =>
        val failMsg = PipelineFunctions.appendStackStrace(e,"Unable to proceed with Snapshot Process")
        logger.log(Level.ERROR, failMsg)
        throw e
    }
  }
}


