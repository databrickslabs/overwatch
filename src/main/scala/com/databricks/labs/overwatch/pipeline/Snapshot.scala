package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row}

class Snapshot (_sourceETLDB: String, _targetPrefix: String, _workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config){


  import spark.implicits._
  private val snapshotRootPath = removeTrailingSlashes(_targetPrefix)
  private val workSpace = _workspace
  private val bronze = Bronze(workSpace)
  private val silver = Silver(workSpace)
  private val gold = Gold(workSpace)

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()
  val Config = _config


  private def parallelism: Int = {
    driverCores
  }

  private[overwatch] def snapStream(cloneDetails: Seq[CloneDetail]): Unit = {

    val cloneDetailsPar = cloneDetails.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
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
        val streamWriter = if (cloneSpec.mode == WriteMode.merge){
          if(Helpers.pathExists(targetLocation)){
            val deltaTable = DeltaTable.forPath(spark,targetLocation)
            val immutableColumns = cloneSpec.immutableColumns

            def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {

              val mergeCondition: String = immutableColumns.map(k => s"updates.$k = target.$k").mkString(" AND ")
              deltaTable.as("target")
                .merge(
                  microBatchOutputDF.as("updates"),
                  mergeCondition)
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute()
            }

            rawStreamingDF
              .writeStream
              .format("delta")
              .outputMode("append")
              .foreachBatch(upsertToDelta _)
              .trigger(Trigger.Once())
              .option("checkpointLocation", checkPointLocation)
              .option("mergeSchema", "true")
              .option("path", targetLocation)
              .start()

          }else{
            rawStreamingDF
              .writeStream
              .format("delta")
              .trigger(Trigger.Once())
              .option("checkpointLocation", checkPointLocation)
              .option("mergeSchema", "true")
              .option("path", targetLocation)
              .start()
          }
        }else{
          rawStreamingDF
            .writeStream
            .format("delta")
            .trigger(Trigger.Once())
            .option("checkpointLocation", checkPointLocation)
            .option("mergeSchema", "true")
            .option("path", targetLocation)
            .start()
        }
        val streamManager = Helpers.getQueryListener(streamWriter,workspace.getConfig, workspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.get.minEventsPerTrigger)
        spark.streams.addListener(streamManager)
        val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter.name}\nID: ${streamWriter.id}"
        if (config.debugFlag) println(listenerAddedMsg)
        logger.log(Level.INFO, listenerAddedMsg)

        streamWriter.awaitTermination()
        spark.streams.removeListener(streamManager)

        logger.log(Level.INFO, s"Streaming COMPLETE: ${cloneSpec.source} --> ${cloneSpec.target}")
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
                                          sourceToSnap: Array[PipelineTable]
                                        ): Seq[CloneDetail] = {

    val finalSnapshotRootPath = s"${snapshotRootPath}/data"
    val cloneSpecs = sourceToSnap.map(dataset => {
      val sourceName = dataset.name.toLowerCase
      val sourcePath = dataset.tableLocation
      val mode = dataset._mode
      val immutableColumns = (dataset.keys ++ dataset.incrementalColumns).distinct
      val targetPath = s"$finalSnapshotRootPath/$sourceName"
      CloneDetail(sourcePath, targetPath, None, "Deep",immutableColumns,mode)
    }).toArray.toSeq
    cloneSpecs
  }

  private[overwatch] def incrementalSnap(
                                          pipeline : String,
                                          excludes: Option[String] = Some("")
                                        ): this.type = {


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

    val cloneSpecs = buildCloneSpecs(sourceToSnapFiltered)
    snapStream(cloneSpecs)
    this
  }

  private[overwatch] def snap(
                               pipeline : String,
                               cloneLevel: String = "DEEP",
                               excludes: Option[String] = Some("")
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

    val cloneSpecs = buildCloneSpecs(sourceToSnapFiltered)
    val cloneReport = Helpers.parClone(cloneSpecs)
    val cloneReportPath = s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").save(cloneReportPath)
    this
  }

}

object Snapshot extends SparkSessionWrapper {


  def apply(workspace: Workspace,
            sourceETLDB : String,
            targetPrefix : String,
            pipeline : String,
            snapshotType: String,
            excludes: Option[String],
            CloneLevel: String
           ): Any = {
    if (snapshotType.toLowerCase()== "incremental")
      new Snapshot(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig).incrementalSnap(pipeline, excludes)
    if (snapshotType.toLowerCase()== "full")
      new Snapshot(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig).snap(pipeline,CloneLevel,excludes)

  }


  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database Name.
   * @param arg(1)        Target snapshotRootPath
   * @param arg(2)        Define the Medallion Layers. Argumnent should be in form of "Bronze, Silver, Gold"(All 3 or any combination of them)
   * @param arg(3)        Type of Snapshot to be performed. Full for Full Snapshot , Incremental for Incremental Snapshot
   * @param arg(4)        Array of table names to exclude from the snapshot
   *                      this is the table name only - without the database prefix
   * @return
   */

  def main(args: Array[String]): Unit = {

    val sourceETLDB = args(0)
    val snapshotRootPath = args(1)
    val pipeline = args(2)
    val snapshotType = args(3)
    val tablesToExclude = args.lift(4).getOrElse("")
    val cloneLevel = args.lift(5).getOrElse("Deep")

    val snapWorkSpace = Helpers.getWorkspaceByDatabase(sourceETLDB)

    val pipelineLower = pipeline.toLowerCase
    if (pipelineLower.contains("bronze")) Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"Bronze",snapshotType,Some(tablesToExclude),cloneLevel)
    if (pipelineLower.contains("silver")) Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"Silver",snapshotType,Some(tablesToExclude),cloneLevel)
    if (pipelineLower.contains("gold")) Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"Gold",snapshotType,Some(tablesToExclude),cloneLevel)
    Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"pipeline_report",snapshotType,Some(tablesToExclude),cloneLevel)

    println("SnapShot Completed")
  }







}


