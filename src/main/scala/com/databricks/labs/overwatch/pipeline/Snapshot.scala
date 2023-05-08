package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

import com.databricks.labs.overwatch.pipeline.PipelineTargets
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
//  private val bronze = Bronze(workSpace)
//  private val silver = Silver(workSpace)
//  private val gold = Gold(workSpace)

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()
  private val Config = _config

  private def parallelism: Int = {
    driverCores
  }

  private[overwatch] def snapStreamNew(cloneDetails: Seq[CloneDetail]): Unit = {

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

        if(Helpers.pathExists(targetLocation)){
          println(s"Target path is not Empty and mode for ${sourceName} is ${cloneSpec.mode}")
          if (cloneSpec.mode == WriteMode.merge){ //If Table mode is Merge then do simple merge
            val deltaTarget = DeltaTable.forPath(spark,targetLocation)
            val updatesDF = rawStreamingDF.alias("updates")
            val immutableColumns = cloneSpec.immutableColumns
            val mergeCondition: String = immutableColumns.map(k => s"updates.$k = target.$k").mkString(" AND ")

            val mergeDetailMsg =
              s"""
                 |Beginning upsert to ${sourceName}.
                 |MERGE CONDITION: $mergeCondition
                 |""".stripMargin
            logger.log(Level.INFO, mergeDetailMsg)
            spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", Config.runID)

            deltaTarget
              .merge(updatesDF, mergeCondition)
              .whenMatched
              .updateAll()
              .whenNotMatched
              .insertAll()
              .execute()
            spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
            CloneReport(cloneSpec, s"Streaming For: ${cloneSpec.source} --> ${cloneSpec.target}", "SUCCESS")

          }else{
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
            val streamWriter1 = streamWriter
              .asInstanceOf[DataStreamWriter[Row]]
              .option("path", targetLocation)
              .start()

            val streamManager = Helpers.getQueryListener(streamWriter1,workspace.getConfig, workspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.get.minEventsPerTrigger)
            spark.streams.addListener(streamManager)
            val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter1.name}\nID: ${streamWriter1.id}"
            logger.log(Level.INFO, listenerAddedMsg)

            streamWriter1.awaitTermination()
            spark.streams.removeListener(streamManager)
            logger.log (Level.INFO, s"Streaming COMPLETE: ${cloneSpec.source} --> ${cloneSpec.target}")
            CloneReport(cloneSpec, s"Streaming For: ${cloneSpec.source} --> ${cloneSpec.target}", "SUCCESS")

          }
        }else{
          println(s"First Time Write to  ${sourceName}")
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
          val streamWriter1 = streamWriter
            .asInstanceOf[DataStreamWriter[Row]]
            .option("path", targetLocation)
            .start()

          val streamManager = Helpers.getQueryListener(streamWriter1,workspace.getConfig, workspace.getConfig.auditLogConfig.azureAuditLogEventhubConfig.get.minEventsPerTrigger)
          spark.streams.addListener(streamManager)
          val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter1.name}\nID: ${streamWriter1.id}"
          logger.log(Level.INFO, listenerAddedMsg)

          streamWriter1.awaitTermination()
          spark.streams.removeListener(streamManager)
          logger.log (Level.INFO, s"Streaming COMPLETE: ${cloneSpec.source} --> ${cloneSpec.target}")
          CloneReport(cloneSpec, s"Streaming For: ${cloneSpec.source} --> ${cloneSpec.target}", "SUCCESS")
        }

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
            println(s"1. Path Exist and ${sourceName} mode is ${cloneSpec.mode}")
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
              .queryName(s"Streaming_${sourceName}")
              .option("mergeSchema", "true")
              .option("path", targetLocation)
              .start()

          }else{
            println(s"2. Path Does not Exist and ${sourceName} mode is ${cloneSpec.mode}")
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
          println(s"3.${sourceName} mode is ${cloneSpec.mode}")
          rawStreamingDF
            .writeStream
            .format("delta")
            .outputMode(s"${cloneSpec.mode}")
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
                                          cloneLevel: String,
                                          sourceToSnap: Array[PipelineTable],
                                          snapshotType : String
                                        ): Seq[CloneDetail] = {

    val finalSnapshotRootPath = if (snapshotType == "incremental"){
          s"${snapshotRootPath}/data"
    }else{
      val currTime = Pipeline.createTimeDetail(System.currentTimeMillis())
      s"$snapshotRootPath/${currTime.asDTString}/${currTime.asUnixTimeMilli.toString}"
    }
    val cloneSpecs = sourceToSnap.map(dataset => {
      val sourceName = dataset.name.toLowerCase
      println("sourceName is")
      val sourcePath = dataset.tableLocation
      val mode = dataset._mode
      val immutableColumns = (dataset.keys ++ dataset.incrementalColumns).distinct
      val targetPath = s"$finalSnapshotRootPath/$sourceName"
      CloneDetail(sourcePath, targetPath, None, cloneLevel,immutableColumns,mode)
    }).toArray.toSeq
    cloneSpecs
  }

  private[overwatch] def incrementalSnap(
                                          pipeline : String,
                                          pipelineTable : Array[PipelineTable],
                                          excludes: Option[String] = Some("")
                                        ): this.type = {

    val sourceToSnap = pipelineTable

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

    val cloneSpecs = buildCloneSpecs("Deep",sourceToSnapFiltered)
    snapStreamNew(cloneSpecs)
    this
  }

  private[overwatch] def snap(
                               pipeline : String,
                               pipelineTable : Array[PipelineTable],
                               cloneLevel: String = "DEEP",
                               excludes: Option[String] = Some("")
                             ): this.type= {
    val acceptableCloneLevels = Array("DEEP", "SHALLOW")
    require(acceptableCloneLevels.contains(cloneLevel.toUpperCase), s"SNAP CLONE ERROR: cloneLevel provided is " +
      s"$cloneLevel. CloneLevels supported are ${acceptableCloneLevels.mkString(",")}.")

    val sourceToSnap = pipelineTable
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

    val cloneSpecs = buildCloneSpecs(cloneLevel,sourceToSnapFiltered,"full")
    val cloneReport = Helpers.parClone(cloneSpecs)
    val cloneReportPath = s"${snapshotRootPath}/clone_report/"
    cloneReport.toDS.write.format("delta").mode("append").save(cloneReportPath)
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
            CloneLevel: String,
            pipelineTable : Array[PipelineTable]
           ): Any = {
    if (snapshotType.toLowerCase()== "incremental")
      new Snapshot(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig).incrementalSnap(pipeline,pipelineTable,excludes)
    if (snapshotType.toLowerCase()== "full")
      new Snapshot(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig).snap(pipeline,pipelineTable,CloneLevel,excludes)

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
    val bronze = Bronze(snapWorkSpace)
    val silver = Silver(snapWorkSpace)
    val gold = Gold(snapWorkSpace)
    val pipelineReport = bronze.pipelineStateTarget

    val pipelineLower = pipeline.toLowerCase
    if (pipelineLower.contains("bronze")) Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"Bronze",snapshotType,Some(tablesToExclude),cloneLevel,bronze.getAllTargets)
    if (pipelineLower.contains("silver")) Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"Silver",snapshotType,Some(tablesToExclude),cloneLevel,silver.getAllTargets)
    if (pipelineLower.contains("gold")) Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"Gold",snapshotType,Some(tablesToExclude),cloneLevel,gold.getAllTargets)
    Snapshot(snapWorkSpace,sourceETLDB,snapshotRootPath,"pipeline_report",snapshotType,Some(tablesToExclude),cloneLevel,Array(pipelineReport))

    println("SnapShot Completed")
  }







}


