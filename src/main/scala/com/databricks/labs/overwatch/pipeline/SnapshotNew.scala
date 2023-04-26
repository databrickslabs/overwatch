package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes

class SnapshotNew (_sourceETLDB: String, _targetPrefix: String, _workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config){


  import spark.implicits._
  private val snapshotRootPath = removeTrailingSlashes(_targetPrefix)
  private val workSpace = _workspace
  private val bronze = Bronze(workSpace)
  private val silver = Silver(workSpace)
  private val gold = Gold(workSpace)

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()


  private def parallelism: Int = {
    driverCores
  }

  private[overwatch] def snapStream(cloneDetails: Seq[CloneDetail]): Unit = {

    val cloneDetailsPar = cloneDetails.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    cloneDetailsPar.tasksupport = taskSupport

    logger.log(Level.INFO, "Streaming START:")
    val cloneReport = cloneDetailsPar.map(cloneSpec => {
      try {
        val rawStreamingDF = spark.readStream.format("delta").option("ignoreChanges", "true").load(s"${cloneSpec.source}")
        val sourceName = s"${cloneSpec.source}".split("/").takeRight(1).head
        val checkPointLocation = s"${snapshotRootPath}/checkpoint/${sourceName}"
        rawStreamingDF
          .writeStream
          .format("delta")
          .trigger(Trigger.Once())
          .option("checkpointLocation", checkPointLocation)
          .option("mergeSchema", "true")
          .option("path", s"${cloneSpec.target}")
          .start()
          .awaitTermination()
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
    cloneReport.toDS.write.mode("append").format("delta").save(cloneReportPath)
  }



  private[overwatch] def buildCloneSpecs(
                                          sourceToSnap: Array[PipelineTable]
                                        ): Seq[CloneDetail] = {

    val finalSnapshotRootPath = s"${snapshotRootPath}/data"
    val cloneSpecs = sourceToSnap.map(dataset => {
      val sourceName = dataset.name.toLowerCase
      val sourcePath = dataset.tableLocation
      val targetPath = s"$finalSnapshotRootPath/$sourceName"
      CloneDetail(sourcePath, targetPath, None, "Deep")
    }).toArray.toSeq
    cloneSpecs
  }

  private[overwatch] def incrementalSnap(
                                          zone : String,
                                          excludes: Array[String] = Array()
                                        ): this.type = {


    val sourceToSnap = {
      if (zone.toLowerCase() == "bronze") bronze.getAllTargets
      else if (zone.toLowerCase() == "silver") silver.getAllTargets
      else if (zone.toLowerCase() == "gold") gold.getAllTargets
      else Array(pipelineStateTarget)
      //      else bronze.getAllTargets :+ silver.getAllTargets :+ gold.getAllTargets :+ Array(pipelineStateTarget)
    }


    val cleanExcludes = excludes.map(_.toLowerCase).map(exclude => {
      if (exclude.contains(".")) exclude.split("\\.").takeRight(1).head else exclude
    })

    val sourceToSnapFiltered = sourceToSnap
      .filter(_.exists()) // source path must exist
      .filterNot(t => cleanExcludes.contains(t.name.toLowerCase))

    val cloneSpecs = buildCloneSpecs(sourceToSnapFiltered)
    snapStream(cloneSpecs)
    this
  }



}

object SnapshotNew extends SparkSessionWrapper {


  def apply(workspace: Workspace,
            sourceETLDB : String,
            targetPrefix : String,
            zone : String,
            snapshotType: String,
           ): Any = {
    if (snapshotType.toLowerCase()== "incremental")
      new SnapshotNew(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig).incrementalSnap(zone, Array())
    if (snapshotType.toLowerCase()== "full")

      new SnapshotNew(sourceETLDB, targetPrefix, workspace, workspace.database, workspace.getConfig).bronze.snapshot(targetPrefix,true,Array())

  }

  def main(args: Array[String]): Unit = {

    val sourceETLDB = args(0)
    val snapshotRootPath = args(1)
    val zones = args(2)
    val snapshotType = args(3)

    val snapWorkSpace = Helpers.getWorkspaceByDatabase(sourceETLDB)

    val zonesLower = zones.toLowerCase
    if (zonesLower.contains("bronze")) SnapshotNew(snapWorkSpace,sourceETLDB,snapshotRootPath,"Bronze",snapshotType)
    if (zonesLower.contains("silver")) SnapshotNew(snapWorkSpace,sourceETLDB,snapshotRootPath,"Silver",snapshotType)
    if (zonesLower.contains("gold")) SnapshotNew(snapWorkSpace,sourceETLDB,snapshotRootPath,"Gold",snapshotType)
    SnapshotNew(snapWorkSpace,sourceETLDB,snapshotRootPath,"pipeline_report",snapshotType)

    println("SnapShot Completed")

    //    val workspace = if (args(0).contains("/")){
    //      val remote_workspace_id = args(3)
    //      val pathToOverwatchGlobalShareData = args(0)
    //      Helpers.getRemoteWorkspaceByPath(s"${pathToOverwatchGlobalShareData}/pipeline_report/",true,remote_workspace_id)
    //    }else {
    //      Helpers.getWorkspaceByDatabase(args(0))
    //    }
    //    val bronze = Bronze(workspace)
    //    if (args(2).toLowerCase() == "true"){
    //      incrementalSnap(bronze,workspace,args(1))
    //    }else{
    //      snap(bronze,workspace,args(1))
    //    }
    //
  }







}


