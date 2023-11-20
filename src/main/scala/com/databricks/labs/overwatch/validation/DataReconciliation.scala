package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Pipeline, PipelineFunctions, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.Helpers.spark
import com.databricks.labs.overwatch.utils.{DeploymentValidationReport, Helpers, ReconReport, SparkSessionWrapper}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row}

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray

/**
 * Data Reconciliation is a new feature of OW which will ensure whether the data is consistent across the current release and previous release.
 * In order to perform the data reconciliation, we need two overwatch deployments with current and previous versions.
 * After running the reconciliation it will generate a report which will contain all comparison results for each table.
 * This reconciliation module is independent of pipeline run and will be used as an helper function.
 */
object DataReconciliation extends SparkSessionWrapper {

  import spark.implicits._


  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Function is the starting point of the reconciliation.
   * @param sourceEtl :  ETL name of Previous version of OW
   * @param targetEtl : ETL name of the current version of OW
   */
  private[overwatch] def performRecon(sourceEtl:String,targetEtl:String): Unit ={
    val sourceOrgIDArr = getAllOrgID(sourceEtl)
    val targetOrgIDArr = getAllOrgID(targetEtl)
    performBasicValidation(sourceOrgIDArr,targetOrgIDArr)
    val sourceWorkspace = getConfig(sourceEtl,targetOrgIDArr(0))
    val targetWorkspace = getConfig(targetEtl,targetOrgIDArr(0))
    val targets = getAllTargets(sourceWorkspace)
    val report = runRecon(targets,sourceEtl,sourceOrgIDArr,targetEtl)
    val reconRunId: String = java.util.UUID.randomUUID.toString
    saveReconReport(report,targetWorkspace.getConfig.etlDataPathPrefix,"ReconReport",reconRunId)
  }

  /**
   * Performs the below comparison between two tables called source table and target table.
   * Count validation in Source
   * Count validation in Target
   * Common data between source and target
   * Missing data in source
   * Missing data in target
   * Deviation percentage: it is calculated with the formula  ((missingSourceCount + missingTargetCount)/SourceCount)*100
   * @param target
   * @param orgId
   * @param sourceEtl
   * @param targetEtl
   * @return
   */
  private def countValidation(target: PipelineTable,orgId: String, sourceEtl:String,targetEtl:String ):ReconReport ={
    val simpleMsg = "Count validation"
    try {
      val sourceQuery = getQuery(s"""${target.tableFullName}""", orgId, target)
      val sourceTable = getTableDF(sourceQuery)
      val targetQuery = getQuery(s"""${target.tableFullName.replaceAll(sourceEtl,targetEtl)}""", orgId,target)
      val targetTable = getTableDF(targetQuery)
      val sourceCount = sourceTable.count()
      val targetCount = targetTable.count()
      val missingSourceCount = targetTable.exceptAll(sourceTable).count()
      val missingTargetCount = sourceTable.exceptAll(targetTable).count()
      val commonDataCount = sourceTable.intersectAll(targetTable).count()
      val deviationFactor = {
        if ((missingSourceCount + missingTargetCount) == 0) {
          1
        } else {
          missingSourceCount + missingTargetCount
        }

      }
      val deviation:Double = {
        if(deviationFactor == 1){
          0
        }else{
          (deviationFactor.toDouble/sourceCount)*100
        }
      }

      val validated: Boolean = {
        if ((sourceCount == targetCount) && (missingSourceCount == 0 && missingTargetCount == 0)) {
          true
        } else {
          false
        }
      }

      ReconReport(validated = validated,
        workspaceId = orgId,
        simpleMsg = simpleMsg,
        sourceDB = sourceEtl,
        targetDB = targetEtl,
        tableName = target.name,
        sourceCount = Some(sourceCount),
        targetCount = Some(targetCount),
        missingInSource = Some(missingSourceCount),
        missingInTarget = Some(missingTargetCount),
        commonDataCount = Some(commonDataCount),
        deviationPercentage = Some(deviation),
        sourceQuery = Some(sourceQuery),
        targetQuery = Some(targetQuery),
        errorMsg = Some(""))

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val fullMsg = PipelineFunctions.appendStackStrace(e, "Got Exception while running recon,")
        ReconReport(
          workspaceId = orgId,
          simpleMsg = simpleMsg,
          sourceDB = sourceEtl,
          targetDB = targetEtl,
          tableName = target.tableFullName,
          errorMsg = Some(fullMsg)
        )
    }



  }

  private[overwatch] def runRecon(targets: ParArray[PipelineTable] ,
                                  sourceEtl:String,
                                  sourceOrgIDArr: Array[String],
                                  targetEtl:String,
                                  ):Array[ReconReport]={

      val reconStatus: ArrayBuffer[ReconReport] = new ArrayBuffer[ReconReport]()
      sourceOrgIDArr.foreach(orgId=> {
        targets.foreach(target => {
          reconStatus.append(countValidation(target, orgId, sourceEtl, targetEtl))
        })
      }
      )
    reconStatus.toArray
  }

  /**
   * Function saves the recon report.
   * @param reconStatusArray
   * @param path
   * @param reportName
   * @param reconRunId
   */
  private def saveReconReport(reconStatusArray: Array[ReconReport], path: String, reportName: String, reconRunId: String): Unit = {
    val validationPath = {
      if (!path.startsWith("dbfs:") && !path.startsWith("s3") && !path.startsWith("abfss") && !path.startsWith("gs")) {
         s"""dbfs:${path}"""
      }else{
        path
      }
    }

    val pipelineSnapTime =  Pipeline.createTimeDetail(LocalDateTime.now(Pipeline.systemZoneId).toInstant(Pipeline.systemZoneOffset).toEpochMilli)
    reconStatusArray.toSeq.toDS().toDF()
      .withColumn("deployment_id", lit(reconRunId))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(s"""${validationPath}/report/${reportName}""")
    println("Validation report has been saved to " + s"""${validationPath}/report/${reportName}""")
  }



private def getQuery(tableName: String,orgId: String,target:PipelineTable): String = {
  val _keys = target.keys.filterNot(_ == "Overwatch_RunID")
  val reconColymns = target.reconColumns
  val finalColumns = if(!reconColymns.isEmpty) {
    _keys++reconColymns
  }else{
    _keys
  }
  s"""select ${finalColumns.mkString(",")} from $tableName where organization_id = ${orgId} """

}

  private def getTableDF(query: String):DataFrame = {
    try{
      val filterDF = spark.sql(query)
      filterDF
    }catch {
      case exception: Exception =>
        println(s"""Exception: Unable to run the query ${query}"""+exception.getMessage)
        spark.emptyDataFrame
    }

  }

  private[overwatch] def getAllTargets(workspace: Workspace): ParArray[PipelineTable] = {
    val b = Bronze(workspace)
    val s = Silver(workspace)
    val g = Gold(workspace)
    (b.getAllTargets ++ s.getAllTargets ++ g.getAllTargets).filter(_.exists(dataValidation = true, catalogValidation = false)).par
  }

  private[overwatch] def performBasicValidation(sourceOrgIDArr:Array[String],targetOrgIDArr:Array[String]):Unit = {
    println("Number of workspace in Source:" + sourceOrgIDArr.size)
    println("Number of workspace in Target:" + targetOrgIDArr.size)
    if(sourceOrgIDArr.size<1 || targetOrgIDArr.size<1){
      val msg ="Number of workspace in source/target etl is 0 , Exiting"
      println(msg)
      throw new Exception(msg)
    }

  }

  private[overwatch] def getConfig(sourceEtl: String, orgID: String): Workspace = {
    Helpers.getWorkspaceByDatabase(sourceEtl, Some(orgID))
  }


  private[overwatch] def getAllOrgID(etlDB: String): Array[String] = {
    try{
      spark.table(s"${etlDB}.pipeline_report").select("organization_id").distinct().collect().map(row => row.getString(0))
    }catch {
      case e:Throwable=>
        val msg = "Got exception while reading from pipeline_report ,"
        println(msg+e.getMessage)
        throw e
    }

  }



}
