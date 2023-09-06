package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Pipeline, PipelineFunctions, PipelineTable, Silver}
import com.databricks.labs.overwatch.utils.Helpers.spark
import com.databricks.labs.overwatch.utils.{DeploymentValidationReport, Helpers, SparkSessionWrapper}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray

object DataReconciliation extends SparkSessionWrapper {

  import spark.implicits._


  private val logger: Logger = Logger.getLogger(this.getClass)

  def performRecon(sourceEtl:String,targetEtl:String,fromTS:Long,untilTS:Long): Unit ={
    val sourceOrgIDArr = getAllOrgID(sourceEtl)
    val targetOrgIDArr = getAllOrgID(targetEtl)
    performBasicValidation(sourceOrgIDArr,targetOrgIDArr)
    val sourceWorkspace = getConfig(sourceEtl,targetOrgIDArr(0))
    val targetWorkspace = getConfig(targetEtl,targetOrgIDArr(0))
    val targets = getAllTargets(targetWorkspace)
    val report = runRecon(targets,sourceEtl,sourceOrgIDArr,targetEtl)
    val reconRunId: String = java.util.UUID.randomUUID.toString
    saveReconReport(report,targetWorkspace.getConfig.etlDataPathPrefix,"ReconReport",reconRunId)
  }

  private def countValidation(target: PipelineTable,orgId: String, sourceEtl:String,targetEtl:String ):ReconReport ={
    val simpleMsg = "Count validation"
    try {
      val sourceTable = getTableDF(s"""${target.tableFullName}""", orgId)
      val targetTable = getTableDF(s"""${target.tableFullName}""", orgId)
      val sourceCount = sourceTable.count()
      val targetCount = targetTable.count()
      val missingSourceCount = targetTable.exceptAll(sourceTable).count()
      val missingTargetCount = sourceTable.exceptAll(targetTable).count()
      val commonDataCount = sourceTable.intersectAll(targetTable).count()
      val deviationFactor = {
        if (missingSourceCount + missingTargetCount == 0l) {
          1
        } else {
          missingSourceCount + missingTargetCount
        }

      }
      val deviation = sourceCount / deviationFactor
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
        errorMsg = Some(""),
        sourceDB = sourceEtl,
        targetDB = targetEtl,
        tableName = target.tableFullName,
        sourceCount = Some(sourceCount),
        targetCount = Some(targetCount),
        missingInSource = Some(missingSourceCount),
        missingInTarget = Some(missingTargetCount),
        commonDataCount = Some(commonDataCount),
        deviationPercentage = Some(deviation))

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("got Exception:" + e.getMessage)
        val fullMsg = PipelineFunctions.appendStackStrace(e, "Got Exception while running recon,")
        ReconReport(
          workspaceId = orgId,
          simpleMsg = simpleMsg,
          errorMsg = Some(fullMsg),
          sourceDB = sourceEtl,
          targetDB = targetEtl,
          tableName = target.tableFullName)
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

  case class ReconReport(
                          validated: Boolean = false,
                          workspaceId: String,
                          simpleMsg: String,
                          errorMsg: Option[String] = None,
                          sourceDB: String,
                          targetDB: String,
                          tableName: String,
                          sourceCount: Option[Long] = None,
                          targetCount: Option[Long] = None,
                          missingInSource: Option[Long] = None,
                          missingInTarget: Option[Long] = None,
                          commonDataCount: Option[Long] = None,
                          deviationPercentage: Option[Long] = None
                        )


  private def getTableDF(tableName: String,orgId: String):DataFrame = {
    println("table name is"+tableName)
    spark.sql(s"""select * from $tableName""").filter('organization_id === orgId).drop("Pipeline_SnapTS", "Overwatch_RunID")
  }

  private[overwatch] def getAllTargets(workspace: Workspace): ParArray[PipelineTable] = {
    val b = Bronze(workspace)
    val s = Silver(workspace)
    val g = Gold(workspace)
    (b.getAllTargets ++ s.getAllTargets ++ g.getAllTargets :+ b.pipelineStateTarget).filter(_.exists(dataValidation = true, catalogValidation = false)).par
  }

  private[overwatch] def performBasicValidation(sourceOrgIDArr:Array[String],targetOrgIDArr:Array[String]):Unit = {
    println("Number of workspace in Source:" + sourceOrgIDArr.size)
    println("Number of workspace in Target:" + targetOrgIDArr.size)
    println("Number of common workspace:" + sourceOrgIDArr.intersect(targetOrgIDArr))
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
