package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils.{Helpers, ReconReport, SparkSessionWrapper}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, hash, lit}

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
    performBasicRecon(sourceOrgIDArr,targetOrgIDArr)
    val sourceWorkspace = getConfig(sourceEtl,sourceOrgIDArr(0))
    val targetWorkspace = getConfig(targetEtl,targetOrgIDArr(0))
    val targets = getAllTargets(sourceWorkspace)
    println("Number of tables for recon: "+targets.length)
    println("Tables for recon: "+targets.foreach(t=>println(t.name)))
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
  private def hashValidation(target: PipelineTable,orgId: String, sourceEtl:String,targetEtl:String ):ReconReport ={
    val reconType = "Validation by hashing"
    try {
      val sourceQuery = getQuery(s"""${target.tableFullName}""", orgId)
      val sourceTable = hashAllColumns(getTableDF(sourceQuery,target))
      val targetQuery = getQuery(s"""${target.tableFullName.replaceAll(sourceEtl,targetEtl)}""", orgId)
      val targetTable = hashAllColumns(getTableDF(targetQuery,target))
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
        reconType = reconType,
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
          reconType = reconType,
          sourceDB = sourceEtl,
          targetDB = targetEtl,
          tableName = target.tableFullName,
          errorMsg = Some(fullMsg)
        )
    }



  }

  /**
   * This method runs the reconciliation for all targets in parallel.
   *
   * @param targets        : Array of PipelineTable
   * @param sourceEtl      : ETL name of Previous version of OW
   * @param sourceOrgIDArr : Array of organization IDs
   * @param targetEtl      : ETL name of the current version of OW
   * @return Array of ReconReport
   */
  private[overwatch] def runRecon(targets: ParArray[PipelineTable] ,
                                  sourceEtl:String,
                                  sourceOrgIDArr: Array[String],
                                  targetEtl:String,
                                  ):Array[ReconReport]={
    spark.conf.set("spark.sql.legacy.allowHashOnMapType","true")
      val reconStatus: ArrayBuffer[ReconReport] = new ArrayBuffer[ReconReport]()
      sourceOrgIDArr.foreach(orgId=> {
        targets.foreach(target => {
          reconStatus.append(hashValidation(target, orgId, sourceEtl, targetEtl))
        })
      }
      )
    spark.conf.set("spark.sql.legacy.allowHashOnMapType","false")
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
      .withColumn("reconRunId", lit(reconRunId))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("timestamp", lit(pipelineSnapTime.asUnixTimeMilli))
      .write.format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(s"""${validationPath}/report/${reportName}""")
    println("ReconRunID:"+reconRunId)
    println("Validation report has been saved to " + s"""${validationPath}/report/${reportName}""")
  }


  /**
   * This method generates a query to fetch data from a table for a specific organization.
   *
   * @param tableName : Name of the table
   * @param orgId     : Organization ID
   * @return String : Query
   */
  private def getQuery(tableName: String, orgId: String): String = {
    s"""select * from $tableName where organization_id = ${orgId} """
  }


  /**
   * This method fetches a DataFrame from a table using a query.
   *
   * @param query  : Query to fetch data
   * @param target : PipelineTable object
   * @return DataFrame
   */
  private def getTableDF(query: String,target: PipelineTable):DataFrame = {
    try{
      val excludedCol = target.excludedReconColumn
      val dropCol = excludedCol ++ Array("Overwatch_RunID", "Pipeline_SnapTS", "__overwatch_ctrl_noise")
      val filterDF = spark.sql(query).drop(dropCol: _ *)
      filterDF
    }catch {
      case exception: Exception =>
        println(s"""Exception: Unable to run the query ${query}"""+exception.getMessage)
        spark.emptyDataFrame
    }

  }

  /**
   * This method fetches all targets for a workspace.
   *
   * @param workspace : Workspace object
   * @return ParArray of PipelineTable
   */
  private[overwatch] def getAllTargets(workspace: Workspace): ParArray[PipelineTable] = {
    val b = Bronze(workspace)
    val s = Silver(workspace)
    val g = Gold(workspace)
    (b.getAllTargets ++ s.getAllTargets ++ g.getAllTargets).filter(_.exists(dataValidation = true, catalogValidation = false)).par
  }

  /**
   * This method performs basic reconciliation between two arrays of organization IDs.
   *
   * @param sourceOrgIDArr : Array of organization IDs from source ETL
   * @param targetOrgIDArr : Array of organization IDs from target ETL
   */
  private[overwatch] def performBasicRecon(sourceOrgIDArr:Array[String],targetOrgIDArr:Array[String]):Unit = {
    println("Number of workspace in Source:" + sourceOrgIDArr.size)
    println("Number of workspace in Target:" + targetOrgIDArr.size)
    if(sourceOrgIDArr.size<1 || targetOrgIDArr.size<1){
      val msg ="Number of workspace in source/target etl is 0 , Exiting"
      println(msg)
      throw new Exception(msg)
    }

  }

  /**
   * This method retrieves the Workspace configuration for a given ETL database and organization ID.
   *
   * @param sourceEtl The name of the ETL database.
   * @param orgID     The organization ID.
   * @return A Workspace object containing the configuration for the specified ETL database and organization ID.
   */
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

  /**
   * This method hashes all columns of a DataFrame.
   *
   * @param df The DataFrame whose columns are to be hashed.
   * @param includeNonHashCol A boolean flag indicating whether to include non-hash columns in the output DataFrame. Default is false.
   * @return A DataFrame with the hashed columns. If includeNonHashCol is true, the original columns are also included.
   *
   * The method works as follows:
   * 1. Retrieves the column names of the input DataFrame.
   * 2. Creates a new set of columns where each column is the hash of the original column.
   * 3. Selects the original columns and the hashed columns from the input DataFrame.
   * 4. If includeNonHashCol is true, returns the DataFrame with both original and hashed columns. Otherwise, returns the DataFrame with only the hashed columns.
   */
  private[overwatch] def hashAllColumns(df: DataFrame, includeNonHashCol:Boolean= false): DataFrame = {
    val columns = df.columns
    val hashCols = columns.map(column => hash(col(column)).alias(s"${column}_hash"))
    val selectDf = df.select((columns.map(name => df(name)) ++ hashCols): _*)
    if(includeNonHashCol){
      selectDf
    }
    else{
      selectDf.select(hashCols: _*)
    }

  }


  /**
   * This method performs a reconciliation between two tables.
   *
   * @param sourceTable       The name of the source table to be reconciled.
   * @param targetTable       The name of the target table to be reconciled.
   * @param includeNonHashCol A boolean flag indicating whether to include non-hash columns in the reconciliation. Default is true.
   * @return A DataFrame containing the data present in the source table but missing from the target table.
   *
   *         The method works as follows:
   *         1. Reads the source and target tables from Spark.
   *         2. Drops the columns "Overwatch_RunID", "Pipeline_SnapTS", "__overwatch_ctrl_noise" from both tables.
   *         3. Hashes all columns of both tables using the `hashAllColumns` method.
   *         4. Finds the data present in the source table but missing from the target table using the `exceptAll` method.
   *         5. Returns the DataFrame containing the missing data.
   */
  private[overwatch] def reconTable(sourceTable: String, targetTable: String, includeNonHashCol: Boolean = true): DataFrame = {
    val sourceHashTable = hashAllColumns(spark.read.table(sourceTable).drop("Overwatch_RunID", "Pipeline_SnapTS", "__overwatch_ctrl_noise"), includeNonHashCol)
    val targetHashTable = hashAllColumns(spark.read.table(targetTable).drop("Overwatch_RunID", "Pipeline_SnapTS", "__overwatch_ctrl_noise"), includeNonHashCol)
    val missingSource = sourceHashTable.exceptAll(targetHashTable)
    missingSource
  }





}
