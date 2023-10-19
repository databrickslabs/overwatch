package com.databricks.labs.overwatch.validation

import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.PipelineValidationHelper

object PipelineValidation extends SparkSessionWrapper {

  def apply(etlDB : String) :Unit = {
    new PipelineValidation(etlDB)
      .setPipelineSnapTime()
      .process()
  }

  def apply(etlDB : String, table : Array[String]) :Unit = {
    new PipelineValidation(etlDB)
      .setPipelineSnapTime()
      .process(table)
  }

  def apply(etlDB : String, table : Array[String],crossTableValidation : Boolean) :Unit = {
    new PipelineValidation(etlDB)
      .setPipelineSnapTime()
      .process(table,crossTableValidation)
  }

}

class PipelineValidation (_etlDB: String) extends PipelineValidationHelper(_etlDB) with SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  def process(tableArray: Array[String] = Array(), crossTableValidation: Boolean = true): Unit = {

    val validPrefixes = List("dbfs:", "s3", "abfss", "gs")
    var healthCheckBasePath = storagePrefix.replace("global_share", "") + "healthCheck"
    if (!validPrefixes.exists(healthCheckBasePath.startsWith)) {
      healthCheckBasePath = s"dbfs:$healthCheckBasePath"
    }

    val healthCheckReportPath = s"""$healthCheckBasePath/heathCheck_report"""
    val quarantineReportPath = s"""$healthCheckBasePath/quarantine_report"""

    val processingStartTime = System.currentTimeMillis()

    if (tableArray.length == 0){
      println(s"By Default Single Table Validation has been configured for ${clsfTable},${jrcpTable},${clusterTable},${sparkJobTable},${sqlQueryHistTable},${jobRunTable},${jobTable}")
    }else{
      println(s"Single Table Validation has been configured for ${tableArray.mkString(",")}")
    }

    if (crossTableValidation) {
      println("Cross Table Validation has been Configured")
      (validations, quarantine) == validateCrossTable()
    } else {
      println("Cross Table Validation has not been Configured")
      logger.log(Level.INFO, "Cross Table Validation is Disabled")
    }

    tableArray.length match {
      case 0 =>
        (validations, quarantine) == handleValidation(clsfTable,clsfDF, validateCLSF, validations, quarantine)
        (validations, quarantine) == handleValidation(jrcpTable,jrcpDF, validateJRCP, validations, quarantine)
        (validations, quarantine) == handleValidation(clusterTable,clusterDF,  validateCluster, validations, quarantine)
        (validations, quarantine) == handleValidation(sparkJobTable,sparkJobDF,  validateSparkJob, validations, quarantine)
        (validations, quarantine) == handleValidation(sqlQueryHistTable,sqlQueryHistDF,  validateSqlQueryHist, validations, quarantine)
        (validations, quarantine) == handleValidation(jobRunTable,jobRunDF,  validateJobRun, validations, quarantine)
        (validations, quarantine) == handleValidation(jobTable,jobDF,  validateJob, validations, quarantine)

      case _ =>
        tableArray.map(_.toLowerCase).foreach {
          case tableName @ "clusterstatefact_gold" =>
            (validations, quarantine) == handleValidation(clsfTable,clsfDF, validateCLSF, validations, quarantine)
          case tableName @ "jobruncostpotentialfact_gold" =>
            (validations, quarantine) == handleValidation(jrcpTable,jrcpDF, validateJRCP, validations, quarantine)
          case tableName @ "cluster_gold" =>
            (validations, quarantine) == handleValidation(clusterTable,clusterDF,  validateCluster, validations, quarantine)
          case tableName @ "sparkjob_gold" =>
            (validations, quarantine) == handleValidation(sparkJobTable,sparkJobDF,  validateSparkJob, validations, quarantine)
          case tableName @ "sql_query_history_gold" =>
            (validations, quarantine) == handleValidation(sqlQueryHistTable,sqlQueryHistDF,  validateSqlQueryHist, validations, quarantine)
          case tableName @ "jobrun_gold" =>
            (validations, quarantine) == handleValidation(jobRunTable,jobRunDF,  validateJobRun, validations, quarantine)
          case tableName @ "job_gold" =>
            (validations, quarantine) == handleValidation(jobTable,jobDF,  validateJob, validations, quarantine)
          case tableName  => println(s"Table $tableName is not recognized or not supported.")
        }
    }

    val notValidatedCount = validations.toDS().toDF().filter(!'healthCheckMsg.contains("Success")).count()

    snapShotHealthCheck(validations.toArray, healthCheckReportPath)
    snapShotQuarantine(quarantine.toArray, quarantineReportPath)

    val processingEndTime = System.currentTimeMillis()
    val msg =
      s"""*********** HealthCheck Report Details *******************
         |Total healthcheck count: ${validations.length}
         |Failed healthcheck count:$notValidatedCount
         |Report run duration in sec : ${(processingEndTime - processingStartTime) / 1000}
         |""".stripMargin

    println(msg)

  }
}