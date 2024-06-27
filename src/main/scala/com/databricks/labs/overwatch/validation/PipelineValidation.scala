package com.databricks.labs.overwatch.validation

import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.PipelineValidationHelper
import org.apache.spark.sql.DataFrame

/**
 * The Purpose of this object is to validate the health of the tables in the overwatch database.
 * @param ETLDB                 ETL Database for on which validation need to be done(Mandatory)
 * @param table                 Table for which validation need to be done(Optional). If not provided all tables
 *                              which are in scope of validation framework would be validated
 * @param crossTableValidation  Cross Table Validation need to be done(Optional). Default value is True. If it is false then only
 *                              single table validation would be performed.
 */
object PipelineValidation extends SparkSessionWrapper {

  def apply(etlDB : String , allRun: Boolean) :Unit = {
    new PipelineValidation(etlDB,allRun)
      .setPipelineSnapTime()
      .process()
  }

  def apply(etlDB : String, allRun: Boolean, table : Array[String]) :Unit = {
    new PipelineValidation(etlDB,allRun)
      .setPipelineSnapTime()
      .process(table)
  }

  def apply(etlDB : String, allRun: Boolean, table : Array[String],crossTableValidation : Boolean) :Unit = {
    new PipelineValidation(etlDB,allRun)
      .setPipelineSnapTime()
      .process(table,crossTableValidation)
  }

}

class PipelineValidation (_etlDB: String, _allRun: Boolean) extends PipelineValidationHelper(_etlDB,_allRun) with SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  def process(tableArray: Array[String] = Array(), crossTableValidation: Boolean = true): Unit = {

      val processingStartTime = System.currentTimeMillis()

      println("By Default Pipeline_Report would be Validated")
      (validations, quarantine) == validatePipelineTable()

      if (crossTableValidation) {
        println("Cross Table Validation has been Configured")
        (validations, quarantine) == validateCrossTable()
      } else {
        println("Cross Table Validation has not been Configured")
        logger.log(Level.INFO, "Cross Table Validation is Disabled")
      }

      tableArray.length match {
        case 0 =>
          println(s"By Default Single Table Validation has been configured for ${clsfTable},${jrcpTable},${clusterTable},${sparkJobTable}," +
            s"${sqlQueryHistTable},${jobRunTable},${jobTable}")
          (validations, quarantine) == handleValidation(clsfTable, clsfDF, validateCLSF, validations, quarantine)
          (validations, quarantine) == handleValidation(jrcpTable, jrcpDF, validateJRCP, validations, quarantine)
          (validations, quarantine) == handleValidation(clusterTable, clusterDF, validateCluster, validations, quarantine)
          (validations, quarantine) == handleValidation(sparkJobTable, sparkJobDF, validateSparkJob, validations, quarantine)
          (validations, quarantine) == handleValidation(sqlQueryHistTable, sqlQueryHistDF, validateSqlQueryHist, validations, quarantine)
          (validations, quarantine) == handleValidation(jobRunTable, jobRunDF, validateJobRun, validations, quarantine)
          (validations, quarantine) == handleValidation(jobTable, jobDF, validateJob, validations, quarantine)

        case _ =>
          println(s"Single Table Validation has been configured for ${tableArray.mkString(",")}")
          tableArray.map(_.toLowerCase).foreach {
            case tableName@"clusterstatefact_gold" =>
              (validations, quarantine) == handleValidation(clsfTable, clsfDF, validateCLSF, validations, quarantine)
            case tableName@"jobruncostpotentialfact_gold" =>
              (validations, quarantine) == handleValidation(jrcpTable, jrcpDF, validateJRCP, validations, quarantine)
            case tableName@"cluster_gold" =>
              (validations, quarantine) == handleValidation(clusterTable, clusterDF, validateCluster, validations, quarantine)
            case tableName@"sparkjob_gold" =>
              (validations, quarantine) == handleValidation(sparkJobTable, sparkJobDF, validateSparkJob, validations, quarantine)
            case tableName@"sql_query_history_gold" =>
              (validations, quarantine) == handleValidation(sqlQueryHistTable, sqlQueryHistDF, validateSqlQueryHist, validations, quarantine)
            case tableName@"jobrun_gold" =>
              (validations, quarantine) == handleValidation(jobRunTable, jobRunDF, validateJobRun, validations, quarantine)
            case tableName@"job_gold" =>
              (validations, quarantine) == handleValidation(jobTable, jobDF, validateJob, validations, quarantine)
            case tableName => println(s"Table $tableName is not recognized or not supported.")
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