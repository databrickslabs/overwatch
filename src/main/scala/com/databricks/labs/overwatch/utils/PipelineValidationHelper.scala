package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.pipeline.Pipeline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.validation._
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

abstract class PipelineValidationHelper(_etlDB: String)  extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _pipelineSnapTime: Long = _

  private val _healthCheck_id: String = java.util.UUID.randomUUID.toString
  private val _quarantine_id: String = java.util.UUID.randomUUID.toString

  private def healthCheckID: String = _healthCheck_id

  private def quarantineID: String = _quarantine_id

  private def etlDB: String = _etlDB

  private var _validations: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
  private var _quarantine: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

   def validations: ArrayBuffer[HealthCheckReport] = _validations

   def quarantine: ArrayBuffer[QuarantineReport] = _quarantine

  val _isOverwatchDB: Boolean = spark.sessionState.catalog.getDatabaseMetadata(etlDB).properties.getOrElse("OVERWATCHDB", "FALSE").toBoolean

  private def isOverwatchDB = _isOverwatchDB

  private val workSpace = if (isOverwatchDB) {
    println(s"$etlDB is Overwatch Database and Suitable for Pipeline Validation")
    Helpers.getWorkspaceByDatabase(etlDB)
  } else {
    val errMsg = s"${etlDB} is Not Overwatch Database.Pipeline Validation can only work on Overwatch Database Tables. Validation aborted!!!"
    throw new BadConfigException(errMsg)
  }

  private val pipelineDF = if (spark.catalog.tableExists(s"$etlDB.pipeline_report")) {
    spark.read.table(s"$etlDB.pipeline_report")
  } else {
    val errMsg = s"pipeline_report is not present in $etlDB. To proceed with pipeline_validation , pipeline_report table needs to be present in the database.Pipeline Validation aborted!!!"
    throw new BadConfigException(errMsg)
  }
  val storagePrefix: String = pipelineDF.select("inputConfig.dataTarget.etlDataPathPrefix").collect()(0)(0).toString
  private val Overwatch_RunID = pipelineDF.orderBy('Pipeline_SnapTS.desc).select("Overwatch_RunID").collect()(0)(0).toString

  private val gold = Gold(workSpace)
  private val goldTargets = gold.GoldTargets


  val jrcpKey: Array[String] = goldTargets.jobRunCostPotentialFactTarget._keys ++ goldTargets.jobRunCostPotentialFactTarget.partitionBy
  val clsfKey: Array[String] = goldTargets.clusterStateFactTarget._keys ++ goldTargets.clusterStateFactTarget.partitionBy
  val jobRunKey: Array[String] = goldTargets.jobRunTarget._keys ++ goldTargets.jobRunTarget.partitionBy
  val nbkey: Array[String] = goldTargets.notebookTarget._keys ++ goldTargets.notebookTarget.partitionBy
  val nbcmdkey: Array[String] = goldTargets.notebookCommandsTarget._keys ++ goldTargets.notebookCommandsTarget.partitionBy
  val clusterKey: Array[String] = goldTargets.clusterTarget._keys ++ goldTargets.clusterTarget.partitionBy
  val sparkJobKey: Array[String] = goldTargets.sparkJobTarget._keys ++ goldTargets.sparkJobTarget.partitionBy
  val sqlQueryHistKey: Array[String] = goldTargets.sqlQueryHistoryTarget._keys ++ goldTargets.sqlQueryHistoryTarget.partitionBy
  val jobKey: Array[String] = goldTargets.jobTarget._keys ++ goldTargets.jobTarget.partitionBy


  val jrcpTable: String = goldTargets.jobRunCostPotentialFactTarget.name
  val clsfTable: String = goldTargets.clusterStateFactTarget.name
  val jobRunTable: String = goldTargets.jobRunTarget.name
  val nbTable: String = goldTargets.notebookTarget.name
  val nbcmdTable: String = goldTargets.notebookCommandsTarget.name
  val clusterTable: String = goldTargets.clusterTarget.name
  val sparkJobTable: String = goldTargets.sparkJobTarget.name
  val sqlQueryHistTable: String = goldTargets.sqlQueryHistoryTarget.name
  val jobTable: String = goldTargets.jobTarget.name

  val jrcpDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$jrcpTable")) spark.read.table(s"$etlDB.$jrcpTable") else spark.emptyDataFrame
  val clsfDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$clsfTable")) spark.read.table(s"$etlDB.$clsfTable") else spark.emptyDataFrame
  val jobRunDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$jobRunTable")) spark.read.table(s"$etlDB.$jobRunTable") else spark.emptyDataFrame
  val nbDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$nbTable")) spark.read.table(s"$etlDB.$nbTable") else spark.emptyDataFrame
  val nbcmdDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$nbcmdTable")) spark.read.table(s"$etlDB.$nbcmdTable") else spark.emptyDataFrame
  val clusterDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$clusterTable")) spark.read.table(s"$etlDB.$clusterTable") else spark.emptyDataFrame
  val sparkJobDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$sparkJobTable")) spark.read.table(s"$etlDB.$sparkJobTable") else spark.emptyDataFrame
  val sqlQueryHistDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$sqlQueryHistTable")) spark.read.table(s"$etlDB.$sqlQueryHistTable") else spark.emptyDataFrame
  val jobDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$jobTable")) spark.read.table(s"$etlDB.$jobTable") else spark.emptyDataFrame

  private[overwatch] def pipelineSnapTime: TimeTypes = {
    Pipeline.createTimeDetail(_pipelineSnapTime)
  }

  private[overwatch] def setPipelineSnapTime(): this.type = {
    _pipelineSnapTime = LocalDateTime.now(Pipeline.systemZoneId).toInstant(Pipeline.systemZoneOffset).toEpochMilli
    logger.log(Level.INFO, s"INIT: Pipeline Snap TS: ${pipelineSnapTime.asUnixTimeMilli}-${pipelineSnapTime.asTSString}")
    this
  }

  def validateNotNull(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, col(configColumns).isNotNull)
  }

  def validateGreaterThanZero(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, col(configColumns) > lit(0))
  }

  def validateLEQOne(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, col(configColumns) <= lit(1) && col(configColumns).isNull)
  }

  def checkRunningDays(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, col(configColumns) === 1)
  }

  def checkColumnInValues(ruleName: String, configColumns: String, value: Array[String]): Rule = {
    Rule(ruleName, col(configColumns).isin(value: _*))
  }

  def validateRuleAndUpdateStatus(
                                   validateNullRuleSet: RuleSet,
                                   table_name: String,
                                   keys: Array[String],
                                   validationStatus: ArrayBuffer[HealthCheckReport],
                                   quarantineStatus: ArrayBuffer[QuarantineReport],
                                   validationType: String = ""
                                 ): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    val vStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    val qStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val validation = validateNullRuleSet.validate()

    val completeReportDF = validation.completeReport

    validateNullRuleSet.getRules.foreach(elem => {
      val colName = elem.inputColumn.toString.split("\\(")(1).split("\\)")(0).split(" ")(0)
      val healthCheckRuleColumn = elem.ruleName
      val dfWithNegativeValidation = completeReportDF.filter((col(s"$healthCheckRuleColumn.passed") === false) ||
        col(s"$healthCheckRuleColumn.passed").isNull).select(keys.map(col): _*)
      val countOfNegativeValidation = dfWithNegativeValidation.count()
      if (countOfNegativeValidation == 0) {
        val healthCheckMsg = "Success"
        vStatus.append(HealthCheckReport(etlDB, table_name, healthCheckRuleColumn,"Single_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
      } else {
        val (healthCheckMsg: String, healthCheckType: String) =
          if (validationType.toLowerCase() == "validate_greater_than_zero") {
            (s"HealthCheck Failed: got $countOfNegativeValidation ${colName}s which are not greater than zero", "Failure")
          } else if (validationType.toLowerCase() == "validate_not_null") {
            (s"HealthCheck Failed: got $countOfNegativeValidation ${colName}s which are null", "Failure")
          } else if (validationType.toLowerCase() == "validate_leq_one") {
            (s"HealthCheck Failed: got $countOfNegativeValidation ${colName}s which are greater than 1", "Failure")
          } else if (validationType.toLowerCase() == "validate_values_in_between") {
            (s"HealthCheck Warning: got $countOfNegativeValidation ${colName}s which are not in between expected values", "Warning")
          }
          else {
            (s"HealthCheck Warning : got $countOfNegativeValidation ${colName}s which are greater than 1", "Warning")
          }
        vStatus.append(HealthCheckReport(etlDB, table_name, healthCheckRuleColumn,"Single_Table_Validation",Some(healthCheckMsg), Overwatch_RunID))
        dfWithNegativeValidation.toJSON.collect().foreach(jsonString => {
          qStatus.append(QuarantineReport(etlDB, table_name, healthCheckRuleColumn,"Single_Table_Validation", healthCheckType, jsonString))
        })
      }
    })
    validationStatus ++= vStatus
    quarantineStatus ++= qStatus

    (validationStatus, quarantineStatus)
  }

  def validateColumnBetweenMultipleTable(
                                          source: PipelineTable,
                                          target: PipelineTable,
                                          sourceDF: DataFrame,
                                          targetDF: DataFrame,
                                          column: String,
                                          key: Array[String],
                                          validationStatus: ArrayBuffer[HealthCheckReport],
                                          quarantineStatus: ArrayBuffer[QuarantineReport]
                                        ): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    val vStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    val qStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()
    val sourceTable = source.name
    val targetTable = target.name

    if (spark.catalog.tableExists(s"$etlDB.$sourceTable") &&  spark.catalog.tableExists(s"$etlDB.$targetTable")){
      val joinedDF = sourceDF.join(targetDF, Seq(column), "anti").select(key.map(col): _*)
      val joinedDFCount = joinedDF.count()
      val ruleName = s"${column}_Present_In_${sourceTable}_But_Not_In_$targetTable"

      if (joinedDFCount == 0) {
        val healthCheckMsg = s"HealthCheck Success: There are $joinedDFCount ${column}s that are present in $sourceTable but not in $targetTable"
        vStatus.append(HealthCheckReport(etlDB, targetTable, ruleName,"Cross_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
      } else {
        val healthCheckMsg = s"HealthCheck Warning: There are $joinedDFCount ${column}s that are present in $sourceTable but not in $targetTable"
        vStatus.append(HealthCheckReport(etlDB, targetTable, ruleName,"Cross_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
        joinedDF.toJSON.collect().foreach(jsonString => {
          qStatus.append(QuarantineReport(etlDB, targetTable, ruleName,"Cross_Table_Validation", "Warning", jsonString))
        })
      }
      (validationStatus ++= vStatus, quarantineStatus ++= qStatus)
    }else{
      val msg = s"Cross table validation between source $sourceTable and target $targetTable is not possible as either of them doesn't exist in the database"
      println(msg)
      logger.log(Level.WARN,msg)
      (validationStatus, quarantineStatus)
    }
  }

  private[overwatch] def validateCLSF(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()
    val table_name = clsfTable

    val validateRules = Seq[Rule](
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
      validateNotNull("Driver_Node_Type_ID_Should_Not_be_NULL", "driver_node_type_id"),
      validateNotNull("Node_Type_ID_Should_Not_be_NULL_for_Multi_Node_Cluster", "node_type_id"),
      validateGreaterThanZero("DBU_Rate_Should_Be_Greater_Than_Zero_for_Runtime_Engine_is_Standard_Or_Photon", "dbu_rate"),
      validateGreaterThanZero("Total_Cost_Should_Be_Greater_Than_Zero_for_Databricks_Billable", "total_cost"),
      checkRunningDays("Check_Whether_Any_Single_Cluster_State_is_Running_For_Multiple_Days", "days_in_state")
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clsfDF).add(validateRules.take(2)),
      table_name, clsfKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clsfDF.where("target_num_workers != 0")).add(validateRules(2)),
      table_name, clsfKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clsfDF.where("runtime_engine IN ('STANDARD','PHOTON')")).add(validateRules(3)),
      table_name, clsfKey, validationStatus, quarantineStatus, "validate_greater_than_zero")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clsfDF.where("databricks_billable is true")).add(validateRules(4)),
      table_name, clsfKey, validationStatus, quarantineStatus, "validate_greater_than_zero")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clsfDF).add(validateRules(5)),
      table_name, clsfKey, validationStatus, quarantineStatus)

    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateJRCP(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()
    val table_name = jrcpTable

    val jrcp_df = jrcpDF.withColumn("days_in_running", size(col("running_days")))

    val validateRules = Seq[Rule](
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      validateNotNull("Driver_Node_Type_ID_Should_Not_be_NULL", "driver_node_type_id"),
      validateLEQOne("Job_Run_Cluster_Util_value_Should_Not_Be_More_Than_One", "Job_run_cluster_util"),
      checkRunningDays("Check_Whether_Any_Job_is_Running_For_Multiple_Days", "days_in_running")
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(jrcp_df).add(validateRules.take(2)),
      table_name, jrcpKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(jrcp_df).add(validateRules(2)),
      table_name, jrcpKey, validationStatus, quarantineStatus, "validate_leq_one")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(jrcp_df).add(validateRules(3)),
      table_name, jrcpKey, validationStatus, quarantineStatus)

    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateCluster(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val table_name = clusterTable

    val validateRules = Seq[Rule](
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
      validateNotNull("Driver_Node_Type_ID_Should_Not_be_NULL", "driver_node_type"),
      validateNotNull("Node_Type_ID_Should_Not_be_NULL_for_Multi_Node_Cluster", "node_type"),
      checkColumnInValues("Cluster_Type_Should_be_In_Between_Serverless_SQL-Analytics_Single-Node_Standard_High-Concurrency", "cluster_type"
        , Array("Serverless", "SQL Analytics", "Single Node", "Standard", "High-Concurrency"))
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clusterDF).add(validateRules.take(2)),
      table_name, clusterKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clusterDF.where("num_workers != 0")).add(validateRules(2)),
      table_name, clusterKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(clusterDF).add(validateRules(3)),
      table_name, clusterKey, validationStatus, quarantineStatus, "validate_values_in_between")

    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateSparkJob(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = sparkJobTable

    val validateRules = Seq[Rule](
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      validateNotNull("db_id_in_job_Should_Not_be_NULL_When_db_Job_Id_is_Not_NULL", "db_id_in_job")
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(sparkJobDF).add(validateRules.take(2)),
      tableName, sparkJobKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(sparkJobDF.where("db_job_id is not NULL")).add(validateRules(2)),
      tableName, sparkJobKey, validationStatus, quarantineStatus, "validate_not_null")

    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateSqlQueryHist(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = sqlQueryHistTable

    val validateRules = Seq[Rule](
      validateNotNull("Warehouse_ID_Should_Not_be_NULL", "warehouse_id"),
      validateNotNull("Query_ID_Should_Not_be_NULL", "query_id")
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(sqlQueryHistDF).add(validateRules),
      tableName, sqlQueryHistKey, validationStatus, quarantineStatus, "validate_not_null")


    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateJobRun(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = jobRunTable

    val validateRules = Seq[Rule](
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      validateNotNull("Run_ID_Should_Not_be_NULL", "run_id"),
      validateNotNull("Job_Run_ID_Should_Not_be_NULL", "job_run_id"),
      validateNotNull("Task_Run_ID_Should_Not_be_NULL", "task_run_id"),
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(jobRunDF).add(validateRules),
      tableName, jobRunKey, validationStatus, quarantineStatus, "validate_not_null")


    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateJob(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = jobTable

    val validateRules = Seq[Rule](
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      checkColumnInValues("Action_Should_be_In_Between_snapimpute_create_reset_update_delete_resetJobAcl_changeJobAcl", "action"
        , Array("snapimpute", "create", "reset", "update", "delete", "resetJobAcl", "changeJobAcl"))
    )

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(jobDF).add(validateRules.head),
      tableName, jobKey, validationStatus, quarantineStatus, "validate_not_null")

    (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
      RuleSet(jobDF).add(validateRules(1)),
      tableName, jobKey, validationStatus, quarantineStatus, "validate_values_in_between")

    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateCrossTable(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    //Job_ID_Present_In_JobRun_Gold_But_Not_In_JobRunCostPotentialFact_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunTarget, goldTargets.jobRunCostPotentialFactTarget,
      jobRunDF, jrcpDF, "job_id", jobRunKey, validationStatus, quarantineStatus)

    //Job_ID_Present_In_JobRunCostPotentialFact_Gold_But_Not_In_JobRun_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunCostPotentialFactTarget, goldTargets.jobRunTarget,
      jrcpDF, jobRunDF, "job_id", jrcpKey, validationStatus, quarantineStatus)

    //Cluster_ID_Present_In_JobRun_Gold_But_Not_In_JobRunCostPotentialFact_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunTarget, goldTargets.jobRunCostPotentialFactTarget,
      jobRunDF, jrcpDF, "cluster_id", jobRunKey, validationStatus, quarantineStatus)

    //Cluster_ID_Present_In_JobRunCostPotentialFact_Gold_But_Not_In_JobRun_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunCostPotentialFactTarget, goldTargets.jobRunTarget,
      jrcpDF, jobRunDF, "cluster_id", jrcpKey, validationStatus, quarantineStatus)

    //    Notebook_Id_Present_In_Notebook_gold_But_Not_In_NotebookCommands_gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.notebookTarget, goldTargets.notebookCommandsTarget,
      nbDF, nbcmdDF, "notebook_id", nbkey, validationStatus, quarantineStatus)

    //    Notebook_Id_Present_In_NotebookCommands_Gold_But_Not_In_Notebook_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.notebookCommandsTarget, goldTargets.notebookTarget,
      nbcmdDF, nbDF, "notebook_id", nbcmdkey, validationStatus, quarantineStatus)

    //    Cluster_ID_Present_In_ClusterStateFact_Gold_But_Not_In_JobRunCostPotentialFact_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.clusterStateFactTarget, goldTargets.jobRunCostPotentialFactTarget,
      clsfDF, jrcpDF, "cluster_id", clsfKey, validationStatus, quarantineStatus)

    //    Cluster_ID_Present_In_JobRunCostPotentialFact_Gold_But_Not_In_ClusterStateFact_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunCostPotentialFactTarget, goldTargets.clusterStateFactTarget,
      jrcpDF, clsfDF, "cluster_id", jrcpKey, validationStatus, quarantineStatus)

    //    Cluster_ID_Present_In_Cluster_Gold_But_Not_In_ClusterStateFact_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.clusterTarget, goldTargets.clusterStateFactTarget,
      clusterDF, clsfDF, "cluster_id", clusterKey, validationStatus, quarantineStatus)

    //    Cluster_ID_Present_In_ClusterStateFact_Gold_But_Not_In_Cluster_Gold
    (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.clusterStateFactTarget, goldTargets.clusterTarget,
      clsfDF, clusterDF, "cluster_id", clsfKey, validationStatus, quarantineStatus)

    (validations ++= validationStatus, quarantine ++= quarantineStatus)

  }

  private[overwatch] def snapShotHealthCheck(validationArray: Array[HealthCheckReport], healthCheckReportPath: String): Unit = {

    validationArray.toSeq.toDS().toDF()
      .withColumn("healthcheck_id", lit(healthCheckID))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .withColumn("quarantine_id", lit(quarantineID))
      .moveColumnsToFront("healthcheck_id")
      .write.format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(healthCheckReportPath)
    println("Validation report has been saved to " + s"""$healthCheckReportPath""")
  }

  private[overwatch] def snapShotQuarantine(quarantineArray: Array[QuarantineReport], quarantineReportPath: String): Unit = {

    quarantineArray.toSeq.toDS().toDF()
      .withColumn("quarantine_id", lit(quarantineID))
      .withColumn("snapTS", lit(pipelineSnapTime.asTSString))
      .moveColumnsToFront("quarantine_id")
      .write
      .partitionBy("quarantine_id")
      .format("delta")
      .option("mergeSchema", "true")
      .mode("append")
      .save(quarantineReportPath)
    println("Quarantine report has been saved to " + s"""$quarantineReportPath""")
  }

  private[overwatch] def handleValidation(
                                tableName: String,
                                df: DataFrame,
                                validationMethod: () => (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]),
                                validations: ArrayBuffer[HealthCheckReport],
                                quarantine: ArrayBuffer[QuarantineReport]): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    if (spark.catalog.tableExists(s"$etlDB.$tableName")) {
      if (df.count() != 0) {
        validationMethod()
      }else{
        println(s"Validation is not required for ${tableName}. The Table doesn't contain any data")
        (validations, quarantine)
      }
    } else {
      println(s"Validation is not possible for ${tableName} as it doesn't exist in the database")
      (validations, quarantine)
    }
  }

}
