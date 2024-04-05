package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.pipeline.Pipeline
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.validation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

/**
 * This class contains the utility functions for PipelineValidation.scala.
 */

class PipelineValidationHelper(_etlDB: String)  extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _pipelineSnapTime: Long = _

  private val _healthCheck_id: String = java.util.UUID.randomUUID.toString
  private val _quarantine_id: String = java.util.UUID.randomUUID.toString

  private def healthCheckID: String = _healthCheck_id

  def quarantineID: String = _quarantine_id

  def etlDB: String = _etlDB

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

  val validPrefixes = List("dbfs:", "s3", "abfss", "gs")
  val storagePrefix: String = workSpace.getConfig.etlDataPathPrefix
  var healthCheckBasePath = storagePrefix.replace("global_share", "") + "healthCheck"
  if (!validPrefixes.exists(healthCheckBasePath.startsWith)) {
    healthCheckBasePath = s"dbfs:$healthCheckBasePath"
  }

  val healthCheckReportPath = s"""$healthCheckBasePath/heathCheck_report"""
  val quarantineReportPath = s"""$healthCheckBasePath/quarantine_report"""

  val Overwatch_RunIDs :Array[String] = if (spark.catalog.tableExists(s"$etlDB.pipeline_report")) {
    val All_Overwatch_RunID = spark.read.table(s"$etlDB.pipeline_report")
      .select("Overwatch_RunID").distinct().collect().map(_.getString(0))
    if (Helpers.pathExists(healthCheckReportPath)) {
      val healthCheckDF = spark.read.load(healthCheckReportPath)
      val Validated_Overwatch_RunIDs = healthCheckDF.select("Overwatch_RunID").distinct().collect().map(_.getString(0))
      All_Overwatch_RunID.diff(Validated_Overwatch_RunIDs)
    } else {
      All_Overwatch_RunID
    }
  }
    else{
    val errMsg = s"pipeline_report is not present in $etlDB. To proceed with pipeline_validation , pipeline_report table needs to be present in the database.Pipeline Validation aborted!!!"
    throw new BadConfigException(errMsg)
  }


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

  val filterCondition = 'Overwatch_RunID.isin(Overwatch_RunIDs:_*)
  val jrcpDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$jrcpTable")) spark.read.table(s"$etlDB.$jrcpTable").filter(filterCondition) else spark.emptyDataFrame
  val clsfDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$clsfTable")) spark.read.table(s"$etlDB.$clsfTable").filter(filterCondition) else spark.emptyDataFrame
  val jobRunDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$jobRunTable")) spark.read.table(s"$etlDB.$jobRunTable").filter(filterCondition) else spark.emptyDataFrame
  val nbDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$nbTable")) spark.read.table(s"$etlDB.$nbTable").filter(filterCondition) else spark.emptyDataFrame
  val nbcmdDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$nbcmdTable")) spark.read.table(s"$etlDB.$nbcmdTable").filter(filterCondition) else spark.emptyDataFrame
  val clusterDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$clusterTable")) spark.read.table(s"$etlDB.$clusterTable").filter(filterCondition) else spark.emptyDataFrame
  val sparkJobDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$sparkJobTable")) spark.read.table(s"$etlDB.$sparkJobTable").filter(filterCondition) else spark.emptyDataFrame
  val sqlQueryHistDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$sqlQueryHistTable")) spark.read.table(s"$etlDB.$sqlQueryHistTable").filter(filterCondition) else spark.emptyDataFrame
  val jobDF: DataFrame = if (spark.catalog.tableExists(s"$etlDB.$jobTable")) spark.read.table(s"$etlDB.$jobTable").filter(filterCondition) else spark.emptyDataFrame


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
    Rule(ruleName, col(configColumns) > lit(1))
  }

  def checkRunningDays(ruleName: String, configColumns: String): Rule = {
    Rule(ruleName, col(configColumns) === 2)
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
                                   validationType: String = "",
                                   Overwatch_RunID:String
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
      if (validationType.toLowerCase() == "validate_not_required"){
        val healthCheckMsg = s"Validation is not required for ${table_name} for Overwatch_RunID ${Overwatch_RunID}. The Table doesn't contain any data"
        vStatus.append(HealthCheckReport(etlDB, table_name, healthCheckRuleColumn,"Single_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
      }
      else if (countOfNegativeValidation == 0) {
        val healthCheckMsg = "Success"
        vStatus.append(HealthCheckReport(etlDB, table_name, healthCheckRuleColumn,"Single_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
      } else {
        val (healthCheckMsg: String, healthCheckType: String) = validationType.toLowerCase() match {
          case "validate_greater_than_zero" =>
            (s"HealthCheck Failed: got $countOfNegativeValidation ${colName}s which are not greater than zero or is NULL", "Failure")
          case "validate_not_null" =>
            (s"HealthCheck Failed: got $countOfNegativeValidation ${colName}s which are null", "Failure")
          case "validate_leq_one" =>
            (s"HealthCheck Failed: got $countOfNegativeValidation ${colName}s which are greater than 1", "Failure")
          case "validate_values_in_between" =>
            (s"HealthCheck Warning: got $countOfNegativeValidation ${colName}s which are not in between expected values", "Warning")
          case _ =>
            (s"HealthCheck Warning : got $countOfNegativeValidation ${colName}s which are greater than 2", "Warning")
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

  /**
   * Function to Validate relation between 2 OW tables. Check whether we have proper data consistency between 2 tables.
   * @param source : Source OW Table
   * @param target : Target OW Table
   * @param sourceDF : Dataframe created from Source OW Table
   * @param targetDF : Dataframe created from Target OW Table
   * @param column : Column on which Data consistency would be validated between 2 tables.
   * @param key : Key Column in Source Tables. Would be used for Reporting Purpose.
   * @param validationStatus : Validation Status Array for Validation Status Report
   * @param quarantineStatus : Quarantine Status Array for Quarantine Report
   * @return
   */
  def validateColumnBetweenMultipleTable(
                                          source: PipelineTable,
                                          target: PipelineTable,
                                          sourceDF: DataFrame,
                                          targetDF: DataFrame,
                                          column: String,
                                          key: Array[String],
                                          validationStatus: ArrayBuffer[HealthCheckReport],
                                          quarantineStatus: ArrayBuffer[QuarantineReport],
                                          Overwatch_RunID:String
                                        ): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    val vStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    val qStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()
    val sourceTable = source.name
    val targetTable = target.name

    if (spark.catalog.tableExists(s"$etlDB.$sourceTable") &&  spark.catalog.tableExists(s"$etlDB.$targetTable")){
      val ruleName = s"${column.toUpperCase()}_Present_In_${sourceTable}_But_Not_In_$targetTable"
      if (sourceDF.count() == 0 || targetDF.count() == 0) {
        val msg = s"Cross table validation between source $sourceTable and target $targetTable is not possible for Overwatch_RunID $Overwatch_RunID as either of them doesn't contain any data"
        vStatus.append(HealthCheckReport(etlDB, targetTable, ruleName,"Cross_Table_Validation", Some(msg), Overwatch_RunID))
        logger.log(Level.WARN,msg)
        return (validationStatus, quarantineStatus)
      }else{
      // In Case of NotebookCommands Table we should only consider the workspaces where verbose auditlog is enabled
      val joinedDF = if (source.name == nbcmdTable || target.name == nbcmdTable){
        val organizationID_list : Array[String] = spark.sql(s"select distinct organization_id from $etlDB.$nbcmdTable").collect().map(_.getString(0))
        sourceDF.filter('organization_id.isin(organizationID_list:_*)).join(targetDF.filter('organization_id.isin(organizationID_list:_*)), Seq(column), "anti").select(key.map(col): _*)
      }else{
        sourceDF.join(targetDF, Seq(column), "anti").select(key.map(col): _*)
      }
      val joinedDFCount = joinedDF.count()
      if (joinedDFCount == 0) {
        val healthCheckMsg = s"HealthCheck Success: There are $joinedDFCount ${column}s that are present in $sourceTable but not in $targetTable"
        vStatus.append(HealthCheckReport(etlDB, targetTable, ruleName,"Cross_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
      } else {
        val healthCheckMsg = s"HealthCheck Warning: There are $joinedDFCount ${column}s that are present in $sourceTable but not in $targetTable"
        vStatus.append(HealthCheckReport(etlDB, targetTable, ruleName, "Cross_Table_Validation", Some(healthCheckMsg), Overwatch_RunID))
        joinedDF.toJSON.collect().foreach(jsonString => {
          qStatus.append(QuarantineReport(etlDB, targetTable, ruleName, "Cross_Table_Validation", "Warning", jsonString))
          })
        }
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

    val tableName = clsfTable
    val key = clsfKey

    val validateRules = Seq[Rule](
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
      validateNotNull("Driver_Node_Type_ID_Should_Not_be_NULL", "driver_node_type_id"),
      validateNotNull("Node_Type_ID_Should_Not_be_NULL_for_Multi_Node_Cluster", "node_type_id"),
      validateGreaterThanZero("DBU_Rate_Should_Be_Greater_Than_Zero_for_Runtime_Engine_is_Standard_Or_Photon", "dbu_rate"),
      validateGreaterThanZero("Total_Cost_Should_Be_Greater_Than_Zero_for_Databricks_Billable", "total_cost"),
      checkRunningDays("Check_Whether_Any_Single_Cluster_State_is_Running_For_Multiple_Days", "days_in_state")
    )

    Overwatch_RunIDs.foreach(Overwatch_RunID =>{
      val clsf_df : DataFrame= clsfDF.filter('Overwatch_RunID === Overwatch_RunID)
      if (clsf_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(clsf_df).add(validateRules),
          tableName, key, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      }else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(clsf_df).add(validateRules.take(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(clsf_df.where("target_num_workers != 0")).add(validateRules(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(clsf_df.where("runtime_engine IN ('STANDARD','PHOTON')")).add(validateRules(3)),
          tableName, key, validationStatus, quarantineStatus, "validate_greater_than_zero", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(clsf_df.where("cluster_name is not null").where("databricks_billable is true")).add(validateRules(4)),
          tableName, key, validationStatus, quarantineStatus, "validate_greater_than_zero", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(clsf_df).add(validateRules(5)),
          tableName, key, validationStatus, quarantineStatus, "", Overwatch_RunID)
      }
    })
    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateJRCP(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = jrcpTable
    val key = jrcpKey

    val validateRules = Seq[Rule](
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      validateNotNull("Driver_Node_Type_ID_Should_Not_be_NULL", "driver_node_type_id"),
      validateLEQOne("Job_Run_Cluster_Util_value_Should_Not_Be_More_Than_One", "Job_run_cluster_util"),
      checkRunningDays("Check_Whether_Any_Job_is_Running_For_Multiple_Days", "days_in_running")
    )

    Overwatch_RunIDs.foreach(Overwatch_RunID =>{
      val jrcp_df = jrcpDF.filter('Overwatch_RunID === Overwatch_RunID)
        .withColumn("days_in_running", size(col("running_days")))
      if (jrcp_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jrcp_df).add(validateRules),
          tableName, key, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      }else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jrcp_df).add(validateRules.take(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jrcp_df).add(validateRules(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_leq_one", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jrcp_df).add(validateRules(3)),
          tableName, key, validationStatus, quarantineStatus, "", Overwatch_RunID)
      }
    })
    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateCluster(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = clusterTable
    val key = clusterKey

    val validateRules = Seq[Rule](
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
      validateNotNull("Driver_Node_Type_ID_Should_Not_be_NULL", "driver_node_type"),
      validateNotNull("Node_Type_ID_Should_Not_be_NULL_for_Multi_Node_Cluster", "node_type"),
      checkColumnInValues("Cluster_Type_Should_be_In_Between_Serverless_SQL-Analytics_Single-Node_Standard_High-Concurrency", "cluster_type"
        , Array("Serverless", "SQL Analytics", "Single Node", "Standard", "High-Concurrency"))
    )

    Overwatch_RunIDs.foreach(Overwatch_RunID =>{
      val cluster_df = clusterDF.filter('Overwatch_RunID === Overwatch_RunID)
      if (cluster_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(cluster_df).add(validateRules),
          tableName, key, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      } else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(cluster_df).add(validateRules.take(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(cluster_df.where("num_workers != 0")).add(validateRules(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(cluster_df).add(validateRules(3)),
          tableName, key, validationStatus, quarantineStatus, "validate_values_in_between", Overwatch_RunID)
      }
    })
    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateSparkJob(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = sparkJobTable
    val key = sparkJobKey

    val validateRules = Seq[Rule](
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      validateNotNull("db_id_in_job_Should_Not_be_NULL_When_db_Job_Id_is_Not_NULL", "db_id_in_job")
    )

    Overwatch_RunIDs.foreach(Overwatch_RunID => {
      val sparkJob_df = sparkJobDF.filter('Overwatch_RunID === Overwatch_RunID)
      if (sparkJob_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(sparkJob_df).add(validateRules),
          tableName, key, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      } else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(sparkJob_df).add(validateRules.take(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(sparkJob_df.where("db_job_id is not NULL")).add(validateRules(2)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)
      }
    })
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

    Overwatch_RunIDs.foreach(Overwatch_RunID => {
      val sqlQueryHist_df = sqlQueryHistDF.filter('Overwatch_RunID === Overwatch_RunID)
      if (sqlQueryHist_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(sqlQueryHist_df).add(validateRules),
          tableName, sqlQueryHistKey, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      } else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(sqlQueryHist_df).add(validateRules),
          tableName, sqlQueryHistKey, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)
      }
    })
    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateJobRun(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = jobRunTable
    val key = jobRunKey

    val validateRules = Seq[Rule](
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      validateNotNull("Run_ID_Should_Not_be_NULL", "run_id"),
      validateNotNull("Job_Run_ID_Should_Not_be_NULL", "job_run_id"),
      validateNotNull("Task_Run_ID_Should_Not_be_NULL", "task_run_id"),
      validateNotNull("Cluster_ID_Should_Not_be_NULL", "cluster_id"),
    )
    Overwatch_RunIDs.foreach(Overwatch_RunID => {
      val jobRun_df = jobRunDF.filter('Overwatch_RunID === Overwatch_RunID)
      if (jobRun_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jobRun_df).add(validateRules),
          tableName, key, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      } else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jobRun_df).add(validateRules.take(4)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(jobRun_df.filter(!'task_type.isin("sqlalert", "sqldashboard", "pipeline"))).add(validateRules(4)),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)
      }
    })
    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateJob(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val tableName = jobTable
    val key = jobKey

    val validateRules = Seq[Rule](
      validateNotNull("Job_ID_Should_Not_be_NULL", "job_id"),
      checkColumnInValues("Action_Should_be_In_Between_snapimpute_create_reset_update_delete_resetJobAcl_changeJobAcl", "action"
        , Array("snapimpute", "create", "reset", "update", "delete", "resetJobAcl", "changeJobAcl"))
    )

    Overwatch_RunIDs.foreach(Overwatch_RunID => {
      val job_df = jobDF.filter('Overwatch_RunID === Overwatch_RunID)
      if (job_df.count() == 0) {
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(job_df).add(validateRules),
          tableName, key, validationStatus, quarantineStatus, "validate_not_required", Overwatch_RunID)
      } else {
        println(s"${tableName} is getting validated for Overwatch_RunID ${Overwatch_RunID}")
        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(job_df).add(validateRules.head),
          tableName, key, validationStatus, quarantineStatus, "validate_not_null", Overwatch_RunID)

        (validationStatus, quarantineStatus) == validateRuleAndUpdateStatus(
          RuleSet(job_df).add(validateRules(1)),
          tableName, key, validationStatus, quarantineStatus, "validate_values_in_between", Overwatch_RunID)
      }
    })
    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  def checkPipelineModules (
                           resultDF: DataFrame,
                           table_name: String,
                           validationStatus: ArrayBuffer[HealthCheckReport],
                           quarantineStatus: ArrayBuffer[QuarantineReport],
                           Overwatch_RunID: String,
                           status: String
                         ) : (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    val vStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    val qStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    val healthCheckRuleColumn = f"Check If Any Module is ${status}"

    val dfWithNegativeValidation = resultDF.filter('status.startsWith(status))
    val countOfNegativeValidation = dfWithNegativeValidation.count()
    if (countOfNegativeValidation == 0) {
      val healthCheckMsg = "Success"
      vStatus.append(HealthCheckReport(etlDB, table_name, healthCheckRuleColumn, "Pipeline_Report_Validation", Some(healthCheckMsg), Overwatch_RunID))
    }else{
      val healthCheckMsg = s"HealthCheck Warning: got $countOfNegativeValidation ${status} Module"
      vStatus.append(HealthCheckReport(etlDB, table_name, healthCheckRuleColumn, "Pipeline_Report_Validation", Some(healthCheckMsg), Overwatch_RunID))
      dfWithNegativeValidation.toJSON.collect().foreach(jsonString => {
        qStatus.append(QuarantineReport(etlDB, table_name, healthCheckRuleColumn, "Pipeline_Report_Validation", "Warning", jsonString))
      })
    }
    validationStatus ++= vStatus
    quarantineStatus ++= qStatus

    (validationStatus, quarantineStatus)
  }

  private[overwatch] def validatePipelineTable(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) ={
    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    Overwatch_RunIDs.foreach(Overwatch_RunID => {
      val pipeline_df = spark.table(s"$etlDB.pipeline_report").filter('Overwatch_RunID === Overwatch_RunID)
      val windowSpec = Window.partitionBy("organization_id","moduleID","Overwatch_RunID").orderBy('Pipeline_SnapTS.desc)
      val resultDF = pipeline_df.select(
        col("organization_id"),
        col("workspace_name"),
        col("moduleID"),
        col("moduleName"),
        col("fromTS"),
        col("untilTS"),
        col("Overwatch_RunID"),
        substring(col("status"), 0, 200).alias("status"),
        col("Pipeline_SnapTS"),
        rank().over(windowSpec).alias("rank1")
      )

      (validationStatus, quarantineStatus) == checkPipelineModules(resultDF,"pipeline_report",validationStatus,quarantineStatus,Overwatch_RunID,"EMPTY")
      (validationStatus, quarantineStatus) == checkPipelineModules(resultDF,"pipeline_report",validationStatus,quarantineStatus,Overwatch_RunID,"FAILED")
    })

    (validations ++= validationStatus, quarantine ++= quarantineStatus)
  }

  private[overwatch] def validateCrossTable(): (ArrayBuffer[HealthCheckReport], ArrayBuffer[QuarantineReport]) = {

    var validationStatus: ArrayBuffer[HealthCheckReport] = new ArrayBuffer[HealthCheckReport]()
    var quarantineStatus: ArrayBuffer[QuarantineReport] = new ArrayBuffer[QuarantineReport]()

    Overwatch_RunIDs.foreach(Overwatch_RunID => {
      val jobRun_df = if (jobRunDF.count == 0)spark.emptyDataFrame else jobRunDF.filter('Overwatch_RunID === Overwatch_RunID)
      val jrcp_df = if (jrcpDF.count == 0)spark.emptyDataFrame else jrcpDF.filter('Overwatch_RunID === Overwatch_RunID)
      val nb_df = if (nbDF.count == 0)spark.emptyDataFrame else nbDF.filter('Overwatch_RunID === Overwatch_RunID)
      val nbcmd_df = if (nbcmdDF.count == 0)spark.emptyDataFrame else nbcmdDF.filter('Overwatch_RunID === Overwatch_RunID)
      val clsf_df = if (clsfDF.count == 0)spark.emptyDataFrame else clsfDF.filter('Overwatch_RunID === Overwatch_RunID)
      val cluster_df = if (clusterDF.count == 0)spark.emptyDataFrame else clusterDF.filter('Overwatch_RunID === Overwatch_RunID)

      //Job_ID_Present_In_JobRun_Gold_But_Not_In_JobRunCostPotentialFact_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunTarget, goldTargets.jobRunCostPotentialFactTarget,
        jobRun_df, jrcp_df, "job_id", jobRunKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Job_ID_Present_In_JobRunCostPotentialFact_Gold_But_Not_In_JobRun_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunCostPotentialFactTarget, goldTargets.jobRunTarget,
        jrcp_df, jobRun_df, "job_id", jrcpKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Cluster_ID_Present_In_JobRun_Gold_But_Not_In_JobRunCostPotentialFact_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunTarget, goldTargets.jobRunCostPotentialFactTarget,
        jobRun_df, jrcp_df, "cluster_id", jobRunKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Cluster_ID_Present_In_JobRunCostPotentialFact_Gold_But_Not_In_JobRun_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunCostPotentialFactTarget, goldTargets.jobRunTarget,
        jrcp_df, jobRun_df, "cluster_id", jrcpKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Notebook_Id_Present_In_Notebook_gold_But_Not_In_NotebookCommands_gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.notebookTarget, goldTargets.notebookCommandsTarget,
        nb_df, nbcmd_df, "notebook_id", nbkey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Notebook_Id_Present_In_NotebookCommands_Gold_But_Not_In_Notebook_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.notebookCommandsTarget, goldTargets.notebookTarget,
        nbcmd_df, nb_df, "notebook_id", nbcmdkey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Cluster_ID_Present_In_ClusterStateFact_Gold_But_Not_In_JobRunCostPotentialFact_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.clusterStateFactTarget, goldTargets.jobRunCostPotentialFactTarget,
        clsf_df, jrcp_df, "cluster_id", clsfKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Cluster_ID_Present_In_JobRunCostPotentialFact_Gold_But_Not_In_ClusterStateFact_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.jobRunCostPotentialFactTarget, goldTargets.clusterStateFactTarget,
        jrcp_df, clsf_df, "cluster_id", jrcpKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Cluster_ID_Present_In_Cluster_Gold_But_Not_In_ClusterStateFact_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.clusterTarget, goldTargets.clusterStateFactTarget,
        cluster_df, clsf_df, "cluster_id", clusterKey, validationStatus, quarantineStatus,Overwatch_RunID)

      //Cluster_ID_Present_In_ClusterStateFact_Gold_But_Not_In_Cluster_Gold
      (validationStatus, quarantineStatus) == validateColumnBetweenMultipleTable(goldTargets.clusterStateFactTarget, goldTargets.clusterTarget,
        clsf_df, cluster_df, "cluster_id", clsfKey, validationStatus, quarantineStatus,Overwatch_RunID)
    })
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

