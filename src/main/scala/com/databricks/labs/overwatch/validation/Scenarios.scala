package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.validation._
import com.databricks.labs.validation.utils.Structures._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Scenarios extends SparkSessionWrapper {

  /**
    * Run Scenario Tests
    * @param dbName
    * @return
    */
  def validate(dbName: String):DataFrame = {

    jobruncostpotentialfact(dbName).union(jrcpPipelineAggRules(dbName)).union(clusterPotentialWCosts(dbName))
  }


  def clusterPotentialWCosts(dbName : String): DataFrame = {

    // dbu prices
    val interactiveDBUPrice = 0.56D
    val automatedDBUPrice = 0.22D

    // cluster state fact table
    val clusterStateFact = spark.table("overwatch_etl.clusterstatefact_gold")

    val clusterPotentialWCosts = clusterStateFact
      .filter(col("unixTimeMS_state_start").isNotNull && col("unixTimeMS_state_end").isNotNull)

    // set source table keys for window partitioning, sort by start timestamp
    val clusterPotentialWCostsKeys: Array[Column] = Array(col("organization_id"), col("cluster_id"))
    val window = Window.partitionBy(clusterPotentialWCostsKeys: _*).orderBy(col("unixTimeMS_state_start"))

    // defines interesting columns for tests
    val testInterestingKeys: Array[Column] = Array(col("current_num_workers"),
      col("target_num_workers"),
      col("databricks_billable"),
      col("cloud_billable"),
      col("dbu_rate"),
      col("workerSpecs.vCPUs").as("vcpus"),
      col("workerSpecs.Hourly_DBUs").as("worker_hourly_dbus"),
      col("workerSpecs.Compute_Contract_Price").as("worker_compute_contract_price"),
      col("worker_dbu_cost"),
      col("worker_compute_cost"),
      col("worker_potential_core_H"),
      col("uptime_in_state_H"),
      col("uptime_in_state_S"),
      col("uptime_since_restart_S"),
      col("state"),
      col("timestamp_state_start"),
      col("timestamp_state_end"),
      col("unixTimeMS_state_start"))

    /* RECONCILIATIONS */
    // add (re)calculation columns
    val clusterPotentialWCostsTestDf = clusterPotentialWCosts
      .select(clusterPotentialWCostsKeys ++ testInterestingKeys: _*)
      // calc worker dbu costs and rates
      .withColumn("_recalculated_worker_dbu_cost", when(col("databricks_billable") === true, col("worker_hourly_dbus") * 'dbu_rate * 'uptime_in_state_H * 'current_num_workers).otherwise(lit(0)))
      .withColumn("_per_hour_worker_dbu_rate", col("worker_dbu_cost") / 'uptime_in_state_H / 'worker_hourly_dbus / 'current_num_workers)
      .withColumn("_per_hour_worker_dbu_cost", col("worker_dbu_cost") / 'uptime_in_state_H / 'worker_hourly_dbus)
      // calc worker compute costs and rates
      .withColumn("_recalculated_worker_compute_cost", when(col("cloud_billable") === true, col("worker_compute_contract_price") * 'uptime_in_state_H * 'target_num_workers).otherwise(lit(0)))
      .withColumn("_per_hour_worker_compute_rate", col("worker_compute_cost") / 'uptime_in_state_H / 'target_num_workers)
      .withColumn("_per_hour_worker_compute_cost", col("worker_compute_cost") / 'uptime_in_state_H)

    /* ROLLUPS */
    // lag calculations for state transition comparison
    val current_num_workersLagCol = col("current_num_workers") - lag(col("current_num_workers"), 1).over(window)
    val target_num_workersLagCol = col("target_num_workers") - lag(col("target_num_workers"), 1).over(window)
    val worker_dbu_costLagCol  = col("worker_dbu_cost") - lag(col("worker_dbu_cost"), 1).over(window)
    val per_hour_worker_dbu_costLagCol  = lag(col("_per_hour_worker_dbu_cost"), 1).over(window)
    val worker_compute_costLagCol  = col("worker_compute_cost") - lag(col("worker_compute_cost"), 1).over(window)
    val per_hour_worker_compute_costLagCol  = lag(col("_per_hour_worker_compute_cost"), 1).over(window)

    // df
    val df_clusterPotentialWCosts = clusterPotentialWCostsTestDf
      .withColumn("_lag_current_num_workers", current_num_workersLagCol)
      .withColumn("_lag_target_num_workers", target_num_workersLagCol)
      .withColumn("_lag_worker_dbu_cost", worker_dbu_costLagCol)
      .withColumn("_lag_per_hour_worker_dbu_cost", per_hour_worker_dbu_costLagCol)
      .withColumn("_lag_worker_compute_cost", worker_compute_costLagCol)
      .withColumn("_lag_per_hour_worker_compute_cost", per_hour_worker_compute_costLagCol)

    // case based tests (true/false)
    // convention: 0 = test failed, 1 = test passed

    /* RECONCILIATIONS & SANITY TESTS */
    // CostDBUWorkerHourlyRateInvalid
    val CostDBUWorkerHourlyRateInvalidCol = when(col("databricks_billable") === true && abs(col("_per_hour_worker_dbu_rate") - 'dbu_rate) > 0.001, lit(0)).otherwise(lit(1))

    // CostDBUWorkerInvalid
    val CostDBUWorkerInvalidCol = when(abs(col("_recalculated_worker_dbu_cost") - 'worker_dbu_cost) > 0.000001, lit(0)).otherwise(lit(1))

    // CostComputeWorkerHourlyRateInvalid
    val CostComputeWorkerHourlyRateInvalidCol = when(col("cloud_billable") === true && abs(col("_per_hour_worker_compute_rate") - 'worker_compute_contract_price) > 0.001, lit(0)).otherwise(lit(1))

    // CostComputeWorkerInvalid
    val CostComputeWorkerInvalidCol = when(abs(col("_recalculated_worker_compute_cost") - 'worker_compute_cost) > 0.000001, lit(0)).otherwise(lit(1))

    // UptimeInStateInvalid
    val UptimeInStateInvalidCol = when(not(col("state").isin("STARTING", "TERMINATING")) && col("uptime_in_state_S") < 'uptime_since_restart_S, lit(0)).otherwise(lit(1))

    // WorkerPotentialCoreHInvalid
    val WorkerPotentialCoreHInvalidCol = when(col("databricks_billable") === true && abs(col("worker_potential_core_H") - col("uptime_in_state_H") * 'current_num_workers * 'vcpus) > 0.000001, lit(0)).otherwise(lit(1))

    /* ROLLUPS */
    // CostDBUWorkerHourlyProportionalToClusterResize
    val CostDBUWorkerHourlyProportionalToClusterResizeCol = when(col("_lag_current_num_workers") =!= 0 && col("_lag_per_hour_worker_dbu_cost") =!= 0 && abs(col("_per_hour_worker_dbu_cost") - '_lag_per_hour_worker_dbu_cost - col("_lag_current_num_workers") * 'dbu_rate) > 0.000001, lit(0)).otherwise(lit(1))

    // CostComputeWorkerHourlyProportionalToClusterResize
    val CostComputeWorkerHourlyProportionalToClusterResizeCol = when(col("_lag_target_num_workers") =!= 0 && col("_lag_per_hour_worker_compute_cost") =!= 0 && abs(col("_per_hour_worker_compute_cost") - '_lag_per_hour_worker_compute_cost - col("_lag_target_num_workers") * 'worker_compute_contract_price) > 0.000001, lit(0)).otherwise(lit(1))


    // define rules
    val clusterPotentialWCostsRules  = Array(
      Rule("CostDBUWorkerHourlyRateInvalid", CostDBUWorkerHourlyRateInvalidCol, Array(1)),
      Rule("CostDBUWorkerInvalid", CostDBUWorkerInvalidCol, Array(1)),
      Rule("CostComputeWorkerHourlyRateInvalid", CostComputeWorkerHourlyRateInvalidCol, Array(1)),
      Rule("CostComputeWorkerInvalid", CostComputeWorkerInvalidCol, Array(1)),
      Rule("UptimeInStateInvalid", UptimeInStateInvalidCol, Array(1)),
      Rule("WorkerPotentialCoreHInvalid", WorkerPotentialCoreHInvalidCol, Array(1)),
      Rule("CostDBUWorkerHourlyProportionalToClusterResize", CostDBUWorkerHourlyProportionalToClusterResizeCol, Array(1)),
      Rule("CostComputeWorkerHourlyProportionalToClusterResize", CostComputeWorkerHourlyProportionalToClusterResizeCol, Array(1))
    )

    // execute
    val validationResults = RuleSet(df_clusterPotentialWCosts)
      .add(clusterPotentialWCostsRules) // case based tests - array
      //.addMinMaxRules(ClusterAvgShareInvalidRule) // bounds based tests - individual
      .validate()

    validationResults.summaryReport
  }

  /**
    *   Approach : Top-down testing to ensure sanity of calculated values. These rules operate in a single line and will surface potential errors in the calculation pipeline.

    Implemented Rules:

    ClusterUtilInvalid: If the job succeeds, we cannot have worker_potential_core_H < spark_task_runtime_H. Also means job_run_cluster_util cannot be > 1.
    ClusterUtilCalculation: Checks if spark_task_runtime_H / worker_potential_core_H = job_run_cluster_util, rounded to 4 decimals
    ClusterAvgShareInvalid: Checks if avg_cluster_share between 0 and 1.
    SparkTaskRuntimeCalculation: Checks if spark_task_runtime_H = spark_task_runtimeMS / 3600000, rounded to 4 decimals
    CostComputeCalculation: Checks if the driver and worker costs add correctly to the total: driver_compute_cost + worker_compute_cost = total_compute_cost, check absolute difference < 0.000002
    CostDBUCalculation: Checks if the driver and worker DBUs add correctly to the total: driver_dbu_cost + worker_dbu_cost = total_dbu_cost, , check absolute difference < 0.000002
    CostTotalCalculation: Checks if the DBU and compute costs add correctly to the total: total_compute_cost + total_dbu_cost = total_cost, , check absolute difference < 0.000002
    JobRunningDaysInvalid: Checks if list of running days is sane: date(startTS) := min(running_days) and date(endTS) = max(running_days)
    * @param dbName
    */
  def jobruncostpotentialfact(dbName: String): DataFrame = {

    // case based tests (true/false)
    // convention: 0 = test failed, 1 = test passed

    // ClusterUtilInvalid
    val ClusterUtilInvalidCol = when(col("run_terminal_state") === "Succeeded" && col("worker_potential_core_H") < col("spark_task_runtime_H"), lit(0)).otherwise(lit(1))


    // ClusterUtilCalculation
    val ClusterUtilCalculationCol = when(col("run_terminal_state") === "Succeeded" && round(col("spark_task_runtime_H") / 'worker_potential_core_H, 4) === 'job_run_cluster_util, lit(1)).otherwise(lit(0))

    // SparkTaskRuntimeCalculation
    val SparkTaskRuntimeCalculationCol = when(col("spark_task_runtime_H") === round(col("spark_task_runtimeMS") / 3600000, 4), lit(1)).otherwise(lit(0))

    // CostComputeCalculation
    val CostComputeCalculationCol = when(abs(col("driver_compute_cost") + "worker_compute_cost" - "total_compute_cost") > 0.000002, lit(1)).otherwise(lit(0))

    // CostDBUCalculation
    val CostDBUCalculationCol = when(abs(col("driver_dbu_cost") + "worker_dbu_cost" - "total_dbu_cost") > 0.000002, lit(1)).otherwise(lit(0))

    // CostTotalCalculation
    val CostTotalCalculationCol = when(abs(col("total_dbu_cost") + "total_compute_cost" - "total_cost") > 0.000002, lit(1)).otherwise(lit(0))

    // JobRunningDaysInvalid
    val JobRunningDaysInvalidCol = when(to_date(col("job_runtime.startTS")) === array_min(col("running_days")) && to_date(col("job_runtime.endTS")) === array_max(col("running_days")), lit(1)).otherwise(lit(0))

    // define rules
    val jobRunCostPotentialFactRules  = Array (
      Rule("ClusterUtilInvalid", ClusterUtilInvalidCol, Array(1)),
      Rule("ClusterUtilCalculation", ClusterUtilCalculationCol, Array(1)),
      Rule("SparkTaskRuntimeCalculation", SparkTaskRuntimeCalculationCol, Array(1)),
      Rule("CostComputeCalculation", CostComputeCalculationCol, Array(1)),
      Rule("CostDBUCalculation", CostDBUCalculationCol, Array(1)),
      Rule("CostTotalCalculation", CostTotalCalculationCol, Array(1)),
      Rule("JobRunningDaysInvalid", JobRunningDaysInvalidCol, Array(1))
    )

    // boundary tests
    val ClusterAvgShareInvalidRule = MinMaxRuleDef("ClusterAvgShareInvalid", col("avg_cluster_share"), Bounds(0.0, 1.0))

    // define input table
    val df_jobRunCostPotentialFact = spark.table(dbName + ".jobruncostpotentialfact_gold")

    // execute
    val validationReport = RuleSet(df_jobRunCostPotentialFact)
      .add(jobRunCostPotentialFactRules) // case based tests - array
      .addMinMaxRules(ClusterAvgShareInvalidRule) // bounds based tests - individual
      .validate()

    validationReport.summaryReport
  }

  /**
    * Scenarios for Aggregate Rules
  Approach : Recompute JRCP in specific scenarios, check if recalculations match.

  Implemented Rules (jrcp aggregates):

  jrcpWorkerPotentialInvalid: Recalculates worker_potential_core_H from cluster state, join with job run and check if the recalculation reconcile.
  Implemented Rules (individual jrcp states):

  jrcpWorkerPotentialInStateInvalid: worker_potential_core_H * run_state_utilization cannot be larger than cluster_potential_core_H in a particular state.
  The actual potential used in a job is at most the worker potential core for that state. It's weighed by run_state_utilization
  jrcpRunStateUtilizationInvalid: Checkf if run_state_utilization in Bounds(0,1)

    * @param dbName
    * @return
    */
  def jrcpPipelineAggRules(dbName : String): DataFrame = {

    import spark.implicits._

    // define input table
    val df_jobRunCostPotentialFact = spark.table(dbName + ".jobruncostpotentialfact_gold")
      .filter(col("run_cluster_states") > 1 && col("run_terminal_state") === "Succeeded") // && 'cluster_id === "1112-110804-soils854" && 'job_id === 61)

    // keys and cols for testing data
    val jrcpTestDataKeys: Array[Column] = Array(col("organization_id"), col("cluster_id"), col("job_id"), col("id_in_job"))
    val jrcpTestDataInterestingCols: Array[Column] = Array(
      col("job_runtime.startEpochMS").as("job_startTS"),
      col("job_runtime.endEpochMS").as("job_endTS"),
      col("run_terminal_state"),
      col("cluster_type").as("job_cluster_type"),
      col("worker_potential_core_H").as("job_worker_potential_core_H"),
      col("total_cost").as("job_total_cost")
    )

    val df_testDataJRCP = df_jobRunCostPotentialFact
      .select(jrcpTestDataKeys ++ jrcpTestDataInterestingCols: _*)

    // Rule Implementation - JRCP Aggregates

    val jrcpAggKeys: Array[Column] = Array(
      col("organization_id"),
      col("cluster_id"),
      col("job_id"),
      col("id_in_job"),
      col("run_terminal_state"),
      col("job_cluster_type"),
      col("driver_node_type_id"),
      col("node_type_id"),
      col("dbu_rate")
    )

    /*val topClusters = df_testDataJRCP
      .filter(col("organization_id").isNotNull && col("cluster_id").isNotNull)
      .groupBy(col("organization_id"), col("cluster_id")).count
      .orderBy(col("count").desc).limit(40)
      .select(array(col("organization_id"), col("cluster_id"))).as[Seq[String]].collect.toSeq*/

    //org.apache.spark.sql.Encoders.kryo[Seq[String]]

    val topClusters = df_testDataJRCP
      .filter(col("organization_id").isNotNull && col("cluster_id").isNotNull)
      .groupBy(col("organization_id"), col("cluster_id")).count
      .orderBy(col("count").desc).limit(40)
      .select(array(col("organization_id"), col("cluster_id"))).as[Seq[String]].collect.toSeq

    // cluster state fact table
    // remove cluster filter later
    val df_clusterPotentialWCosts = spark.table(dbName + ".clusterstatefact_gold")
      .filter(col("unixTimeMS_state_start").isNotNull && col("unixTimeMS_state_end").isNotNull)
      .filter(col("databricks_billable") === true || col("cloud_billable") === true)
    //.filter('cluster_id === "1112-110804-soils854")

    // keys and cols for testing data
    val cpwcTestDataKeys: Array[Column] = Array(col("organization_id"), col("cluster_id"))
    val cpwcTestDataInterestingCols: Array[Column] = Array(
      col("unixTimeMS_state_start").as("cluster_startTS"),
      col("unixTimeMS_state_end").as("cluster_endTS"),
      col("state"),
      col("uptime_in_state_H"),
      col("worker_potential_core_H").as("cluster_worker_potential_core_H"),
      col("driver_node_type_id"),
      col("node_type_id"),
      col("dbu_rate"),
      col("total_cost").as("cluster_total_cost")
    )

    val df_testDataCPWC = df_clusterPotentialWCosts
      .select(cpwcTestDataKeys ++ cpwcTestDataInterestingCols: _*)

    val clusterPotentialInitialState = df_testDataCPWC.withColumn("timestamp", col("cluster_startTS"))
    val clusterPotentialIntermediateStates = df_testDataCPWC
    val clusterPotentialTerminalState = df_testDataCPWC.withColumn("timestamp", col("cluster_endTS"))


    val jobRunInitialState = df_testDataJRCP
      .withColumn("timestamp", col("job_startTS"))
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialInitialState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 64, maxLookAhead = Long.MaxValue
      ).df
      .drop("timestamp")
      .withColumn("job_runtime_in_state",
        when(col("state").isin("CREATING", "STARTING") || col("job_cluster_type") === "new", col("uptime_in_state_H")) // get true cluster time when state is guaranteed fully initial
          .otherwise((array_min(array(col("cluster_endTS"), col("job_endTS"))) - 'job_startTS) / lit(1000) / 3600)) // otherwise use jobStart as beginning time and min of stateEnd or jobEnd for end time
      //   .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))
      .withColumn("lifecycleState", lit("init"))

    val stateLifecycleKeys = Seq("organization_id", "id_in_job", "cluster_id", "cluster_startTS")

    val jobRunTerminalState = df_testDataJRCP
      .withColumn("timestamp", col("job_endTS"))
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialTerminalState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 64, maxLookback = 0L, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .filter(col("cluster_startTS").isNotNull && col("cluster_endTS").isNotNull && col("cluster_endTS") > 'job_endTS)
      .join(jobRunInitialState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti")
      .withColumn("job_runtime_in_state", (col("job_endTS") - array_max(array(col("cluster_startTS"), col("job_startTS")))) / lit(1000) / 3600)
      //   .withColumn("worker_potential_core_H", when('databricks_billable, 'worker_cores * 'current_num_workers * 'uptime_in_state_H).otherwise(lit(0)))
      .withColumn("lifecycleState", lit("terminal"))

    val jobRunIntermediateStates = df_testDataJRCP.alias("jr")
      .join(clusterPotentialIntermediateStates.alias("cpot").hint("SKEW", Seq("organization_id", "cluster_id"), topClusters),
        col("jr.organization_id") === col("cpot.organization_id") &&
          col("jr.cluster_id") === col("cpot.cluster_id") &&
          col("cpot.cluster_startTS") > col("jr.job_startTS") && // only states beginning after job start and ending before
          col("cpot.cluster_endTS") < col("jr.job_endTS")
      )
      .drop(col("cpot.cluster_id")).drop(col("cpot.organization_id"))
      .join(jobRunInitialState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti")
      .join(jobRunTerminalState.select(stateLifecycleKeys map col: _*), stateLifecycleKeys, "leftanti")
      .withColumn("job_runtime_in_state", (col("cluster_endTS") - col("cluster_startTS")) / lit(1000) / 3600)
      .withColumn("lifecycleState", lit("intermediate"))

    val concState = Window.partitionBy(col("cluster_id"), col("job_startTS"), col("job_endTS"))
    val jobRunWPotential = jobRunInitialState
      .unionByName(jobRunIntermediateStates)
      .unionByName(jobRunTerminalState)
      .withColumn("run_state_utilization", col("job_runtime_in_state") / sum(col("job_runtime_in_state")).over(concState)) // given n hours of uptime in state 1/n of said uptime was used by this job job
      .withColumn("job_worker_potential_core_H_recalc", col("job_worker_potential_core_H") * col("run_state_utilization"))

    val jobRunCostPotential = jobRunWPotential
      .groupBy(jrcpAggKeys: _*)
      .agg(
        sum(lit(1)).alias("cluster_run_states"),
        greatest(round(sum(col("job_worker_potential_core_H_recalc")), 6), lit(0)).alias("worker_potential_core_H"),
        greatest(max(col("job_worker_potential_core_H")), lit(0)).alias("original_potential_core_H")
      )

    // jrcpWorkerPotentialInvalid
    val jrcpWorkerPotentialInvalidCol = when(abs(col("worker_potential_core_H") - col("original_potential_core_H")) > 0.000002, lit(0)).otherwise(lit(1))

    // define rules
    val jobRunCostPotentialFactRules  = Array(
      Rule("jrcpWorkerPotentialInvalid", jrcpWorkerPotentialInvalidCol, Array(1))
    )

    val validationReport = RuleSet(jobRunCostPotential)
      .add(jobRunCostPotentialFactRules) // case based tests - array
      .validate()

    validationReport.summaryReport
  }
}
