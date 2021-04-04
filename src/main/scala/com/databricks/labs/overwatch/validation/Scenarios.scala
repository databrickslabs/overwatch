package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures._
import com.databricks.labs.validation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


object Scenarios extends SparkSessionWrapper{

  /**
    * Validate Scenarios
    * @param dbName
    * @return
    */
  def validate(dbName : String): DataFrame = {
    validateJobruncostpotentialfact(dbName)
  }

  /**
    * Approach : Top-down testing to ensure sanity of calculated values. These rules operate in a single line and will surface potential errors in the calculation pipeline.

  Implemented Rules:

  ClusterUtilInvalid: If the job succeeds, we cannot have worker_potential_core_H < spark_task_runtime_H. Also means job_run_cluster_util cannot be > 1.
  ClusterUtilCalculation: Checks if spark_task_runtime_H / worker_potential_core_H = job_run_cluster_util, rounded to 4 decimals
  ClusterAvgShareInvalid: Checks if avg_cluster_share between 0 and 1.
  SparkTaskRuntimeCalculation: Checks if spark_task_runtime_H = spark_task_runtimeMS / 3600000, rounded to 4 decimals
  CostComputeCalculation: Checks if the driver and worker costs add correctly to the total: driver_compute_cost + worker_compute_cost = total_compute_cost, check absolute difference < 0.000002
  CostDBUCalculation: Checks if the driver and worker DBUs add correctly to the total: driver_dbu_cost + worker_dbu_cost = total_dbu_cost, , check absolute difference < 0.000002
  CostTotalCalculation: Checks if the DBU and compute costs add correctly to the total: total_compute_cost + total_dbu_cost = total_cost, , check absolute difference < 0.000002
  JobRunningDaysInvalid: Checks if list of running days is sane: date(startTS) = min(running_days) and date(endTS) = max(running_days)

    * @param dbName
    * @return
    */
  def validateJobruncostpotentialfact(dbName : String): DataFrame = {
    // case based tests (true/false)
    // convention: 0 = test failed, 1 = test passed

    // ClusterUtilInvalid
    val ClusterUtilInvalidCol = when(col("run_terminal_state") === "Succeeded" && col("worker_potential_core_H") < 'spark_task_runtime_H, lit(0)).otherwise(lit(1))

    // ClusterUtilCalculation
    val ClusterUtilCalculationCol = when(col("run_terminal_state") === "Succeeded" && round(col("spark_task_runtime_H") / 'worker_potential_core_H, 4) === 'job_run_cluster_util, lit(1)).otherwise(lit(0))

    // SparkTaskRuntimeCalculation
    val SparkTaskRuntimeCalculationCol = when(col("spark_task_runtime_H") === round(col("spark_task_runtimeMS") / 3600000, 4), lit(1)).otherwise(lit(0))

    // CostComputeCalculation
    val CostComputeCalculationCol = when(abs(col("driver_compute_cost") + 'worker_compute_cost - 'total_compute_cost) > 0.000002, lit(1)).otherwise(lit(0))

    // CostDBUCalculation
    val CostDBUCalculationCol = when(abs(col("driver_dbu_cost") + 'worker_dbu_cost - 'total_dbu_cost) > 0.000002, lit(1)).otherwise(lit(0))

    // CostTotalCalculation
    val CostTotalCalculationCol = when(abs(col("total_dbu_cost") + 'total_compute_cost - 'total_cost) > 0.000002, lit(1)).otherwise(lit(0))

    // JobRunningDaysInvalid
    val JobRunningDaysInvalidCol = when(to_date(col("job_runtime.startTS")) === array_min(col("running_days")) && to_date(col("job_runtime.endTS")) === array_max(col("running_days")), lit(1)).otherwise(lit(0))


    // define rules
    val jobRunCostPotentialFactRules  = Array(
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


    val df_jobRunCostPotentialFact = spark.table("overwatch_etl.jobruncostpotentialfact_gold")

    // execute
    val (rulesReport, passed) = RuleSet(df_jobRunCostPotentialFact)
      .add(jobRunCostPotentialFactRules) // case based tests - array
      .addMinMaxRules(ClusterAvgShareInvalidRule) // bounds based tests - individual
      .validate()

    rulesReport
  }
}
