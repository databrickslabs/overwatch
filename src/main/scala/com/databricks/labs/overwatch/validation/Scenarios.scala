package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures._
import com.databricks.labs.validation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import org.apache.spark.sql.expressions.Window


object Scenarios extends SparkSessionWrapper{

  /**
    * Validate Scenarios
    * @param dbName
    * @return
    */
  def validate(dbName : String): DataFrame = {
    validateJobruncostpotentialfact(dbName).union(jrcpPipelineAggRules(dbName))
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

    // define input table
    val df_jobRunCostPotentialFact = spark.table("overwatch_etl.jobruncostpotentialfact_gold")
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


    // cluster state fact table
    // remove cluster filter later
    val df_clusterPotentialWCosts = spark.table("overwatch_etl.clusterstatefact_gold")
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

    // set timestamp in start/end and intermediate states
    // almost verbatim from goldtransformations

    val clusterPotentialInitialState = df_testDataCPWC.withColumn("timestamp", col("cluster_startTS"))
    val clusterPotentialIntermediateStates = df_testDataCPWC
    val clusterPotentialTerminalState = df_testDataCPWC.withColumn("timestamp", col("cluster_endTS"))

    val jobRunInitialState = df_testDataJRCP
      .withColumn("timestamp", col("job_startTS"))
      .toTSDF("timestamp", "organization_id", "cluster_id")
      .lookupWhen(
        clusterPotentialInitialState
          .toTSDF("timestamp", "organization_id", "cluster_id"),
        tsPartitionVal = 64, maxLookAhead = 1L
      ).df
      .drop("timestamp")
      .withColumn("job_runtime_in_state",
        when(col("state").isin("CREATING", "STARTING") || col("job_cluster_type") === "new", 'uptime_in_state_H) // get true cluster time when state is guaranteed fully initial
          .otherwise((array_min(array(col("cluster_endTS"), col("job_endTS"_)) - 'job_startTS) / lit(1000) / 3600))) // otherwise use jobStart as beginning time and min of stateEnd or jobEnd for end time
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

    val topClusters = df_testDataJRCP
      .filter(col("organization_id").isNotNull && col("cluster_id").isNotNull)
      .groupBy(col("organization_id"), col("cluster_id")).count
      .orderBy(col("count").desc).limit(40)
      .select(array(col("organization_id"), col("cluster_id"))).as[Seq[String]].collect.toSeq

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
      .withColumn("job_worker_potential_core_H_recalc", col("job_worker_potential_core_H") * 'run_state_utilization)

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

    val jobRunCostPotential = jobRunWPotential
      .groupBy(jrcpAggKeys: _*)
      .agg(
        sum(lit(1)).alias("cluster_run_states"),
        greatest(round(sum(col("job_worker_potential_core_H_recalc")), 6), lit(0)).alias("worker_potential_core_H"),
        greatest(max(col("job_worker_potential_core_H")), lit(0)).alias("original_potential_core_H")
      )

    // case based tests (true/false)
    // convention: 0 = test failed, 1 = test passed

    // jrcpWorkerPotentialInStateInvalid
    val jrcpWorkerPotentialInStateInvalidCol = when(col("job_worker_potential_core_H_recalc") > col("cluster_worker_potential_core_H"), lit(0)).otherwise(lit(1))

    // define rules
    val jobRunCostPotentialStateFactRules  = Array(
      Rule("jrcpWorkerPotentialInStateInvalid", jrcpWorkerPotentialInStateInvalidCol, Array(1)))

    val jrcpWorkerPotentialInStateInvalidRule = MinMaxRuleDef("jrcpRunStateUtilizationInvalid", col("run_state_utilization"), Bounds(0.0, 1.0))

    // execute, take df from dataprep above
    val (rulesReport, passed) = RuleSet(jobRunWPotential)
      .add(jobRunCostPotentialStateFactRules) // case based tests - array
      .addMinMaxRules(jrcpWorkerPotentialInStateInvalidRule) // bounds based tests - individual
      .validate()

    rulesReport

  }


}
