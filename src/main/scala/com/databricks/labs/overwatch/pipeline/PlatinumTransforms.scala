package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.WorkflowsTransforms._
import com.databricks.labs.overwatch.utils._
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

trait PlatinumTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  protected def buildClusterPlatinum(
                                    config: Config,
                                    clusterDF: DataFrame,
                                 )(clsfDF: DataFrame): DataFrame = {

    val clsf_raw = clsfDF

    val processed_runID_path = s"${config.etlDataPathPrefix}/platinum/cluster/runID"

    try {
      val clsf = if (Helpers.pathExists(processed_runID_path)) {
        val processed_ID = spark.read.format("delta").load(processed_runID_path).select("overwatch_runID").distinct().collect().map(x => x(0).toString).toList

        val clsf_raw_filtered = clsf_raw.filter(!'overwatch_runID.isin(processed_ID: _*))

        val current_dates = clsf_raw_filtered
          .filter("custom_tags not like '%SqlEndpointId%' AND unixTimeMS_state_end > unixTimeMS_state_start")
          .withColumn("date", explode(col("state_dates")))
          .select("date").distinct().collect().map(x => x(0).toString).toList

        val clsf_latest = clsf_raw_filtered
          .filter("custom_tags not like '%SqlEndpointId%' AND unixTimeMS_state_end > unixTimeMS_state_start")
          .withColumn("date", explode(col("state_dates")))

        // Rollback to Previous Entry of CLSF
        val clsf_old = clsf_raw
          .filter("custom_tags not like '%SqlEndpointId%' AND unixTimeMS_state_end > unixTimeMS_state_start")
          .filter('overwatch_runID.isin(processed_ID: _*))
          .withColumn("date", explode(col("state_dates")))
          .filter('date.isin(current_dates: _*))

        clsf_old.union(clsf_latest)
          .select(
            "cluster_id",
            "organization_id",
            "workspace_name",
            "cluster_name",
            "state_dates",
            "total_DBU_cost",
            "total_dbus",
            "total_compute_cost",
            "days_in_state",
            "custom_tags",
            "isAutomated",
            "date",
            "overwatch_runID"
          )
          .withColumn("dbu_cost", col("total_DBU_cost") / col("days_in_state"))
          .withColumn("total_dbu_spend", col("total_dbus") / col("days_in_state"))
          .withColumn("compute_cost", col("total_compute_cost") / col("days_in_state"))

      } else {
        clsf_raw
          .filter("custom_tags not like '%SqlEndpointId%' AND unixTimeMS_state_end > unixTimeMS_state_start")
          .select(
            "cluster_id",
            "organization_id",
            "workspace_name",
            "cluster_name",
            "state_dates",
            "total_DBU_cost",
            "total_dbus",
            "total_compute_cost",
            "days_in_state",
            "custom_tags",
            "isAutomated",
            "overwatch_runID"
          )
          .withColumn("date", explode(col("state_dates")))
          .withColumn("dbu_cost", col("total_DBU_cost") / col("days_in_state"))
          .withColumn("total_dbu_spend", col("total_dbus") / col("days_in_state"))
          .withColumn("compute_cost", col("total_compute_cost") / col("days_in_state"))
      }

      if (clsf.isEmpty) {
        throw new NoNewDataException("No New Data", Level.WARN, true)
      }

      val cluster = clusterDF.select(
        "organization_id",
        "cluster_id",
        "date",
        "cluster_type",
        "is_automated",
        "auto_termination_minutes"
      )

      val clsf_master = clsf
        .join(cluster, Seq("cluster_id", "organization_id"), "inner")
        .withColumn("cluster_category", expr(
          """case
            | when isAutomated = 'true' and cluster_type not in ('Serverless','SQL Analytics','Single Node') then 'Automated'
            | when cluster_name like 'dlt%' and cluster_type = 'Standard' then 'Standard'
            | when isAutomated = 'false' and cluster_type not in ('Serverless','SQL Analytics','Single Node') then 'Interactive'
            | when (isAutomated = 'false' or isAutomated = 'true' or isAutomated is null) and cluster_type = 'SQL Analytics' then 'Warehouse'
            | when (isAutomated = 'false' or isAutomated = 'true' or isAutomated is null) and cluster_type = 'Serverless' then 'High-Concurrency'
            | when (isAutomated = 'false' or isAutomated = 'true' or isAutomated is null) and cluster_type = 'Single Node' then 'Single Node'
            | else 'Unidentified'
            | end""".stripMargin
        ))
        .select(
          "cluster_id",
          "cluster_category"
        ).distinct()

      val cluster_agg = clsf
        .groupBy("date", "organization_id", "workspace_name", "cluster_id", "cluster_name")
        .agg(
          sum("total_dbu_spend").alias("total_dbus"),
          sum("dbu_cost").alias("total_dbu_cost_USD"), // Fixed column name
          sum("compute_cost").alias("total_compute_cost_USD") // Fixed column name
        )
        .orderBy(col("organization_id"), col("date").desc)
        .cache()

      val cluster_cost_details = clsf_master
        .join(cluster_agg, Seq("cluster_id"), "inner")
      clsf.select("overwatch_runID").distinct().write.format("delta").mode("append").save(processed_runID_path)
      cluster_cost_details
    }
  catch
  {
    case e: Throwable =>
      val appendTrackerErrorMsg = s"cluster_platinum table operation is failed" + e.getMessage
      logger.log(Level.ERROR, appendTrackerErrorMsg, e)
      println(appendTrackerErrorMsg, e)
      throw e
  }

  }
}
