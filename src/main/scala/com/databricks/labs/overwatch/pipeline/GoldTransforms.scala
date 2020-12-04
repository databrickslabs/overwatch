package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

trait GoldTransforms extends SparkSessionWrapper{

  import spark.implicits._

  protected def buildCluster()(df: DataFrame): DataFrame = {
    val clusterCols: Array[Column] = Array(
      'cluster_id,
      'actionName.alias("action"),
      'timestamp.alias("unixTimeMS"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").alias("timestamp"),
      from_unixtime('timestamp.cast("double") / 1000).cast("timestamp").cast("date").alias("date"),
      'cluster_name,
      'driver_node_type_id.alias("driver_node_type"),
      'node_type_id.alias("node_type"),
      'num_workers,
      'autoscale,
      'autoTermination_minutes.alias("auto_termination_minutes"),
      'enable_elastic_disk,
      'cluster_log_conf,
      'init_scripts,
      'custom_tags,
      'cluster_source,
      'spark_env_vars,
      'spark_conf,
      'acl_path_prefix,
      'instance_pool_id,
      'spark_version,
      'idempotency_token,
      'organization_id,
      'deleted_by,
      'createdBy.alias("created_by"),
      'lastEditedBy.alias("last_edited_by"),
      'Pipeline_SnapTS,
      'Overwatch_RunID
    )
    df.select(clusterCols: _*)
  }
  protected val clusterViewColumnMapping: String =
    """
      |cluster_id, action, unixTimeMS, timestamp, date, cluster_name, driver_node_type, node_type, num_workers,
      |autoscale, auto_termination_minutes, enable_elastic_disk, cluster_log_conf, init_scripts, custom_tags,
      |cluster_source, spark_env_vars, spark_conf, acl_path_prefix, instance_pool_id, spark_version,
      |idempotency_token, organization_id, deleted_by, created_by, last_edited_by, Pipeline_SnapTS, Overwatch_RunID
      |""".stripMargin
}
