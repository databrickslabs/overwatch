package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.PipelineFunctions.fillForward
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object DbsqlTransforms extends SparkSessionWrapper {

  import spark.implicits._
  private val logger: Logger = Logger.getLogger(this.getClass)
  val responseSuccessFilter: Column = $"response.statusCode" === 200

  /**
   * BEGIN DBSQL generic functions
   */

  def deriveWarehouseId(): Column = {
    when(('actionName === "createEndpoint" || 'actionName === "createWarehouse"),
      get_json_object($"response.result", "$.id"))
      .otherwise('id)
  }

  def deriveWarehouseBase()(auditRawDF: DataFrame): DataFrame = {
    val warehouse_name_gen_w = Window.partitionBy('organization_id, 'warehouse_id)
      .orderBy('timestamp).rowsBetween(Window.unboundedPreceding, 1000)

    val warehouseRaw = auditRawDF
      .select(
        'timestamp,
        'date,
        'serviceName,
        'actionName,
        'userEmail,
        'requestId,
        'response,
        'warehouse_id,
        PipelineFunctions.fillForward("warehouse_name",warehouse_name_gen_w),
        PipelineFunctions.fillForward("cluster_size",warehouse_name_gen_w),
        PipelineFunctions.fillForward("min_num_clusters",warehouse_name_gen_w),
        PipelineFunctions.fillForward("max_num_clusters",warehouse_name_gen_w),
        PipelineFunctions.fillForward("auto_stop_mins",warehouse_name_gen_w),
        PipelineFunctions.fillForward("spot_instance_policy",warehouse_name_gen_w),
        PipelineFunctions.fillForward("enable_photon",warehouse_name_gen_w),
        PipelineFunctions.fillForward("channel",warehouse_name_gen_w),
        PipelineFunctions.fillForward("tags",warehouse_name_gen_w),
        PipelineFunctions.fillForward("enable_serverless_compute",warehouse_name_gen_w),
        PipelineFunctions.fillForward("warehouse_type",warehouse_name_gen_w)
      )


    val warehouseWithStructs = warehouseRaw
      .withColumn("tags", SchemaTools.structFromJson(spark, warehouseRaw, "tags"))
      .scrubSchema

    warehouseWithStructs
      .withColumn("tags", SchemaTools.structToMap(warehouseWithStructs, "tags"))
  }

  /**
   * warehouseBaseFilled - if first run, baseline warehouse spec for existing warehouses that haven't been edited since
   * commencement of audit logs. Allows for joins directly to gold warehouse work even if they haven't yet been edited.
   * Several of the fields are unavailable through this method but many are and they are very valuable when
   * present in gold
   */
  def deriveWarehouseBaseFilled(isFirstRun: Boolean, bronzeWarehouseSnapUntilCurrent: DataFrame)
                                (warehouseBaseWMetaDF: DataFrame): DataFrame = {
    if (isFirstRun) {
      val firstRunMsg = "Silver_WarehouseSpec -- First run detected, will impute warehouse state from bronze to derive " +
        "current initial state for all existing warehouses."
      logger.log(Level.INFO, firstRunMsg)
      println(firstRunMsg)
      val missingWarehouseIds = bronzeWarehouseSnapUntilCurrent.select('organization_id,
        'warehouse_id).distinct
        .join(
          warehouseBaseWMetaDF
            .select('organization_id, 'warehouse_id).distinct,
          Seq("organization_id", "warehouse_id"), "anti"
        )
      val latestWarehouseSnapW = Window.partitionBy('organization_id, 'warehouse_id).orderBy('Pipeline_SnapTS.desc)

      val missingWareHouseBaseFromSnap = bronzeWarehouseSnapUntilCurrent
        .join(missingWarehouseIds, Seq("organization_id", "warehouse_id"))
        .withColumn("rnk", rank().over(latestWarehouseSnapW))
        .filter('rnk === 1).drop("rnk")
        .select(
          'organization_id,
          'warehouse_id,
          lit("warehouses").alias("serviceName"),
          lit("snapImpute").alias("actionName"),
          'name.alias("warehouse_name"),
          'state.alias("warehouse_state"),
          'size,
          'cluster_size,
          'min_num_clusters,
          'max_num_clusters,
          'auto_stop_mins,
          'auto_resume,
          'creator_id,
          'spot_instance_policy,
          'enable_photon,
          'channel,
          'tags,
          'enable_serverless_compute,
          'warehouse_type,
          'num_clusters,
          'num_active_sessions,
          'jdbc_url,
          'odbc_params,
          (unix_timestamp('Pipeline_SnapTS) * 1000).alias("timestamp"),
          'Pipeline_SnapTS.cast("date").alias("date"),
          'creator_name.alias("createdBy")
        ).unionByName(warehouseBaseWMetaDF, allowMissingColumns = true)
      unionWithMissingAsNull(warehouseBaseWMetaDF, missingWareHouseBaseFromSnap)
    } else warehouseBaseWMetaDF
  }

  def deriveInputForWarehouseBase(auditLogDf: DataFrame, warehouseSpecSilver: PipelineTable
                                  , auditBaseCols: Array[Column]) : DataFrame = {

    val warehouseSummaryCols = auditBaseCols ++ Array[Column](
      deriveWarehouseId.alias("warehouse_id"),
      'name.alias("warehouse_name"),
      'cluster_size,
      'min_num_clusters,
      'max_num_clusters,
      'auto_stop_mins,
      'spot_instance_policy,
      'enable_photon,
      get_json_object('channel, "$.name").alias("channel"),
      'tags,
      'enable_serverless_compute,
      'warehouse_type
    )

    val filteredAuditLogDf = auditLogDf
      .filter('actionName.isin("createEndpoint", "editEndpoint", "createWarehouse",
        "editWarehouse", "deleteEndpoint", "deleteWarehouse")
        && responseSuccessFilter
        && 'serviceName === "databrickssql")
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .select(warehouseSummaryCols: _*)


    val filteredDf = if(warehouseSpecSilver.exists(dataValidation = true)) {
      filteredAuditLogDf
        .unionByName(warehouseSpecSilver.asDF
          .select(warehouseSummaryCols: _*))
    }
    else
      filteredAuditLogDf

    filteredDf
  }

}
