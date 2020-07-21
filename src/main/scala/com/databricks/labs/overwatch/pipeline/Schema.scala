package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Module, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Schema extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val requiredSchemas: Map[Int, StructType] = Map(
    // SparkJobs
    2006 -> StructType(Seq(
      StructField("Event", StringType, nullable = true),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("JobID", StringType, nullable = true),
      StructField("StageIDs", StringType, nullable = true),
      StructField("SubmissionTime", StringType, nullable = true),
      StructField("Pipeline_SnapTS", StringType, nullable = true),
      StructField("Downstream_Processed", StringType, nullable = true),
      StructField("filenameGroup", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("Properties",
        StructType(Seq(
          StructField("AppName", StringType, nullable = true),
          StructField("WorkspaceURL", StringType, nullable = true),
          StructField("JobGroupID", StringType, nullable = true),
          StructField("CloudProvider", StringType, nullable = true),
          StructField("ClusterDetails",
            StructType(Seq(
              StructField("ClusterSource", StringType, nullable = true),
              StructField("AutoTerminationMinutes", StringType, nullable = true),
              StructField("ClusterTags", StringType, nullable = true),
              StructField("ClusterAvailability", StringType, nullable = true),
              StructField("ClusterID", StringType, nullable = true),
              StructField("InstancePoolID", StringType, nullable = true),
              StructField("MaxWorkers", StringType, nullable = true),
              StructField("MinWorkers", StringType, nullable = true),
              StructField("Name", StringType, nullable = true),
              StructField("OwnerUserID", StringType, nullable = true),
              StructField("ScalingType", StringType, nullable = true),
              StructField("SpotBidPricePercent", StringType, nullable = true),
              StructField("TargetWorkers", StringType, nullable = true),
              StructField("ActualWorkers", StringType, nullable = true),
              StructField("ZoneID", StringType, nullable = true),
              StructField("Region", StringType, nullable = true),
              StructField("DriverNodeType", StringType, nullable = true),
              StructField("WorkerNodeType", StringType, nullable = true),
              StructField("SparkVersion", StringType, nullable = true),
              StructField("CluserAvailability", StringType, nullable = true)
            )), nullable = true),
          StructField("NotebookID", StringType, nullable = true),
          StructField("NotebookPath", StringType, nullable = true),
          StructField("SparkContextID", StringType, nullable = true),
          StructField("DriverHostIP", StringType, nullable = true),
          StructField("DriverMaxResults", StringType, nullable = true),
          StructField("ExecutorID", StringType, nullable = true),
          StructField("ExecutorMemory", StringType, nullable = true),
          StructField("ExecutionID", StringType, nullable = true),
          StructField("ExecutionParent", StringType, nullable = true),
          StructField("ShufflePartitions", StringType, nullable = true),
          StructField("UserEmail", StringType, nullable = true),
          StructField("UserID", StringType, nullable = true)
        )), nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
    )),
    // JobStatus
    2010 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("sessionId", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("requestParams",
        StructType(Seq(
          StructField("jobId", StringType, nullable = true),
          StructField("job_id", StringType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("job_type", StringType, nullable = true),
          StructField("jobTerminalState", StringType, nullable = true),
          StructField("jobTriggerType", StringType, nullable = true),
          StructField("jobTaskType", StringType, nullable = true),
          StructField("jobClusterType", StringType, nullable = true),
          StructField("timeout_seconds", StringType, nullable = true),
          StructField("schedule", StringType, nullable = true),
          StructField("notebook_task", StringType, nullable = true),
          StructField("new_settings", StringType, nullable = true),
          StructField("existing_cluster_id", StringType, nullable = true),
          StructField("new_cluster", StringType, nullable = true)
        )), nullable = true),
      StructField("response",
        StructType(Seq(
          StructField("errorMessage", StringType, nullable = true),
          StructField("result", StringType, nullable = true),
          StructField("statusCode", LongType, nullable = true)
        )), nullable = true),
      StructField("userIdentity",
        StructType(Seq(
          StructField("email", StringType, nullable = true)
        )), nullable = true)
    )),
    // JobRuns
    2011 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("sessionId", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("requestParams",
        StructType(Seq(
          StructField("jobId", StringType, nullable = true),
          StructField("job_id", StringType, nullable = true),
          StructField("runId", StringType, nullable = true),
          StructField("run_id", StringType, nullable = true),
          StructField("run_name", StringType, nullable = true),
          StructField("idInJob", StringType, nullable = true),
          StructField("job_type", StringType, nullable = true),
          StructField("jobTerminalState", StringType, nullable = true),
          StructField("jobTriggerType", StringType, nullable = true),
          StructField("jobTaskType", StringType, nullable = true),
          StructField("jobClusterType", StringType, nullable = true),
          StructField("timeout_seconds", StringType, nullable = true),
          StructField("schedule", StringType, nullable = true),
          StructField("notebook_task", StringType, nullable = true),
          StructField("new_settings", StringType, nullable = true),
          StructField("existing_cluster_id", StringType, nullable = true),
          StructField("new_cluster", StringType, nullable = true)
        )), nullable = true),
      StructField("response",
        StructType(Seq(
          StructField("errorMessage", StringType, nullable = true),
          StructField("result", StringType, nullable = true),
          StructField("statusCode", LongType, nullable = true)
        )), nullable = true),
      StructField("userIdentity",
        StructType(Seq(
          StructField("email", StringType, nullable = true)
        )), nullable = true)
    )),
    // ClusterSpec
    2014 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("requestParams",
        StructType(Seq(
          StructField("clusterId", StringType, nullable = true),
          StructField("cluster_id", StringType, nullable = true),
          StructField("clusterName", StringType, nullable = true),
          StructField("cluster_name", StringType, nullable = true),
          StructField("clusterState", StringType, nullable = true),
          StructField("driver_node_type_id", StringType, nullable = true),
          StructField("node_type_id", StringType, nullable = true),
          StructField("num_workers", StringType, nullable = true),
          StructField("autoscale", StringType, nullable = true),
          StructField("clusterWorkers", StringType, nullable = true),
          StructField("autotermination_minutes", StringType, nullable = true),
          StructField("enable_elastic_disk", StringType, nullable = true),
          StructField("start_cluster", StringType, nullable = true),
          StructField("clusterOwnerUserId", StringType, nullable = true),
          StructField("cluster_log_conf", StringType, nullable = true),
          StructField("init_scripts", StringType, nullable = true),
          StructField("custom_tags", StringType, nullable = true),
          StructField("cluster_source", StringType, nullable = true),
          StructField("spark_env_vars", StringType, nullable = true),
          StructField("spark_conf", StringType, nullable = true),
          StructField("acl_path_prefix", StringType, nullable = true),
          StructField("instance_pool_id", StringType, nullable = true),
          StructField("instance_pool_name", StringType, nullable = true),
          StructField("spark_version", StringType, nullable = true),
          StructField("cluster_creator", StringType, nullable = true),
          StructField("idempotency_token", StringType, nullable = true),
          StructField("organization_id", StringType, nullable = true),
          StructField("user_id", StringType, nullable = true),
          StructField("ssh_public_keys", StringType, nullable = true)
        )), nullable = true),
      StructField("userIdentity",
        StructType(Seq(
          StructField("email", StringType, nullable = true)
        )), nullable = true),
      StructField("response",
        StructType(Seq(
          StructField("errorMessage", StringType, nullable = true),
          StructField("result", StringType, nullable = true),
          StructField("statusCode", LongType, nullable = true)
        )), nullable = true)
    )),
    // Cluster Status
    2015 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("requestParams",
        StructType(Seq(
          StructField("clusterId", StringType, nullable = true),
          StructField("cluster_id", StringType, nullable = true),
          StructField("cluster_name", StringType, nullable = true),
          StructField("clusterName", StringType, nullable = true),
          StructField("clusterState", StringType, nullable = true),
          StructField("driver_node_type_id", StringType, nullable = true),
          StructField("node_type_id", StringType, nullable = true),
          StructField("num_workers", StringType, nullable = true),
          StructField("autoscale", StringType, nullable = true),
          StructField("clusterWorkers", StringType, nullable = true),
          StructField("autotermination_minutes", StringType, nullable = true),
          StructField("enable_elastic_disk", StringType, nullable = true),
          StructField("start_cluster", StringType, nullable = true),
          StructField("clusterOwnerUserId", StringType, nullable = true),
          StructField("cluster_log_conf", StringType, nullable = true),
          StructField("init_scripts", StringType, nullable = true),
          StructField("custom_tags", StringType, nullable = true),
          StructField("cluster_source", StringType, nullable = true),
          StructField("spark_env_vars", StringType, nullable = true),
          StructField("spark_conf", StringType, nullable = true),
          StructField("acl_path_prefix", StringType, nullable = true),
          StructField("instance_pool_id", StringType, nullable = true),
          StructField("instance_pool_name", StringType, nullable = true),
          StructField("spark_version", StringType, nullable = true),
          StructField("cluster_creator", StringType, nullable = true),
          StructField("idempotency_token", StringType, nullable = true),
          StructField("organization_id", StringType, nullable = true),
          StructField("user_id", StringType, nullable = true),
          StructField("ssh_public_keys", StringType, nullable = true)
        )), nullable = true),
      StructField("userIdentity",
        StructType(Seq(
          StructField("email", StringType, nullable = true)
        )), nullable = true),
      StructField("response",
        StructType(Seq(
          StructField("errorMessage", StringType, nullable = true),
          StructField("result", StringType, nullable = true),
          StructField("statusCode", LongType, nullable = true)
        )), nullable = true)
    )),
    // User Logins
    2016 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("requestParams",
        StructType(Seq(
          StructField("user", StringType, nullable = true),
          StructField("userName", StringType, nullable = true),
          StructField("user_name", StringType, nullable = true),
          StructField("userID", StringType, nullable = true),
          StructField("email", StringType, nullable = true)
        )), nullable = true),
      StructField("userIdentity",
        StructType(Seq(
          StructField("email", StringType, nullable = true)
        )), nullable = true)
    )),
    // Notebook Summary
    2018 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("requestParams",
        StructType(Seq(
          StructField("notebookId", StringType, nullable = true),
          StructField("notebookName", StringType, nullable = true),
          StructField("path", StringType, nullable = true),
          StructField("oldName", StringType, nullable = true),
          StructField("newName", StringType, nullable = true),
          StructField("oldPath", StringType, nullable = true),
          StructField("newPath", StringType, nullable = true),
          StructField("parentPath", StringType, nullable = true),
          StructField("clusterId", StringType, nullable = true)
        )), nullable = true),
      StructField("response",
        StructType(Seq(
          StructField("errorMessage", StringType, nullable = true),
          StructField("result", StringType, nullable = true),
          StructField("statusCode", LongType, nullable = true)
        )), nullable = true),
      StructField("userIdentity",
        StructType(Seq(
          StructField("email", StringType, nullable = true)
        )), nullable = true)
    ))
  )

  // TODO -- move this to schemaTools -- probably
  private def getPrefixedString(prefix: String, fieldName: String): String = {
    if (prefix == null) fieldName else s"${prefix}.${fieldName}"
  }

  def correctAndValidate(dfSchema: StructType, minimumSchema: StructType, prefix: String = null): Array[Column] = {

    logger.log(Level.DEBUG, s"Top Level DFSchema Fields: ${dfSchema.fieldNames.mkString(",")}")
    logger.log(Level.DEBUG, s"Top Level DFSchema Fields: ${dfSchema.fieldNames.mkString(",")}")
    // if in sourceDF but not in minimum required
    minimumSchema.fields.flatMap(requiredField => {
      // If df contains required field -- set it to null
      logger.log(Level.DEBUG, s"Required fieldName: ${requiredField.name}")
      if (dfSchema.fields.map(_.name.toLowerCase).contains(requiredField.name.toLowerCase)) {
        // If it does contain validate the type
        requiredField.dataType match {
          // If the required type is a struct and the source type is a struct -- recurse
          // if the required type is a struct and sourceDF has that column name but it's not a struct set
          //   entire struct column to null and send warning
          case dt: StructType =>
            logger.log(Level.DEBUG, s"Required FieldType: ${requiredField.dataType.typeName}")
            val matchedDFField = dfSchema.fields.filter(_.name.equalsIgnoreCase(requiredField.name)).head
            logger.log(Level.DEBUG, s"Matched Source Field: ${matchedDFField.name}")
            if (!matchedDFField.dataType.isInstanceOf[StructType]) {
              logger.log(Level.WARN, s"Required Field: ${requiredField.name} must be a struct. The " +
                s"source Dataframe contains this field but it's not originally a struct. This column will be type" +
                s"casted to the required type but any data originally in this field will be lost.")
              Array(lit(null).cast(dt).alias(requiredField.name))
            } else {
              val returnStruct = Array(
                struct(
                  correctAndValidate(
                    matchedDFField.dataType.asInstanceOf[StructType],
                    requiredField.dataType.asInstanceOf[StructType],
                    prefix = if (prefix == null) requiredField.name else s"${prefix}.${requiredField.name}"
                  ): _*
                ).alias(requiredField.name)
              )
              logger.log(Level.DEBUG, s"Struct Built and Returned: ${returnStruct.head.expr}")
              returnStruct
            }
          case _ =>
            val validatedCol = Array(col(getPrefixedString(prefix, requiredField.name)))
            logger.log(Level.DEBUG, s"Validated and Selected: ${validatedCol.head.expr}")
            validatedCol
        }
      } else {
        val createdNullCol = Array(lit(null).cast(requiredField.dataType).alias(requiredField.name))
        logger.log(Level.DEBUG, s"Creating NULL Column -- Source Dataframe was missing required value: " +
          s"${requiredField.name} as ${createdNullCol.head.expr}")
        createdNullCol
      }
    })
  }

  def verifyDF(df: DataFrame, module: Module): DataFrame = {
    val requiredSchema = requiredSchemas.get(module.moduleID)
    if (requiredSchema.nonEmpty) {
      df.select(
        correctAndValidate(df.schema, requiredSchema.get): _*
      )
    } else {
      logger.log(Level.WARN, s"Schema Validation has not been implemented for ${module.moduleName}." +
        s"Attempting without validation")
      df
    }
  }

}
