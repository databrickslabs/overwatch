package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Module, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * The purpose of this Object is to validate that AT LEAST the columns required for ETL are present. Furthermore,
 * if all values of a target struct are null when a schema is derived, the output type of the structure won't be a
 * structure at all but rather a simple string, thus in these cases, the required complex structure must be generated
 * with all nulls so that it can be inserted into the target. To do this, the necessary null structs are created so
 * that source DFs can match the target even when schema inference is used in the reader. The entry function is the
 * "verifyDF" function.
 */
object Schema extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Minimum required schema by module. "Minimum Requierd Schema" means that at least these columns of these types
   * must exist for the downstream ETLs to function.
   * The preceeding 2 in module ID 2XXX key integer maps to 2 being silver.
   * Silver Layer modules 2xxx
   * Gold Layer 3xxx
   */
  private val requiredSchemas: Map[Int, StructType] = Map(
    // SparkExecutors
    2003 -> StructType(Seq(
      StructField("Event", StringType, nullable = true),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("ExecutorID", StringType, nullable = true),
      StructField("RemovedReason", StringType, nullable = true),
      StructField("Timestamp", LongType, nullable = true),
      StructField("executorInfo",
        StructType(Seq(
          StructField("Host", StringType, nullable = true),
          StructField("LogUrls",
            StructType(Seq(
              StructField("stderr", StringType, nullable = true),
              StructField("stdout", StringType, nullable = true)
            )), nullable = true),
          StructField("Resources",
            StructType(Seq(
              StructField("gpu",
                StructType(Seq(
                  StructField("addresses", ArrayType(StringType), nullable = true),
                  StructField("name", StringType, nullable = true)
                )), nullable = true)
            )), nullable = true),
          StructField("TotalCores", LongType, nullable = true)
        )), nullable = true),
      StructField("filenameGroup",
        StructType(Seq(
          StructField("filename", StringType, nullable = true),
          StructField("byCluster", StringType, nullable = true),
          StructField("byDriverHost", StringType, nullable = true),
          StructField("bySparkContext", StringType, nullable = true)
        )), nullable = true)
    )),
    // SparkExecutions
    2005 -> StructType(Seq(
      StructField("Event", StringType, nullable = true),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("details", StringType, nullable = true),
      StructField("executionId", LongType, nullable = true),
      StructField("time", LongType, nullable = true),
      StructField("filenameGroup",
        StructType(Seq(
          StructField("filename", StringType, nullable = true),
          StructField("byCluster", StringType, nullable = true),
          StructField("byDriverHost", StringType, nullable = true),
          StructField("bySparkContext", StringType, nullable = true)
        )), nullable = true)
    )),
    // SparkJobs
    2006 -> StructType(Seq(
      StructField("Event", StringType, nullable = true),
      StructField("clusterId", StringType, nullable = true),
      StructField("SparkContextID", StringType, nullable = true),
      StructField("JobID", StringType, nullable = true),
      StructField("JobResult", StringType, nullable = true),
      StructField("CompletionTime", StringType, nullable = true),
      StructField("StageIDs", StringType, nullable = true),
      StructField("SubmissionTime", StringType, nullable = true),
      StructField("Pipeline_SnapTS", StringType, nullable = true),
      StructField("Downstream_Processed", StringType, nullable = true),
      StructField("filenameGroup", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("Properties", MapType(
        StringType, StringType, valueContainsNull = true
      )),
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
          StructField("new_cluster", StringType, nullable = true),
          StructField("resourceId", StringType, nullable = true),
          StructField("aclPermissionSet", StringType, nullable = true),
          StructField("grants", StringType, nullable = true),
          StructField("targetUserId", StringType, nullable = true)
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
          StructField("name", StringType, nullable = true),
          StructField("runId", StringType, nullable = true),
          StructField("run_id", StringType, nullable = true),
          StructField("run_name", StringType, nullable = true),
          StructField("idInJob", StringType, nullable = true),
          StructField("job_type", StringType, nullable = true),
          StructField("orgId", StringType, nullable = true),
          StructField("jobTerminalState", StringType, nullable = true),
          StructField("jobTriggerType", StringType, nullable = true),
          StructField("jobTaskType", StringType, nullable = true),
          StructField("jobClusterType", StringType, nullable = true),
          StructField("libraries", StringType, nullable = true),
          StructField("timeout_seconds", StringType, nullable = true),
          StructField("schedule", StringType, nullable = true),
          StructField("notebook_task", StringType, nullable = true),
          StructField("notebook_params", StringType, nullable = true),
          StructField("new_settings", StringType, nullable = true),
          StructField("existing_cluster_id", StringType, nullable = true),
          StructField("new_cluster", StringType, nullable = true),
          StructField("workflow_context", StringType, nullable = true),
          StructField("spark_python_task", StringType, nullable = true),
          StructField("spark_jar_task", StringType, nullable = true),
          StructField("shell_command_task", StringType, nullable = true)
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
  /**
   * In a nested struct this returns the entire dot delimited path to the current struct field.
   * @param prefix All higher-level structs that supercede the current field
   * @param fieldName Name of current field
   * @return
   */
  private def getPrefixedString(prefix: String, fieldName: String): String = {
    if (prefix == null) fieldName else s"${prefix}.${fieldName}"
  }

  /**
   * The meat of this Object.
   * Validates the required columns are present in the source DF and when NOT, and IF STRUCTTYPE, creates the structure
   * with all nulls to allow downstream ETL to continue processing with the missing/nulled strings created from
   * inferred schema df readers
   * @param dfSchema schema from source df to compare with the minimum required schema
   * @param minimumSchema Required minimum schema
   * @param prefix if drilling into/through structs this will be set to track the depth map
   * @return
   */
  private def correctAndValidate(dfSchema: StructType, minimumSchema: StructType, prefix: String = null): Array[Column] = {

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

  /**
   * Public facing function used to validate and correct a DF before the ETL stage.
   * @param df DF to validate
   * @param module Module for which the DF is the source
   * @return
   */
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
