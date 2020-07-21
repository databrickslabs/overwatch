package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Schema extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val requiredSchemas: Map[Int, StructType] = Map(
    // JobStatus
    2010 -> StructType(Seq(
      StructField("serviceName", StringType, nullable = true),
      StructField("actionName", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("job_type", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("timeout_seconds", StringType, nullable = true),
      StructField("schedule", StringType, nullable = true),
      StructField("notebook_task", StringType, nullable = true),
      StructField("new_settings", StringType, nullable = true),
      StructField("existing_cluster_id", StringType, nullable = true),
      StructField("new_cluster", StringType, nullable = true),
      StructField("sessionId", StringType, nullable = true),
      StructField("requestId", StringType, nullable = true),
      StructField("userAgent", StringType, nullable = true),
      StructField("response",
        StructType(Seq(
          StructField("errorMessage", StringType, nullable = true),
          StructField("result", StringType, nullable = true),
          StructField("statusCode", LongType, nullable = true)
        )), nullable = true),
      StructField("sourceIPAddress", StringType, nullable = true),
      StructField("version", StringType, nullable = true)
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
    ))
  )

  def getPrefixedString(prefix: String, fieldName: String): String = {
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

  def getSchemaByModule(df: DataFrame, moduleId: Int): DataFrame = {

  }

}
