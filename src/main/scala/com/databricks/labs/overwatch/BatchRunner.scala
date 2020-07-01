package com.databricks.labs.overwatch

import java.time.{LocalDateTime, ZoneId, ZoneOffset}

import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Pipeline, Silver, SilverTransforms}
import com.databricks.labs.overwatch.utils.{Config, OverwatchParams, SchemaTools, SparkSessionWrapper}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object BatchRunner extends SparkSessionWrapper with SilverTransforms{

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._
  private def setGlobalDeltaOverrides(): Unit = {
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * 128)
  }

  def main(args: Array[String]): Unit = {

    envInit("ERROR")

////    def sanitizeSchema(dataType: DataType): DataType = {
////      dataType match {
////        case dt: StructType =>
////          val dtStruct = dt.asInstanceOf[StructType]
////          dtStruct.copy(fields = generateUniques(dtStruct.fields.map(sanitizeFields)))
////        case dt: ArrayType =>
////          val dtArray = dt.asInstanceOf[ArrayType]
////          dtArray.copy(elementType = sanitizeSchema(dtArray.elementType))
////        case _ => dataType
////      }
////    }
//
//
//    def getChildDetail(child: Array[StructField]): Column = {
//      simplifySparkPlan(child.filter(_.name == "children").head.dataType)
//      struct(
//        col(child.filter(_.name == "estRowCount").map(_.name).head),
//        col(child.filter(_.name == "metadata").map(_.name).head)
//      )
//    }
//
//    def simplifySparkPlan(dataType: DataType): Column = {
//      dataType match {
//        case dt: StructType =>
//          val dtStruct = dt.asInstanceOf[StructType]
//          getChildDetail(dtStruct.fields)
//        case dt: ArrayType =>
//          simplifySparkPlan(dt.elementType)
//      }
//    }
//
//    def getChildDetail(df: StructType): Array[Column] = {
//
//      df.fields.filter(f => f.name == "children").map(f => {
//        val childCol = col(f.name)
//        f.dataType match {
//          case dt: ArrayType => {
//            val childrenCount = size(childCol)
//
//            dt.elementType match {
//              case et: StructType => {
//                when(size(childCol(0)) > 0,
//                  getChildDetail(et)
//                struct(
//                  c.getField("estRowCount"),
//                  c.getField("metadata")
//                ).alias("childDetail")
//              }
//              case _ => {
//                struct(
//                  c.getField("estRowCount"),
//                  c.getField("metadata")
//                ).alias("childDetail")
//              }
//            }
//          }
//        }
//      })
//    }
//
//    def simplifySparkPlan(schema: StructType): Array[Column] = {
//      val sparkPlan = schema.fields.filter(_.name == "sparkPlanInfo").head
//      getChildDetail(sparkPlan.asInstanceOf[StructType])
//    }
//
//    val df = spark.read.option("badRecordsPath", "badrecords")
//      .json("C:\\Users\\tomesd\\Downloads\\events")
//      .drop("Classpath Entries", "Spark Properties", "System Properties")
//      .withColumn("filename", input_file_name)
//      .withColumn("pathSize", size(split('filename, "/")))
//      .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
//      .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
//      .drop("pathSize", "Stage ID")
//
//    val executionsStart = df.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")
//      .select( 'clusterId, 'SparkContextID, 'description, 'details, 'executionId.alias("ExecutionID"), 'sparkPlanInfo,
//        'time.alias("SqlExecStartTime"))
//
//    val executionsEnd = df.filter('Event === "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")
//      .select('clusterId, 'SparkContextID, 'executionId.alias("ExecutionID"),
//        'time.alias("SqlExecEndTime"))
//
//    val x = executionsStart
//      .join(executionsEnd, Seq("clusterId", "SparkContextID", "ExecutionID"))
//
//    val y = x
//      .withColumn("simpleSparkPlan",
//        array(getChildDetail(x.schema, 'sparkPlanInfo): _*))
//
//    try {
//      y.select('simpleSparkPlan).show(5,false)
//    } catch {
//      case e: Throwable => println("ERROR", e)
//    }


    envInit()
//    setGlobalDeltaOverrides()

//    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.11\\overwatch_2.11-0.1.jar")
//    sc.addFile("C:\\Dev\\git\\Databricks--Overwatch\\src\\main\\resources\\ec2_details_tbl", true)
//    spark.sql("drop database if exists overwatch_local cascade")
//
//    val workspace = if (args.length != 0) {
//      Initializer(args)
//    } else {
//      Initializer(Array())
//    }


//    val config = workspace.getConfig
//    if (config.debugFlag) envInit("DEBUG")
//    val fakeTime = LocalDateTime.of(2020,6,8,13,44).atZone(ZoneId.of("Etc/UTC"))
//      .toInstant.toEpochMilli
//    config.setPipelineSnapTime(fakeTime)

//    logger.log(Level.INFO, "Starting Bronze")
//    Bronze(workspace).run()
//    if (config.isFirstRun) {
//      spark.read.format("delta").load("/tmp/overwatch/aws_ec2_details_raw")
//        .coalesce(1).write.format("delta")
//        .saveAsTable(s"${config.databaseName}.instanceDetails")
//    }
//    logger.log(Level.INFO, "Starting Silver")
//    Silver(workspace).run()

//    Silver(workspace).run()

//    Bronze(workspace).run()
//    Silver(workspace).run()


  }


}

