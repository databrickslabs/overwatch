package com.databricks.labs.overwatch

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Pipeline, Silver}
import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object BatchRunner extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {


    envInit()

//    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.11\\overwatch_2.11-0.1_wildlife.jar")
//    sc.addFile("C:\\Dev\\git\\Databricks--Overwatch\\src\\main\\resources\\ec2_details_tbl", true)
//    spark.sql("drop database if exists overwatch_local cascade")
//


    val workspace = if (args.length != 0) {
      Initializer(args)
    } else {
      Initializer(Array())
    }

    val config = workspace.getConfig
//    val fakeTime = LocalDateTime.of(2020,5,8,13,44).atZone(ZoneId.of("Etc/UTC"))
//      .toInstant.toEpochMilli
//    config.setPipelineSnapTime(fakeTime)

    logger.log(Level.INFO, "Starting Bronze")
    Bronze(workspace).run()
    if (config.isFirstRun) {
      spark.read.format("delta").load("/tmp/overwatch/aws_ec2_details_raw")
        .coalesce(1).write.format("delta").saveAsTable("overwatch.instanceDetails")
    }
    logger.log(Level.INFO, "Starting Silver")
    Silver(workspace).run()

//    Silver(workspace).run()

//    Bronze(workspace).run()
//    Silver(workspace).run()

//    val silver = Silver(workspace)
//    silver.appendNotebookSummaryProcess.process()

//    spark.table("overwatch_local.spark_events_bronze").printSchema
//    silver.appendJobRunsProcess.process()
//    silver.appendTasksProcess.process()


    //  //    TEST TABLE ACCESS
//    spark.table("overwatch_local.cluster_events_bronze")
//      .drop("details")
//      .groupBy('cluster_id, 'type, 'Pipeline_SnapTS)
//      .count()
//      .orderBy('count.desc)
//      .show(20, false)

//
//    spark.read.format("parquet")
//      .load("C:\\Dev\\git\\Databricks--Overwatch\\spark-warehouse\\overwatch.db\\cluster_events_bronze")
//      .show(20, false)


  }


}

