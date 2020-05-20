package com.databricks.labs.overwatch

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.temporal.ChronoUnit

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Pipeline, Silver}
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BatchRunner extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    envInit()

    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.11\\overwatch_2.11-0.1.jar")
//    spark.sql("drop database if exists overwatch_local cascade")
//

    val workspace = if (args.length != 0) {
      Initializer(args)
    } else {
      Initializer(Array())
    }

    Bronze(workspace).run()
    Silver(workspace).run()

//    val config = workspace.getConfig
//    val fakeTime = LocalDateTime.of(2020,4,30,13,44).atZone(ZoneId.of("Etc/UTC"))
//        .toInstant.toEpochMilli
//    config.setPipelineSnapTime(fakeTime)


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


//    spark.read.table("overwatch.pipeline_report").show(20, false)


  }
}
