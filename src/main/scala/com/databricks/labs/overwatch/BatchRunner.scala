package com.databricks.labs.overwatch

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Pipeline, Silver}
import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object BatchRunner extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  private def setGlobalDeltaOverrides(): Unit = {
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * 128)
  }

  def main(args: Array[String]): Unit = {

    envInit()
    setGlobalDeltaOverrides()

    val workspace = if (args.length != 0) {
      Initializer(args, debugFlag = true)
    } else {
      Initializer(Array())
    }

    val config = workspace.getConfig

    logger.log(Level.INFO, "Starting Bronze")
    Bronze(workspace).run()
    if (config.isFirstRun) {
      spark.read.format("delta").load("/tmp/overwatch/aws_ec2_details_raw")
        .coalesce(1).write.format("delta").saveAsTable("overwatch.instanceDetails")
    }
    logger.log(Level.INFO, "Starting Silver")
    Silver(workspace).run()

  }


}

