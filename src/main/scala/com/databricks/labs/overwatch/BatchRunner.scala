package com.databricks.labs.overwatch

import java.time.{LocalDateTime, ZoneId}

import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Silver}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BatchRunner extends SparkSessionWrapper{

  import spark.implicits._
  private val logger: Logger = Logger.getLogger(this.getClass)

  private def setGlobalDeltaOverrides(): Unit = {
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * 128)
  }

  def main(args: Array[String]): Unit = {
    envInit()
    setGlobalDeltaOverrides()

//    JARS for databricks remote
    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.11\\overwatch_2.11-0.2.jar")
//    spark.sql("drop database if exists overwatch_local cascade")

    val workspace = if (args.length != 0) {
      Initializer(args)
    } else {
      Initializer(Array())
    }

//    logger.log(Level.INFO, "Starting Bronze")
    Bronze(workspace).run()

    logger.log(Level.INFO, "Starting Silver")
    Silver(workspace).run()


  }


}

