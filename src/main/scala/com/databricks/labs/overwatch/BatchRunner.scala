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

  private def loadLocalResource(path: String): DataFrame = {
    val fileLocation = getClass.getResourceAsStream("/AWS_Instance_Details.csv")
    val source = scala.io.Source.fromInputStream(fileLocation).mkString
    val csvData = spark.sparkContext.parallelize(source.stripMargin.lines.toList).toDS()
    spark.read.option("header", true).option("inferSchema",true).csv(csvData).coalesce(1)
  }

  def main(args: Array[String]): Unit = {
    envInit()
    setGlobalDeltaOverrides()

//    JARS for databricks remote
//    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.11\\overwatch_2.11-0.1_wildlife.jar")
//    spark.sql("drop database if exists overwatch_local cascade")

    val workspace = if (args.length != 0) {
      Initializer(args)
    } else {
      Initializer(Array())
    }

    val config = workspace.getConfig

    logger.log(Level.INFO, "Starting Bronze")
    Bronze(workspace).run()
    if (config.isFirstRun) {
      val instanceDetailsDF = config.cloudProvider match {
        case "aws" =>  loadLocalResource("/AWS_Instance_Details.csv")
        case "azure" =>  loadLocalResource("/Azure_Instance_Details.csv")
        case _ => throw(new IllegalArgumentException("Overwatch only supports cloud providers, AWS and Azure."))
      }

      instanceDetailsDF
        .write.format("delta")
        .saveAsTable(s"${config.databaseName}.instanceDetails")
    }
    logger.log(Level.INFO, "Starting Silver")
    Silver(workspace).run()


  }


}

