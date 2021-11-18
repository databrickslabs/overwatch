package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.pipeline.{Bronze, Gold, Initializer, Silver}
import com.databricks.labs.overwatch.utils.{BadConfigException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}

object BatchRunner extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private def setGlobalDeltaOverrides(): Unit = {
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * 128)
    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 128)
  }

  /**
   * if args length == 2, 0 = pipeline of bronze, silver, or gold and 1 = overwatch args
   * if args length == 1, overwatch args
   * if args length == 0, throw exception
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    envInit()
    setGlobalDeltaOverrides()

    //    JARS for databricks remote
    //    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.11\\overwatch_2.11-0.2.jar")
    //    spark.sql("drop database if exists overwatch_local cascade")

    val workspace = if (args.length == 2) {
      Initializer(args(1))
    } else if (args.length == 1) {
      Initializer(args(0))
    } else {
      val argsLength = args.length
      val errMsg = s"Main class requires 1 or 2 input arguments, received $argsLength arguments. Please review the " +
        s"docs to compose the input arguments appropriately."
      throw new BadConfigException(errMsg)
    }

    if (args.length == 2) {
      args(0).toLowerCase match {
        case "bronze" =>
          logger.log(Level.INFO, "Starting Bronze")
          Bronze(workspace).run()
        case "silver" =>
          logger.log(Level.INFO, "Starting Silver")
          Silver(workspace).run()
        case "gold" =>
          logger.log(Level.INFO, "Starting Gold")
          Gold(workspace).run()
        case _ =>
          val pipelineFlag = args(0).toLowerCase
          throw new BadConfigException(s"Pipeline Flag must be 'bronze', 'silver', or 'gold'. Received $pipelineFlag")
      }
    } else {
      val runNotifyMsg = "No specific pipeline flag received, running entire Overwatch Pipeline."
      if (workspace.getConfig.debugFlag) println(runNotifyMsg)
      logger.log(Level.INFO, runNotifyMsg)
      logger.log(Level.INFO, "Starting Bronze")
      Bronze(workspace).run()

      logger.log(Level.INFO, "Starting Silver")
      Silver(workspace).run()

      logger.log(Level.INFO, "Starting Gold")
      Gold(workspace).run()
    }


  }


}

