package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Pipeline}
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BatchRunner extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    import spark.implicits._
//    spark.sql("drop database if exists overwatch cascade")


    val workspace = if (args.length != 0) {
      Initializer(args)
    } else {
      Initializer(Array())
    }

    Bronze(workspace).run()

//    spark.read.table("overwatch.pipeline_report").show(20, false)


  }
}
