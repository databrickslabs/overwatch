package com.databricks.labs.overwatch.env

import com.databricks.backend.common.rpc.CommandContext
import com.databricks.labs.overwatch.utils.GlobalStructures._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.DBWrapper
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Workspace(
                 ctx: CommandContext,
                 url: String,
                 token: String
               ) extends SparkSessionWrapper {

  import spark.implicits._
  val dbWrapper: DBWrapper = new DBWrapper

  def getJobs: DataFrame = {
    dbWrapper

    val jobsEndpoint = "jobs/list"

    spark.read.json(Seq(dbWrapper.executeGet(jobsEndpoint)).toDS).toDF
      .withColumn("data", explode('jobs))
      .drop("jobs")

  }
  def getClusters = ???

}

object Workspace {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val ctx: CommandContext = dbutils.notebook.getContext()

  private def initUrl: String = {

    try {
      ctx.apiUrl.get
    } catch {
      case e: Throwable => {
        val e = new Exception("Cannot Acquire Databricks URL")
        logger.log(Level.FATAL, e.getStackTrace.toString)
        throw e
      }
    }
  }

  private def initToken(tokenSecret: Option[TokenSecret]): String = {
    if (tokenSecret.nonEmpty &&
      tokenSecret.get.key.nonEmpty &&
      tokenSecret.get.scope.nonEmpty) {
      try {
        val scope = tokenSecret.get.scope.get
        val key = tokenSecret.get.key.get
        dbutils.secrets.get(scope, key)
      } catch {
        case e: Throwable => logger.log(Level.FATAL, e); "NULL"
      }
    } else {
      try {
        val token = ctx.apiToken.get
        println("No valid scope provided, running with default credentials.")
        token
      } catch {
        case e: Throwable => logger.log(Level.FATAL, e); "NULL"
      }
    }
  }

  def apply(params: OverwatchParams): Workspace = {
    new Workspace(ctx, initUrl, initToken(params.tokenSecret))
  }

}
