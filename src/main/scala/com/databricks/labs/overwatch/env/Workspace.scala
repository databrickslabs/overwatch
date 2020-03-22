package com.databricks.labs.overwatch.env

import com.databricks.backend.common.rpc.CommandContext
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.DBWrapper
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.GlobalStructures._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Workspace(
                 url: String,
                 token: String
               ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._
//  lazy private val ctx: CommandContext = dbutils.notebook.getContext()
  val dbWrapper: DBWrapper = new DBWrapper

  def getJobs: DataFrame = {

    val jobsEndpoint = "jobs/list"

    spark.read.json(Seq(dbWrapper.executeGet(jobsEndpoint)).toDS).toDF
      .withColumn("data", explode('jobs))
      .drop("jobs")

  }
  def getClusters = ???

}

object Workspace {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private def initUrl: String = {

    try {
      if (System.getenv("OVERWATCH_ENV").nonEmpty) {
        System.getenv("OVERWATCH_ENV")
      } else { dbutils.notebook.getContext().apiUrl.get }
    } catch {
      case e: Throwable => {
        val e = new Exception("Cannot Acquire Databricks URL")
        logger.log(Level.FATAL, e.getStackTrace.toString)
        throw e
      }
    }
  }

  private def initToken(tokenSecret: Option[TokenSecret]): String = {
    if (tokenSecret.nonEmpty) {
      try {
        val scope = tokenSecret.get.scope
        val key = tokenSecret.get.key
        val token = if (scope == "LOCALTESTING" && key == "TOKEN") {
          System.getenv("OVERWATCH_TOKEN")
        } else { dbutils.secrets.get(scope, key) }
        val authMsg = s"Executing with token located in secret, $scope : $key"
        logger.log(Level.INFO, authMsg)
        token
      } catch {
        case e: Throwable => logger.log(Level.FATAL, e); "NULL"
      }
    } else {
      try {
        val token = dbutils.notebook.getContext().apiToken.get
        println("Token scope and key not provided, running with default credentials.")
        token
      } catch {
        case e: Throwable => logger.log(Level.FATAL, e); "NULL"
      }
    }
  }

  def apply(params: OverwatchParams): Workspace = {
    new Workspace(initUrl, initToken(params.tokenSecret))
  }

  def apply(): Workspace = { new Workspace(initUrl, initToken(None))}

}
