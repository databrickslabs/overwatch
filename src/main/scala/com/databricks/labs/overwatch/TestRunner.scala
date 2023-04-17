package com.databricks.labs.overwatch

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.Initializer
import com.databricks.labs.overwatch.utils.{MultiWSDeploymentReport, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}

import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object TestRunner extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private def getOrgId1(ec: ExecutionContextExecutor): Future[Map[Long, String]] = {
    try {
      val fs = Future {
        val threadId = Thread.currentThread().getId
        val t = dbutils.notebook.getContext.tags("orgId")
        val apiURL = dbutils.notebook.getContext.apiUrl.getOrElse("NONE APIURL")
        val orgId = apiURL.split("\\.")(0).split("/").lastOption.getOrElse("NONE ORGID")
        val response = s"ORGID TAG --> $t | APIURL --> $apiURL | ORGID --> $orgId"
        logger.log(Level.ERROR, s"GETORGID1: TEST RESULT: $response")
        Map[Long, String](threadId -> response)
      }(ec)
      fs
    } catch {
      case e: Throwable =>
        println(s"FAILEd getOrgId1 -- ", e)
        throw new Exception("FAILEd getOrgId1")
    }
  }

  private def getOrgId2(ec: ExecutionContextExecutor): Future[Map[Long, String]] = {
    try {
      val fs = Future {
        val threadId = Thread.currentThread().getId
        val t = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
        val apiURL = dbutils.notebook.getContext.apiUrl.getOrElse("NONE APIURL")
        val orgId = apiURL.split("\\.")(0).split("/").lastOption.getOrElse("NONE ORGID")
        val response = s"ORGID TAG --> $t | APIURL --> $apiURL | ORGID --> $orgId"
        logger.log(Level.ERROR, s"GETORGID2: TEST RESULT: $response")
        Map[Long, String](threadId -> response)
      }(ec)
      fs
    } catch {
      case e: Throwable =>
        println(s"FAILEd getOrgId2 -- ", e)
        throw new Exception("FAILEd getOrgId2")
    }
  }

  private def getOrgId3: String = {
    try {
      val clusterOwnerOrgID = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
      if (clusterOwnerOrgID == " " || clusterOwnerOrgID == "0") {
        val t = dbutils.notebook.getContext.apiUrl.get.split("\\.")(0).split("/").lastOption.getOrElse("NONE")
        logger.log(Level.ERROR, s"GETORGID3: TEST RESULT: $t")
        t
      } else clusterOwnerOrgID
    } catch {
      case e: Throwable =>
        println(s"FAILEd getOrgId3 -- ", e)
        "FAILED"
    }
  }

  private def getOrgId4: String = {
    try {
      if (dbutils.notebook.getContext.tags("orgId") == "0") {
        val t = dbutils.notebook.getContext.apiUrl.get.split("\\.")(0).split("/").lastOption.getOrElse("NONE")
        logger.log(Level.ERROR, s"GETORGID4: TEST RESULT: $t")
        t
      } else dbutils.notebook.getContext.tags("orgId")
    } catch {
      case e: Throwable =>
        println(s"FAILEd getOrgId4 -- ", e)
        "FAILED"
    }
  }

  private def getOrgId5(ec: ExecutionContextExecutor, apiUrl: Option[String]): Future[Map[Long, String]] = {
    try {
      val fs = Future {
        val threadId = Thread.currentThread().getId
        val orgId = Initializer.getOrgId(apiUrl)
        val response = s"APIURL --> ${apiUrl.getOrElse("NONE APIRURL")} | ORGID --> $orgId"
        logger.log(Level.ERROR, s"GETORGID1: TEST RESULT: $response")
        Map[Long, String](threadId -> response)
      }(ec)
      fs
    } catch {
      case e: Throwable =>
        println(s"FAILEd getOrgId5 -- ", e)
        throw new Exception("FAILEd getOrgId5")
    }
  }

  def main(args: Array[String]): Unit = {
    val threads = Range.inclusive(0, getNumberOfWorkerNodes).toArray
    val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(getNumberOfWorkerNodes))
    args(0) match {
      case "test1" =>
        val s = threads.map(_ => getOrgId1(ec))
          .flatMap(r => {
            Await.result(r, Duration.Inf)
          })
          .map(r => {
            s"Result: ${r._2} -- THREAD: ${r._1}"
          }).mkString("\n")
        println(s)
      case "test2" =>
        val s = threads.map(_ => getOrgId2(ec))
          .flatMap(r => {
            Await.result(r, Duration.Inf)
          })
          .map(r => {
            s"Result: ${r._2} -- THREAD: ${r._1}"
          }).mkString("\n")
        println(s)
      case "test3" => println(getOrgId3)
      case "test4" => println(getOrgId4)
      case "test5" =>
        val s = threads.map(_ => getOrgId5(ec, None))
          .flatMap(r => {
            Await.result(r, Duration.Inf)
          })
          .map(r => {
            s"Result: ${r._2} -- THREAD: ${r._1}"
          }).mkString("\n")
        println(s)
      case "test6" =>
        val apiURL = args(1)
        val s = threads.map(_ => getOrgId5(ec, Some(apiURL)))
          .flatMap(r => {
            Await.result(r, Duration.Inf)
          })
          .map(r => {
            s"Result: ${r._2} -- THREAD: ${r._1}"
          }).mkString("\n")
        println(s)
    }
  }


}
