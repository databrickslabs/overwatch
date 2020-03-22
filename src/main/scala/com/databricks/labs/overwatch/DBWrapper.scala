package com.databricks.labs.overwatch

import com.fasterxml.jackson.databind.ObjectMapper

import scala.sys.process._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class DBWrapper extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var curlCommand: String = _

  def getCurlCommand: String = curlCommand

  def executeGet(apiName: String, query: String = ""): String = {
    try {
      val queryString = if (query != "") s"?$query" else ""
      val call = if (System.getenv("OVERWATCH") != "LOCAL") {
        val ctx = dbutils.notebook.getContext
        val url = ctx.apiUrl.get
        val token = ctx.apiToken.get
        val req = s"${url}/api/2.0/${apiName}"
        val call = Seq("curl", "-H", s"Authentication: Bearer ${token}", "-s", s"${req}${queryString}")
        curlCommand = call.map(p => if (p.contains("Authentication")) "REDACTED" else p).mkString(" ")
        logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
        call
      } else {
        val url = System.getenv("OVERWATCH_ENV")
        val token = System.getenv("OVERWATCH_TOKEN")
        val req = s"${url}/api/2.0/${apiName}"
        val call = Seq("curl", "-H", s"Authentication: Bearer ${token}", "-s", s"${req}${queryString}")
        curlCommand = call.map(p => if (p.contains("Authentication")) "REDACTED" else p).mkString(" ")
        logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
        call
        //      curl -n -H "Accept-Encoding: gzip" https://<databricks-instance>/api/2.0/clusters/list > clusters.gz
      }
      val result = call.!!
      val mapper = new ObjectMapper
      mapper.writeValueAsString(mapper.readTree(result))
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could not execute API call.", e); ""
    }
  }

  def executePost(apiName: String, jsonQuery: String = ""): String = {

    // TODO -- Add proper try catch
    try {
      val call = if (System.getenv("OVERWATCH") != "LOCAL") {
        val ctx = dbutils.notebook.getContext
        val url = ctx.apiUrl.get
        val token = ctx.apiToken.get
        val req = s"${url}/api/2.0/${apiName}"
        val call = Seq("curl", "-H", s"Authentication: Bearer ${token}", "-s", s"${req}", "-d", jsonQuery)
        curlCommand = call.map(p => if (p.contains("Authentication")) "REDACTED" else p).mkString(" ")
        logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
        call
      } else {
        val url = System.getenv("OVERWATCH_ENV")
        val token = System.getenv("OVERWATCH_TOKEN")
        val req = s"${url}/api/2.0/${apiName}"
        val call = Seq("curl", "-H", s"Authentication: Bearer ${token}", "-s", s"${req}", "-d", jsonQuery)
        curlCommand = call.map(p => if (p.contains("Authentication")) "REDACTED" else p).mkString(" ")
        logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
        call
      }
      val result = call.!!
      val mapper = new ObjectMapper
      mapper.writeValueAsString(mapper.readTree(result))
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could not execute API call.", e); ""
    }
  }

}
