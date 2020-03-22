package com.databricks.labs.overwatch

import com.fasterxml.jackson.databind.ObjectMapper

import scala.sys.process._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class DBWrapper extends SparkSessionWrapper {

  def executeGet(apiName: String, query: String=""): String = {
    val queryString = if(query != "") s"?$query" else ""
    val call = if (System.getenv("OVERWATCH") != "LOCAL") {
      val ctx = dbutils.notebook.getContext
      val url = ctx.apiUrl.get
      val token = ctx.apiToken.get
      val req = s"${url}/api/2.0/${apiName}"
      Seq("curl", "-H", s"Authentication: Bearer ${token}", "-s", s"${req}${queryString}")
    } else {
      val url = System.getenv("OVERWATCH_ENV")
      val token = System.getenv("OVERWATCH_TOKEN")
      val req = s"${url}/api/2.0/${apiName}"
      Seq("curl", "-H", s"Authentication: Bearer ${token}", "-s", s"${req}${queryString}")
      //      curl -n -H "Accept-Encoding: gzip" https://<databricks-instance>/api/2.0/clusters/list > clusters.gz
    }
    val result = call.!!
    val mapper = new org.codehaus.jackson.map.ObjectMapper
    mapper.writeValueAsString(mapper.readTree(result))
  }

}
