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
    val ctx = dbutils.notebook.getContext
    val url = ctx.apiUrl.get
    val token = ctx.apiToken.get
    val req = s"${url}/api/2.0/${apiName}"
    val queryString = if(query != "") s"?$query" else ""
    val call = Seq("curl", "-H", s"Authentication: Bearer ${token}", "-v", s"${req}${queryString}")
    val result = call.!!
    val mapper = new org.codehaus.jackson.map.ObjectMapper
    mapper.writeValueAsString(mapper.readTree(result))
  }

}
