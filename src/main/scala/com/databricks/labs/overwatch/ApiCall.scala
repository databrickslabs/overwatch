package com.databricks.labs.overwatch

import com.databricks.backend.common.rpc.CommandContext
import com.fasterxml.jackson.databind.ObjectMapper

import scala.sys.process._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.{Cipher, JsonUtils, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class ApiCall extends SparkSessionWrapper {

  import spark.implicits._

  final private val cipher = new Cipher
  private val logger: Logger = Logger.getLogger(this.getClass)
  private var curlCommand: String = _
  private var _apiName: String = _
  private var _query: String = _
  private var _initialQueryMap: Map[String, Any] = _
  private var _result: String = _
  private var _limit: Int = _
  private var _ctx: CommandContext = _
  private var _url: String = _
  private var _token: Array[Byte] = _
  private var _req: String = _

  private def setQuery(value: String): this.type = {
    _query = value; this
  }

  private def setApiName(value: String): this.type = {
    _apiName = value; this
  }

  private def init(): this.type = {
    if (_query != "") {
      _initialQueryMap = JsonUtils.jsonToMap(_query)
      _limit = _initialQueryMap.getOrElse("limit", 50).toString.toInt
    } else _limit = 50

    if (System.getenv("OVERWATCH") != "LOCAL") {
      _ctx = dbutils.notebook.getContext
      _url = _ctx.apiUrl.get
      _token = cipher.encrypt(_ctx.apiToken.get)
      _req = s"${_url}/api/2.0/${_apiName}"
    } else {
      _url = System.getenv("OVERWATCH_ENV")
      _token = cipher.encrypt(System.getenv("OVERWATCH_TOKEN"))
      _req = s"${_url}/api/2.0/${_apiName}"
    }

    this
  }

  private def token: String = cipher.decrypt(_token)

  private def req: String = _req

  private def query: String = _query

  private def limit: Long = _limit

  private def result: String = _result

  def getCurlCommand: String = curlCommand

  def asString: String = _result

  def asDF(schema: Option[String] = None) : DataFrame = {
    if (schema.nonEmpty) spark.read.schema(schema.get).json(Seq(_result).toDS).toDF
    else spark.read.json(Seq(_result).toDS).toDF
  }

  private[overwatch] def executeGet(query: String = _query, pageCall: Boolean = false): this.type = {
    try {
      val call = Seq("curl", "-H", s""""Authorization: Bearer ${token}"""", s"${req}${query}")
      curlCommand = call.map(p => if (p.contains("Authorization")) "REDACTED" else p).mkString(" ")
      logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
      val x = call.mkString(" ")
      val result = call.mkString(" ").!!
      val mapper = JsonUtils.objectMapper
      _result = mapper.writeValueAsString(mapper.readTree(result))
      if (!pageCall) {
        val totalCount = JsonUtils.jsonToMap(_result).getOrElse("total_count", 0).toString.toLong
        if (totalCount > limit) getPaginatedResults(totalCount)
      }
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could not execute API call.", e)
    }
    this
  }

  // TODO - Use RDD if any of the results start getting to big
  private def getPaginatedResults(totalCount: Long): Unit = {

    val callingMethod = Thread.currentThread.getStackTrace()(2).getMethodName
    val offsets = (0L to totalCount by limit).toArray
    offsets.foreach(offset => {
      val pagedQuery: String = JsonUtils.objToJson(_initialQueryMap +
        ("offset" -> offset), "limit" -> limit).compactString
      callingMethod match {
        case "executePost" =>
          _result += executePost(Some(pagedQuery), pageCall = true)
        //          case "executeGet" =>
        //            executeGet(apiName, pageQuery, limit, pageCall = true)
      }
    })
  }

  private[overwatch] def executePost(queryOverride: Option[String] = None, pageCall: Boolean = false): this.type = {

//    curl --location --request POST 'https://demo.cloud.databricks.com/api/2.0/clusters/events' \
//    --header 'Authorization: Bearer dapic5733c7b95601a1084b6d22ba6141a6b' \
//    --header 'Content-Type: text/plain' \
//    --data-raw '{"start_time":1577836800000,"end_time":1586357096644,"offset":50,"cluster_id":"0321-201717-chows241","limit":10,"select":"total_count"}'
    // TODO -- Add proper try catch
    try {
      val finalQuery = s"${queryOverride.getOrElse(query)}".replace("\"", "\\\"")
      val call = Seq("curl", "-X POST", "-s", "-H", s""""Authorization: Bearer ${token}"""", "-d", s""""${finalQuery}"""", s"${req}")
      curlCommand = call.map(p => if (p.contains("Authorization")) "REDACTED" else p).mkString(" ")
      logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
      val result = call.mkString(" ").!!
      val mapper = JsonUtils.objectMapper
      _result = mapper.writeValueAsString(mapper.readTree(result))
      if (!pageCall) {
        val totalCount = JsonUtils.jsonToMap(_result).getOrElse("total_count", 0).toString.toLong
        if (totalCount > limit) getPaginatedResults(totalCount)
      }
      this
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could not execute API call.", e); this
    }
  }

}

object ApiCall {
  def apply(apiName: String, jsonQuery: String = ""): ApiCall = {
    new ApiCall().setApiName(apiName)
      .setQuery(jsonQuery)
      .init()
  }

  // TODO -- Accept jsonQuery as Map
}
