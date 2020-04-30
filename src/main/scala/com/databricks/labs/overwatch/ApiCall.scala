package com.databricks.labs.overwatch

import scala.sys.process._
import com.databricks.labs.overwatch.utils.{Config, JsonUtils, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

class ApiCall extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var curlCommand: String = _
  private var _apiName: String = _
  private var _query: String = _
  private var _initialQueryMap: Map[String, Any] = _
  private val results = ArrayBuffer[String]()
  private var _limit: Int = _
  private var _req: String = _
  private val mapper = JsonUtils.objectMapper

  private def setQuery(value: String): this.type = {
    _query = value;
    this
  }

  private def setApiName(value: String): this.type = {
    _apiName = value;
    this
  }

  private def init(): this.type = {
    if (_query != "") {
      if (_query.startsWith("{") || _query.startsWith("[")) {
        _initialQueryMap = JsonUtils.jsonToMap(_query)
        _limit = _initialQueryMap.getOrElse("limit", 50).toString.toInt
      } else {
        _query = s"?${_query}"
      }
    } else _limit = 50


    if (!Config.isLocalTesting) {
      _req = s"${Config.workspaceURL}/api/2.0/${_apiName}"
    } else {
      _req = s"${Config.workspaceURL}/api/2.0/${_apiName}"
    }

    this
  }

  private def req: String = _req

  private def query: String = _query

  private def limit: Long = _limit

  def getCurlCommand: String = curlCommand

  def asStrings: Array[String] = results.toArray

  def asDF: DataFrame = {
    try {
      if (dataCol == "*") spark.read.json(Seq(results: _*).toDS)
      else spark.read.json(Seq(results: _*).toDS).select(explode(col(dataCol)).alias(dataCol)).select(col(s"${dataCol}.*"))
    } catch {
      case e: Throwable =>
        val emptyDF = sc.parallelize(Seq("")).toDF()
        if (results(0) == "{}") {
          logger.log(Level.INFO,
            s"No data returned for api endpoint ${_apiName}")
          emptyDF
        }
        else {
          logger.log(Level.ERROR,
            s"Acquiring data from ${_apiName} failed.", e)
          emptyDF
        }
    }
  }

  private def dataCol: String = {
    try {
      _apiName match {
        case "jobs/list" => "jobs"
        case "clusters/list" => "clusters"
        case "clusters/events" => "events"
        case "dbfs/list" => "files"
        case "instance-pools/list" => "instance_pools"
        case "instance-profiles/list" => "instance_profiles"
        case "workspace/list" => "objects"
      }
    } catch {
      case _: scala.MatchError => logger.log(Level.WARN, "API not configured, returning full dataset"); "*"
      case e: Throwable => logger.log(Level.ERROR, "API Not Supported.", e); ""
    }
  }

  private[overwatch] def executeGet(query: String = _query, pageCall: Boolean = false): this.type = {
    try {
      val call = Seq("curl", "-H", s""""Authorization: Bearer ${Config.token}"""", s"${req}${query}")
      val x = call.mkString(" ")
      curlCommand = call.map(p => if (p.contains("Authorization")) "REDACTED" else p).mkString(" ")
      logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
      val result = call.mkString(" ").!!
      if (!pageCall) {
        results.append(mapper.writeValueAsString(mapper.readTree(result)))
        val totalCount = JsonUtils.jsonToMap(results(0)).getOrElse("total_count", 0).toString.toLong
        if (totalCount > limit) paginate(totalCount)
      } else {
        results.append(mapper.writeValueAsString(mapper.readTree(result)))
      }
    } catch {
      case e: Throwable => logger.log(Level.ERROR, "Could not execute API call.", e)
    }
    this
  }

  // TODO - Use RDD if any of the results start getting to big
  private def paginate(totalCount: Long): Unit = {

    val callingMethod = Thread.currentThread.getStackTrace()(2).getMethodName
    val offsets = (0L to totalCount by limit).toArray
    if (!_initialQueryMap.contains("end_time")) {
      _initialQueryMap = _initialQueryMap + ("end_time" -> System.currentTimeMillis().toString) // TODO -- Change this to start fromTime in config
    }
    offsets.foreach(offset => {
      val pagedQuery: String = JsonUtils.objToJson(_initialQueryMap +
        ("offset" -> offset), "limit" -> limit).compactString
      callingMethod match {
        case "executePost" =>
          executePost(Some(pagedQuery), pageCall = true)
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
      val call = Seq("curl", "-X POST", "-s", "-H", s""""Authorization: Bearer ${Config.token}"""", "-d", s""""${finalQuery}"""", s"${req}")
      curlCommand = call.map(p => if (p.contains("Authorization")) "REDACTED" else p).mkString(" ")
      logger.log(Level.INFO, s"Executing curl: ${curlCommand}")
      val result = call.mkString(" ").!!
      if (!pageCall) {
        results.append(mapper.writeValueAsString(mapper.readTree(result)))
        val totalCount = JsonUtils.jsonToMap(results(0)).getOrElse("total_count", 0).toString.toLong
        if (totalCount > limit) paginate(totalCount)
      } else {
        results.append(mapper.writeValueAsString(mapper.readTree(result)))
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

  def apply(apiName: String, queryMap: Map[String, Any]): ApiCall = {
    val jsonQuery = JsonUtils.objToJson(queryMap).compactString
    new ApiCall().setApiName(apiName)
      .setQuery(jsonQuery)
      .init()
  }

  // TODO -- Accept jsonQuery as Map
}
