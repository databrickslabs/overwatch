package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils.{ApiCallFailure, ApiEnv, Config, JsonUtils, NoNewDataException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scalaj.http.Http

import scala.collection.mutable.ArrayBuffer

class ApiCall(env: ApiEnv) extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var curlCommand: String = _
  private var _apiName: String = _
  private var _jsonQuery: String = _
  private var _getQueryString: String = _
  private var _initialQueryMap: Map[String, Any] = _
  private val results = ArrayBuffer[String]()
  private var _limit: Long = _
  private var _req: String = _
  private var _status: String = "SUCCESS"
  private var _errorFlag: Boolean = false
  private val mapper = JsonUtils.objectMapper

  private def setQuery(value: Option[Map[String, Any]]): this.type = {
    if (value.nonEmpty) {
      _limit = value.get.getOrElse("limit", 150).toString.toInt
      _initialQueryMap = value.get + ("limit" -> _limit) + ("offset" -> value.get.getOrElse("offset", 0))
    } else {
      _limit = 150
      _initialQueryMap = Map("limit" -> _limit, "offset" -> 0)
    }
    _jsonQuery = JsonUtils.objToJson(_initialQueryMap).compactString
    _getQueryString = "?" + _initialQueryMap.map { case(k, v) => s"$k=$v"}.mkString("&")
    this
  }

  private def setApiName(value: String): this.type = {
    _apiName = value

    if (!env.isLocal) {
      _req = s"${env.workspaceURL}/api/2.0/${_apiName}"
    } else {
      _req = s"${env.workspaceURL}/api/2.0/${_apiName}"
    }
    this
  }

  private def setStatus(value: String, level: Level, e: Option[Throwable] = None): Unit = {
    error()
    _status = value
    if (e.nonEmpty) logger.log(level, value, e.get)
    else logger.log(level, value)
  }

  private def error(): Unit = _errorFlag = true

  private def req: String = _req

  private def jsonQuery: String = _jsonQuery

  private def getQueryString: String = _getQueryString

  private def limit: Long = _limit

  private[overwatch] def status: String = _status

  private[overwatch] def isError: Boolean = _errorFlag

  def getCurlCommand: String = curlCommand

  def asStrings: Array[String] = results.toArray

  def asDF: DataFrame = {
    try {
      if (dataCol == "*") spark.read.json(Seq(results: _*).toDS)
      else spark.read.json(Seq(results: _*).toDS).select(explode(col(dataCol)).alias(dataCol))
        .select(col(s"${dataCol}.*"))
    } catch {
      case e: Throwable =>
        val emptyDF = sc.parallelize(Seq("")).toDF()
        if (results.isEmpty) {
          val msg = s"No data returned for api endpoint ${_apiName}"
          setStatus(msg, Level.INFO, Some(e))
          emptyDF
        }
        else {
          val msg = s"Acquiring data from ${_apiName} failed."
          setStatus(msg, Level.ERROR, Some(e))
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
      case e: Throwable => {
        val msg = "API Not Supported."
        setStatus(msg, Level.ERROR, Some(e))
        ""
      }
    }
  }

  @throws(classOf[ApiCallFailure])
  def executeGet(pageCall: Boolean = false): this.type = {
    try {
      val result = Http(req + getQueryString)
        .headers(Map[String, String](
          "Content-Type" -> "application/json",
          "Charset"-> "UTF-8",
          "Authorization" -> s"Bearer ${env.cipher.decrypt(env.encryptedToken)}"
        )).asString
      if (result.isError) {
        if (mapper.readTree(result.body).has("error_code")) {
          val err = mapper.readTree(result.body).get("error_code").asText()
          throw new ApiCallFailure(s"${_apiName} could not execute: ${err}")
        } else {
          throw new ApiCallFailure(s"${_apiName} could not execute: ${result.code}")
        }
      }
      if (!pageCall) {
        // Append initial results
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
        paginate()
      } else {
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
      }
      this
    } catch {
      case e: Throwable => {
        val msg = "Could not execute API call."
        setStatus(msg, Level.ERROR, Some(e))
        this
      }
    }
  }

  private def paginate(): Unit = {
    var hasMore = JsonUtils.jsonToMap(results(0)).getOrElse("has_more", false).toString.toBoolean
    var i: Int = 1
    while (hasMore) {
      _initialQueryMap += ("offset" -> (_initialQueryMap.getOrElse("offset", 0).toString.toInt + limit))
      setQuery(Some(_initialQueryMap))
      executeGet(true)
      hasMore = JsonUtils.jsonToMap(results(i)).getOrElse("has_more", false).toString.toBoolean
      i+=1
    }
  }

  // TODO - Use RDD if any of the results start getting to big
  private def paginate(totalCount: Long): Unit = {

    val offsets = (limit to totalCount by limit).toArray
    if (!_initialQueryMap.contains("end_time")) {
      _initialQueryMap += ("end_time" -> System.currentTimeMillis().toString) // TODO -- Change this to start fromTime in config
    }

    offsets.foreach(offset => {
      _initialQueryMap += ("offset" -> offset)
      setQuery(Some(_initialQueryMap))
      executePost(pageCall = true)
    })

  }

  @throws(classOf[NoNewDataException])
  def executePost(pageCall: Boolean = false): this.type = {

    // TODO -- Add proper try catch
    try {
      val jsonQuery = JsonUtils.objToJson(_initialQueryMap).compactString
      val result = Http(req)
        .postData(jsonQuery)
        .headers(Map[String, String](
          "Content-Type" -> "application/json",
          "Charset"-> "UTF-8",
          "Authorization" -> s"Bearer ${env.cipher.decrypt(env.encryptedToken)}"
        )).asString
      if (result.isError) throw new ApiCallFailure(s"${_apiName} could not execute")
      if (!pageCall) {
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
        val totalCount = JsonUtils.jsonToMap(results(0)).getOrElse("total_count", 0).toString.toLong
        if (totalCount > limit) paginate(totalCount)
      } else {
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
      }
      this
    } catch {
      case e: Throwable =>
        val msg = s"POST FAILED: Endpoint: ${_apiName} Query: ${jsonQuery}"
        setStatus(msg, Level.ERROR, Some(e))
        this
    }
  }

}

object ApiCall {

  def apply(apiName: String, apiEnv: ApiEnv, queryMap: Option[Map[String, Any]] = None): ApiCall = {
    new ApiCall(apiEnv).setApiName(apiName)
      .setQuery(queryMap)
  }

  // TODO -- Accept jsonQuery as Map
}
