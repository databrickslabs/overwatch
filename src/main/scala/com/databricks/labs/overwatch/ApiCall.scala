package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scalaj.http.{Http, HttpOptions}

import scala.collection.mutable.ArrayBuffer

class ApiCall(env: ApiEnv) extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _apiName: String = _
  private var _jsonQuery: String = _
  private var _getQueryString: String = _
  private var _initialQueryMap: Map[String, Any] = _
  private val results = ArrayBuffer[String]()
  private var _limit: Long = _
  private var _paginate: Boolean = false
  private var _req: String = _
  private var _maxResults: Int = _
  private var _status: String = "SUCCESS"
  private var _errorFlag: Boolean = false
  private var _allowUnsafeSSL: Boolean = false

  private val mapper = JsonUtils.defaultObjectMapper
  private val httpHeaders = Seq[(String, String)](
    ("Content-Type", "application/json"),
    ("Charset",  "UTF-8"),
    ("User-Agent",  s"databricks-labs-overwatch-${env.packageVersion}"),
    ("Authorization", s"Bearer ${env.rawToken}")
  )

  private def allowUnsafeSSL = _allowUnsafeSSL
  private def reqOptions: Seq[HttpOptions.HttpOption] = {
    val baseOptions = Seq(
      HttpOptions.connTimeout(ApiCall.connTimeoutMS),
      HttpOptions.connTimeout(ApiCall.readTimeoutMS)
    )
    if (allowUnsafeSSL) baseOptions :+ HttpOptions.allowUnsafeSSL else baseOptions

  }

  private def setQuery(value: Option[Map[String, Any]]): this.type = {
    if (value.nonEmpty && _paginate) {
      _limit = value.get.getOrElse("limit", 150).toString.toInt
      _initialQueryMap = value.get + ("limit" -> _limit) + ("offset" -> value.get.getOrElse("offset", 0))
    } else if (_paginate) {
      _limit = 150
      _initialQueryMap = Map("limit" -> _limit, "offset" -> 0)
    } else _initialQueryMap = value.getOrElse(Map[String, Any]())
    _limit = _initialQueryMap.getOrElse("limit", 150).toString.toInt
    _jsonQuery = JsonUtils.objToJson(_initialQueryMap).compactString
    _getQueryString = "?" + _initialQueryMap.map { case(k, v) => s"$k=$v"}.mkString("&")
    this
  }

  private def setMaxResults(value: Int): this.type = {
    _maxResults = value
    this
  }

  private def setPaginate(value: Boolean): this.type = {
    _paginate = value
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

  private def getQueryString: String = _getQueryString

  private def limit: Long = _limit

  private[overwatch] def status: String = _status

  private[overwatch] def isError: Boolean = _errorFlag

  def asStrings: Array[String] = results.toArray

  def asDF: DataFrame = {
    try {
      val apiResultDF = spark.read.json(Seq(results: _*).toDS)
      if (dataCol == "*") apiResultDF
      else apiResultDF.select(explode(col(dataCol)).alias(dataCol))
        .select(col(s"$dataCol.*"))
    } catch {
      case e: Throwable =>
        if (results.isEmpty) {
          val msg = s"No data returned for api endpoint ${_apiName}"
          setStatus(msg, Level.INFO, Some(e))
          spark.emptyDataFrame
        }
        else {
          val msg = s"Acquiring data from ${_apiName} failed."
          setStatus(msg, Level.ERROR, Some(e))
          spark.emptyDataFrame
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
      case e: Throwable =>
        val msg = "API Not Supported."
        setStatus(msg, Level.ERROR, Some(e))
        ""
    }
  }

  // TODO -- Issue_87 -- Simplify differences between get and post
  //  and validate the error handling
  @throws(classOf[ApiCallFailure])
  @throws(classOf[java.lang.NoClassDefFoundError])
  def executeGet(pageCall: Boolean = false): this.type = {
    logger.log(Level.INFO, s"Loading $req -> query: $getQueryString")
    try {
      val result = try {
        Http(req + getQueryString)
          .copy(headers = httpHeaders)
          .options(reqOptions)
          .asString
      } catch {
        case _: javax.net.ssl.SSLHandshakeException => // for PVC with ssl errors
          val sslMSG = "ALERT: DROPPING BACK TO UNSAFE SSL: SSL handshake errors were detected, allowing unsafe " +
            "ssl. If this is unexpected behavior, validate your ssl certs."
          logger.log(Level.WARN, sslMSG)
          println(sslMSG)
          _allowUnsafeSSL = true
          Http(req + getQueryString)
            .copy(headers = httpHeaders)
            .options(reqOptions)
            .asString
      }
      if (result.isError) {
        if (result.code == 429) {
          Thread.sleep(2000)
          executeGet(pageCall) // rate limit retry
        }
        if (mapper.readTree(result.body).has("error_code")) {
          val err = mapper.readTree(result.body).get("error_code").asText()
          throw new ApiCallFailure(s"${_apiName} could not execute: $err")
        } else {
          throw new ApiCallFailure(s"${_apiName} could not execute: ${result.code}")
        }
      }
      if (!pageCall && _paginate) {
        // Append initial results
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
        paginate()
      } else {
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
      }
      this
    } catch {
      case e: java.lang.NoClassDefFoundError =>
        val msg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        println(msg, e)
        throw new java.lang.NoClassDefFoundError
      case e: Throwable =>
        val msg = "Could not execute API call."
        setStatus(msg, Level.ERROR, Some(e))
        this
    }
  }

  // Paginate for Get Calls
  private def paginate(): Unit = {
    var hasMore = JsonUtils.jsonToMap(results(0)).getOrElse("has_more", false).toString.toBoolean
    var i: Int = 1
    var cumTotal: Long = 0
    // This allows for duplicates but they will be removed in the df using .distinct
    while (hasMore && cumTotal < _maxResults) {
      _initialQueryMap += ("offset" -> (_initialQueryMap.getOrElse("offset", 0).toString.toInt + limit))
      setQuery(Some(_initialQueryMap))
      executeGet(true)
      hasMore = JsonUtils.jsonToMap(results(i)).getOrElse("has_more", false).toString.toBoolean
      i+=1
      cumTotal+=limit
    }
  }

  // TODO - Use RDD if any of the results start getting to big
  // Pagination for POST calls
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
    val jsonQuery = JsonUtils.objToJson(_initialQueryMap).compactString
    logger.log(Level.INFO, s"Loading $req -> query: $jsonQuery")
    try {
      val result = try {
        Http(req)
          .copy(headers = httpHeaders)
          .postData(jsonQuery)
          .options(reqOptions)
          .asString
      } catch {
        case _: javax.net.ssl.SSLHandshakeException => // for PVC with ssl errors
          val sslMSG = "ALERT: DROPPING BACK TO UNSAFE SSL: SSL handshake errors were detected, allowing unsafe " +
            "ssl. If this is unexpected behavior, validate your ssl certs."
          logger.log(Level.WARN, sslMSG)
          println(sslMSG)
          _allowUnsafeSSL = true
          Http(req)
            .copy(headers = httpHeaders)
            .postData(jsonQuery)
            .options(reqOptions)
            .asString
      }
      if (result.isError) {
        val err = mapper.readTree(result.body).get("error_code").asText()
        val msg = mapper.readTree(result.body).get("message").asText()
        setStatus(s"$err -> $msg", Level.WARN)
        throw new ApiCallFailure(s"$err -> $msg")
      }
      if (!pageCall && _paginate) {
        val jsonResult = mapper.writeValueAsString(mapper.readTree(result.body))
        val totalCount = mapper.readTree(result.body).get("total_count").asInt(0)
        if (totalCount > 0) results.append(jsonResult)
        if (totalCount > limit) paginate(totalCount)
      } else {
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
      }
      this
    } catch {
      case e: java.lang.NoClassDefFoundError =>
        val msg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        println(msg, e)
        throw new java.lang.NoClassDefFoundError
      case _: JsonMappingException =>
        val msg = s"API POST: NO NEW DATA -> ${_apiName} Query: $jsonQuery"
        setStatus(msg, Level.WARN)
        this
      case e: Throwable =>
        val msg = s"POST FAILED: Endpoint: ${_apiName} Query: $jsonQuery"
        setStatus(msg, Level.ERROR, Some(e))
        this
    }
  }

}

object ApiCall {

  val readTimeoutMS = 60000
  val connTimeoutMS = 10000
  def apply(apiName: String, apiEnv: ApiEnv, queryMap: Option[Map[String, Any]] = None,
            maxResults: Int = Int.MaxValue, paginate: Boolean = true): ApiCall = {
    new ApiCall(apiEnv).setApiName(apiName)
      .setQuery(queryMap)
      .setMaxResults(maxResults)
      .setPaginate(paginate)
  }

}
