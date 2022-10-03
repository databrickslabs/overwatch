package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scalaj.http.{Http, HttpOptions, HttpResponse}

import scala.collection.mutable.ArrayBuffer

class ApiCall(env: ApiEnv) extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _apiName: String = "EMPTY"
  private var _jsonQuery: String = "EMPTY"
  private var _getQueryString: String = "EMPTY"
  private var _initialQueryMap: Map[String, Any] = Map()
  private val results = ArrayBuffer[String]()
  private val _rawResults = ArrayBuffer[HttpResponse[String]]()
  private var _limit: Long = -1L
  private var _paginate: Boolean = false
  private var _debugFlag: Boolean = false
  private var _req: String = "EMPTY"
  private var _maxResults: Int = -1
  private var _allowUnsafeSSL: Boolean = false

  private val mapper = JsonUtils.defaultObjectMapper
  private val httpHeaders = Seq[(String, String)](
    ("Content-Type", "application/json"),
    ("Charset", "UTF-8"),
    ("User-Agent", s"databricks-labs-overwatch-${env.packageVersion}"),
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
    _getQueryString = "?" + _initialQueryMap.map { case (k, v) => s"$k=$v" }.mkString("&")
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

  private def setDebugFlag(value: Boolean): this.type = {
    _debugFlag = value
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

  //  private def setStatus(value: String, level: Level, e: Option[Throwable] = None): Unit = {
  //    error()
  //    _status = value
  //    if (e.nonEmpty) logger.log(level, value, e.get)
  //    else logger.log(level, value)
  //  }

  //  private def error(): Unit = _errorFlag = true

  private def req: String = _req

  private def getQueryString: String = _getQueryString

  private def limit: Long = _limit

  private def debugFlag: Boolean = _debugFlag

  def asRawResponses: Array[HttpResponse[String]] = _rawResults.toArray

  def errorsFound: Boolean = asRawResponses.exists(r => r.isError && r.code != 429) // don't consider rate limit an error

  def asStrings: Array[String] = results.toArray

  def asDF: DataFrame = {
    if (results.isEmpty) {
      if (errorsFound) {
        val errMsg = s"API CALL CONTAINS ERRORS and resulting DF is empty. " +
          s"FAILING MODULE, details below:\n$buildGenericErrorMessage"
        throw new ApiCallEmptyResponse(errMsg, false)
      } else {
        val errMsg = s"API CALL Resulting DF is empty BUT no errors detected, progressing module. " +
          s"Details Below:\n$buildGenericErrorMessage"
        throw new ApiCallEmptyResponse(errMsg, true)
      }
    }
    val apiResultDF = spark.read.json(Seq(results: _*).toDS)
    val resultDFFieldNames = apiResultDF.schema.fieldNames
    if (!resultDFFieldNames.contains(dataCol) && dataCol != "*") { // if known API but return column doesn't exist
     val asDFErrMsg = s"The API endpoint is not returning the " +
       s"expected structure, column $dataCol is expected and is not present in the dataframe.\nIf this module " +
       s"references data that does not exist or to which the Overwatch account does have access, please remove the " +
       s"scope. For example: if you have 'pools' scope enabled but there are no pools or Overwatch PAT doesn't " +
       s"have access to any pools, this scope must be removed."
      logger.log(Level.ERROR, asDFErrMsg)
      if (debugFlag) println(asDFErrMsg)
      throw new Exception(asDFErrMsg)
    }
    if (dataCol == "*") apiResultDF
    else apiResultDF.select(explode(col(dataCol)).alias(dataCol))
      .select(col(s"$dataCol.*"))

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
        logger.log(Level.ERROR, msg, e)
        ""
    }
  }

  private def buildGenericErrorMessage: String = {
    s"""API CALL FAILED: Endpoint: ${_apiName}
       |isPaginate: ${_paginate}
       |limit: ${_limit}
       |initQueryMap: ${_initialQueryMap}
       |jsonQuery: ${JsonUtils.objToJson(_initialQueryMap).compactString}
       |queryString: $getQueryString
       |""".stripMargin
  }

  private def buildGetErrorMessage: String = {
    s"""GET FAILED: Endpoint: ${_apiName}
       |isPaginate: ${_paginate}
       |limit: ${_limit}
       |initQueryMap: ${_initialQueryMap}
       |queryString: $getQueryString
       |""".stripMargin
  }

  private def buildPostErrorMessage: String = {
    s"""POST FAILED: Endpoint: ${_apiName}
       |jsonQuery: ${JsonUtils.objToJson(_initialQueryMap).compactString}
       |isPaginate: ${_paginate}
       |limit: ${_limit}
       |initQueryMap: ${_initialQueryMap}
       |""".stripMargin
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
      i += 1
      cumTotal += limit
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

  def executeGet(pageCall: Boolean = false): this.type = {
    logger.log(Level.INFO, s"Loading $req -> query: $getQueryString")
    try {
      val result = try {
        Http(req + getQueryString)
          .copy(headers = httpHeaders)
          .options(reqOptions)
          .asString
      } catch {
        case _: javax.net.ssl.SSLHandshakeException if !allowUnsafeSSL => // for PVC with ssl errors
          val sslMSG = "ALERT: DROPPING BACK TO UNSAFE SSL: SSL handshake errors were detected, allowing unsafe " +
            "ssl. If this is unexpected behavior, validate your ssl certs."
          logger.log(Level.WARN, sslMSG)
          if (debugFlag) println(sslMSG)
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
        } else throw new ApiCallFailure(result, buildGetErrorMessage, debugFlag = debugFlag)
      }
      if (!pageCall && _paginate) {
        // Append initial rawResults
        _rawResults.append(result)
        // Append initial results
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
        paginate()
      } else {
        _rawResults.append(result)
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
      }
      this
    } catch {
      case e: java.lang.NoClassDefFoundError =>
        val msg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        println(msg, e)
        throw new java.lang.NoClassDefFoundError
      case e: ApiCallFailure if e.failPipeline => throw e
      case e: Throwable =>
        logger.log(Level.ERROR, buildGetErrorMessage, e)
        if (debugFlag) println(buildGetErrorMessage)
        throw e
    }
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
          if (debugFlag) println(sslMSG)
          _allowUnsafeSSL = true
          Http(req)
            .copy(headers = httpHeaders)
            .postData(jsonQuery)
            .options(reqOptions)
            .asString
      }

      if (result.isError) {
        if (result.code == 429) {
          Thread.sleep(2000)
          executePost(pageCall) // rate limit retry
        } else throw new ApiCallFailure(result, buildPostErrorMessage, debugFlag = debugFlag)
      }
      if (!pageCall && _paginate) {
        // if paginate == true and is first api call
        // determine page call and make first call and then make recursive call to paginate through
        _rawResults.append(result)
        val jsonResult = mapper.writeValueAsString(mapper.readTree(result.body))
        val totalCount = mapper.readTree(result.body).get("total_count").asInt(0)
        logger.log(Level.INFO, s"Total Count for Query: $jsonQuery is $totalCount by Limit: $limit")
        if (totalCount > 0) results.append(jsonResult)
        if (totalCount > limit) paginate(totalCount) // paginate if more
      } else { // if no pagination make single post call
        _rawResults.append(result)
        results.append(mapper.writeValueAsString(mapper.readTree(result.body)))
      }
      this
    } catch {
      case e: java.lang.NoClassDefFoundError =>
        val msg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        println(msg, e)
        throw new java.lang.NoClassDefFoundError(msg)
      case _: JsonMappingException => {
        if (debugFlag) println(buildPostErrorMessage)
        val rawBodyResults = asRawResponses.map(_.body).mkString("\n\n")
        throw new ApiCallEmptyResponse(s"No or malformed data returned: $buildPostErrorMessage\n" +
          s"RAW return Value: ${rawBodyResults}", !errorsFound)
      }
      case e: ApiCallFailure if e.failPipeline => throw e
      case e: Throwable =>
        logger.log(Level.ERROR, buildPostErrorMessage, e)
        if (debugFlag) println(buildPostErrorMessage)
        throw e
    }
  }

}

object ApiCall {

  val readTimeoutMS = 60000
  val connTimeoutMS = 10000

  def apply(apiName: String, apiEnv: ApiEnv, queryMap: Option[Map[String, Any]] = None,
            maxResults: Int = Int.MaxValue, paginate: Boolean = true, debugFlag: Boolean = false): ApiCall = {
    new ApiCall(apiEnv).setApiName(apiName)
      .setQuery(queryMap)
      .setMaxResults(maxResults)
      .setPaginate(paginate)
      .setDebugFlag(debugFlag)
  }

}
