package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scalaj.http.{Http, HttpOptions, HttpResponse}

import java.util

/**
 * Companion object for APICallV2.
 */
object ApiCallV2 extends SparkSessionWrapper {

  def apply(apiEnv: ApiEnv, apiName: String) = {
    new ApiCallV2(apiEnv)
      .setApiName(apiName)
  }

  def apply(apiEnv: ApiEnv, apiName: String, queryJsonString: String) = {
    new ApiCallV2(apiEnv)
      .setApiName(apiName)
      .setQuery(queryJsonString)
  }

  def apply(apiEnv: ApiEnv, apiName: String, queryJsonString: String, tempSuccessPath: String) = {
    new ApiCallV2(apiEnv)
      .setApiName(apiName)
      .setQuery(queryJsonString)
      .setSuccessTempPath(tempSuccessPath)
  }

}

/**
 * Api class is used to perform the API calls to different API end points and give the result back in the form of DataFrame.
 *
 * @param apiEnv param contains the workspace url and the PAT token which will be used to create the URL and authenticate the request.
 */
class ApiCallV2(apiEnv: ApiEnv) extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _endPoint: String = _ //API end point.
  private var _jsonQuery: String = _ //Extra parameters for API request.
  private var _apiResponseArray: util.ArrayList[String] = _ //JsonArray containing the responses from API call.
  private var _serverBusyCount: Int = 0 // Keep track of 429 error occurrence.
  private var _token: String = _ //Authentication token for API request.
  private var _successTempPath: String = _ //Unique String which is used as folder name in a temp location to save the responses.
  private var _unsafeSSLErrorCount = 0; //Keep track of SSL error occurrence.
  private var _apiMeta: ApiMeta = null //Metadata for the API call.
  private var _debugFlag = false //Debug flag to print the information when required. d
  private var _allowUnsafeSSL = false //Flag to make the unsafe ssl.
  private var _jsonKey: String = "" //Key name for pagination.
  private var _jsonValue: String = "" //Key value for pagination.
  private val readTimeoutMS = 60000 //Read timeout.
  private val connTimeoutMS = 10000 //Connection timeout.
  private var _printFlag: Boolean = true

  def endPoint: String = _endPoint

  def jsonQuery: String = _jsonQuery

  def serverBusyCount: Int = _serverBusyCount

  def printFlag: Boolean = _printFlag

  def token: String = _token

  def successTempPath: String = _successTempPath

  def unsafeSSLErrorCount: Int = _unsafeSSLErrorCount

  def apiMeta: ApiMeta = _apiMeta

  def debugFlag: Boolean = _debugFlag

  def allowUnsafeSSL: Boolean = _allowUnsafeSSL

  def jsonKey: String = _jsonKey

  def jsonValue: String = _jsonValue

  def apiResponseArray: util.ArrayList[String] = _apiResponseArray

  private[overwatch] def setApiResponseArray(value: util.ArrayList[String]): this.type = {
    _apiResponseArray = value
    this
  }

  private[overwatch] def setJsonValue(value: String): this.type = {
    _jsonValue = value
    this
  }

  private[overwatch] def setJsonKey(value: String): this.type = {
    _jsonKey = value
    this
  }

  private[overwatch] def setAllowUnsafeSSL(value: Boolean): this.type = {
    _allowUnsafeSSL = value
    this
  }

  private[overwatch] def setDebugFlag(value: Boolean): this.type = {
    _debugFlag = value
    this
  }

  private[overwatch] def setApiMeta(value: ApiMeta): this.type = {
    _apiMeta = value
    this
  }

  private[overwatch] def setUnsafeSSLErrorCount(value: Int): this.type = {
    _unsafeSSLErrorCount = value
    this
  }

  private[overwatch] def setSuccessTempPath(value: String): this.type = {
    _successTempPath = value
    this
  }

  private[overwatch] def setToken(value: String): this.type = {
    _token = value
    this
  }

  private[overwatch] def setEndPoint(value: String): this.type = {
    _endPoint = value
    this
  }

  private[overwatch] def setJsonQuery(value: String): this.type = {
    _jsonQuery = value
    this
  }

  private[overwatch] def setPrintFlag(value: Boolean): this.type = {
    _printFlag = value
    this
  }

  private[overwatch] def setServerBusyCount(value: Int): this.type = {
    _serverBusyCount = value
    this
  }

  /**
   * Setting up the api name and api metadata for that api.
   *
   * @param value
   * @return
   */
  private def setApiName(value: String): this.type = {
    setEndPoint(value)
    setToken(apiEnv.rawToken)
    setApiMeta(new ApiMetaFactory().getApiClass(endPoint))
    setApiResponseArray(new util.ArrayList[String]())
    logger.log(Level.INFO, apiMeta.toString)
    this
  }

  /**
   * Setting the extra parameters which can be used to fetch the data from API.
   *
   * @param query
   * @return
   */
  private def setQuery(query: String): this.type = {
    setJsonQuery(query)
    jsonToMap
    this
  }


  /**
   * Parse the json string and get the data in the form of key and value.
   */
  private def jsonToMap: Unit = {
    try {
      val (jsonKey, jsonValue) = JsonUtils.getJsonKeyValue(jsonQuery)
      setJsonKey(jsonKey)
      setJsonValue(jsonValue)
    } catch {
      case e: Throwable => {
        val excMsg = "Got the exception while parsing provided query json "
        logger.log(Level.WARN, excMsg, e)
        throw e
      }
    }
  }

  /**
   * Hibernate in-case of too many API call request.
   *
   * @param response
   */
  private def hibernate(response: HttpResponse[String]): Unit = {
    logger.log(Level.WARN, "Received response code 429: Too many request per second")
    _serverBusyCount += 1
    _serverBusyCount match {
      case 1 => {
        logger.log(Level.INFO, "Sleeping..... for 2000 millis")
        Thread.sleep(2000)
      }
      case 2 => {
        logger.log(Level.INFO, "Sleeping..... for 5000 millis")
        Thread.sleep(5000)
      }
      case 3 => {
        logger.log(Level.INFO, "Sleeping..... for 10000 millis")
        Thread.sleep(10000)
      }
      case _ => {
        logger.log(Level.ERROR, " Too many request 429 error")
        throw new ApiCallFailure(response, buildGenericErrorMessage, debugFlag = _debugFlag)
      }
    }
  }

  /**
   * Check the response code from the response which got received from the API request and perform actions accordingly.
   *
   * @param responseCode
   * @param response
   */
  private def responseCodeHandler(response: HttpResponse[String]): Unit = {

    response.code match {
      case 200 => setServerBusyCount(0)
        println("resetting server-busy count") //200 for all good

      case 429 => //The Databricks REST API supports a maximum of 30 requests/second per workspace. Requests that exceed the rate limit will receive a 429 response status code.
        hibernate(response)
        execute()

      case _ =>
        throw new ApiCallFailure(response, buildGenericErrorMessage, debugFlag = _debugFlag)

    }

  }

  /**
   * Check if the response contains the key for next page and creates the required variable which can be used to create the API call for next page.
   *
   * @param jsonObject response as jsonObject received from API call.
   */
  private def paginate(response: String): Unit = {
    val mapper = new ObjectMapper()
    val jsonObject = mapper.readTree(response);
    if (jsonObject.get(apiMeta.paginationKey) != null) {
      if (apiMeta.hasNextPage(jsonObject)) { //Pagination key for sql/history/queries can return true or false
        if (apiMeta.tuplePaginationObject) {
          val (key, value) = apiMeta.getPaginationLogicForTuple(jsonObject)
          if (value != null) {
            setJsonKey(key)
            setJsonValue(value)
            execute()
          }
        } else {
          setJsonQuery(apiMeta.getPaginationLogicForSingleObject(jsonObject))
          execute()
        }
      }

    }

  }


  /**
   * Headers for the API call.
   */
  private val httpHeaders = Seq[(String, String)](
    ("Content-Type", "application/json"),
    ("Charset", "UTF-8"),
    ("User-Agent", s"databricks-labs-overwatch-${apiEnv.packageVersion}"),
    ("Authorization", s"Bearer ${apiEnv.rawToken}")
  )

  /**
   * Options for the API call.
   *
   * @return
   */
  private def reqOptions: Seq[HttpOptions.HttpOption] = {
    val baseOptions = Seq(
      HttpOptions.connTimeout(connTimeoutMS),
      HttpOptions.connTimeout(readTimeoutMS)
    )
    if (allowUnsafeSSL) baseOptions :+ HttpOptions.allowUnsafeSSL else baseOptions

  }

  private def buildGetDetailMessage: String = {
    s"""Get API call Endpoint: ${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}
       |Token Key : ${jsonKey}
       |Token Value: ${jsonValue}
       |""".stripMargin
  }

  private def buildPostDetailMessage: String = {
    s"""Post API call Endpoint: ${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}
       |queryString: ${jsonQuery}
       |""".stripMargin
  }

  /**
   * Perform the API call and get the response.
   *
   * @return response which is received on performing the API call.
   */
  private def getResponse: HttpResponse[String] = {
    var response: HttpResponse[String] = null
    apiMeta.apiCallType match {
      case "POST" =>
        if (printFlag) {
          var commonMsg = buildPostDetailMessage
          httpHeaders.foreach(x =>
            if (x._2.contains("Bearer")) {
              commonMsg = commonMsg + x._1 + ": REDACTED "
            }
            else {
              commonMsg = commonMsg + x._1 + " : " + x._2 + " "
            }

          ).toString
          logger.log(Level.INFO, commonMsg)
          setPrintFlag(false)
        }
        response =
          try {
            Http(s"""${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}""")
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
              if (unsafeSSLErrorCount == 0) { //Check for 1st occurrence of SSL Handshake error.
                setUnsafeSSLErrorCount(unsafeSSLErrorCount+1)
                setAllowUnsafeSSL(true)
                Http(s"""${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}""")
                  .copy(headers = httpHeaders)
                  .postData(jsonQuery)
                  .options(reqOptions)
                  .asString
              } else {
                throw new Exception(sslMSG)
              }


          }

      case "GET" =>
        if (printFlag) {
          var commonMsg = buildGetDetailMessage
          httpHeaders.foreach(x =>
            if (x._2.contains("Bearer")) {
              commonMsg = commonMsg + x._1 + " : REDACTED "
            }
            else {
              commonMsg = commonMsg + x._1 + " : " + x._2 + " "
            }
          ).toString
          logger.log(Level.INFO, commonMsg)
          setPrintFlag(false)
        }
        response = try {
          Http(s"""${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}""")
            .param(jsonKey, jsonValue)
            .copy(headers = httpHeaders)
            .options(reqOptions)
            .asString
        } catch {
          case e: javax.net.ssl.SSLHandshakeException if !allowUnsafeSSL => // for PVC with ssl errors
            val sslMSG = "ALERT: DROPPING BACK TO UNSAFE SSL: SSL handshake errors were detected, allowing unsafe " +
              "ssl. If this is unexpected behavior, validate your ssl certs."
            logger.log(Level.WARN, sslMSG)
            if (debugFlag) println(sslMSG)
            if (unsafeSSLErrorCount == 0) { //Check for 1st occurrence of SSL Handshake error.
              setUnsafeSSLErrorCount(unsafeSSLErrorCount+1)
              setAllowUnsafeSSL(true)
              Http(s"""${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}""")
                .param(jsonKey, jsonValue)
                .copy(headers = httpHeaders)
                .options(reqOptions)
                .asString
            } else {
              logger.log(Level.ERROR, e)
              throw new Exception(sslMSG)
            }
          case e: Throwable => throw e
        }
    }
    response
  }


  /**
   * Creating a generic error message
   *
   * @return
   */
  private def buildGenericErrorMessage: String = {
    s"""API CALL FAILED: Endpoint: ${endPoint}
       |_jsonQuery:${jsonQuery}
       |""".stripMargin
  }


  /**
   * Get the required columns from the received dataframe.
   *
   * @param rawDF The raw dataframe which is received as part of API response.
   * @return Dataframe which contains the required columns.
   */
  private def extrapolateSupportedStructed(rawDF: DataFrame): DataFrame = {
    val resultDFFieldNames = rawDF.schema.fieldNames
    if (!resultDFFieldNames.contains(apiMeta.dataframeColumns) && apiMeta.dataframeColumns != "*") { // if known API but return column doesn't exist
      val asDFErrMsg = s"The API endpoint is not returning the " +
        s"expected structure, column ${apiMeta.dataframeColumns} is expected and is not present in the dataframe.\nIf this module " +
        s"references data that does not exist or to which the Overwatch account does have access, please remove the " +
        s"scope. For example: if you have 'pools' scope enabled but there are no pools or Overwatch PAT doesn't " +
        s"have access to any pools, this scope must be removed."
      logger.log(Level.ERROR, asDFErrMsg)
      if (debugFlag) println(asDFErrMsg)
      throw new Exception(asDFErrMsg)
    } else if (apiMeta.dataframeColumns == "*") { //Selecting all of the column.
      rawDF
    }
    else { //Selecting specific columns as per the metadata.
      rawDF.select(explode(col(apiMeta.dataframeColumns)).alias(apiMeta.dataframeColumns)).select(col(apiMeta.dataframeColumns + ".*"))
    }
  }

  /**
   * Converting the API response to Dataframe.
   *
   * @return Dataframe which is created from the API response.
   */
  def asDF(): DataFrame = {
    var apiResultDF: DataFrame = null;
    if (apiResponseArray.size == 0 && !apiMeta.storeInTempLocation) { //If response contains no Data.
      val errMsg = s"API CALL Resulting DF is empty BUT no errors detected, progressing module. " +
        s"Details Below:\n$buildGenericErrorMessage"
      throw new ApiCallEmptyResponse(errMsg, true)
    } else if (apiResponseArray.size != 0 && !apiMeta.storeInTempLocation) { //If API response don't have pagination/volume of response is not huge then we directly convert the response which is in-memory to spark DF.
      apiResultDF = spark.read.json(Seq(apiResponseArray.toString).toDS())
    } else if (apiMeta.storeInTempLocation) { //Read the response from the Temp location/Disk and convert it to Dataframe.
      apiResultDF = spark.read.json(successTempPath)

    }
    extrapolateSupportedStructed(apiResultDF)
  }


  def executeMultiThread(): util.ArrayList[String] = {
    try {
      val response = getResponse
      responseCodeHandler(response)
      apiResponseArray.add(response.body)
      if (apiMeta.storeInTempLocation) {
        if (apiEnv.successBatchSize <= apiResponseArray.size()) { //Checking if its right time to write the batches into persistent storage
          val responseFlag = Helpers.writeMicroBatchToTempLocation(successTempPath, apiResponseArray.toString)
          if (responseFlag) { //Clearing the resultArray in-case of successful write
            setApiResponseArray(new util.ArrayList[String]())
          }
        }
      }
      paginate(response.body)
      apiResponseArray
    } catch {
      case e: java.lang.NoClassDefFoundError => {
        val excMsg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        logger.log(Level.ERROR, excMsg, e)
        throw e
      }
      case e: ApiCallFailure => {
        val excMsg = "Got the exception while performing get request "
        logger.log(Level.WARN, excMsg, e)
        if (e.failPipeline) {
          throw e
        }
        logger.log(Level.ERROR, excMsg, e)
        throw new ApiCallFailureV2(jsonQueryToApiErrorDetail(e))
      }
      case e: Throwable => {
        val excMsg = "Got the exception while performing get request "
        logger.log(Level.WARN, excMsg, e)
        throw e
      }

    }
  }


  /**
   * Function responsible for making the API request.
   *
   * @return
   */
  def execute(): this.type = {
    try {
      val response = getResponse
      responseCodeHandler(response)
      apiResponseArray.add(response.body)
      paginate(response.body)
      this
    } catch {
      case e: java.lang.NoClassDefFoundError => {
        val excMsg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        logger.log(Level.ERROR, excMsg, e)
        throw e
      }
      case e: ApiCallFailure => {
        val excMsg = "Got the exception while performing get request "
        logger.log(Level.WARN, excMsg, e)
        if (e.failPipeline) {
          throw e
        }
        throw new Exception(e)
      }
      case e: Throwable => {
        val excMsg = "Got the exception while performing get request "
        e.printStackTrace()
        logger.log(Level.WARN, excMsg, e)
        throw e
      }

    }
  }


  private def jsonQueryToApiErrorDetail(e: Throwable): String = {
    val mapper = new ObjectMapper()
    val jsonObject = mapper.readTree(jsonQuery);
    val clusterId = jsonObject.get("cluster_id").toString
    val start_time = jsonObject.get("start_time").asLong()
    val end_time = jsonObject.get("end_time").asLong()
    val errorData = s"""{"cluster_id":${clusterId},"from_epoch":${start_time},"until_epoch":${end_time},"error":${e.getMessage}}"""
    errorData
  }


}


