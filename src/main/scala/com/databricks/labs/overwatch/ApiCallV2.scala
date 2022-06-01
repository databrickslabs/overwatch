package com.databricks.labs.overwatch

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
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

  def apply(apiEnv: ApiEnv, apiName: String, queryJsonString: String, tempSuccessPath: String, errorTempPath: String) = {
    new ApiCallV2(apiEnv)
      .setApiName(apiName)
      .setQuery(queryJsonString)
      .setTempLocation(tempSuccessPath)
      .setErrorTempLocation(errorTempPath)
  }

  def apply(apiEnv: ApiEnv, apiName: String, queryJsonArray: Array[String], tempSuccessPath: String, errorTempPath: String) = {
    new ApiCallV2(apiEnv)
      .setApiName(apiName)
      .setQueryJsonArray(queryJsonArray)
      .setTempLocation(tempSuccessPath)
      .setErrorTempLocation(errorTempPath)

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
  private var endPoint = "" //API end point.
  private var jsonQuery: String = "EMPTY" //Extra parameters for API request.
  private var apiResponseArray = new util.ArrayList[String]() //JsonArray containing the responses from API call.
  private var serverBusyCount: Int = 0 // Keep track of 429 error occurrence.
  private var token = "" //Authentication token for API request.
  private var successTempPath: String = "" //Unique String which is used as folder name in a temp location to save the responses.
  private var unsafeSSLErrorCount = 0; //Keep track of SSL error occurrence.
  private var apiMeta: ApiMeta = null //Metadata for the API call.
  private val debugFlag = false //Debug flag to print the information when required. d
  private var allowUnsafeSSL = false //Flag to make the unsafe ssl.
  private var jsonKey = "" //Key name for pagination.
  private var jsonValue = "" //Key value for pagination.
  private val readTimeoutMS = 60000 //Read timeout.
  private val connTimeoutMS = 10000 //Connection timeout.
  private var errorTempPath: String = ""
  private var queryJsonArray: Array[String] = null
  private var errorArray: util.ArrayList[String] = new util.ArrayList[String]();

  /**
   * Setting up the api name and api metadata for that api.
   *
   * @param value
   * @return
   */
  private def setApiName(value: String): this.type = {
    endPoint = value
    token = apiEnv.rawToken
    apiMeta = new ApiMetaFactory().getApiClass(endPoint)
    this
  }

  /**
   * Setting the extra parameters which can be used to fetch the data from API.
   *
   * @param query
   * @return
   */
  private def setQuery(query: String): this.type = {
    jsonQuery = query
    jsonToMap
    this
  }

  private def setTempLocation(path: String): this.type = {
    successTempPath = path
    this
  }

  private def setErrorTempLocation(errorTempPath: String): this.type = {
    this.errorTempPath = errorTempPath
    this
  }

  private def setQueryJsonArray(queryJsonArray: Array[String]): this.type = {
    this.queryJsonArray = queryJsonArray
    this
  }

  /**
   * Parse the json string and get the data in the form of key and value.
   */
  private def jsonToMap: Unit = {
    try {
      val (jsonKey, jsonValue) = JsonUtils.getJsonKeyValue(jsonQuery)
      this.jsonKey = jsonKey
      this.jsonValue = jsonValue
    } catch {
      case e: Exception => {
        val excMsg = "Got the exception while parsing provided query json "
        logger.log(Level.WARN, excMsg + e.getMessage)
        throw new Exception(e)
      }
    }
  }

  /**
   * Hibernate in-case of too many API call request.
   *
   * @param response
   */
  private def hibernate(response: HttpResponse[String]): Unit = {
    logger.log(Level.WARN, "Too many request per second")
    serverBusyCount += 1
    serverBusyCount match {
      case 1 => {
        logger.log(Level.INFO, "Sleeping..... for 1 Minutes")
        Thread.sleep(60000)
      }
      case 2 => {
        logger.log(Level.INFO, "Sleeping..... for 2 Minutes")
        Thread.sleep(120000)
      }
      case 3 => {
        logger.log(Level.INFO, "Sleeping..... for 3 Minutes")
        Thread.sleep(180000)
      }
      case _ => {
        logger.log(Level.ERROR, " Too many request 429 error")
        throw new ApiCallFailure(response, buildGenericErrorMessage, debugFlag = debugFlag)
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
      case 200 => { //200 for all good

      }
      case 429 => { //The Databricks REST API supports a maximum of 30 requests/second per workspace. Requests that exceed the rate limit will receive a 429 response status code.
        hibernate(response)
        execute()
      }
      case _ => {
        throw new ApiCallFailure(response, buildGenericErrorMessage, debugFlag = debugFlag)
      }
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
      if (endPoint.equals("sql/history/queries")) { //For sql/history/queries api we have a different mechanism to get the next page token
        if (jsonObject.get(apiMeta.paginationKey).asBoolean()) { //Pagination key for sql/history/queries can return true or false
          jsonKey = "page_token"
          jsonValue = jsonObject.get(apiMeta.paginationToken).asText()
          execute()
        }

      } else {
        jsonQuery = jsonObject.get(apiMeta.paginationKey).toString
        execute()
      }

    }

  }


  /**
   * Write the responses to a temp location if API call has pagination.
   *
   * @param resultJsonArray containing responses from the API call.
   * @return true encase of successfully write to the temp location.
   */
  private def writeMicroBatchToTempLocation(path: String, resultJsonArray: String): Boolean = {
    try {
      val fineName = java.util.UUID.randomUUID.toString + ".json"
      dbutils.fs.put(path + "/" + fineName, resultJsonArray, true)
      logger.info("File Successfully written:" + path + "/" + fineName)
      true
    } catch {
      case e: Throwable =>
        logger.log(Level.WARN, "Unable to write in local" + e.getMessage)
        false
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

  /**
   * Perform the API call and get the response.
   *
   * @return response which is received on performing the API call.
   */
  private def getResponse: HttpResponse[String] = {
    var response: HttpResponse[String] = null
    apiMeta.apiCallType match {
      case "POST" => {
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
                unsafeSSLErrorCount += 1
                allowUnsafeSSL = true
                Http(s"""${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}""")
                  .copy(headers = httpHeaders)
                  .postData(jsonQuery)
                  .options(reqOptions)
                  .asString
              } else {
                throw new Exception(sslMSG)
              }


          }
      }
      case "GET" => {
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
              unsafeSSLErrorCount += 1
              allowUnsafeSSL = true
              Http(s"""${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}""")
                .param(jsonKey, jsonValue)
                .copy(headers = httpHeaders)
                .options(reqOptions)
                .asString
            } else {
              throw new Exception(sslMSG)
            }
          case e: Exception => throw e
        }
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
       |jsonQuery:${jsonQuery}
       |""".stripMargin
  }


  /**
   * Get the required columns from the received dataframe.
   *
   * @param rawDF The raw dataframe which is received as part of API response.
   * @return Dataframe which contains the required columns.
   */
  private def processRawData(rawDF: DataFrame): DataFrame = {
    val resultDFFieldNames = rawDF.schema.fieldNames
    if (!resultDFFieldNames.contains(apiMeta.dataframeColumns) && apiMeta.dataframeColumns != "*") { // if known API but return column doesn't exist
      val asDFErrMsg = s"The API endpoint is not returning the " +
        s"expected structure, column $apiMeta.dataframeColumns is expected and is not present in the dataframe.\nIf this module " +
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
    processRawData(apiResultDF)
  }


  /**
   * Function make the API call for given list of clusterIds
   *
   * @return
   */
  def executeBatch(): this.type = {
    try {
      for (i <- 0 to queryJsonArray.length - 1) {
        jsonQuery = queryJsonArray(i)
        execute()
      }
      if (apiResponseArray.size() > 0) {
        writeMicroBatchToTempLocation(successTempPath, apiResponseArray.toString)
      }
      if (errorArray.size() > 0) {
        writeMicroBatchToTempLocation(errorTempPath, errorArray.toString)
      }

    } catch {
      case e: Throwable =>
        throw e
    }
    this
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
      if (apiMeta.storeInTempLocation) {
        if (apiEnv.successBatchSize <= apiResponseArray.size()) { //Checking if its right time to write the batches into persistent storage
          val responseFlag = writeMicroBatchToTempLocation(successTempPath, apiResponseArray.toString)
          if (responseFlag) { //Clearing the resultArray in-case of successful write
            apiResponseArray = new util.ArrayList[String]()
          }
        }
      }
      paginate(response.body)
      if (apiMeta.storeInTempLocation && queryJsonArray == null && apiResponseArray.size() > 0) {
        writeMicroBatchToTempLocation(successTempPath, apiResponseArray.toString)
        apiResponseArray = new util.ArrayList[String]()
      }
      this
    } catch {
      case e: java.lang.NoClassDefFoundError => {
        val msg = "DEPENDENCY MISSING: scalaj. Ensure that the proper scalaj library is attached to your cluster"
        println(msg, e)
        if (apiMeta.storeInTempLocation) {
          writeErrorToTemp(e)
        }
        throw new java.lang.NoClassDefFoundError
      }
      case e: ApiCallFailure => {
        val excMsg = "Got the exception while performing get request "
        logger.log(Level.WARN, excMsg + e.getMessage)
        if (apiMeta.storeInTempLocation) {
          writeErrorToTemp(e)
        }
        if (e.failPipeline) {
          throw e
        }
        logger.error(e.getMessage)
        this
      }
      case e: Exception => {
        val excMsg = "Got the exception while performing get request "
        e.printStackTrace()
        logger.log(Level.WARN, excMsg + e.getMessage)
        if (apiMeta.storeInTempLocation) {
          writeErrorToTemp(e)
        }
        throw new Exception(e)
      }

    }
  }

  /**
   * Write the errors to the temp location.
   *
   * @param e
   */
  private def writeErrorToTemp(e: Throwable): Unit = {
    errorArray.add(parseJsonQuery(e))
    if (apiEnv.errorBatchSize <= errorArray.size()) { //Checking if its right time to write the batches into persistent storage
      val responseFlag = writeMicroBatchToTempLocation(errorTempPath, errorArray.toString)
      if (responseFlag) { //Clearing the resultArray in-case of successful write
        errorArray = new util.ArrayList[String]()
      }
    }
  }

  private def parseJsonQuery(e: Throwable): String = {
    val mapper = new ObjectMapper()
    val jsonObject = mapper.readTree(jsonQuery);
    val clusterId = jsonObject.get("cluster_id").toString
    val start_time = jsonObject.get("start_time").asLong()
    val end_time = jsonObject.get("end_time").asLong()
    val errorData = s"""{"cluster_id":${clusterId},"from_epoch":${start_time},"until_epoch":${end_time},"error":${e.getMessage}}"""
    errorData
  }


}


