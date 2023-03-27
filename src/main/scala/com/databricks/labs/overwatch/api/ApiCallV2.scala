package com.databricks.labs.overwatch.api

import com.databricks.labs.overwatch.pipeline.PipelineFunctions
import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.util.LongAccumulator
import org.json.JSONObject
import scalaj.http.{HttpOptions, HttpResponse}

import java.util
import java.util.Collections
import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
 * Companion object for APICallV2.
 */
object ApiCallV2 extends SparkSessionWrapper {

  /**
   * Companion Object which takes two parameter and initialise the ApiCallV2.
   * @param apiEnv ApiEnv which contains api related information.
   * @param apiName Name of the api.
   * @return
   */
  def apply(apiEnv: ApiEnv, apiName: String): ApiCallV2 = {
    new ApiCallV2(apiEnv)
      .setEndPoint(apiName)
      .buildMeta(apiName)
  }

  /**
   * Companion Object which takes three parameter and initialise the ApiCallV2.
   *
   * @param apiEnv  ApiEnv which contains api related information.
   * @param apiName Name of the api.
   * @param queryJsonString query as json string.
   * @return
   */
  def apply(apiEnv: ApiEnv, apiName: String, queryJsonString: String): ApiCallV2 = {
    new ApiCallV2(apiEnv)
      .setEndPoint(apiName)
      .buildMeta(apiName)
      .setQuery(queryJsonString)
  }


  /**
   * Companion Object which takes five parameter and initialise the ApiCallV2.
   * @param apiEnv ApiEnv which contains api related information.
   * @param apiName Name of the api.
   * @param queryMap Map containing the filter conditions.
   * @param tempSuccessPath Path in which the api response will be written.
   * @param accumulator To make track of number of api request.
   * @return
   */
  def apply(apiEnv: ApiEnv, apiName: String, queryMap: Map[String, String], tempSuccessPath: String
            ): ApiCallV2 = {
    new ApiCallV2(apiEnv)
      .setEndPoint(apiName)
      .buildMeta(apiName)
      .setQueryMap(queryMap)
      .setSuccessTempPath(tempSuccessPath)
  }


  /**
   * Companion Object which takes three parameter and initialise the ApiCallV2.
   *
   * @param apiEnv ApiEnv which contains api related information.
   * @param apiName Name of the api.
   * @param queryMap Map containing the filter conditions.
   * @return
   */
  def apply(apiEnv: ApiEnv, apiName: String, queryMap: Map[String, String]): ApiCallV2 = {
    new ApiCallV2(apiEnv)
      .setEndPoint(apiName)
      .buildMeta(apiName)
      .setQueryMap(queryMap)
  }

  /**
   * Companion Object which takes three parameter and initialise the ApiCallV2.
   *
   * @param apiEnv   ApiEnv which contains api related information.
   * @param apiName  Name of the api.
   * @param queryMap Map containing the filter conditions.
   * @param apiVersion Version of the Api call.
   * @return
   */
  def apply(apiEnv: ApiEnv, apiName: String, queryMap: Map[String, String], apiVersion: Double = 2.0): ApiCallV2 = {
    new ApiCallV2(apiEnv)
      .setEndPoint(apiName)
      .buildMeta(apiName)
      .setQueryMap(queryMap)
      .setApiV(apiVersion)
  }

  /**
   *
   * @param apiEnv
   * @param apiName
   * @param queryMap
   * @param tempSuccessPath
   * @param apiVersion
   * @return
   */
  def apply(apiEnv: ApiEnv, apiName: String, queryMap: Map[String, String],
            tempSuccessPath: String, apiVersion: Double): ApiCallV2 = {
    new ApiCallV2(apiEnv)
      .setEndPoint(apiName)
      .buildMeta(apiName)
      .setQueryMap(queryMap)
      .setSuccessTempPath(tempSuccessPath)
      .setApiV(apiVersion)
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
  private var _apiResponseArray: util.ArrayList[String] = new util.ArrayList[String]() //JsonArray containing the responses from API call.
  private var _serverBusyCount: Int = 0 // Keep track of 429 error occurrence.
  private var _successTempPath: Option[String] = None //Unique String which is used as folder name in a temp location to save the responses.
  private var _unsafeSSLErrorCount = 0; //Keep track of SSL error occurrence.
  private var _apiMeta: ApiMeta = null //Metadata for the API call.
  private var _allowUnsafeSSL: Boolean = false //Flag to make the unsafe ssl.
  private val readTimeoutMS = 60000 //Read timeout.
  private val connTimeoutMS = 10000 //Connection timeout.
  private var _printFlag: Boolean = true
  private var _totalSleepTime: Int = 0
  private var _apiSuccessCount: Int = 0
  private var _apiFailureCount: Int = 0
  private var _printFinalStatusFlag: Boolean = true
  private var _queryMap: Map[String, String] = Map[String, String]()
  private var _accumulator: LongAccumulator = sc.longAccumulator("ApiAccumulator") //Multithreaded call accumulator will make track of the request.

  protected def apiSuccessCount: Int = _apiSuccessCount

  protected def apiFailureCount: Int = _apiFailureCount

  protected def totalSleepTime: Int = _totalSleepTime

  protected def endPoint: String = _endPoint

  protected def jsonQuery: String = _jsonQuery

  protected def serverBusyCount: Int = _serverBusyCount

  protected def successTempPath: Option[String] = _successTempPath

  protected def unsafeSSLErrorCount: Int = _unsafeSSLErrorCount

  protected def apiMeta: ApiMeta = _apiMeta

  protected def queryMap: Map[String, String] = _queryMap

  private[overwatch] def setAccumulator(value: LongAccumulator): this.type = {
    _accumulator = value
    this
  }

  private[overwatch] def setApiV(value: Double): this.type = {
    apiMeta.setApiV("api/"+value)
    this
  }

  private[overwatch] def setQueryMap(value: Map[String, String]): this.type = {
    _queryMap = value
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    _jsonQuery = mapper.writeValueAsString(value)
    this
  }

  private[overwatch] def setApiResponseArray(value: util.ArrayList[String]): this.type = {
    _apiResponseArray = value
    this
  }


  private[overwatch] def setAllowUnsafeSSL(value: Boolean): this.type = {
    _allowUnsafeSSL = value
    this
  }

  private[overwatch] def setPrintFinalStatsFlag(value: Boolean): this.type = {
    _printFinalStatusFlag = value
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

  private[overwatch] def setApiSuccessCount(value: Int): this.type = {
    _apiSuccessCount = value
    this
  }

  private[overwatch] def setApiFailureCount(value: Int): this.type = {
    _apiFailureCount = value
    this
  }

  private[overwatch] def setTotalSleepTime(value: Int): this.type = {
    _totalSleepTime = value
    this
  }

  private[overwatch] def setSuccessTempPath(value: String): this.type = {
    _successTempPath = Some(value)
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

  def asStrings: Array[String] = _apiResponseArray.toArray(new Array[String](_apiResponseArray.size))

  /**
   * Setting up the api name and api metadata for that api.
   *
   * @param value
   * @return
   */
  private def buildMeta(value: String): this.type = {
    setApiMeta(new ApiMetaFactory().getApiClass(endPoint))
    apiMeta.setApiEnv(apiEnv).setApiName(endPoint)
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
    setQueryMap(Map[String, String]())
    this
  }


  private def buildDetailMessage(): String = {
    s"""API call Endpoint: ${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}
       |queryString: ${jsonQuery}
       |Server Busy Count:${serverBusyCount}
       |Total Sleep Time:${totalSleepTime} seconds
       |Successful api response count:${apiSuccessCount}
       |""".stripMargin
  }


  /**
   * Hibernate in-case of too many API call request.
   *
   * @param response
   */
  private def hibernate(response: HttpResponse[String]): Unit = { //TODO work in progress
    println("Received response code 429: Too many request per second")
    setServerBusyCount(serverBusyCount + 1)
    if (serverBusyCount < 40) { //40 and expose it  10 sec sleep 5 mints ,failuer count and success count  broth in log in println warn.
      val sleepFactor = 1 + serverBusyCount % 5
      if (serverBusyCount > 5) {
        setTotalSleepTime(totalSleepTime + 10)
        println(buildDetailMessage() + "Current action: Sleeping for 10 seconds")
        logger.log(Level.WARN, buildDetailMessage() + "Current action: Sleeping for 10seconds")
        Thread.sleep(10 * 1000) //Sleeping for 10 secs
      }
      else {
        setTotalSleepTime(totalSleepTime + sleepFactor)
        println(buildDetailMessage() + "Current action: Sleeping for " + sleepFactor + " seconds")
        logger.log(Level.WARN, buildDetailMessage() + "Current action: Sleeping for " + sleepFactor + " seconds")
        Thread.sleep(sleepFactor * 1000)
      }

    } else {
      println("Too many request 429 error, Total waiting time " + totalSleepTime)
      logger.log(Level.ERROR, buildDetailMessage() + "Current action: Shutting Down...")
      throw new ApiCallFailure(response, buildDetailMessage() + "Current action: Shutting Down...", debugFlag = false)

    }

  }

  /**
   * Check the response code from the response which got received from the API request and perform actions accordingly.
   *
   * @param responseCode
   * @param response
   */
  private def responseCodeHandler(response: HttpResponse[String]): Unit = {

    if (response.code == 200) { //200 for all good
      setServerBusyCount(0)
      setApiSuccessCount(apiSuccessCount + 1)
    } else if (response.code == 429 || (response.code > 499 && response.code < 600)) { //The Databricks REST API supports a maximum of 30 requests/second per workspace.
      // Requests that exceed the rate limit will receive a 429 response status code.
      setApiFailureCount(apiFailureCount + 1)
      hibernate(response)
      execute()
    } else {
      throw new ApiCallFailure(response, buildGenericErrorMessage, debugFlag = false)
    }

  }

  /**
   * Check if the response contains the key for next page and creates the required variable which can be used to create the API call for next page.
   *
   * @param jsonObject response as jsonObject received from API call.
   */
  private def paginate(response: String): Boolean = {
    var paginate = false
    val mapper = new ObjectMapper()
    val jsonObject = mapper.readTree(response);
    if (jsonObject.get(apiMeta.paginationKey) != null) {
      if (apiMeta.hasNextPage(jsonObject)) { //Pagination key for sql/history/queries can return true or false
        if (apiMeta.isDerivePaginationLogic) {
          val nextPageParams = apiMeta.getPaginationLogic(jsonObject, queryMap)
          if (nextPageParams != null) {
            setQueryMap(nextPageParams)
            paginate = true
          }
        } else {
          setJsonQuery(apiMeta.getPaginationLogicForSingleObject(jsonObject))
          paginate = true
        }
      }
    }
    paginate
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
    if (_allowUnsafeSSL) baseOptions :+ HttpOptions.allowUnsafeSSL else baseOptions

  }


  private def buildApiDetailMessage: String = {
    s"""API call Endpoint: ${apiEnv.workspaceURL}/${apiMeta.apiV}/${endPoint}
       |Api call type: ${apiMeta.apiCallType}
       |queryString: ${jsonQuery}
       |queryMap: ${queryMap}
       |""".stripMargin
  }

  /**
   * Perform the API call and get the response.
   *
   * @return response which is received on performing the API call.
   */
  private def getResponse: HttpResponse[String] = {
    var response: HttpResponse[String] = null
    if (_printFlag) {
      var commonMsg = buildApiDetailMessage
      httpHeaders.foreach(header =>
        if (header._2.contains("Bearer")) {
          commonMsg = commonMsg + header._1 + " : REDACTED "
        }
        else {
          commonMsg = commonMsg + header._1 + " : " + header._2 + " "
        }
      ).toString
      logger.log(Level.INFO, commonMsg)
      setPrintFlag(false)
    }
    apiMeta.apiCallType match {
      case "POST" =>
        response =
          try {
            apiMeta.getBaseRequest()
              .postData(jsonQuery)
              .options(reqOptions)
              .asString
          } catch {
            case e: javax.net.ssl.SSLHandshakeException => // for PVC with ssl errors
              handleSSLHandShakeException(e)
            case e: Throwable => throw e
          }

      case "GET" =>
        response = try {
            apiMeta.getBaseRequest()
            .params(queryMap)
            .options(reqOptions)
            .asString
        } catch {
          case e: javax.net.ssl.SSLHandshakeException => // for PVC with ssl errors
            handleSSLHandShakeException(e)
          case e: Throwable => throw e
        }
    }
    response
  }

  def handleSSLHandShakeException(e: Exception): HttpResponse[String] = {
    val sslMSG = "ALERT: DROPPING BACK TO UNSAFE SSL: SSL handshake errors were detected, allowing unsafe " +
      "ssl. If this is unexpected behavior, validate your ssl certs."
    logger.log(Level.WARN, sslMSG)
    if (unsafeSSLErrorCount == 0) { //Check for 1st occurrence of SSL Handshake error.
      setUnsafeSSLErrorCount(unsafeSSLErrorCount + 1)
      setAllowUnsafeSSL(true)
      getResponse
    } else {
      logger.log(Level.ERROR, e)
      throw new Exception(sslMSG)
    }
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
  private def extrapolateSupportedStructure(rawDF: DataFrame): DataFrame = {
    val resultDFFieldNames = rawDF.schema.fieldNames
    if (!resultDFFieldNames.contains(apiMeta.dataframeColumn) && apiMeta.dataframeColumn != "*") { // if known API but return column doesn't exist
      val asDFErrMsg = s"The API endpoint is not returning the " +
        s"expected structure, column ${apiMeta.dataframeColumn} is expected and is not present in the dataframe.\nIf this module " +
        s"references data that does not exist or to which the Overwatch account does have access, please remove the " +
        s"scope. For example: if you have 'pools' scope enabled but there are no pools or Overwatch PAT doesn't " +
        s"have access to any pools, this scope must be removed."
      logger.log(Level.ERROR, asDFErrMsg)
      throw new Exception(asDFErrMsg)
    } else if (apiMeta.dataframeColumn == "*") { //Selecting all of the column.
      rawDF
    }
    else { //Selecting specific columns as per the metadata.
      rawDF.select(explode(col(apiMeta.dataframeColumn)).alias(apiMeta.dataframeColumn)).select(col(apiMeta.dataframeColumn + ".*"))
    }
  }

  /**
   * Converting the API response to Dataframe.
   *
   * @return Dataframe which is created from the API response.
   */
  def asDF(): DataFrame = {
    var apiResultDF: DataFrame = null;
    if (_apiResponseArray.size == 0 && !apiMeta.storeInTempLocation) { //If response contains no Data.
      val errMsg = s"API CALL Resulting DF is empty BUT no errors detected, progressing module. " +
        s"Details Below:\n$buildGenericErrorMessage"
      throw new ApiCallEmptyResponse(errMsg, true)
    } else if (_apiResponseArray.size != 0 && successTempPath.isEmpty) { //If API response don't have pagination/volume of response is not huge then we directly convert the response which is in-memory to spark DF.
      apiResultDF = spark.read.json(Seq(_apiResponseArray.toString).toDS())
    } else if (apiMeta.storeInTempLocation && successTempPath.nonEmpty) { //Read the response from the Temp location/Disk and convert it to Dataframe.
      apiResultDF = try {
        spark.read.json(successTempPath.get)
      } catch {
        case e: AnalysisException if e.getMessage().contains("Path does not exist") => spark.emptyDataFrame
      }

    }

    if (emptyDFCheck(apiResultDF)) {
      val errMsg =
        s"""API CALL Resulting DF is empty BUT no errors detected, progressing module.
           |Details Below:\n$buildGenericErrorMessage""".stripMargin
      logger.error(errMsg)
      spark.emptyDataFrame
    }else {
      extrapolateSupportedStructure(apiResultDF)
    }
  }

  private def jsonQueryToApiErrorDetail(e: ApiCallFailure): String = {
    val mapper = new ObjectMapper()
    val jsonObject = mapper.readTree(jsonQuery);
    val clusterId = jsonObject.get("cluster_id").toString.replace("\"", "")
    val start_time = jsonObject.get("start_time").asLong()
    val end_time = jsonObject.get("end_time").asLong()
    val errorObj = mapper.readTree(e.getMessage);
    val newJsonObject = new JSONObject();
    newJsonObject.put("cluster_id", clusterId)
    newJsonObject.put("from_epoch", start_time)
    newJsonObject.put("until_epoch", end_time)
    newJsonObject.put("error", errorObj.get("error_code").toString.replace("\"", "") + " " + errorObj.get("message").toString.replace("\"", ""))
    newJsonObject.toString
  }

  /**
   * Checks the contains of the response and decide whether the response contains actual data or not.
   * @param apiResultDF
   * @return
   */
  private def emptyDFCheck(apiResultDF: DataFrame): Boolean = {
    if (apiResultDF.columns.length == 0) { //Check number of columns in result Dataframe
      true
    } else if (apiResultDF.columns.size == 1 && apiResultDF.columns.contains(apiMeta.paginationKey)) { //Check if only pagination key in present in the response
      true
    } else {
      false
    }
  }


  /**
   * Performs api calls in parallel.
   * @return
   */
  def executeMultiThread(accumulator: LongAccumulator): util.ArrayList[String] = {
    @tailrec def executeThreadedHelper(): util.ArrayList[String] = {
      val response = getResponse
      responseCodeHandler(response)
      _apiResponseArray.add(response.body)
      if (apiMeta.storeInTempLocation && successTempPath.nonEmpty) {
        accumulator.add(1)
        if (apiEnv.successBatchSize <= _apiResponseArray.size()) { //Checking if its right time to write the batches into persistent storage
          val responseFlag = PipelineFunctions.writeMicroBatchToTempLocation(successTempPath.get, _apiResponseArray.toString)
          if (responseFlag) { //Clearing the resultArray in-case of successful write
            setApiResponseArray(new util.ArrayList[String]())
          }
        }
      }
      if (_printFinalStatusFlag) {
        logger.log(Level.INFO, buildDetailMessage())
        setPrintFinalStatsFlag(false)
      }
      if (paginate(response.body)) executeThreadedHelper() else _apiResponseArray
    }
    try {
      executeThreadedHelper()
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
   * Performs the Api call.
   *
   * @return
   */
  def execute(): this.type = {
    @tailrec def executeHelper(): this.type = {
      val response = getResponse
      responseCodeHandler(response)
      _apiResponseArray.add(response.body)
      if (apiMeta.storeInTempLocation && successTempPath.nonEmpty) {
        if (apiEnv.successBatchSize <= _apiResponseArray.size()) { //Checking if its right time to write the batches into persistent storage
          val responseFlag = PipelineFunctions.writeMicroBatchToTempLocation(successTempPath.get, _apiResponseArray.toString)
          if (responseFlag) { //Clearing the resultArray in-case of successful write
            setApiResponseArray(new util.ArrayList[String]())
          }
        }
      }
      if (_printFinalStatusFlag) {
        logger.log(Level.INFO, buildDetailMessage())
        setPrintFinalStatsFlag(false)
      }
      if (paginate(response.body)) executeHelper() else this
    }
    try {
      executeHelper()
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

  /**
   * Function to make parallel API calls. Currently this functions supports only SqlQueryHistory and ClusterEvents
   * @param endpoint
   * @param jsonInput
   * @param config
   * @return
   */
  def makeParallelApiCalls(endpoint: String, jsonInput: Map[String, String], config: Config): String = {
    val tempEndpointLocation = endpoint.replaceAll("/","")
    val acc = sc.longAccumulator(tempEndpointLocation)

    val tmpSuccessPath = if(jsonInput.contains("tmp_success_path")) jsonInput.get("tmp_success_path").get
    else s"${config.tempWorkingDir}/${tempEndpointLocation}/${System.currentTimeMillis()}"

    val tmpErrorPath = if(jsonInput.contains("tmp_error_path")) jsonInput.get("tmp_error_path").get
    else s"${config.tempWorkingDir}/errors/${tempEndpointLocation}/${System.currentTimeMillis()}"

    var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
    var apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
    val apiResponseCounter = Collections.synchronizedList(new util.ArrayList[Int]())
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(config.apiEnv.threadPoolSize))
    val apiMetaFactoryObj = new ApiMetaFactory().getApiClass(endpoint)
    val dataFrame_column = apiMetaFactoryObj.dataframeColumn
    val parallelApiCallsParams = apiMetaFactoryObj.getParallelAPIParams(jsonInput)
    var startValue = parallelApiCallsParams.get("start_value").get.toLong
    val endValue = parallelApiCallsParams.get("end_value").get.toLong
    val incrementCounter = parallelApiCallsParams.get("increment_counter").get.toLong
    val finalResponseCount = parallelApiCallsParams.get("final_response_count").get.toLong

    while (startValue < endValue){
      val jsonQuery = apiMetaFactoryObj.getAPIJsonQuery(startValue, endValue, jsonInput)

      //call future
      val future = Future {
      val apiObj = ApiCallV2(
        config.apiEnv,
        endpoint,
        jsonQuery,
        tempSuccessPath = tmpSuccessPath
      ).executeMultiThread(acc)

        synchronized {
          apiObj.forEach(
            obj=>if(obj.contains(dataFrame_column)){
              apiResponseArray.add(obj)
            }
          )
          if (apiResponseArray.size() >= config.apiEnv.successBatchSize) {
            PipelineFunctions.writeMicroBatchToTempLocation(tmpSuccessPath, apiResponseArray.toString)
            apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
          }
        }
      }
      future.onComplete {
        case Success(_) =>
          apiResponseCounter.add(1)

        case Failure(e) =>
          if (e.isInstanceOf[ApiCallFailureV2]) {
            synchronized {
              apiErrorArray.add(e.getMessage)
              if (apiErrorArray.size() >= config.apiEnv.errorBatchSize) {
                PipelineFunctions.writeMicroBatchToTempLocation(tmpErrorPath, apiErrorArray.toString)
                apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
              }
            }
            logger.log(Level.ERROR, "Future failure message: " + e.getMessage, e)
          }
          apiResponseCounter.add(1)
      }
      startValue = startValue + incrementCounter
    }

    val timeoutThreshold = config.apiEnv.apiWaitingTime // 5 minutes
    var currentSleepTime = 0
    var accumulatorCountWhileSleeping = acc.value
    while (apiResponseCounter.size() < finalResponseCount && currentSleepTime < timeoutThreshold) {
      //As we are using Futures and running 4 threads in parallel, We are checking if all the treads has completed
      // the execution or not. If we have not received the response from all the threads then we are waiting for 5
      // seconds and again revalidating the count.
      if (currentSleepTime > 120000) //printing the waiting message only if the waiting time is more than 2 minutes.
      {
        println(
          s"""Waiting for other queued API Calls to complete; cumulative wait time ${currentSleepTime / 1000}
             |seconds; Api response yet to receive ${finalResponseCount - apiResponseCounter.size()}""".stripMargin)
      }
      Thread.sleep(5000)
      currentSleepTime += 5000
      if (accumulatorCountWhileSleeping < acc.value) { //new API response received while waiting.
        currentSleepTime = 0 //resetting the sleep time.
        accumulatorCountWhileSleeping = acc.value
      }
    }
    if (apiResponseCounter.size() != finalResponseCount) { // Checking whether all the api responses has been received or not.
      logger.log(Level.ERROR,
        s"""Unable to receive all the ${endpoint} api responses; Api response
           |received ${apiResponseCounter.size()};Api response not
           |received ${finalResponseCount - apiResponseCounter.size()}""".stripMargin)
      throw new Exception(
        s"""Unable to receive all the ${endpoint} api responses; Api response received
           |${apiResponseCounter.size()};
           |Api response not received ${finalResponseCount - apiResponseCounter.size()}""".stripMargin)
    }
    if (apiResponseArray.size() > 0) { //In case of response array didn't hit the batch-size as a
      // final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tmpSuccessPath, apiResponseArray.toString)
      apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())


    }
    if (apiErrorArray.size() > 0) { //In case of error array didn't hit the batch-size
      // as a final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tmpErrorPath, apiErrorArray.toString)
      apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
    }
    tmpSuccessPath
  }

}


