package com.databricks.labs.overwatch.api

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.ApiEnv
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.json.JSONObject
import scalaj.http.{Http, HttpRequest, HttpResponse}

/**
 * Configuration for each API.
 */
trait ApiMeta {

  val logger: Logger = Logger.getLogger(this.getClass)
  protected var _paginationKey: String = ""
  protected var _paginationToken: String = _
  protected var _dataframeColumn: String = "*"
  protected var _apiCallType: String = _
  protected var _batchPersist  = false
  protected var _apiV = "api/2.0"
  protected var _isDerivePaginationLogic = false
  protected var _apiEnv: ApiEnv = _
  protected var _apiName: String = _
  protected var _emptyResponseColumn: String = ""

  protected[overwatch] def apiName: String = _apiName
  protected[overwatch] def apiEnv: ApiEnv = _apiEnv
  protected[overwatch] def paginationKey: String = _paginationKey

  protected[overwatch] def paginationToken: String = _paginationToken

  protected[overwatch] def dataframeColumn: String = _dataframeColumn

  protected[overwatch] def apiCallType: String = _apiCallType

  protected[overwatch] def batchPersist : Boolean = _batchPersist

  protected[overwatch] def apiV: String = _apiV

  protected[overwatch] def isDerivePaginationLogic: Boolean = _isDerivePaginationLogic

  protected[overwatch] def emptyResponseColumn: String = _emptyResponseColumn

  private[overwatch] def setApiV(value: String): this.type = {
    _apiV = value
    this
  }

  private[overwatch] def setApiName(value: String): this.type = {
    _apiName = value
    this
  }
  private[overwatch] def setApiEnv(value: ApiEnv): this.type = {
    _apiEnv = value
    this
  }

  private[overwatch] def setBatchPersist(value: Boolean): this.type = {
    _batchPersist = value
    this
  }

  private[overwatch] def setIsDerivePaginationLogic(value: Boolean): this.type = {
    _isDerivePaginationLogic = value
    this
  }

  private[overwatch] def setApiCallType(value: String): this.type = {
    _apiCallType = value
    this
  }

  private[overwatch] def setDataframeColumn(value: String): this.type = {
    _dataframeColumn = value
    this
  }

  private[overwatch] def setPaginationKey(value: String): this.type = {
    _paginationKey = value
    this
  }

  private[overwatch] def setPaginationToken(value: String): this.type = {
    _paginationToken = value
    this
  }


  private[overwatch] def getPaginationLogicForSingleObject(jsonObject: JsonNode): (String) = {
    jsonObject.get(this._paginationKey).toString
  }

  private[overwatch] def getPaginationLogic(jsonObject: JsonNode, requestMap: Map[String, String]): Map[String, String] = {
    null
  }

  private[overwatch] def hasNextPage(jsonObject: JsonNode): Boolean = {
    true
  }

  private[overwatch] def setEmptyResponseColumn(value: String): this.type = {
    _emptyResponseColumn = value
    this
  }

  /**
   * Function generates a basic HttpRequest based on the ApiMeta.
   * @return
   */
  private[overwatch] def getBaseRequest(): HttpRequest = {
    var request = Http(s"""${apiEnv.workspaceURL}/${apiV}/${apiName}""")
      .copy(headers = httpHeaders)
    if (apiEnv.proxyHost.nonEmpty && apiEnv.proxyPort.nonEmpty) {
      request = request.proxy(apiEnv.proxyHost.get, apiEnv.proxyPort.get)
      logger.log(Level.INFO, s"""Proxy has been set to IP: ${apiEnv.proxyHost.get}  PORT:${apiEnv.proxyPort.get}""")
    }
    if (apiEnv.proxyUserName.nonEmpty && apiEnv.proxyPasswordScope.nonEmpty && apiEnv.proxyPasswordKey.nonEmpty) {
      val password = try {
        dbutils.secrets.get(scope = apiEnv.proxyPasswordScope.get, apiEnv.proxyPasswordKey.get)
      } catch {
        case e: IllegalArgumentException if e.getMessage.contains("Secret does not exist") =>
          val failMsg =
            s"""Error getting proxy secret details using:
               |ProxyPasswordScope: ${apiEnv.proxyPasswordScope}
               |ProxyPasswordKey: ${apiEnv.proxyPasswordKey}
               |""".stripMargin
          throw new Exception(failMsg, e)
      }
      request = request.proxyAuth(apiEnv.proxyUserName.get, password)
      logger.log(Level.INFO, s"""Proxy UserName set to IP: ${apiEnv.proxyUserName.get}  scope:${apiEnv.proxyPasswordScope.get} key:${apiEnv.proxyPasswordKey.get}""")
    }
    request
  }

  /**
   * Headers for the API call.
   */
  private def httpHeaders = Seq[(String, String)](
    ("Content-Type", "application/json"),
    ("Charset", "UTF-8"),
    ("User-Agent", s"databricks-labs-overwatch-${apiEnv.packageVersion}"),
    ("Authorization", s"Bearer ${apiEnv.rawToken}")
  )

  override def toString: String = {
    s"""API Meta paginationKey: ${paginationKey}
       |paginationToken: ${paginationToken}
       |dataframeColumns: ${dataframeColumn}
       |apiCallType: ${apiCallType}
       |storeInTempLocation: ${batchPersist}
       |apiV: ${apiV}
       |isDerivePaginationLogic: ${isDerivePaginationLogic}
       |""".stripMargin
  }

  private[overwatch] def getAPIJsonQuery(startValue: Long, endValue: Long, jsonInput: Map[String, String]): Map[String, String] = {
    logger.log(Level.INFO, s"""Needs to be override for specific API for manipulating the input JSON Query""")
    Map[String, String]()
  }

  private[overwatch] def getParallelAPIParams(jsonInput: Map[String, String]): Map[String, String] = {
    logger.log(Level.INFO, s"""Needs to be override for specific API for intializing Parallel API call function""")
    Map[String, String]()
  }

  /**
   * Function will add the meta info to the api response.
   *
   * @param response
   * @param jsonQuery
   * @param queryMap
   * @return a string containing the api response and the meta for the api call.
   */
  private[overwatch] def enrichAPIResponse(response: HttpResponse[String], jsonQuery: String, queryMap: Map[String, String]): String = {
    val filter: String = if (apiCallType.equals("POST")) jsonQuery else {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.writeValueAsString(queryMap)
    }
    val jsonObject = new JSONObject();
    val apiTraceabilityMeta = new JSONObject();
    apiTraceabilityMeta.put("endPoint", apiName)
    apiTraceabilityMeta.put("type", apiCallType)
    apiTraceabilityMeta.put("apiVersion", apiV)
    apiTraceabilityMeta.put("responseCode", response.code)
    apiTraceabilityMeta.put("batchKeyFilter", filter)
    jsonObject.put("rawResponse", response.body.trim)
    jsonObject.put("apiTraceabilityMeta", apiTraceabilityMeta)
    jsonObject.toString
  }

}

/**
 * Factory class for api Metadata.
 */
class ApiMetaFactory {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def getApiClass(_apiName: String): ApiMeta = {

    val meta = _apiName match {
      case "jobs/list" => new JobListApi
      case "clusters/list" => new ClusterListApi
      case "clusters/get" => new ClusterGetApi
      case "clusters/events" => new ClusterEventsApi
      case "dbfs/list" => new DbfsListApi
      case "instance-pools/list" => new InstancePoolsListApi
      case "instance-profiles/list" => new InstanceProfileListApi
      case "workspace/list" => new WorkspaceListApi
      case "sql/history/queries" => new SqlQueryHistoryApi
      case "clusters/resize" => new ClusterResizeApi
      case "jobs/runs/get" => new JobRunGetApi
      case "dbfs/search-mounts" => new DbfsSearchMountsApi
      case "jobs/runs/list" => new JobRunsApi
      case "libraries/all-cluster-statuses" => new ClusterLibraryApi
      case "policies/clusters/list" => new ClusterPolicesApi
      case "token/list" => new TokensApi
      case "global-init-scripts" => new GlobalInitsScriptsApi
      case "sql/warehouses" => new WarehouseListApi
      case _ => new UnregisteredApi
    }
    logger.log(Level.INFO, meta.toString)
    meta
  }
}

trait CommonApiMethods {
  def getParallelAPIParams(jsonInput: Map[String, String]): Map[String, String] = {
    Map(
      "start_value" -> s"""${jsonInput.get("start_value").get.toLong}""",
      "end_value" -> s"""${jsonInput.get("end_value").get.toLong}""",
      "increment_counter" -> s"""${jsonInput.get("increment_counter").get.toLong}""",
      "final_response_count" -> s"""${jsonInput.get("final_response_count").get.toLong}"""
    )
  }
}

class UnregisteredApi extends ApiMeta {
  setApiCallType("GET")
}

class DbfsSearchMountsApi extends ApiMeta{
  setApiCallType("GET")
  setDataframeColumn("mounts")
}

class JobRunGetApi extends ApiMeta{
  setApiCallType("GET")
}


class ClusterResizeApi extends ApiMeta {
  setApiCallType("POST")
}

class SqlQueryHistoryApi extends ApiMeta with CommonApiMethods {
  setPaginationKey("has_next_page")
  setPaginationToken("next_page_token")
  setDataframeColumn("res")
  setApiCallType("GET")
  setBatchPersist(true)
  setIsDerivePaginationLogic(true)

  private[overwatch] override def hasNextPage(jsonObject: JsonNode): Boolean = {
    jsonObject.get(paginationKey).asBoolean()
  }

  private[overwatch] override def getPaginationLogic(jsonObject: JsonNode, requestMap: Map[String, String]): Map[String, String] = {
//    val jsonPageFilterKey = "page_token"
    val _jsonValue = jsonObject.get(paginationToken).asText()
//    logger.info(s"DEBUG - NEXT_PAGE_TOKEN = ${_jsonValue}")
    requestMap.filterNot { case (k, _) => k.toLowerCase.startsWith("filter_by")} ++ Map(s"page_token" -> s"${_jsonValue}")
  }

  private[overwatch] override def getAPIJsonQuery(startValue: Long, endValue: Long, jsonInput: Map[String, String]): Map[String, String] = {
    val (startTime, endTime) = if ((endValue - startValue)/(1000*60*60) > 1) {
      (startValue,
        startValue+(1000*60*60))
    }
    else{
      (startValue,
        endValue)
    }
    Map(
      "max_results" -> "50",
      "include_metrics" -> "true",
      "filter_by.query_start_time_range.start_time_ms" ->  s"$startTime",
      "filter_by.query_start_time_range.end_time_ms" ->  s"$endTime"
    )
  }
}

class WorkspaceListApi extends ApiMeta {
  setDataframeColumn("objects")
  setApiCallType("GET")
}

class InstanceProfileListApi extends ApiMeta {
  setDataframeColumn("instance_profiles")
  setApiCallType("GET")
}

class InstancePoolsListApi extends ApiMeta {
  setDataframeColumn("instance_pools")
  setApiCallType("GET")
}

class DbfsListApi extends ApiMeta {
  setDataframeColumn("files")
  setApiCallType("GET")
}


class ClusterListApi extends ApiMeta {
  setDataframeColumn("clusters")
  setApiCallType("GET")
}

class ClusterGetApi extends ApiMeta with CommonApiMethods{
  setDataframeColumn("cluster_name")
  setApiCallType("GET")
  setBatchPersist(true)
  private[overwatch] override def getAPIJsonQuery(startValue: Long, endValue: Long, jsonInput: Map[String, String]): Map[String, String] = {
    val clusterIDs = jsonInput.get("cluster_ids").get.split(",").map(_.trim).toArray
    Map("cluster_id" -> s"""${clusterIDs(startValue.toInt)}"""
    )
  }
}

class JobListApi extends ApiMeta {
  setDataframeColumn("jobs")
  setApiCallType("GET")
  setPaginationKey("has_more")
  setPaginationToken("next_page_token")
  setIsDerivePaginationLogic(true)

  private[overwatch] override def hasNextPage(jsonObject: JsonNode): Boolean = {
    jsonObject.get(paginationKey).asBoolean()
  }

  private[overwatch] override def getPaginationLogic(jsonObject: JsonNode, requestMap: Map[String, String]): Map[String, String] = {
    val _jsonValue = jsonObject.get(paginationToken).asText()
    requestMap ++ Map(s"page_token" -> s"${_jsonValue}")
  }
}

class ClusterEventsApi extends ApiMeta with CommonApiMethods{
  setPaginationKey("next_page")
  setPaginationToken("next_page")
  setDataframeColumn("events")
  setApiCallType("POST")
  setBatchPersist(true)

  private[overwatch] override def getAPIJsonQuery(startValue: Long, endValue: Long,jsonInput: Map[String, String]): Map[String, String] = {
    val clusterIDs = jsonInput.get("cluster_ids").get.split(",").map(_.trim).toArray
    val startTime = jsonInput.get("start_time").get.toLong
    val endTime = jsonInput.get("end_time").get.toLong

    Map("cluster_id" -> s"""${clusterIDs(startValue.toInt)}""",
      "start_time" -> s"""${startTime}""",
      "end_time" -> s"""${endTime}""",
      "limit" -> "500"
    )
  }
}

class JobRunsApi extends ApiMeta {
  setDataframeColumn("runs")
  setApiCallType("GET")
  setPaginationKey("has_more")
  setPaginationToken("next_page_token")
  setIsDerivePaginationLogic(true)
  setBatchPersist(true)

  private[overwatch] override def hasNextPage(jsonObject: JsonNode): Boolean = {
    jsonObject.get(paginationKey).asBoolean()
  }

  private[overwatch] override def getPaginationLogic(jsonObject: JsonNode, requestMap: Map[String, String]): Map[String, String] = {
    val _jsonValue = jsonObject.get(paginationToken).asText()
    requestMap ++ Map(s"page_token" -> s"${_jsonValue}")
  }
}

class ClusterLibraryApi extends ApiMeta {
  setDataframeColumn("statuses")
  setApiCallType("GET")
}

class ClusterPolicesApi extends ApiMeta {
  setDataframeColumn("policies")
  setApiCallType("GET")
  setEmptyResponseColumn("total_count")
}

class TokensApi extends  ApiMeta {
  setDataframeColumn("token_infos")
  setApiCallType("GET")
}

class GlobalInitsScriptsApi extends ApiMeta {
  setDataframeColumn("scripts")
  setApiCallType("GET")
}

class WarehouseListApi extends ApiMeta {
  setDataframeColumn("warehouses")
  setApiCallType("GET")
}
