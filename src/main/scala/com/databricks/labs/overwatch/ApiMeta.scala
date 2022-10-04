package com.databricks.labs.overwatch

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.ApiEnv
import com.fasterxml.jackson.databind.JsonNode
import org.apache.log4j.{Level, Logger}
import scalaj.http.{Http, HttpRequest}

/**
 * Configuration for each API.
 */
trait ApiMeta {

  val logger: Logger = Logger.getLogger(this.getClass)
  protected var _paginationKey: String = _
  protected var _paginationToken: String = _
  protected var _dataframeColumn: String = "*"
  protected var _apiCallType: String = _
  protected var _storeInTempLocation = false
  protected var _apiV = "api/2.0"
  protected var _isDerivePaginationLogic = false
  protected var _apiEnv: ApiEnv = _
  protected var _apiName: String = _

  protected[overwatch] def apiName: String = _apiName
  protected[overwatch] def apiEnv: ApiEnv = _apiEnv
  protected[overwatch] def paginationKey: String = _paginationKey

  protected[overwatch] def paginationToken: String = _paginationToken

  protected[overwatch] def dataframeColumn: String = _dataframeColumn

  protected[overwatch] def apiCallType: String = _apiCallType

  protected[overwatch] def storeInTempLocation: Boolean = _storeInTempLocation

  protected[overwatch] def apiV: String = _apiV

  protected[overwatch] def isDerivePaginationLogic: Boolean = _isDerivePaginationLogic

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

  private[overwatch] def setStoreInTempLocation(value: Boolean): this.type = {
    _storeInTempLocation = value
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

  private[overwatch] def getBaseRequest(): HttpRequest = {
    var request = Http(s"""${apiEnv.workspaceURL}/${apiV}/${apiName}""")
      .copy(headers = httpHeaders)
    if (apiEnv.proxyHost.nonEmpty && apiEnv.proxyPort.nonEmpty) {
      request = request.proxy(apiEnv.proxyHost.get, apiEnv.proxyPort.get)
      logger.log(Level.INFO, s"""Proxy has been set to IP: ${apiEnv.proxyHost.get}  PORT:${apiEnv.proxyPort.get}""")
    }
    if (apiEnv.proxyUserName.nonEmpty && apiEnv.proxyPasswordScope.nonEmpty && apiEnv.proxyPasswordKey.nonEmpty) {
      val password = dbutils.secrets.get(scope = apiEnv.proxyPasswordScope.get, apiEnv.proxyPasswordKey.get)
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
       |storeInTempLocation: ${storeInTempLocation}
       |apiV: ${apiV}
       |isDerivePaginationLogic: ${isDerivePaginationLogic}
       |""".stripMargin
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
      case "clusters/events" => new ClusterEventsApi
      case "dbfs/list" => new DbfsListApi
      case "instance-pools/list" => new InstancePoolsListApi
      case "instance-profiles/list" => new InstanceProfileListApi
      case "workspace/list" => new WorkspaceListApi
      case "sql/history/queries" => new SqlQueryHistoryApi
      case "clusters/resize" => new ClusterResizeApi
      case _ => logger.log(Level.WARN, "API not configured, returning full dataset"); throw new Exception("API NOT SUPPORTED")
    }
    logger.log(Level.INFO, meta.toString)
    meta
  }
}

class ClusterResizeApi extends ApiMeta {
  setApiCallType("POST")
}

class SqlQueryHistoryApi extends ApiMeta {
  setPaginationKey("has_next_page")
  setPaginationToken("next_page_token")
  setDataframeColumn("res")
  setApiCallType("GET")
  setStoreInTempLocation(true)
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


class JobListApi extends ApiMeta {
  setDataframeColumn("jobs")
  setApiCallType("GET")
  setPaginationKey("has_more")
  setIsDerivePaginationLogic(true)

  private[overwatch] override def hasNextPage(jsonObject: JsonNode): Boolean = {
    jsonObject.get(paginationKey).asBoolean()
  }

  private[overwatch] override def getPaginationLogic(jsonObject: JsonNode, requestMap: Map[String, String]): Map[String, String] = {
    val limit = Integer.parseInt(requestMap.get("limit").get)
    var offset = Integer.parseInt(requestMap.get("offset").get)
    val expand_tasks = requestMap.get("expand_tasks").get
    offset = offset + limit
    Map(
      "limit" -> s"${limit}",
      "expand_tasks" -> s"${expand_tasks}",
      "offset" -> s"${offset}"
    )
  }

}

class ClusterEventsApi extends ApiMeta {
  setPaginationKey("next_page")
  setPaginationToken("next_page")
  setDataframeColumn("events")
  setApiCallType("POST")
  setStoreInTempLocation(true)
}
