package com.databricks.labs.overwatch

import com.fasterxml.jackson.databind.JsonNode
import org.apache.log4j.{Level, Logger}
import com.fasterxml.jackson.databind.JsonNode

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
  protected val _apiV = "api/2.0"
  protected var _tuplePaginationObject = false

  def paginationKey: String = _paginationKey

  def paginationToken: String = _paginationToken

  def dataframeColumn: String = _dataframeColumn

  def apiCallType: String = _apiCallType

  def storeInTempLocation: Boolean = _storeInTempLocation

  def apiV: String = _apiV

  def tuplePaginationObject: Boolean = _tuplePaginationObject

  private[overwatch] def setStoreInTempLocation(value: Boolean): this.type = {
    _storeInTempLocation = value
    this
  }

  private[overwatch] def setTuplePaginationObject(value: Boolean): this.type = {
    _tuplePaginationObject = value
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

  private[overwatch] def getPaginationLogicForTuple(jsonObject: JsonNode): (String, String) = {
    (null, null) //Implement logic according to the API call
  }

  private[overwatch] def hasNextPage(jsonObject: JsonNode): Boolean = {
    true
  }

  override def toString: String = {
    s"""API Meta paginationKey: ${paginationKey}
       |paginationToken: ${paginationToken}
       |dataframeColumns: ${dataframeColumn}
       |apiCallType: ${apiCallType}
       |storeInTempLocation: ${storeInTempLocation}
       |apiV: ${apiV}
       |tuplePaginationObject: ${tuplePaginationObject}
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
      case "sql/history/queries" => new SqlHistoryQueriesApi
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

class SqlHistoryQueriesApi extends ApiMeta {
  setPaginationKey("has_next_page")
  setPaginationToken("next_page_token")
  setDataframeColumn("res")
  setApiCallType("GET")
  setTuplePaginationObject(true)

  private[overwatch]  override def hasNextPage(jsonObject: JsonNode): Boolean = {
    jsonObject.get(paginationKey).asBoolean()
  }

  private[overwatch] override def getPaginationLogicForTuple(jsonObject: JsonNode): (String, String) = {
    if (jsonObject.get(paginationKey).asBoolean()) { //Pagination key for sql/history/queries can return true or false
      val _jsonKey = "page_token"
      val _jsonValue = jsonObject.get(paginationToken).asText()
      (_jsonKey, _jsonValue)
    } else {
      (null, null)
    }
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
}

class ClusterEventsApi extends ApiMeta {
  setPaginationKey("next_page")
  setPaginationToken("next_page")
  setDataframeColumn("events")
  setApiCallType("POST")
  setStoreInTempLocation(true)
}
