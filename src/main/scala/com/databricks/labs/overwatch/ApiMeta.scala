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
  protected var _dataframeColumns: String = "*"
  protected var _apiCallType: String = _
  protected var _storeInTempLocation = false
  protected val _apiV = "api/2.0"
  protected var _tuplePaginationObject = false

  def paginationKey: String = _paginationKey

  def paginationToken: String = _paginationToken

  def dataframeColumns: String = _dataframeColumns

  def apiCallType: String = _apiCallType

  def storeInTempLocation: Boolean = _storeInTempLocation

  def apiV: String = _apiV

  def tuplePaginationObject: Boolean = _tuplePaginationObject

  def getPaginationLogicForSingleObject(jsonObject: JsonNode): (String) = {
    jsonObject.get(this._paginationKey).toString
  }

  def getPaginationLogicForTuple(jsonObject: JsonNode): (String, String) = {
    (null, null) //Implement logic according to the API call
  }

  def hasNextPage(jsonObject: JsonNode): Boolean = {
    true
  }

  override def toString: String = {
    s"""API Meta paginationKey: ${paginationKey}
       |paginationToken: ${paginationToken}
       |dataframeColumns: ${dataframeColumns}
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
    _apiName match {
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
  }
}

class ClusterResizeApi extends ApiMeta {
  _apiCallType = "POST"
}

class SqlHistoryQueriesApi extends ApiMeta {
  _paginationKey = "has_next_page"
  _paginationToken = "next_page_token"
  _dataframeColumns = "res"
  _apiCallType = "GET"
  _tuplePaginationObject = true

  override def hasNextPage(jsonObject: JsonNode): Boolean = {
    jsonObject.get(this._paginationKey).asBoolean()
  }

  override def getPaginationLogicForTuple(jsonObject: JsonNode): (String, String) = {
    if (jsonObject.get(this._paginationKey).asBoolean()) { //Pagination key for sql/history/queries can return true or false
      val _jsonKey = "page_token"
      val _jsonValue = jsonObject.get(this._paginationToken).asText()
      (_jsonKey, _jsonValue)
    } else {
      (null, null)
    }
  }
}

class WorkspaceListApi extends ApiMeta {
  _dataframeColumns = "objects"
  _apiCallType = "GET"
}

class InstanceProfileListApi extends ApiMeta {
  _dataframeColumns = "instance_profiles"
  _apiCallType = "GET"
}

class InstancePoolsListApi extends ApiMeta {
  _dataframeColumns = "instance_pools"
  _apiCallType = "GET"
}

class DbfsListApi extends ApiMeta {
  _dataframeColumns = "files"
  _apiCallType = "GET"
}


class ClusterListApi extends ApiMeta {
  _dataframeColumns = "clusters"
  _apiCallType = "GET"
}


class JobListApi extends ApiMeta {
  _dataframeColumns = "jobs"
  _apiCallType = "GET"


}

class ClusterEventsApi extends ApiMeta {
  _paginationKey = "next_page"
  _paginationToken = "next_page"
  _dataframeColumns = "events"
  _apiCallType = "POST"
  _storeInTempLocation = true
}
