package com.databricks.labs.overwatch
import org.apache.log4j.{Level, Logger}

/**
 * Configuration for each API.
 */
trait ApiMeta {

  private val logger: Logger = Logger.getLogger(this.getClass)
  var paginationKey:String="";
  var paginationToken:String="";
  var dataframeColumns:String="";
  var apiCallType:String="";
  var microBatchSize = 50
  var tempLocation = "/local_disk0/OverWatch"
  var storeInTempLocation=false
  val apiV = "api/2.0"
  var encodedToken=false

}

/**
 * Factory class for api Metadata.
 */
class ApiMetaFactory{
  private val logger: Logger = Logger.getLogger(this.getClass)
  def getApiClass(_apiName:String): ApiMeta = {
    _apiName match {
       case "jobs/list" => new JobListApi
       case "clusters/list" => new ClusterListApi
       case "clusters/events" => new ClusterEventsApi
       case "dbfs/list" => new DbfsListApi
       case "instance-pools/list" => new InstancePoolsListApi
       case "instance-profiles/list" => new InstanceProfileListApi
       case "workspace/list" => new WorkspaceListApi
       case "sql/history/queries" => new SqlHistoryQueriesApi
      case _ => logger.log(Level.WARN, "API not configured, returning full dataset"); throw new Exception("API NOT SUPPORTED")
    }
  }
}

class SqlHistoryQueriesApi extends ApiMeta{
  paginationKey = "has_next_page"
  paginationToken = "next_page_token"
  dataframeColumns = "res"
  apiCallType = "GET"
  encodedToken=true
}

class WorkspaceListApi extends ApiMeta{
  dataframeColumns = "objects"
  apiCallType = "GET"
}

class InstanceProfileListApi extends ApiMeta{
  dataframeColumns = "instance_profiles"
  apiCallType = "GET"
}

class InstancePoolsListApi extends ApiMeta{
  dataframeColumns = "instance_pools"
  apiCallType = "GET"
}

class DbfsListApi extends ApiMeta{
  dataframeColumns = "files"
  apiCallType = "GET"
}


class ClusterListApi extends ApiMeta{
  dataframeColumns = "clusters"
  apiCallType = "GET"
}


class JobListApi extends ApiMeta{
  dataframeColumns = "jobs"
  apiCallType= "GET"


}

class ClusterEventsApi extends ApiMeta{
  paginationKey = "next_page"
  paginationToken = "next_page"
  dataframeColumns = "events"
  apiCallType = "POST"
  storeInTempLocation = true


}
