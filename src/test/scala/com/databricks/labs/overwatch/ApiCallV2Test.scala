package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils.ApiEnv
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class ApiCallV2Test extends AnyFunSpec with BeforeAndAfterAll{

  var apiEnv:ApiEnv=null
  val pkgVr = "6.5"

  override def beforeAll(): Unit = {
    System.setProperty("baseUrl", "" ) //Workspace URL
    System.setProperty("DATABRICKS_TOKEN", "" ) //PAT token
    apiEnv = ApiEnv(false, System.getProperty("baseUrl"), System.getProperty("DATABRICKS_TOKEN"), pkgVr)
  }

  describe("API v2 test "){
      it("Consume data from clusters/events API"){
        val jsonQuery = """{"cluster_id":"0804-220509-stead130","limit":500}"""
        val endPoint = "clusters/events"
        val t1 = System.nanoTime
        assert(ApiCallV2(apiEnv,endPoint,jsonQuery).execute().asDF()!=null)
        val duration = (System.nanoTime - t1) / 1e9d
        print(duration)
      }

     it("Consume data from jobs/list API"){
      val endPoint = "jobs/list"
       assert(ApiCallV2(apiEnv,endPoint).execute().asDF()!=null)
    }

    it("Consume data from clusters/list API"){
      val endPoint = "clusters/list"
      assert(ApiCallV2(apiEnv,endPoint).execute().asDF() !=null)
    }
    it("Consume data from dbfs/list API"){
      val endPoint = "dbfs/list"
      val jsonQuery = """{"path":"/tmp"}"""
      assert(ApiCallV2(apiEnv,endPoint,jsonQuery).execute().asDF() !=null)
    }

    it("Consume data from instance-pools/list API"){
      val endPoint = "instance-pools/list"
      assert(ApiCallV2(apiEnv,endPoint).execute() !=null)
    }

    it("Consume data from instance-profiles/list API"){
      val endPoint = "instance-profiles/list"
      assert( ApiCallV2(apiEnv,endPoint).execute().asDF() !=null)
    }

    it("Consume data from workspace/list API"){
      val endPoint = "workspace/list"
      val jsonQuery =s"""{"path":"/Users"}"""
      assert(ApiCallV2(apiEnv,endPoint,jsonQuery).execute().asDF() !=null)
    }

    it("Consume data from sql/history/queries API"){
        val endPoint = "sql/history/queries"
      assert(ApiCallV2(apiEnv,endPoint).execute().asDF() !=null)
    }

  }

  describe("OLD api Test"){

    it("test clusters/events api"){
      val query = Map("cluster_id" -> "0804-220509-stead130",
        "limit" -> 500
      )
      val t1 = System.nanoTime
      assert( ApiCall("clusters/events", apiEnv, Some(query)).executePost().asDF !=null)
      val duration = (System.nanoTime - t1) / 1e9d
      print(duration)
    }

    it("Consume data from jobs/list API"){
      assert( ApiCall("jobs/list", apiEnv).executeGet().asDF !=null)
    }
    it("Consume data from clusters/list API"){
      assert( ApiCall("clusters/list", apiEnv).executeGet().asDF !=null)
    }
    it("Consume data from clusters/events API"){
      val query = Map("cluster_id" -> "0804-220509-stead130",
        "limit" -> 500
      )
      assert( ApiCall("clusters/events", apiEnv,Some(query)).executePost().asDF !=null)
    }
    it("Consume data from dbfs/listAPI"){
      val query = Map("path" -> "/Users"
           )
      assert( ApiCall("dbfs/list", apiEnv,Some(query)).executeGet().asDF !=null)
    }
    it("Consume data from workspace/list API"){
      val query = Map("path" -> "/Users"
           )
      assert(ApiCall("workspace/list", apiEnv,Some(query)).executeGet().asDF !=null)
    }
    it("Consume data from instance-pools/list API"){
      assert(ApiCall("instance-pools/list", apiEnv).executeGet().asDF !=null)
    }
    it("Consume data from instance-profiles/list API"){
      assert(ApiCall("instance-profiles/list", apiEnv).executeGet().asDF !=null)
    }


  }
  describe("API comparison test"){
    it("comparison test for clusters/events API"){
      val jsonQuery = """{"cluster_id":"0804-220509-stead130","limit":500}"""
      val endPoint = "clusters/events"
      val query = Map("cluster_id" -> "0804-220509-stead130",
        "limit" -> 500
      )
      val oldAPI=ApiCall(endPoint, apiEnv, Some(query)).executePost().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint,jsonQuery).execute().asDF
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
    it("comparison test for jobs/list API"){
      val endPoint = "jobs/list"
      val oldAPI=ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint).execute().asDF()
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
    it("comparison test for clusters/list API"){
      val endPoint = "clusters/list"
      val oldAPI=ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint).execute().asDF()
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
    it("comparison test for dbfs/list API"){
      val endPoint = "dbfs/list"
      val jsonQuery = """{"path":"/tmp"}"""
      val query = Map("path" -> "/tmp"
      )
      val oldAPI= ApiCall(endPoint, apiEnv,Some(query)).executeGet().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint,jsonQuery).execute().asDF()
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
    it("comparison test for instance-pools/list API"){
      val endPoint = "instance-pools/list"
      val oldAPI=ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint).execute().asDF()
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
    it("comparison test for instance-profiles/list API"){
      val endPoint = "instance-profiles/list"
      val oldAPI=ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint).execute().asDF()
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
    it("comparison test for workspace/list API"){
      val endPoint = "workspace/list"
      val jsonQuery = """{"path":"/Users"}"""
      val query = Map("path" -> "/Users"
      )
      val oldAPI= ApiCall(endPoint, apiEnv,Some(query)).executeGet().asDF
      val newAPI=ApiCallV2(apiEnv,endPoint,jsonQuery).execute().asDF()
      assert(oldAPI.count()==newAPI.count() && oldAPI.except(newAPI).count()==0 && newAPI.except(oldAPI).count()==0)
    }
  }




}
