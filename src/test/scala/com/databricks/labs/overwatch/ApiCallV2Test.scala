package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.ApiCallV2.sc
import com.databricks.labs.overwatch.pipeline.PipelineFunctions
import com.databricks.labs.overwatch.utils.{ApiCallFailureV2, ApiEnv}
import org.scalatest.{BeforeAndAfterAll, Ignore, Tag}
import org.scalatest.funspec.AnyFunSpec

import java.util
import java.util.Collections
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

@Ignore
class ApiCallV2Test extends AnyFunSpec with BeforeAndAfterAll {

  var apiEnv: ApiEnv = null
  val pkgVr = "6.5"

  override def beforeAll(): Unit = {
    System.setProperty("baseUrl", "") //Workspace URL
    System.setProperty("DATABRICKS_TOKEN", "") //PAT token
    apiEnv = ApiEnv(false, System.getProperty("baseUrl"), System.getProperty("DATABRICKS_TOKEN"), pkgVr)
  }

  describe("API v2 test ") {

    it("Consume data from clusters/events API" )  {
      val endPoint = "clusters/events"
      val t1 = System.nanoTime
      val query = Map("cluster_id" -> "0804-220509-stead130",
        "limit"->"500"
      )
      ApiCallV2(apiEnv, endPoint, query).execute()
      val duration = (System.nanoTime - t1) / 1e9d
      print(duration)
    }

    it("Consume data from jobs/list API") {
      val endPoint = "jobs/list"
      val query = Map(
        "limit" -> "25",
        "expand_tasks" -> "true",
        "offset" -> "0"
      )
      ApiCallV2(apiEnv, endPoint, query, 2.0).execute().asDF()
    }

    it("Consume data from clusters/list API") {
      val endPoint = "clusters/list"
      assert(ApiCallV2(apiEnv, endPoint).execute().asDF() != null)
    }
    it("Consume data from dbfs/list API") {
      val endPoint = "dbfs/list"
      val query = Map("path" -> "/tmp"
      )
      assert(ApiCallV2(apiEnv, endPoint, query).execute().asDF() != null)
    }

    it("Consume data from instance-pools/list API") {
      val endPoint = "instance-pools/list"
      assert(ApiCallV2(apiEnv, endPoint).execute() != null)
    }

    it("Consume data from instance-profiles/list API") {
      val endPoint = "instance-profiles/list"
      assert(ApiCallV2(apiEnv, endPoint).execute().asDF() != null)
    }

    it("Consume data from workspace/list API") {
      val endPoint = "workspace/list"
      val query = Map("path" -> "/Users"
      )
      //ApiCallV2(apiEnv, endPoint, query).execute().asDF().show(false)
      assert(ApiCallV2(apiEnv, endPoint, query).execute().asDF() != null)
    }

    it("Consume data from sql/history/queries API") {
      val endPoint = "sql/history/queries"
      val query = Map("max_results" -> "100",
        "include_metrics" -> "true",
        "filter_by.query_start_time_range.start_time_ms" -> "1652775426000",
        "filter_by.query_start_time_range.end_time_ms" -> "1655453826000"
      )
      assert(ApiCallV2(apiEnv, endPoint, query).execute().asDF() != null)
    }

    it("Consume data from clusters/resize API") {
      val endpoint = "clusters/resize"
      val query = Map(
        "cluster_id" -> "0511-074343-c456edtd",
        "num_workers" -> "4"
      )
      ApiCallV2(apiEnv, endpoint, query).execute()
    }


  }

  describe("OLD api Test") {

    it("test clusters/events api") {
      val query = Map("cluster_id" -> "0804-220509-stead130",
        "limit" -> 500
      )
      val t1 = System.nanoTime
      assert(ApiCall("clusters/events", apiEnv, Some(query)).executePost().asDF != null)
      val duration = (System.nanoTime - t1) / 1e9d
      print(duration)
    }

    it("Consume data from jobs/list API") {
      assert(ApiCall("jobs/list", apiEnv).executeGet().asDF != null)
    }
    it("Consume data from clusters/list API") {
      assert(ApiCall("clusters/list", apiEnv).executeGet().asDF != null)
    }
    it("Consume data from clusters/events API") {
      val query = Map("cluster_id" -> "0804-220509-stead130",
        "limit" -> 500
      )
      assert(ApiCall("clusters/events", apiEnv, Some(query)).executePost().asDF != null)
    }
    it("Consume data from dbfs/listAPI") {
      val query = Map("path" -> "/Users"
      )
      assert(ApiCall("dbfs/list", apiEnv, Some(query)).executeGet().asDF != null)
    }
    it("Consume data from workspace/list API") {
      val query = Map("path" -> "/Users"
      )
      assert(ApiCall("workspace/list", apiEnv, Some(query)).executeGet().asDF != null)
    }
    it("Consume data from instance-pools/list API") {
      assert(ApiCall("instance-pools/list", apiEnv).executeGet().asDF != null)
    }
    it("Consume data from instance-profiles/list API") {
      assert(ApiCall("instance-profiles/list", apiEnv).executeGet().asDF != null)
    }


  }
  describe("API comparison test") {
    it("comparison test for jobs/list API") {
      val endPoint = "jobs/list"
      val oldAPI = ApiCall(endPoint, apiEnv).executeGet().asDF
       val query = Map(
         "limit"->"2",
         "expand_tasks"->"true",
         "offset"->"0"
       )
       val newAPI =  ApiCallV2(apiEnv, endPoint,query,2.1).execute().asDF()
       println(oldAPI.count()+"old api count")
       println(newAPI.count()+"new api count")
       assert(oldAPI.count() == newAPI.count() && oldAPI.except(newAPI).count() == 0 && newAPI.except(oldAPI).count() == 0)

    }
    it("comparison test for clusters/list API") {
      val endPoint = "clusters/list"
      val oldAPI = ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI = ApiCallV2(apiEnv, endPoint).execute().asDF()
      assert(oldAPI.count() == newAPI.count() && oldAPI.except(newAPI).count() == 0 && newAPI.except(oldAPI).count() == 0)
    }
    it("comparison test for dbfs/list API") {
      val endPoint = "dbfs/list"
      val query = Map("path" -> "/tmp"
      )
      val oldAPI = ApiCall(endPoint, apiEnv, Some(query)).executeGet().asDF
      val newAPI = ApiCallV2(apiEnv, endPoint, query).execute().asDF()
      assert(oldAPI.count() == newAPI.count() && oldAPI.except(newAPI).count() == 0 && newAPI.except(oldAPI).count() == 0)
    }
    it("comparison test for instance-pools/list API") {
      val endPoint = "instance-pools/list"
      val oldAPI = ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI = ApiCallV2(apiEnv, endPoint).execute().asDF()
      assert(oldAPI.count() == newAPI.count() && oldAPI.except(newAPI).count() == 0 && newAPI.except(oldAPI).count() == 0)
    }
    it("comparison test for instance-profiles/list API") {
      val endPoint = "instance-profiles/list"
      val oldAPI = ApiCall(endPoint, apiEnv).executeGet().asDF
      val newAPI = ApiCallV2(apiEnv, endPoint).execute().asDF()
      assert(oldAPI.count() == newAPI.count() && oldAPI.except(newAPI).count() == 0 && newAPI.except(oldAPI).count() == 0)
    }
    it("comparison test for workspace/list API") {
      val endPoint = "workspace/list"
      val query = Map("path" -> "/Users"
      )
      val oldAPI = ApiCall(endPoint, apiEnv, Some(query)).executeGet().asDF
      val newAPI = ApiCallV2(apiEnv, endPoint, query).execute().asDF()
      assert(oldAPI.count() == newAPI.count() && oldAPI.except(newAPI).count() == 0 && newAPI.except(oldAPI).count() == 0)
    }
    it("test multithreading") {
      val endPoint = "clusters/list"
      val clusterIDsDf = ApiCallV2(apiEnv, endPoint).execute().asDF().select("cluster_id")
      clusterIDsDf.show(false)
      val clusterIDs = clusterIDsDf.collect()
      val finalResponseCount = clusterIDs.length
      var responseCounter = 0
      var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
      var apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
      implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(apiEnv.threadPoolSize))
      val tmpClusterEventsSuccessPath = ""
      val tmpClusterEventsErrorPath = ""
      val accumulator = sc.longAccumulator("ClusterEventsAccumulator")
      for (i <- clusterIDs.indices) {
       val jsonQuery = Map("cluster_id" -> s"""${clusterIDs(i).get(0)}""",
          "start_time"->"1052775426000",
          "end_time"->"1655453826000",
          "limit" -> "500"
        )
        println(jsonQuery)
        val future = Future {
          val apiObj = ApiCallV2(apiEnv, "clusters/events", jsonQuery, tmpClusterEventsSuccessPath,accumulator).executeMultiThread()
          synchronized {
            apiResponseArray.addAll(apiObj)
            if (apiResponseArray.size() >= apiEnv.successBatchSize) {
              PipelineFunctions.writeMicroBatchToTempLocation(tmpClusterEventsSuccessPath, apiResponseArray.toString)
              apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
            }
          }

        }
        future.onComplete {
          case Success(_) =>
            responseCounter = responseCounter + 1

          case Failure(e) =>
            if (e.isInstanceOf[ApiCallFailureV2]) {
              synchronized {
                apiErrorArray.add(e.getMessage)
                if (apiErrorArray.size() >= apiEnv.errorBatchSize) {
                  PipelineFunctions.writeMicroBatchToTempLocation(tmpClusterEventsErrorPath, apiErrorArray.toString)
                  apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
                }
              }

            }
            responseCounter = responseCounter + 1
            println("Future failure message: " + e.getMessage, e)
        }
      }
      val timeoutThreshold = 300000 // 5 minutes
      var currentSleepTime = 0;
      var accumulatorCountWhileSleeping = accumulator.value
      while (responseCounter < finalResponseCount && currentSleepTime < timeoutThreshold) {
        //As we are using Futures and running 4 threads in parallel, We are checking if all the treads has completed the execution or not.
        // If we have not received the response from all the threads then we are waiting for 5 seconds and again revalidating the count.
        // println("Sleeping for 5 seconds...Total sleep time in millis : "+currentSleepTime+" Response received count : " + responseCounter + ",Expected Response count : " + finalResponseCount)
        if (currentSleepTime > 120000) //printing the waiting message only if the waiting time is more than 2 minutes
        {
          println(s"""Waiting for other queued API Calls to complete; cumulative wait time ${currentSleepTime / 1000} seconds; Api response yet to receive ${finalResponseCount - responseCounter}""")
        }
        Thread.sleep(5000)
        currentSleepTime += 5000
        if (accumulatorCountWhileSleeping <  accumulator.value) {//new API response received while waiting
          currentSleepTime = 0 //resetting the sleep time
          accumulatorCountWhileSleeping =  accumulator.value
        }
      }
      if (responseCounter != finalResponseCount) {
        throw new Exception(s"""Unable to receive all the clusters/events api responses; Api response received ${responseCounter};Api response not received ${finalResponseCount - responseCounter}""")
      }
    }

  }


}

