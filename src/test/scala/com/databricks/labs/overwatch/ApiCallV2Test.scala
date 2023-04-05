package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.api.ApiCallV2.sc
import com.databricks.labs.overwatch.api.{ApiCall, ApiCallV2}
import com.databricks.labs.overwatch.pipeline.PipelineFunctions
import com.databricks.labs.overwatch.utils.{ApiCallFailureV2, ApiEnv, SparkSessionWrapper}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, Ignore}

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
      ApiCallV2(apiEnv, endPoint, query, 2.1).execute().asDF()
    }

    it("Consume data from dbfs/search-mounts API") {
      val endPoint = "dbfs/search-mounts"
      ApiCallV2(apiEnv, endPoint).execute().asDF()
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
      val query = Map("max_results" -> "20",
        "include_metrics" -> "true",
        "filter_by.query_start_time_range.start_time_ms" -> "1663977600000",
        "filter_by.query_start_time_range.end_time_ms" -> "1664236800000"
      )
      val acc = sc.longAccumulator("sqlQueryHistoryAccumulator")
      assert(ApiCallV2(
        apiEnv,
        endPoint,
        query,
        tempSuccessPath = "src/test/scala/tempDir/sqlqueryhistory"
      ).execute().asDF() != null)
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
        "limit" -> "2",
        "expand_tasks" -> "true",
        "offset" -> "0"
      )
      val newAPI = ApiCallV2(apiEnv, endPoint, query, 2.1).execute().asDF()
      println(oldAPI.count() + "old api count")
      println(newAPI.count() + "new api count")
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
      SparkSessionWrapper.parSessionsOn = true
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
          "start_time" -> "1052775426000",
          "end_time" -> "1655453826000",
          "limit" -> "500"
        )
        println(jsonQuery)
        val future = Future {
          val apiObj = ApiCallV2(apiEnv, "clusters/events", jsonQuery, tmpClusterEventsSuccessPath).executeMultiThread(accumulator)
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
        if (accumulatorCountWhileSleeping < accumulator.value) { //new API response received while waiting
          currentSleepTime = 0 //resetting the sleep time
          accumulatorCountWhileSleeping = accumulator.value
        }
      }
      if (responseCounter != finalResponseCount) {
        throw new Exception(s"""Unable to receive all the clusters/events api responses; Api response received ${responseCounter};Api response not received ${finalResponseCount - responseCounter}""")
      }
    }

    it("test sqlHistoryDF") {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark session")
          .config("spark.sql.shuffle.partitions", "1")
          .getOrCreate()
      }
      lazy val sc: SparkContext = spark.sparkContext
      val sqlQueryHistoryEndpoint = "sql/history/queries"
      val acc = sc.longAccumulator("sqlQueryHistoryAccumulator")
      //      val startTime =  "1665878400000".toLong // subtract 2 days for running query merge
      //      val startTime =  "1669026035073".toLong -(1000*60*60*24)// subtract 2 days for running query merge
      val startTime = "1669026035073".toLong
      val untilTime = "1669112526676".toLong
      val tempWorkingDir = ""
      println("test_started")
      val jsonQuery = Map(
        "max_results" -> "50",
        "include_metrics" -> "true",
        "filter_by.query_start_time_range.start_time_ms" -> s"$startTime",
        "filter_by.query_start_time_range.end_time_ms" -> s"${untilTime}"
      )
      ApiCallV2(
        apiEnv,
        sqlQueryHistoryEndpoint,
        jsonQuery,
        tempSuccessPath = s"${tempWorkingDir}/sqlqueryhistory_silver/${System.currentTimeMillis()}"
      )
        .execute()
        .asDF()
        .withColumn("organization_id", lit("1234"))
        .show(false)
    }

    it("test sqlQueryHistoryParallel") {
      println("test")
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark session")
          .config("spark.sql.shuffle.partitions", "1")
          .getOrCreate()
      }

      lazy val sc: SparkContext = spark.sparkContext
      val sqlQueryHistoryEndpoint = "sql/history/queries"
      val acc = sc.longAccumulator("sqlQueryHistoryAccumulator")
      var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
      var apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
      var responseCounter = 0
      implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(apiEnv.threadPoolSize))
      val tmpSqlQueryHistorySuccessPath = "/tmp/test/"
      val tmpSqlQueryHistoryErrorPath = ""
      val startTime = "1669026035073".toLong
      val untilTimeMs = "1669112526676".toLong

      //      val untilTimeMs = "1666182197381".toLong
      //      val untilTimeMs = "1662249600000".toLong
      //      var fromTimeMs = "1662249600000".toLong - (1000*60*60*24*2)  //subtracting 2 days for running query merge
      //      var fromTimeMs = "1666182197381".toLong - (1000*60*60*24*2)
      var fromTimeMs = "1669026035073".toLong
      val finalResponseCount = scala.math.ceil((untilTimeMs - fromTimeMs).toDouble / (1000 * 60 * 60)) // Total no. of API Calls

      while (fromTimeMs < untilTimeMs) {
        val (startTime, endTime) = if ((untilTimeMs - fromTimeMs) / (1000 * 60 * 60) > 1) {
          (fromTimeMs,
            fromTimeMs + (1000 * 60 * 60))
        }
        else {
          (fromTimeMs,
            untilTimeMs)
        }

        //create payload for the API calls
        val jsonQuery = Map(
          "max_results" -> "50",
          "include_metrics" -> "true",
          "filter_by.query_start_time_range.start_time_ms" -> s"$startTime", // Do we need to subtract 2 days for every API call?
          "filter_by.query_start_time_range.end_time_ms" -> s"$endTime"
        )
        /** TODO:
         * Refactor the below code to make it more generic
         */
        //call future
        val future = Future {
          val apiObj = ApiCallV2(
            apiEnv,
            sqlQueryHistoryEndpoint,
            jsonQuery,
            tempSuccessPath = tmpSqlQueryHistorySuccessPath
          ).executeMultiThread(acc)

          synchronized {
            apiObj.forEach(
              obj => if (obj.contains("res")) {
                apiResponseArray.add(obj)
              }
            )
            println(apiResponseArray.toString)
            if (apiResponseArray.size() >= apiEnv.successBatchSize) {
              PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistorySuccessPath, apiResponseArray.toString)
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
                  PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistoryErrorPath, apiErrorArray.toString)
                  apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
                }
              }

            }
            responseCounter = responseCounter + 1
        }
        fromTimeMs = fromTimeMs + (1000 * 60 * 60)
      }
      val timeoutThreshold = apiEnv.apiWaitingTime // 5 minutes
      var currentSleepTime = 0
      var accumulatorCountWhileSleeping = acc.value
      while (responseCounter < finalResponseCount && currentSleepTime < timeoutThreshold) {
        //As we are using Futures and running 4 threads in parallel, We are checking if all the treads has completed the execution or not.
        // If we have not received the response from all the threads then we are waiting for 5 seconds and again revalidating the count.
        if (currentSleepTime > 120000) //printing the waiting message only if the waiting time is more than 2 minutes.
        {
          println(s"""Waiting for other queued API Calls to complete; cumulative wait time ${currentSleepTime / 1000} seconds; Api response yet to receive ${finalResponseCount - responseCounter}""")
        }
        Thread.sleep(5000)
        currentSleepTime += 5000
        if (accumulatorCountWhileSleeping < acc.value) { //new API response received while waiting.
          currentSleepTime = 0 //resetting the sleep time.
          accumulatorCountWhileSleeping = acc.value
        }
      }
      if (responseCounter != finalResponseCount) { // Checking whether all the api responses has been received or not.

        throw new Exception(s"""Unable to receive all the sql/history/queries api responses; Api response received ${responseCounter};Api response not received ${finalResponseCount - responseCounter}""")
      }
      if (apiResponseArray.size() > 0) { //In case of response array didn't hit the batch-size as a final step we will write it to the persistent storage.
        PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistorySuccessPath, apiResponseArray.toString)
        apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
      }
      if (apiErrorArray.size() > 0) { //In case of error array didn't hit the batch-size as a final step we will write it to the persistent storage.
        PipelineFunctions.writeMicroBatchToTempLocation(tmpSqlQueryHistorySuccessPath, apiErrorArray.toString)
        apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
      }

      println(tmpSqlQueryHistorySuccessPath)
      print(apiResponseArray.toString)
      //      spark.read.json(tmpSqlQueryHistorySuccessPath)
      //        .select(explode(col("res")).alias("res")).select(col("res" + ".*"))
      //        .withColumn("organization_id", lit("123"))

    }


    it("test jobRunsList") {
      lazy val spark: SparkSession = {
        SparkSession
          .builder()
          .master("local")
          .appName("spark session")
          .config("spark.sql.shuffle.partitions", "1")
          .getOrCreate()
      }
      var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())

      lazy val sc: SparkContext = spark.sparkContext
      val jobsRunsEndpoint = "jobs/runs/list"
      val fromTime = "1676160000000".toLong
      val untilTime = "1676574760631".toLong
      val tempWorkingDir = "/tmp/test/"
      println("test_started")

      val jsonQuery = Map(
        "limit" -> "25",
        "expand_tasks" -> "true",
        "offset" -> "0",
        "start_time_from" -> s"${fromTime}",
        "start_time_to" -> s"${untilTime}"
      )
      val acc = sc.longAccumulator("jobRunsListAccumulator")
      val apiObj = ApiCallV2(
        apiEnv,
        jobsRunsEndpoint,
        jsonQuery,
        tempSuccessPath = tempWorkingDir,
        2.1
      ).executeMultiThread(acc)
      //        .execute()
      //        .asDF()
      //        .withColumn("organization_id", lit("1234"))
      //        .show(false)

      apiObj.forEach(
        obj => if (obj.contains("res")) {
          apiResponseArray.add(obj)
        }
      )
      println(apiObj.size())

      if (apiResponseArray.size() > 0) { //In case of response array didn't hit the batch-size as a final step we will write it to the persistent storage.
        PipelineFunctions.writeMicroBatchToTempLocation(tempWorkingDir, apiResponseArray.toString)
      }

    }
  }

}

