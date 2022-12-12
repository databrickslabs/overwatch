package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.eventhubs.AadAuthInstance
import com.databricks.labs.overwatch.pipeline.WorkflowsTransforms.{workflowsCleanseJobClusters, workflowsCleanseTasks}
import com.databricks.labs.overwatch.utils.Helpers.getDatesGlob
import com.databricks.labs.overwatch.utils.SchemaTools.structFromJson
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.{ApiCall, ApiCallV2}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.util.SerializableConfiguration

import java.time.LocalDateTime
import java.util
import java.util.Collections
import java.util.concurrent.Executors
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


trait BronzeTransforms extends SparkSessionWrapper {

  import TransformFunctions._
  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _newDataRetrieved: Boolean = true

  case class ErrorDetail(errorMsg: String, startTime: Long, endTime: Long)

  case class ClusterIdsWEventCounts(clusterId: String, count: Long)

  case class ClusterEventCall(
                               cluster_id: String,
                               payload: Array[String],
                               succeeded: Boolean,
                               error: Option[ErrorDetail]
                             )

  protected def setNewDataRetrievedFlag(value: Boolean): this.type = {
    _newDataRetrieved = value
    this
  }

  protected def newDataRetrieved: Boolean = _newDataRetrieved

  private def buildClusterEventsErrorDF(clusterEventCalls: Array[ClusterEventCall]): DataFrame = {
    if (!clusterEventCalls.forall(_.succeeded)) {
      val errorCount = clusterEventCalls.filterNot(_.succeeded).length
      val errMessage = s"WARNING: $errorCount ERRORS DETECTED in Bronze_ClusterEvents. At least one batch " +
        s"could not be loaded. Review cluster_events_errors_bronze for more details."
      logger.log(Level.WARN, errMessage)
      println(errMessage)
    }

    clusterEventCalls.filterNot(_.succeeded).map(e => {
      val err = e.error.get
      (
        e.cluster_id,
        err.startTime,
        err.endTime,
        err.errorMsg
      )
    }).toSeq.toDF("cluster_id", "from_epoch", "until_epoch", "error")
      .withColumn("from_ts", toTS(col("from_epoch")))
      .withColumn("until_ts", toTS(col("until_epoch")))
  }

  private def callClusterEventApi(
                                   cluster_id: String,
                                   fromTSMilli: Long,
                                   untilTSMilli: Long,
                                   apiCall: ApiCall
                                 ): ClusterEventCall = {
    // goal here is to allow single api call failures for specific cluster_ids and not fail the pipeline but
    // still track the failure
    try { // success
      ClusterEventCall(cluster_id, apiCall.executePost().asStrings, succeeded = true, None)
    } catch { // failures
      case e: ApiCallEmptyResponse =>
        val errorDetail = ErrorDetail(e.apiCallDetail, fromTSMilli, untilTSMilli)
        ClusterEventCall(cluster_id, Array[String](), succeeded = false, Some(errorDetail))
      case e: ApiCallFailure =>
        val errorDetail = ErrorDetail(e.msg, fromTSMilli, untilTSMilli)
        ClusterEventCall(cluster_id, Array[String](), succeeded = false, Some(errorDetail))
      case e: Throwable =>
        val errorDetail = ErrorDetail(e.getMessage, fromTSMilli, untilTSMilli)
        ClusterEventCall(cluster_id, Array[String](), succeeded = false, Some(errorDetail))
    }
  }

  private def persistErrors(
                             errorsDF: DataFrame,
                             database: Database,
                             errorsTarget: PipelineTable,
                             pipelineSnapTS: TimeTypes,
                             orgId: String
                           ): Unit = {
    if (!errorsDF.isEmpty) {
      database.write(
        errorsDF.withColumn("organization_id", lit(orgId)),
        errorsTarget,
        pipelineSnapTS.asColumnTS
      )
    }
  }

  private def clusterEventsByClusterBatch(
                                           startTime: TimeTypes,
                                           endTime: TimeTypes,
                                           apiEnv: ApiEnv,
                                           ids: Array[String]
                                         ): (Array[String], DataFrame) = {
    //    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(6))
    //    val idsPar = ids.par
    //    idsPar.tasksupport = taskSupport
    //    DEBUG
    //    val idsPar = Array("0827-194754-tithe1")
    // removing parallelization for now to see if it fixes some weird errors
    // CONFIRMED -- Parallelizing this breaks the token cipher
    // TODO - Identify why parallel errors
    val results = ids.map(id => {
      val query = Map("cluster_id" -> id,
        "start_time" -> startTime.asUnixTimeMilli,
        "end_time" -> endTime.asUnixTimeMilli,
        "limit" -> 500
      )

      callClusterEventApi(id, startTime.asUnixTimeMilli, endTime.asUnixTimeMilli, ApiCall("clusters/events", apiEnv, Some(query)))
    }) //.toArray // -- needed when using par

    val clusterEvents = results.filter(_.succeeded).flatMap(_.payload)

    // create DF to persist errored clusterEvent API calls
    val erroredEventsDF = buildClusterEventsErrorDF(results)

    (clusterEvents, erroredEventsDF)
  }

  private def validateCleanPaths(
                                  azureRawAuditLogTarget: PipelineTable,
                                  isFirstRun: Boolean,
                                  ehConfig: AzureAuditLogEventhubConfig,
                                  etlDataPathPrefix: String,
                                  etlDBLocation: String,
                                  consumerDBLocation: String
                                ): Unit = {

    val pathsToValidate = Array(
      ehConfig.auditRawEventsChk.get
    )

    logger.log(Level.INFO, s"Checkpoint paths to validate: ${pathsToValidate.mkString(",")}")
    val dataTargetPaths = Array(etlDataPathPrefix, etlDBLocation, consumerDBLocation).map(_.toLowerCase)

    val baseErrMsg = "ERROR: Azure Event Hub checkpoint directory issue."
    pathsToValidate.foreach(p => {
      logger.log(Level.INFO, s"Validating: $p")
      val exists = Helpers.pathExists(p)

      if (isFirstRun && dataTargetPaths.contains(p.toLowerCase)) { // on new pipeline, data target paths != eh paths
        val errMsg = s"$baseErrMsg\nOne or more data target paths == the event hub state parent directory. Event Hub checkpoint " +
          s"directories may not be in the same path as your data targets, please select another directory.\nDATA " +
          s"TARGETS: ${dataTargetPaths.mkString(", ")}\nEVENT HUB STATE PATH: $p"
        logger.log(Level.ERROR, errMsg)
        println(errMsg)
        throw new BadConfigException(errMsg)
      }

      if (exists && isFirstRun) { // Path cannot already exist on first run
        val errMsg = s"$baseErrMsg\nPATH: ${p} is not empty. First run requires empty checkpoints."
        logger.log(Level.ERROR, errMsg)
        println(errMsg)
        throw new BadConfigException(errMsg)
      }

      if (!exists && !isFirstRun) { // If not first run checkpoint paths must already exist.
        if (azureRawAuditLogTarget.exists(dataValidation = true)) {
          val warnMsg = s"$baseErrMsg\nPath: ${p} does not exist. To append new data, a checkpoint dir must " +
            s"exist and be current. Attempting to recover state from Overwatch metadata."
          logger.log(Level.WARN, warnMsg)
          println(warnMsg)
          throw new BadConfigException(warnMsg, failPipeline = false)
        } else {
          val errMsg = s"$baseErrMsg\nPath: ${p} does not exist. To append new data, a checkpoint dir must " +
            s"exist and be current."
          logger.log(Level.ERROR, errMsg)
          println(errMsg)
          throw new BadConfigException(errMsg)
        }
      }
    })
  }

  @throws(classOf[BadConfigException])
  protected def landAzureAuditLogDF(azureRawAuditLogTarget: PipelineTable,
                                    ehConfig: AzureAuditLogEventhubConfig,
                                    etlDataPathPrefix: String,
                                    etlDBLocation: String,
                                    consumerDBLocation: String,
                                    isFirstRun: Boolean,
                                    organizationId: String,
                                    runID: String): DataFrame = {

    val connectionString = ConnectionStringBuilder(
      PipelineFunctions.parseAndValidateEHConnectionString(ehConfig.connectionString, ehConfig.azureClientId.isEmpty))
      .setEventHubName(ehConfig.eventHubName)
      .build

    val ehConf = try {
      validateCleanPaths(azureRawAuditLogTarget, isFirstRun, ehConfig, etlDataPathPrefix, etlDBLocation, consumerDBLocation)

      if (isFirstRun) {
        EventHubsConf(connectionString)
          .setMaxEventsPerTrigger(ehConfig.maxEventsPerTrigger)
          .setStartingPosition(EventPosition.fromStartOfStream)
      } else {
        EventHubsConf(connectionString)
          .setMaxEventsPerTrigger(ehConfig.maxEventsPerTrigger)
      }
    } catch { // chk dir is missing BUT raw audit hist has latest enq time and can resume from there creating a new chkpoint
      case e: BadConfigException if (!e.failPipeline) =>
        val lastEnqTime = azureRawAuditLogTarget.asDF()
          .select(max('enqueuedTime))
          .as[java.sql.Timestamp]
          .first
          .toInstant

        EventHubsConf(connectionString)
          .setMaxEventsPerTrigger(ehConfig.maxEventsPerTrigger)
          .setStartingPosition(EventPosition.fromEnqueuedTime(lastEnqTime))
    }

    val eventHubsConf = if (ehConfig.azureClientId.nonEmpty) {
      val aadParams = Map("aad_tenant_id" -> PipelineFunctions.maybeGetSecret(ehConfig.azureTenantId.get),
        "aad_client_id" -> PipelineFunctions.maybeGetSecret(ehConfig.azureClientId.get),
        "aad_client_secret" -> PipelineFunctions.maybeGetSecret(ehConfig.azureClientSecret.get),
        "aad_authority_endpoint" -> ehConfig.azureAuthEndpoint)
      AadAuthInstance.addAadAuthParams(ehConf, aadParams)
    } else
      ehConf

    spark.readStream
      .format("eventhubs")
      .options(eventHubsConf.toMap)
      .load()
      .withColumn("deserializedBody", 'body.cast("string"))
      .withColumn("organization_id", lit(organizationId))
      .withColumn("Overwatch_RunID", lit(runID))

  }

  protected def cleanseRawJobsSnapDF(keys: Array[String], runId: String)(df: DataFrame): DataFrame = {
    val emptyKeysDF = Seq.empty[(String, Long, String)].toDF("organization_id", "job_id", "Overwatch_RunID")
    val outputDF = SchemaScrubber.scrubSchema(df)
      .withColumn("Overwatch_RunID", lit(runId))

    val cleansedTasksDF = workflowsCleanseTasks(outputDF, keys, emptyKeysDF, "settings.tasks")
    val cleansedJobClustersDF = workflowsCleanseJobClusters(outputDF, keys, emptyKeysDF, "settings.job_clusters")

    val changeInventory = Map[String, Column](
      "settings.tasks" -> col("cleansedTasks"),
      "settings.job_clusters" -> col("cleansedJobsClusters"),
      "settings.tags" -> SchemaTools.structToMap(outputDF, "settings.tags"),
      "settings.notebook_task.base_parameters" -> SchemaTools.structToMap(outputDF, "settings.notebook_task.base_parameters")
    ) ++ PipelineFunctions.newClusterCleaner(outputDF, "settings.tasks.new_cluster")

    outputDF
      .join(cleansedTasksDF, keys.toSeq, "left")
      .join(cleansedJobClustersDF, keys.toSeq, "left")
      .modifyStruct(changeInventory)
      .drop("cleansedTasks", "cleansedJobsClusters") // cleanup temporary cleaner fields
      .scrubSchema(SchemaScrubber(cullNullTypes = true))
  }

  protected def cleanseRawClusterSnapDF(df: DataFrame): DataFrame = {
    val outputDF = SchemaScrubber.scrubSchema(df)

    outputDF
      .withColumn("default_tags", SchemaTools.structToMap(outputDF, "default_tags"))
      .withColumn("custom_tags", SchemaTools.structToMap(outputDF, "custom_tags"))
      .withColumn("spark_conf", SchemaTools.structToMap(outputDF, "spark_conf"))
      .withColumn("spark_env_vars", SchemaTools.structToMap(outputDF, "spark_env_vars"))
      .withColumn(s"aws_attributes", SchemaTools.structToMap(outputDF, s"aws_attributes"))
      .withColumn(s"azure_attributes", SchemaTools.structToMap(outputDF, s"azure_attributes"))

  }

  protected def cleanseRawPoolsDF()(df: DataFrame): DataFrame = {
    val outputDF = SchemaScrubber.scrubSchema(df)
    outputDF
      .withColumn("custom_tags", SchemaTools.structToMap(outputDF, "custom_tags"))
      .withColumn("default_tags", SchemaTools.structToMap(outputDF, "default_tags"))
      .withColumn(s"aws_attributes", SchemaTools.structToMap(outputDF, s"aws_attributes"))
      .withColumn(s"azure_attributes", SchemaTools.structToMap(outputDF, s"azure_attributes"))
  }

  //noinspection ScalaCustomHdfsFormat
  protected def getAuditLogsDF(auditLogConfig: AuditLogConfig,
                               cloudProvider: String,
                               fromTime: LocalDateTime,
                               untilTime: LocalDateTime,
                               auditRawLand: PipelineTable,
                               overwatchRunID: String,
                               organizationId: String
                              ): DataFrame = {
    val fromDT = fromTime.toLocalDate
    val untilDT = untilTime.toLocalDate
    if (cloudProvider == "azure") {
      val azureAuditSourceFilters = 'Overwatch_RunID === lit(overwatchRunID) && 'organization_id === organizationId
      val rawBodyLookup = auditRawLand.asDF
        .filter(azureAuditSourceFilters)
      val schemaBuilders = auditRawLand.asDF
        .filter(azureAuditSourceFilters)
        .withColumn("parsedBody", structFromJson(spark, rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"), 'organization_id)
        .selectExpr("streamRecord.*", "organization_id")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"), 'organization_id)
        .selectExpr("*", "properties.*").drop("properties")


      val baselineAuditLogs = auditRawLand.asDF
        .filter(azureAuditSourceFilters)
        .withColumn("parsedBody", structFromJson(spark, rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"), 'organization_id)
        .selectExpr("streamRecord.*", "organization_id")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"), 'organization_id)
        .withColumn("userIdentity", structFromJson(spark, schemaBuilders, "userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")
        .withColumn("requestParams", structFromJson(spark, schemaBuilders, "requestParams"))

      PipelineFunctions.cleanseCorruptAuditLogs(spark, baselineAuditLogs)
        .withColumn("response", structFromJson(spark, schemaBuilders, "response"))
        .drop("logId")

    } else {

      // inclusive from exclusive to
      val datesGlob = if (fromDT == untilDT) {
        Array(s"${auditLogConfig.rawAuditPath.get}/date=${fromDT.toString}")
      } else {
        getDatesGlob(fromDT, untilDT)
          .map(dt => s"${auditLogConfig.rawAuditPath.get}/date=${dt}")
          .filter(Helpers.pathExists)
      }

      val auditLogsFailureMsg = s"Audit Logs Module Failure: Audit logs are required to use Overwatch and no data " +
        s"was found in the following locations: ${datesGlob.mkString(", ")}"

      if (datesGlob.nonEmpty) {
        val rawDF = try {
          spark.read.format(auditLogConfig.auditLogFormat).load(datesGlob: _*)
        } catch { // corrupted audit logs with duplicate columns in the source
          case e: AnalysisException if e.message.contains("Found duplicate column(s) in the data schema") =>
            spark.conf.set("spark.sql.caseSensitive", "true")
            spark.read.format(auditLogConfig.auditLogFormat).load(datesGlob: _*)
        }
        // clean corrupted source audit logs even when there is only one of the duplicate columns in the source
        // but still will conflict with the existing columns in the target
        val cleanRawDF = PipelineFunctions.cleanseCorruptAuditLogs(spark, rawDF)

        val baseDF = if (auditLogConfig.auditLogFormat == "json") cleanRawDF else {
          val rawDFWRPJsonified = cleanRawDF
            .withColumn("requestParams", to_json('requestParams))
          rawDFWRPJsonified
            .withColumn("requestParams", structFromJson(spark, rawDFWRPJsonified, "requestParams"))
        }

        baseDF
          // When globbing the paths, the date must be reconstructed and re-added manually
          .withColumn("organization_id", lit(organizationId))
          .withColumn("filename", input_file_name)
          .withColumn("filenameAR", split(input_file_name, "/"))
          .withColumn("date",
            split(expr("filter(filenameAR, x -> x like ('date=%'))")(0), "=")(1).cast("date"))
          .drop("filenameAR")
      } else {
        throw new Exception(auditLogsFailureMsg)
      }
    }
  }

  private def buildClusterEventBatches(apiEnv: ApiEnv,
                                       batchSize: Double,
                                       startTime: TimeTypes,
                                       endTime: TimeTypes,
                                       clusterIDs: Array[String]
                                      ): (Array[Array[String]], Array[ClusterEventCall]) = {

    case class ClusterEventBuffer(clusterId: String, batchId: Int)
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(8))
    val clusterIdsPar = clusterIDs.par
    clusterIdsPar.tasksupport = taskSupport


    var cumSum = 0L

    logger.log(Level.INFO, s"OVERWATCH: BUILDING CLUSTER EVENTS for ${clusterIDs.length} Clusters. " +
      s"Large dynamic clusters, " +
      s"will take some time, especially on first runs. At present " +
      "the cluster events can only be acquired via api calls which are rate limited.\n" +
      s"BATCH SIZE: ${batchSize}\n" +
      s"CLUSTERS COUNT: ${clusterIDs.length}\n" +
      s"START TIMESTAMP: ${startTime.asUnixTimeMilli}\n" +
      s"END TIMESTAMP: ${endTime.asUnixTimeMilli} \n" +
      s"CLUSTERIDs: ${clusterIDs.mkString(", ")}")

    val lastEventByClusterResults = clusterIdsPar.map(clusterId => {
      val lastEventQuery = Map[String, Any](
        "cluster_id" -> clusterId,
        "start_time" -> startTime.asUnixTimeMilli,
        "end_time" -> endTime.asUnixTimeMilli,
        "order" -> "DESC",
        "limit" -> 1
      )

      callClusterEventApi(clusterId, startTime.asUnixTimeMilli, endTime.asUnixTimeMilli,
        ApiCall("clusters/events", apiEnv, Some(lastEventQuery), paginate = false)
      )

    }).toArray

    val failedApiCalls = lastEventByClusterResults.filterNot(_.succeeded)

    val clusterEventsBuffer = lastEventByClusterResults.filter(_.succeeded).map(lastEventWrapper => {
      val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
        .registerModule(DefaultScalaModule)
        .registerModule(new SimpleModule())
        .asInstanceOf[ObjectMapper with ScalaObjectMapper]

      val totalCount = mapper.readTree(lastEventWrapper.payload.head).get("total_count").asLong(0L)
      ClusterIdsWEventCounts(lastEventWrapper.cluster_id, totalCount)
    }).map(clusterEvents => {
      cumSum += clusterEvents.count
      val batchId = Math.ceil(cumSum / batchSize).toInt
      (clusterEvents.clusterId, batchId)
    }).groupBy(_._2).map(_._2.map(_._1).toArray).toArray

    (clusterEventsBuffer, failedApiCalls)

  }

  private def landClusterEvents(clusterIDs: Array[String],
                                startTime: TimeTypes,
                                endTime: TimeTypes,
                                apiEnv: ApiEnv,
                                tmpClusterEventsSuccessPath: String,
                                tmpClusterEventsErrorPath: String) = {
    val finalResponseCount = clusterIDs.length
    var apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
    var apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
    val apiResponseCounter = Collections.synchronizedList(new util.ArrayList[Int]())
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(apiEnv.threadPoolSize))
    //TODO identify the best practice to implement the future.
    val accumulator = sc.longAccumulator("ClusterEventsAccumulator")
    for (i <- clusterIDs.indices) {
      val jsonQuery = Map("cluster_id" -> s"""${clusterIDs(i)}""",
        "start_time" -> s"""${startTime.asUnixTimeMilli}""",
        "end_time" -> s"""${endTime.asUnixTimeMilli}""",
        "limit" -> "500"
      )
      val future = Future {
        val apiObj = ApiCallV2(apiEnv, "clusters/events", jsonQuery, tmpClusterEventsSuccessPath,accumulator).executeMultiThread()
        synchronized {
          apiObj.forEach(
            obj => if (obj.contains("events")) {
              apiResponseArray.add(obj)
            }else{
              logger.log(Level.INFO,"NO real events found:"+obj)
            }

          )
          if (apiResponseArray.size() >= apiEnv.successBatchSize) {
            PipelineFunctions.writeMicroBatchToTempLocation(tmpClusterEventsSuccessPath, apiResponseArray.toString)
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
              if (apiErrorArray.size() >= apiEnv.errorBatchSize) {
                PipelineFunctions.writeMicroBatchToTempLocation(tmpClusterEventsErrorPath, apiErrorArray.toString)
                apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
              }
            }
            logger.log(Level.ERROR, "Future failure message: " + e.getMessage, e)
          }
          apiResponseCounter.add(1)
      }
    }
    val timeoutThreshold = apiEnv.apiWaitingTime // 5 minutes
    var currentSleepTime = 0
    var accumulatorCountWhileSleeping = accumulator.value
    while (apiResponseCounter.size() < finalResponseCount && currentSleepTime < timeoutThreshold) {
      //As we are using Futures and running 4 threads in parallel, We are checking if all the treads has completed the execution or not.
      // If we have not received the response from all the threads then we are waiting for 5 seconds and again revalidating the count.
      if (currentSleepTime > 120000) //printing the waiting message only if the waiting time is more than 2 minutes.
      {
        println(s"""Waiting for other queued API Calls to complete; cumulative wait time ${currentSleepTime / 1000} seconds; Api response yet to receive ${finalResponseCount - apiResponseCounter.size()}""")
      }
      Thread.sleep(5000)
      currentSleepTime += 5000
      if (accumulatorCountWhileSleeping < accumulator.value) { //new API response received while waiting.
        currentSleepTime = 0 //resetting the sleep time.
        accumulatorCountWhileSleeping = accumulator.value
      }
    }
    if (apiResponseCounter.size() != finalResponseCount) { // Checking whether all the api responses has been received or not.
      logger.log(Level.ERROR, s"""Unable to receive all the clusters/events api responses; Api response received ${apiResponseCounter.size()};Api response not received ${finalResponseCount - apiResponseCounter.size()}""")
      throw new Exception(s"""Unable to receive all the clusters/events api responses; Api response received ${apiResponseCounter.size()};Api response not received ${finalResponseCount - apiResponseCounter.size()}""")
    }
    if (apiResponseArray.size() > 0) { //In case of response array didn't hit the batch-size as a final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tmpClusterEventsSuccessPath, apiResponseArray.toString)
      apiResponseArray = Collections.synchronizedList(new util.ArrayList[String]())
    }
    if (apiErrorArray.size() > 0) { //In case of error array didn't hit the batch-size as a final step we will write it to the persistent storage.
      PipelineFunctions.writeMicroBatchToTempLocation(tmpClusterEventsErrorPath, apiErrorArray.toString)
      apiErrorArray = Collections.synchronizedList(new util.ArrayList[String]())
    }
    logger.log(Level.INFO, " Cluster event landing completed")
  }

  private def processClusterEvents(tmpClusterEventsSuccessPath: String, organizationId: String, erroredBronzeEventsTarget: PipelineTable): DataFrame = {
    logger.log(Level.INFO, "COMPLETE: Cluster Events acquisition, building data")
    if (Helpers.pathExists(tmpClusterEventsSuccessPath)) {
      if (spark.read.json(tmpClusterEventsSuccessPath).columns.contains("events")) {
        try {
          val tdf = SchemaScrubber.scrubSchema(
            spark.read.json(tmpClusterEventsSuccessPath)
              .select(explode('events).alias("events"))
              .select(col("events.*"))
          ).scrubSchema

          val changeInventory = Map[String, Column](
            "details.attributes.custom_tags" -> SchemaTools.structToMap(tdf, "details.attributes.custom_tags"),
            "details.attributes.spark_conf" -> SchemaTools.structToMap(tdf, "details.attributes.spark_conf"),
            "details.attributes.azure_attributes" -> SchemaTools.structToMap(tdf, "details.attributes.azure_attributes"),
            "details.attributes.aws_attributes" -> SchemaTools.structToMap(tdf, "details.attributes.aws_attributes"),
            "details.attributes.spark_env_vars" -> SchemaTools.structToMap(tdf, "details.attributes.spark_env_vars"),
            "details.previous_attributes.custom_tags" -> SchemaTools.structToMap(tdf, "details.previous_attributes.custom_tags"),
            "details.previous_attributes.spark_conf" -> SchemaTools.structToMap(tdf, "details.previous_attributes.spark_conf"),
            "details.previous_attributes.azure_attributes" -> SchemaTools.structToMap(tdf, "details.previous_attributes.azure_attributes"),
            "details.previous_attributes.aws_attributes" -> SchemaTools.structToMap(tdf, "details.previous_attributes.aws_attributes"),
            "details.previous_attributes.spark_env_vars" -> SchemaTools.structToMap(tdf, "details.previous_attributes.spark_env_vars")
          )

          val clusterEventsDF = tdf
            .modifyStruct(changeInventory)
            .withColumn("organization_id", lit(organizationId))

          val clusterEventsCaptured = clusterEventsDF.count
          val logEventsMSG = s"CLUSTER EVENTS CAPTURED: ${clusterEventsCaptured}"
          logger.log(Level.INFO, logEventsMSG)
          clusterEventsDF

        } catch {
          case e: Throwable =>
            throw new Exception(e)
        }
      }
      else {
        logger.info("Events column not found in dataset")
        throw new NoNewDataException(s"EMPTY: No New Cluster Events.Events column not found in dataset, Progressing module but it's recommended you " +
          s"validate there no api call errors in ${erroredBronzeEventsTarget.tableFullName}", Level.WARN, allowModuleProgression = true)
      }
    } else {
      logger.info("EMPTY MODULE: Cluster Events")
      throw new NoNewDataException(s"EMPTY: No New Cluster Events. Progressing module but it's recommended you " +
        s"validate there no api call errors in ${erroredBronzeEventsTarget.tableFullName}", Level.WARN, allowModuleProgression = true)
    }
  }

  protected def prepClusterEventLogs(
                                      filteredAuditLogDF: DataFrame,
                                      startTime: TimeTypes,
                                      endTime: TimeTypes,
                                      pipelineSnapTS: TimeTypes,
                                      apiEnv: ApiEnv,
                                      organizationId: String,
                                      database: Database,
                                      erroredBronzeEventsTarget: PipelineTable,
                                      tempWorkingDir: String
                                    )(clusterSnapshotDF: DataFrame): DataFrame = {

    val clusterIDs = getClusterIdsWithNewEvents(filteredAuditLogDF, clusterSnapshotDF)
      .as[String]
      .collect()

    if (clusterIDs.isEmpty) throw new NoNewDataException(s"No clusters could be found with new events. Please " +
      s"validate your audit log input and clusters_snapshot_bronze tables to ensure data is flowing to them " +
      s"properly. Skipping!", Level.ERROR)

    val processingStartTime = System.currentTimeMillis();
    logger.log(Level.INFO, "Calling APIv2, Number of cluster id:" + clusterIDs.length + " run id :" + apiEnv.runID)
    val tmpClusterEventsSuccessPath = s"$tempWorkingDir/clusterEventsBronze/success" + apiEnv.runID
    val tmpClusterEventsErrorPath = s"$tempWorkingDir/clusterEventsBronze/error" + apiEnv.runID

    landClusterEvents(clusterIDs, startTime, endTime, apiEnv, tmpClusterEventsSuccessPath, tmpClusterEventsErrorPath)
    if (Helpers.pathExists(tmpClusterEventsErrorPath)) {
      persistErrors(
        spark.read.json(tmpClusterEventsErrorPath)
          .withColumn("from_ts", toTS(col("from_epoch")))
          .withColumn("until_ts", toTS(col("until_epoch"))),
        database,
        erroredBronzeEventsTarget,
        pipelineSnapTS,
        organizationId
      )
      logger.log(Level.INFO, "Persist error completed")
    }
    spark.conf.set("spark.sql.caseSensitive", "true")
    val clusterEventDf = processClusterEvents(tmpClusterEventsSuccessPath, organizationId, erroredBronzeEventsTarget)
    spark.conf.set("spark.sql.caseSensitive", "false")
    val processingEndTime = System.currentTimeMillis();
    logger.log(Level.INFO, " Duration in millis :" + (processingEndTime - processingStartTime))
    clusterEventDf
  }


  private def appendNewFilesToTracker(database: Database,
                                      newFiles: DataFrame,
                                      trackerTarget: PipelineTable,
                                      orgId: String,
                                      pipelineSnapTime: Column
                                     ): Unit = {
    val fileTrackerDF = newFiles
      .withColumn("failed", lit(false))
      .withColumn("organization_id", lit(orgId))
    //      .coalesce(4) // narrow, short table -- each append will == spark event log files processed
    database.write(fileTrackerDF, trackerTarget, pipelineSnapTime)
  }

  /**
   * Remove already processed and bad files
   * Before loading spark events files, ensure that only new, good files are ingested for processing
   *
   * @param badRecordsPath
   * @param eventLogsDF
   * @param processedLogFiles
   * @return
   */
  private def retrieveNewValidSparkEventsWMeta(badRecordsPath: String,
                                               eventLogsDF: DataFrame,
                                               processedLogFiles: PipelineTable): DataFrame = {
    val validNewFiles = if (processedLogFiles.exists(dataValidation = true)) {
      val alreadyProcessed = processedLogFiles.asDF
        .filter(!'failed && 'withinSpecifiedTimeRange)
        .select('filename)
        .distinct

      if (Helpers.pathExists(badRecordsPath)) {
        val badFiles = spark.read.format("json")
          .schema(Schema.badRecordsSchema)
          .load(s"${badRecordsPath}/*/*/")
          .select('path.alias("filename"))
          .distinct
        eventLogsDF.select('filename).except(alreadyProcessed.unionByName(badFiles))
      } else {
        eventLogsDF.select('filename).except(alreadyProcessed)
      }
    } else {
      eventLogsDF.select('filename)
        .distinct
    }

    validNewFiles.join(eventLogsDF, Seq("filename"))

  }

  private def groupFilename(filename: Column): Column = {
    val segmentArray = split(filename, "/")
    val byCluster = array_join(slice(segmentArray, 1, 3), "/").alias("byCluster")
    val byClusterHost = array_join(slice(segmentArray, 1, 5), "/").alias("byDriverHost")
    val bySparkContextID = array_join(slice(segmentArray, 1, 6), "/").alias("bySparkContext")
    struct(filename, byCluster, byClusterHost, bySparkContextID)
  }

  private def getSparkEventsSchemaScrubber(df: DataFrame): SchemaScrubber = {
    val propertiesScrubException = SanitizeFieldException(
      field = SchemaTools.colByName(df)("Properties"),
      rules = List(
        SanitizeRule("\\s", ""),
        SanitizeRule("\\.", ""),
        SanitizeRule("[^a-zA-Z0-9]", "_")
      ),
      recursive = true
    )
    SchemaScrubber(exceptions = Array(propertiesScrubException))
  }

  def generateEventLogsDF(database: Database,
                          badRecordsPath: String,
                          processedLogFilesTracker: PipelineTable,
                          organizationId: String,
                          runID: String,
                          pipelineSnapTime: TimeTypes
                         )(eventLogsDF: DataFrame): DataFrame = {

    logger.log(Level.INFO, "Searching for Event logs")
    val tempDir = processedLogFilesTracker.config.tempWorkingDir
    // Caching is done to ensure a single scan of the event log file paths
    // From here forward there should be no more direct scans for new records, just loading data direct from paths
    // eager force cache
    // TODO -- Delta auto-optimize seems to be scanning the source files again anyway during
    //  execute at DeltaInvariantCheckerExec.scala:95 -- review again after upgrade to DBR 7.x+
    //    val cachedEventLogs = eventLogsDF.cache()
    //    val eventLogsCount = cachedEventLogs.count()
    //    logger.log(Level.INFO, s"EVENT LOGS FOUND: Total Found --> ${eventLogsCount}")

    if (!eventLogsDF.isEmpty) { // newly found file names
      // All new files scanned including failed and outOfTimeRange files
      val validNewFilesWMetaDF = retrieveNewValidSparkEventsWMeta(badRecordsPath, eventLogsDF, processedLogFilesTracker)
      // Filter out files that are Out of scope and sort data to attempt to get largest files into execution first to maximize stage time
      val pathsGlob = validNewFilesWMetaDF
        .filter(!'failed && 'withinSpecifiedTimeRange)
        .orderBy('fileSize.desc)
        .select('fileName)
        .as[String].collect
      if (pathsGlob.nonEmpty) { // new files less bad files and already-processed files
        logger.log(Level.INFO, s"VALID NEW EVENT LOGS FOUND: COUNT --> ${pathsGlob.length}")
        try {
          logger.log(Level.INFO, "Updating Tracker with new files")
          // appends all newly scanned files including files that were scanned but not loaded due to OutOfTime window
          // and/or failed during lookup -- these are kept for tracking
          appendNewFilesToTracker(database, validNewFilesWMetaDF, processedLogFilesTracker, organizationId, pipelineSnapTime.asColumnTS)
        } catch {
          case e: Throwable => {
            val appendTrackerErrorMsg = s"Append to Event Log File Tracker Failed. Event Log files glob included files " +
              s"${pathsGlob.mkString(", ")}"
            logger.log(Level.ERROR, appendTrackerErrorMsg, e)
            println(appendTrackerErrorMsg, e)
            throw e
          }
        }

        // Dropping 'Spark Infos' because Overwatch ETLs utilize joins to go from jobs -> stages -> tasks and thus
        // No value is lost in dropping Spark Infos. Furthermore, Spark Infos is often null for some of the nested structs
        // which causes a schema failure when appending to existing spark_events_bronze.
        val dropCols = Array("Classpath Entries", "System Properties", "sparkPlanInfo", "Spark Properties",
          "System Properties", "HadoopProperties", "Hadoop Properties", "SparkContext Id", "Stage Infos")

        // GZ files -- very compressed, need to get sufficienct parallelism but too much and there can be too
        // many tasks to serialize the returned schema from each task
        //        val tempMaxPartBytes = if (daysToProcess >= 3) 1024 * 1024 * 32 else 1024 * 1024 * 16
        //        logger.log(Level.INFO, s"Temporarily setting spark.sql.files.maxPartitionBytes --> ${tempMaxPartBytes}")
        //        spark.conf.set("spark.sql.files.maxPartitionBytes", tempMaxPartBytes)

        val baseEventsDF = try {
          /**
           * Event org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent has a duplicate column
           * "timestamp" where the type is a string and the column name is "timestamp". This conflicts with the rest
           * of the event log where the column name is "Timestamp" and its type is "Long"; thus, the catch for
           * the aforementioned event is specifically there to resolve the timestamp issue when this event is present.
           */
          val streamingQueryListenerTS = 'Timestamp.isNull && 'timestamp.isNotNull && 'Event === "org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent"

          // Enable Spark to read case sensitive columns
          spark.conf.set("spark.sql.caseSensitive", "true")

          // read the df and convert the timestamp column
          val baseDF = spark.read.option("badRecordsPath", badRecordsPath)
            .json(pathsGlob: _*)
            .drop(dropCols: _*)

          val hasUpperTimestamp = baseDF.schema.fields.map(_.name).contains("Timestamp")
          val hasLower_timestamp = baseDF.schema.fields.map(_.name).contains("timestamp")

          val fixDupTimestamps = if (hasUpperTimestamp && hasLower_timestamp) {
            when(streamingQueryListenerTS, TransformFunctions.stringTsToUnixMillis('timestamp)).otherwise('Timestamp)
          } else if (hasLower_timestamp) {
            TransformFunctions.stringTsToUnixMillis('timestamp)
          } else col("Timestamp")

          baseDF
            .withColumn("Timestamp", fixDupTimestamps)
            .drop("timestamp")


        } catch {
          case e: Throwable => {
            val failFilesSQL =
              s"""
                 |update ${processedLogFilesTracker.tableFullName} set failed = true where
                 |Overwatch_RunID = '$runID'
                 |""".stripMargin
            spark.sql(failFilesSQL)
            spark.conf.set("spark.sql.caseSensitive", "false")
            throw e
          }
        }

        // Handle custom metrics and listeners in streams
        val progressCol = if (baseEventsDF.schema.fields.map(_.name.toLowerCase).contains("progress")) {
          to_json(col("progress")).alias("progress")
        } else {
          lit(null).cast("string").alias("progress")
        }

        // Temporary Solution for Speculative Tasks bad Schema - SC-38615
        val stageIDColumnOverride: Column = if (baseEventsDF.columns.contains("stageId")) {
          when('Event === "org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted", col("stageId"))
            .otherwise(col("Stage ID"))
        } else col("Stage ID")

        val stageAttemptIDColumnOverride: Column = if (baseEventsDF.columns.contains("stageAttemptId")) {
          when('Event === "org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted", col("stageAttemptId"))
            .otherwise(col("Stage Attempt ID"))
        } else col("Stage Attempt ID")

        val bronzeSparkEventsScrubber = getSparkEventsSchemaScrubber(baseEventsDF)

        val rawScrubbed = if (baseEventsDF.columns.count(_.toLowerCase().replace(" ", "") == "stageid") > 1) {
          baseEventsDF
            .withColumn("progress", progressCol)
            .withColumn("filename", input_file_name)
            .withColumn("pathSize", size(split('filename, "/")))
            .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
            .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
            .withColumn("StageID", stageIDColumnOverride)
            .withColumn("StageAttemptID", stageAttemptIDColumnOverride)
            .drop("pathSize", "Stage ID", "stageId", "Stage Attempt ID", "stageAttemptId")
            .withColumn("filenameGroup", groupFilename('filename))
            .scrubSchema(bronzeSparkEventsScrubber)
        } else {
          baseEventsDF
            .withColumn("progress", progressCol)
            .withColumn("filename", input_file_name)
            .withColumn("pathSize", size(split('filename, "/")))
            .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
            .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
            .drop("pathSize")
            .withColumn("filenameGroup", groupFilename('filename))
            .scrubSchema(bronzeSparkEventsScrubber)
        }

        // persist to temp to ensure all raw files are not read multiple times
        val sparkEventsTempPath = s"$tempDir/sparkEventsBronze/${pipelineSnapTime.asUnixTimeMilli}"

        rawScrubbed.withColumn("Properties", SchemaTools.structToMap(rawScrubbed, "Properties"))
          .withColumn("modifiedConfigs", SchemaTools.structToMap(rawScrubbed, "modifiedConfigs"))
          .withColumn("extraTags", SchemaTools.structToMap(rawScrubbed, "extraTags"))
          .withColumnRenamed("executorId", "blackListedExecutorIds")
          .join(eventLogsDF, Seq("filename"))
          .withColumn("organization_id", lit(organizationId))
          .withColumn("Properties", expr("map_filter(Properties, (k,v) -> k not in ('sparkexecutorextraClassPath'))"))
          .write.format("delta")
          .mode("overwrite")
          .save(sparkEventsTempPath)

        val bronzeEventsFinal = spark.read.format("delta").load(sparkEventsTempPath)
          .verifyMinimumSchema(Schema.sparkEventsRawMasterSchema)
          .cullNestedColumns("TaskMetrics", Array("UpdatedBlocks"))

        spark.conf.set("spark.sql.caseSensitive", "false")
        // TODO -- PERF test without unpersist, may be unpersisted before re-utilized
        //        cachedEventLogs.unpersist()

        bronzeEventsFinal
      } else {
        val msg = "Path Globs Empty, exiting"
        println(msg)
        throw new NoNewDataException(msg, Level.WARN, true)
      }
    } else {
      val msg = "Event Logs DF is empty, Exiting"
      println(msg)
      throw new NoNewDataException(msg, Level.WARN, true)
    }
  }

  protected def collectEventLogPaths(
                                      fromTime: TimeTypes,
                                      untilTime: TimeTypes,
                                      daysToProcess: Int,
                                      historicalAuditLookupDF: DataFrame,
                                      clusterSnapshotTable: PipelineTable,
                                      sparkLogClusterScaleCoefficient: Double
                                    )(incrementalAuditDF: DataFrame): DataFrame = {

    logger.log(Level.INFO, "Collecting Event Log Paths Glob. This can take a while depending on the " +
      "number of new paths.")

    /**
     * Multiplying current totalCores by scaleCoeff because the cluster will not have scaled up by the time this
     * variable is set, thus this must account for the impending scale up event by scaleCoeff
     */
    val coreCount = (getTotalCores * sparkLogClusterScaleCoefficient).toInt
    val fromTimeEpochMillis = fromTime.asUnixTimeMilli
    val untilTimeEpochMillis = untilTime.asUnixTimeMilli

    val clusterSnapshot = clusterSnapshotTable.asDF
    // Shoot for partitions coreCount < 16 partitions per day < 576
    // This forces autoscaling clusters to scale up appropriately to handle the volume
    val optimizeParCount = math.min(math.max(coreCount * 2, daysToProcess * 32), 1024)
    val incrementalClusterIDs = getClusterIdsWithNewEvents(incrementalAuditDF, clusterSnapshot)

    // clusterIDs with activity identified from audit logs since last run
    val incrementalClusterWLogging = historicalAuditLookupDF
      .withColumn("global_cluster_id", cluster_idFromAudit)
      .select('global_cluster_id.alias("cluster_id"), $"requestParams.cluster_log_conf")
      .join(incrementalClusterIDs, Seq("cluster_id"))
      .withColumn("cluster_log_conf", coalesce(get_json_object('cluster_log_conf, "$.dbfs"), get_json_object('cluster_log_conf, "$.s3")))
      .withColumn("cluster_log_conf", get_json_object('cluster_log_conf, "$.destination"))
      .filter('cluster_log_conf.isNotNull)

    // Get latest incremental snapshot of clusters with logging dirs but not existing in audit updates
    // This captures clusters that have not been edited/restarted since the last run with
    // log confs as they will not be in the audit logs
    val latestSnapW = Window.partitionBy('organization_id, 'cluster_id).orderBy('Pipeline_SnapTS.desc)
    val newLogDirsNotIdentifiedInAudit = clusterSnapshot
      .join(incrementalClusterIDs, Seq("cluster_id"))
      .withColumn("snapRnk", rank.over(latestSnapW))
      .filter('snapRnk === 1)
      .withColumn("cluster_log_conf", coalesce($"cluster_log_conf.dbfs.destination", $"cluster_log_conf.s3.destination"))
      .filter('cluster_id.isNotNull && 'cluster_log_conf.isNotNull)
      .select('cluster_id, 'cluster_log_conf)

    // Build root level eventLog path prefix from clusterID and log conf
    // /some/log/prefix/cluster_id/eventlog
    val allEventLogPrefixes = newLogDirsNotIdentifiedInAudit
      .unionByName(incrementalClusterWLogging)
      .withColumn("cluster_log_conf", when('cluster_log_conf.endsWith("/"), 'cluster_log_conf.substr(lit(0), length('cluster_log_conf) - 1)).otherwise('cluster_log_conf))
      .withColumn("topLevelTargets", array(col("cluster_log_conf"), col("cluster_id"), lit("eventlog")))
      .withColumn("wildPrefix", concat_ws("/", 'topLevelTargets))
      .select('wildPrefix)
      .distinct()

    // all files considered for ingest
    val hadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val eventLogPaths = allEventLogPrefixes
      .repartition(optimizeParCount)
      .as[String]
      .map(x => Helpers.parListFiles(x, hadoopConf)) // parallelized file lister since large / shared / long-running (months) clusters will have MANy paths
      .select(explode('value).alias("logPathPrefix"))
      .withColumn("logPathPrefix", concat_ws("/", 'logPathPrefix, lit("*"), lit("eventlo*")))
      .repartition(optimizeParCount)
      .as[String]
      .map(p => Helpers.globPath(p, hadoopConf, Some(fromTimeEpochMillis), Some(untilTimeEpochMillis)))
      .select(explode('value).alias("simpleFileStatus"))
      .selectExpr("simpleFileStatus.*")
      .withColumnRenamed("pathString", "filename")
      .withColumn("fileCreateTS", from_unixtime('fileCreateEpochMS / lit(1000)).cast("timestamp"))
      .withColumn("fileCreateDate", 'fileCreateTS.cast("date"))
      .repartition().cache()

    // eager execution to minimize secondary compute downstream
    eventLogPaths.count()
    eventLogPaths

  }

}
