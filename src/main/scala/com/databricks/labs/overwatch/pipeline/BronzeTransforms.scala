package com.databricks.labs.overwatch.pipeline

import java.io.FileNotFoundException
import java.time.{Duration, LocalDate, LocalDateTime}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.{SparkSessionWrapper, _}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.catalyst.expressions.Slice
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool


trait BronzeTransforms extends SparkSessionWrapper {

  import spark.implicits._
  import TransformFunctions._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _newDataRetrieved: Boolean = true

  //  case class ClusterEventsBuffer(clusterId: String, batch: Int, extraQuery: Map[String, Long])
  case class ClusterIdsWEventCounts(clusterId: String, count: Long)

  private def structFromJson(df: DataFrame, c: String): Column = {
    require(df.schema.fields.map(_.name).contains(c), s"The dataframe does not contain col $c")
    require(df.schema.fields.filter(_.name == c).head.dataType.isInstanceOf[StringType], "Column must be a json formatted string")
    val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
    if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
      println(s"WARNING: The json schema for column $c was not parsed correctly, please review.")
    }
    from_json(col(c), jsonSchema).alias(c)
  }

  protected def setNewDataRetrievedFlag(value: Boolean): this.type = {
    _newDataRetrieved = value
    this
  }

  protected def newDataRetrieved: Boolean = _newDataRetrieved

  private def apiByID[T](endpoint: String, apiEnv: ApiEnv,
                         apiType: String,
                         ids: Array[T], idsKey: String,
                         extraQuery: Option[Map[String, Any]] = None): Array[String] = {
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(24))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport
    //    DEBUG
    //    val idsPar = Array("0827-194754-tithe1")
    // removing parallelization for now to see if it fixes some weird errors
    // CONFIRMED -- Parallelizing this breaks the token cipher
    // TODO - Identify why parallel errors
    val results = ids.flatMap(id => {
      val rawQuery = Map(
        idsKey -> id
      )

      val query = if (extraQuery.nonEmpty) {
        extraQuery.get ++ rawQuery
      } else rawQuery

      val apiCall = ApiCall(endpoint, apiEnv, Some(query))
      if (apiType == "post") apiCall.executePost().asStrings
      else apiCall.executeGet().asStrings
    }) //.toArray // -- needed when using par
    if (results.isEmpty) {
      Array()
    }
    else results
  }

//  private def datesStream(fromDate: LocalDate): Stream[LocalDate] = {
//    fromDate #:: datesStream(fromDate plusDays 1)
//  }

  private def validateCleanPaths(isFirstRun: Boolean,
                                 ehConfig: AzureAuditLogEventhubConfig): Boolean = {
    var firstRunValid = true
    var appendRunValid = true
    val pathsToValidate = Array(
      ehConfig.auditRawEventsChk.get
    )

    logger.log(Level.INFO, s"chkpoint paths for validation are ${pathsToValidate.mkString(",")}")

    pathsToValidate.foreach(path => {
      try { // succeeds if path does exist
        logger.log(Level.INFO, s"Validating: ${path}")
        dbutils.fs.ls(path).head.isDir
        firstRunValid = false
        if (isFirstRun) {
          logger.log(Level.INFO, s"${path} exists as directory confirmed. Invalid first run")
          logger.log(Level.ERROR, s"${path} is not empty.")
          println(s"${path} is not empty. First run requires empty checkpoints.")
        }
      } catch { // path does not exist
        case _: FileNotFoundException =>
          appendRunValid = false
          logger.log(Level.INFO, s"Path: ${path} Does not exist. To append new data, a checkpoint dir must " +
            s"exist and be current.")
        case e: Throwable => logger.log(Level.ERROR, s"Could not validate path ${path}", e)
      }
    })

    if (isFirstRun) firstRunValid
    else appendRunValid
  }

  @throws(classOf[BadConfigException])
  protected def landAzureAuditLogDF(ehConfig: AzureAuditLogEventhubConfig,
                                    isFirstRun: Boolean,
                                    organizationId: String,
                                    runID: String
                                   ): DataFrame = {

    if (!validateCleanPaths(isFirstRun, ehConfig))
      throw new BadConfigException("Azure Event Hub Paths are not empty on first run")

    val connectionString = ConnectionStringBuilder(ehConfig.connectionString)
      .setEventHubName(ehConfig.eventHubName)
      .build

    val eventHubsConf = if (isFirstRun) {
      EventHubsConf(connectionString)
        .setMaxEventsPerTrigger(ehConfig.maxEventsPerTrigger)
        .setStartingPosition(EventPosition.fromStartOfStream)
    } else {
      EventHubsConf(connectionString)
        .setMaxEventsPerTrigger(ehConfig.maxEventsPerTrigger)
    }

    spark.readStream
      .format("eventhubs")
      .options(eventHubsConf.toMap)
      .load()
      .withColumn("deserializedBody", 'body.cast("string"))
      .withColumn("organization_id", lit(organizationId))
      .withColumn("Overwatch_RunID", lit(runID))

  }

  protected def cleanseRawClusterSnapDF(cloudProvider: String)(df: DataFrame): DataFrame = {
    var outputDF = SchemaTools.scrubSchema(df)

    outputDF = outputDF
      .withColumn("custom_tags", SchemaTools.structToMap(outputDF, "custom_tags"))
      .withColumn("spark_conf", SchemaTools.structToMap(outputDF, "spark_conf"))
      .withColumn("spark_env_vars", SchemaTools.structToMap(outputDF, "spark_env_vars"))

    if (cloudProvider == "aws") outputDF = outputDF
      .withColumn("aws_attributes", SchemaTools.structToMap(outputDF, "aws_attributes"))

    outputDF
  }

  protected def cleanseRawPoolsDF()(df: DataFrame): DataFrame = {
    df.withColumn("custom_tags", SchemaTools.structToMap(df, "custom_tags"))
  }

  protected def getAuditLogsDF(auditLogConfig: AuditLogConfig,
                               isFirstRun: Boolean,
                               cloudProvider: String,
                               fromTime: LocalDateTime,
                               untilTime: LocalDateTime,
                               auditRawLand: PipelineTable,
                               overwatchRunID: String,
                               organizationId: String
                              ): DataFrame = {
    if (cloudProvider == "azure") {
      val rawBodyLookup = spark.table(auditRawLand.tableFullName)
        .filter('Overwatch_RunID === lit(overwatchRunID))
        .filter('organization_id === organizationId)
      val schemaBuilders = spark.table(auditRawLand.tableFullName)
        .filter('Overwatch_RunID === lit(overwatchRunID))
        .filter('organization_id === organizationId)
        .withColumn("parsedBody", structFromJson(rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"), 'organization_id)
        .selectExpr("streamRecord.*", "organization_id")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"), 'organization_id)
        .selectExpr("*", "properties.*").drop("properties")


      spark.table(auditRawLand.tableFullName)
        .filter('Overwatch_RunID === lit(overwatchRunID))
        .withColumn("parsedBody", structFromJson(rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"), 'organization_id)
        .selectExpr("streamRecord.*", "organization_id")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"), 'organization_id)
        .withColumn("userIdentity", structFromJson(schemaBuilders, "userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")
        .withColumn("requestParams", structFromJson(schemaBuilders, "requestParams"))
        .withColumn("response", structFromJson(schemaBuilders, "response"))
        .drop("logId")

    } else {

      // TODO -- fix this -- SO UGLY
      //  might be good to build from time in initializer better on a source case basis
      //  don't attempt this until after tests are in place
      val fromDT = fromTime.toLocalDate
      val untilDT = untilTime.toLocalDate
      // inclusive from exclusive to
      val datesGlob = datesStream(fromDT).takeWhile(_.isBefore(untilDT)).toArray
        .map(dt => s"${auditLogConfig.rawAuditPath.get}/date=${dt}")
        .filter(Helpers.pathExists)

      if (datesGlob.nonEmpty) {
        spark.read.json(datesGlob: _*)
          // When globbing the paths, the date must be reconstructed and re-added manually
          .withColumn("organization_id", lit(organizationId))
          .withColumn("filename", input_file_name)
          .withColumn("filenameAR", split(input_file_name, "/"))
          .withColumn("date",
            split(expr("filter(filenameAR, x -> x like ('date=%'))")(0), "=")(1).cast("date"))
          .drop("filenameAR")
      } else {
        //        throw new NoNewDataException(s"EMPTY: Audit Logs Bronze, no new data found between ${fromDT.toString}-${untilDT.toString}")
        spark.emptyDataFrame
        //        Seq("No New Records").toDF("__OVERWATCHEMPTY")
      }
    }
  }

  private def buildClusterEventBatches(apiEnv: ApiEnv, batchSize: Double, startTSMilli: Long,
                                       endTSMilli: Long, clusterIDs: Array[String]): Array[Array[String]] = {

    case class ClusterEventBuffer(clusterId: String, batchId: Int)
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(24))
    val clusterIdsPar = clusterIDs.par
    clusterIdsPar.tasksupport = taskSupport


    var cumSum = 0L

    logger.log(Level.INFO, s"OVERWATCH: BUILDING CLUSTER EVENTS for ${clusterIDs.length}. Large dynamic clusters," +
      s"will take some time, especially on first runs. At present" +
      "the cluster events can only be acquired via api calls which are rate limited.\n" +
      s"BATCH SIZE: ${batchSize}\n" +
      s"CLUSTERS COUNT: ${clusterIDs.length}\n" +
      s"START TIMESTAMP: ${startTSMilli}\n" +
      s"END TIMESTAMP: ${endTSMilli} \n" +
      s"CLUSTERIDs: ${clusterIDs.mkString(", ")}")

    clusterIdsPar.map(clusterId => {

      try {

        val getLastEvent = Map[String, Any](
          "cluster_id" -> clusterId,
          "start_time" -> startTSMilli,
          "end_time" -> endTSMilli,
          "order" -> "DESC",
          "limit" -> 1
        )

        val lastEventRaw = ApiCall("clusters/events", apiEnv, Some(getLastEvent), paginate = false).executePost().asStrings
        if (lastEventRaw.nonEmpty) {
          val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
            .registerModule(DefaultScalaModule)
            .registerModule(new SimpleModule())
            .asInstanceOf[ObjectMapper with ScalaObjectMapper]

          val totalCount = mapper.readTree(lastEventRaw.head).get("total_count").asLong(0L)
          Some(ClusterIdsWEventCounts(clusterId, totalCount))

        } else {
          None
        }

      } catch {
        case e: Throwable => {
          logger.log(Level.ERROR, s"ERROR: Failed to acquire events for Cluster: ${clusterId}", e)
          None
        }
      }
    }).toArray.filter(_.nonEmpty).map(clusterEvents => {
      cumSum += clusterEvents.get.count
      val batchId = Math.ceil(cumSum / batchSize).toInt
      (clusterEvents.get.clusterId, batchId)
    }).groupBy(_._2).map(_._2.map(_._1).toArray).toArray

  }

  //  protected
  def prepClusterEventLogs(auditLogsTable: PipelineTable,
                           start_time: TimeTypes, end_time: TimeTypes,
                           apiEnv: ApiEnv,
                           organizationId: String): DataFrame = {
    val extraQuery = Map(
      "start_time" -> start_time.asUnixTimeMilli, // 1588935326000L, //
      "end_time" -> end_time.asUnixTimeMilli, //1589021726000L //
      "limit" -> 500
    )

    // TODO -- upgrade to incrementalDF
    val auditDFBase = auditLogsTable.asDF
      .filter(
        'date.between(start_time.asColumnTS.cast("date"), end_time.asColumnTS.cast("date")) &&
          'timestamp.between(lit(start_time.asUnixTimeMilli), lit(end_time.asUnixTimeMilli))
      )

    val existingClusterIds = auditDFBase
      .filter('serviceName === "clusters" && 'actionName.like("%Result"))
      .select($"requestParams.clusterId".alias("cluster_id"))
      .filter('cluster_id.isNotNull)
      .distinct

    val newClusterIds = auditDFBase
      .filter('serviceName === "clusters" && 'actionName === "create")
      .select(get_json_object($"response.result", "$.cluster_id").alias("cluster_id"))
      .filter('cluster_id.isNotNull)
      .distinct

    val clusterIDs = existingClusterIds
      .unionByName(newClusterIds)
      .distinct
      .as[String]
      .collect()

    /**
     * NOTE *** IMPORTANT
     * Large batches are more efficient but can result in OOM with driver.maxResultSize. To avoid this it's
     * important to increase the driver.maxResultSize for non-periodic runs with this module
     * Ensure large enough driver (memory) and add this to cluster config
     * spark.driver.maxResultSize 32g
     */
    val batchSize = 500000D
    //    val batchSize = 50000D // DEBUG
    val tmpClusterEventsPath = "/tmp/overwatch/bronze/clusterEventsBatches"
    val clusterEventsBuffer = buildClusterEventBatches(apiEnv, batchSize, start_time.asUnixTimeMilli, end_time.asUnixTimeMilli, clusterIDs)

    logger.log(Level.INFO, s"NUMBER OF BATCHES: ${clusterEventsBuffer.length} \n" +
      s"ESTIMATED EVENTS: ${clusterEventsBuffer.length * batchSize.toInt}")

    var batchCounter = 0
    clusterEventsBuffer.foreach(clusterIdsBatch => {
      batchCounter += 1
      logger.log(Level.INFO, s"BEGINNING BATCH ${batchCounter} of ${clusterEventsBuffer.length}")
      val clusterEvents = apiByID("clusters/events", apiEnv, "post",
        clusterIdsBatch, "cluster_id", Some(extraQuery))

      try {
        val tdf = SchemaTools.scrubSchema(
          spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
            .select(col("events.*"))
        )

        // DEBUG
        //        tdf.printSchema()

        val changeInventory = Map[String, Column](
          "details.attributes.custom_tags" -> SchemaTools.structToMap(tdf, "details.attributes.custom_tags"),
          "details.attributes.spark_conf" -> SchemaTools.structToMap(tdf, "details.attributes.spark_conf"),
          "details.attributes.spark_env_vars" -> SchemaTools.structToMap(tdf, "details.attributes.spark_env_vars"),
          "details.previous_attributes.custom_tags" -> SchemaTools.structToMap(tdf, "details.previous_attributes.custom_tags"),
          "details.previous_attributes.spark_conf" -> SchemaTools.structToMap(tdf, "details.previous_attributes.spark_conf"),
          "details.previous_attributes.spark_env_vars" -> SchemaTools.structToMap(tdf, "details.previous_attributes.spark_env_vars")
        )

        SchemaTools.scrubSchema(tdf.select(SchemaTools.modifyStruct(tdf.schema, changeInventory): _*))
          .withColumn("organization_id", lit(organizationId))
          .write.mode("append").format("delta")
          .option("mergeSchema", "true")
          .save(tmpClusterEventsPath)

      } catch {
        case e: Throwable => {
          logger.log(Level.WARN, s"While attempting to grab events data for clusters below, an error occurred" +
            s"\n${clusterIdsBatch.foreach(println)}", e)
        }
      }

    })

    logger.log(Level.INFO, "COMPLETE: Cluster Events acquisition, building data")
    if (Helpers.pathExists(tmpClusterEventsPath)) {
      val clusterEventsDF = spark.read.format("delta").load(tmpClusterEventsPath)
      val clusterEventsCaptured = clusterEventsDF.count
      val logEventsMSG = s"CLUSTER EVENTS CAPTURED: ${clusterEventsCaptured}"
      logger.log(Level.INFO, logEventsMSG)
      clusterEventsDF
    } else {
      println("EMPTY MODULE: Cluster Events")
      spark.emptyDataFrame
      //      Seq("").toDF("__OVERWATCHEMPTY")
      //      throw new NoNewDataException("EMPTY: No New Cluster Events")
    }

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
      .coalesce(4) // narrow, short table -- each append will == spark event log files processed
    database.write(fileTrackerDF, trackerTarget, pipelineSnapTime)
  }

  /**
   * Remove already processed and bad files
   * Before loading spark events files, ensure that only new, good files are ingested for processing
   * @param badRecordsPath
   * @param eventLogsDF
   * @param processedLogFiles
   * @return
   */
  private def retrieveNewValidSparkEventsWMeta(badRecordsPath: String,
                                               eventLogsDF: DataFrame,
                                               processedLogFiles: PipelineTable): DataFrame = {
    val validNewFiles = if (processedLogFiles.exists) {
      val alreadyProcessed = processedLogFiles.asDF
        .filter(!'failed)
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
    struct(filename, byCluster, byClusterHost, bySparkContextID).alias("filnameGroup")
  }


  //  val index = df.schema.fieldIndex("properties")
  //  val propSchema = df.schema(index).dataType.asInstanceOf[StructType]
  //  var columns = mutable.LinkedHashSet[Column]()
  //  propSchema.fields.foreach(field =>{
  //    columns.add(lit(field.name))
  //    columns.add(col("properties." + field.name))
  //  })

  def generateEventLogsDF(database: Database,
                          badRecordsPath: String,
                          processedLogFilesTracker: PipelineTable,
                          organizationId: String,
                          rundID: String,
                          pipelineSnapTime: Column
                         )(eventLogsDF: DataFrame): DataFrame = {

    logger.log(Level.INFO, "Searching for Event logs")
    // Caching is done to ensure a single scan of the event log file paths
    // From here forward there should be no more direct scans for new records, just loading data direct from paths
    // eager force cache
    // TODO -- Delta auto-optimize seems to be scanning the source files again anyway during
    //  execute at DeltaInvariantCheckerExec.scala:95 -- review again after upgrade to DBR 7.x+
    val cachedEventLogs = eventLogsDF.cache()
    val eventLogsCount = cachedEventLogs.count()
    logger.log(Level.INFO, s"EVENT LOGS FOUND: Total Found --> ${eventLogsCount}")

    if (eventLogsCount > 0) { // newly found file names
      // All new files scanned including failed and outOfTimeRange files
      val validNewFilesWMetaDF = retrieveNewValidSparkEventsWMeta(badRecordsPath, cachedEventLogs, processedLogFilesTracker)
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
          appendNewFilesToTracker(database, validNewFilesWMetaDF, processedLogFilesTracker, organizationId, pipelineSnapTime)
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
          val basedDF = spark.read.option("badRecordsPath", badRecordsPath)
            .json(pathsGlob: _*)
            .drop(dropCols: _*)

          val hasUpperTimestamp = basedDF.schema.fields.map(_.name).contains("Timestamp")
          val hasLower_timestamp = basedDF.schema.fields.map(_.name).contains("timestamp")

          val fixDupTimestamps = if (hasUpperTimestamp && hasLower_timestamp) when(streamingQueryListenerTS, TransformFunctions.stringTsToUnixMillis('timestamp)).otherwise('Timestamp)
          else if (hasLower_timestamp) TransformFunctions.stringTsToUnixMillis('timestamp)
          else col("Timestamp")

          basedDF
            .withColumn("Timestamp", fixDupTimestamps)
            .drop("timestamp")

        } catch {
          case e: Throwable => {
            val failFilesSQL =
              s"""
                 |update ${processedLogFilesTracker.tableFullName} set failed = true where
                 |Overwatch_RunID = $rundID
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
        val stageIDColumnOverride: Column = if (baseEventsDF.columns.contains("Stage ID")) {
          when('StageID.isNull && $"Stage ID".isNotNull, $"Stage ID").otherwise('StageID)
        } else 'StageID

        val rawScrubbed = if (baseEventsDF.columns.count(_.toLowerCase().replace(" ", "") == "stageid") > 1) {
          SchemaTools.scrubSchema(baseEventsDF
            .withColumn("progress", progressCol)
            .withColumn("filename", input_file_name)
            .withColumn("pathSize", size(split('filename, "/")))
            .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
            .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
            .withColumn("StageID", stageIDColumnOverride)
            .drop("pathSize", "Stage ID")
            .withColumn("filenameGroup", groupFilename('filename))
          )
        } else {
          SchemaTools.scrubSchema(baseEventsDF
            .withColumn("progress", progressCol)
            .withColumn("filename", input_file_name)
            .withColumn("pathSize", size(split('filename, "/")))
            .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
            .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
            .drop("pathSize")
            .withColumn("filenameGroup", groupFilename('filename))
          )
        }

        val bronzeEventsFinal = rawScrubbed.withColumn("Properties", SchemaTools.structToMap(rawScrubbed, "Properties"))
          .join(cachedEventLogs, Seq("filename"))
          .withColumn("organization_id", lit(organizationId))
        //TODO -- use map_filter to remove massive redundant useless column to save space
        // asOf Spark 3.0.0
        //.withColumn("Properties", expr("map_filter(Properties, (k,v) -> k not in ('sparkexecutorextraClassPath'))"))

        spark.conf.set("spark.sql.caseSensitive", "false")
        cachedEventLogs.unpersist()

        bronzeEventsFinal
      } else {
        val msg = "Path Globs Empty, exiting"
        println(msg)
        throw new NoNewDataException(msg, Level.WARN, true)
//        spark.emptyDataFrame
        //        throw new NoNewDataException("EMPTY: No new Spark Events Files")
        //        Seq("No New Event Logs Found").toDF("__OVERWATCHEMPTY")
      }
    } else {
      val msg = "Event Logs DF is empty, Exiting"
      println(msg)
      throw new NoNewDataException(msg, Level.WARN, true)

//      spark.emptyDataFrame
      //      throw new NoNewDataException("EMPTY: No new Spark Events Files")
      //      Seq("No New Event Logs Found").toDF("__OVERWATCHEMPTY")
    }
  }

  protected def collectEventLogPaths(
                                      fromTime: TimeTypes,
                                      untilTime: TimeTypes,
                                      processedEventLogFiles: PipelineTable,
                                      clusterSnapshot: PipelineTable,
                                      isFirstRun: Boolean
                                    )(df: DataFrame): DataFrame = {

    logger.log(Level.INFO, "Collecting Event Log Paths Glob. This can take a while depending on the " +
      "number of new paths.")

    val coreCount = getTotalCores
    val fromTimeEpochMillis = fromTime.asUnixTimeMilli
    val untilTimeEpochMillis = untilTime.asUnixTimeMilli
    val fromDate = fromTime.asLocalDateTime.toLocalDate
    val untilDate = untilTime.asLocalDateTime.toLocalDate
    val daysToProcess = Duration.between(fromDate.atStartOfDay(), untilDate.plusDays(1L).atStartOfDay())
      .toDays.toInt

    // Shoot for partitions coreCount < 16 partitions per day < 576
    // This forces autoscaling clusters to scale up appropriately to handle the volume
    val optimizeParCount = math.min(math.max(coreCount, daysToProcess * 16), 576)

    // baseline of clusters from incremental audit logs
    // Incremental throughout this function means the incrementally loaded data between time x and time y
    val incrementalClusterBase = df
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .filter('serviceName === "clusters" && 'actionName.isin("create", "edit"))
      .withColumn("cluster_id", when('actionName === "create", get_json_object($"response.result", "$.cluster_id"))
        .when('actionName =!= "create" && 'cluster_id.isNull, 'clusterId)
        .otherwise('cluster_id).alias("cluster_id")
      )
      .select('organization_id, 'timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)

    // Get incremental snapshot of clusters during current run
    // This captures clusters that have not been edited/restarted (still not terminated) since the last run with
    // log confs as they will not be in the audit logs
    val latestSnapW = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
    val currentlyDefinedClustersWithLogging = clusterSnapshot.asDF
      .withColumn("snapRnk", rank.over(latestSnapW))
      .filter('snapRnk === 1)
      .withColumn("cluster_log_conf", to_json('cluster_log_conf))
      .filter('cluster_id.isNotNull && 'cluster_log_conf.isNotNull)
      .select('cluster_id, 'cluster_log_conf)

    // captures all incremental created/edited clusters from the audit log with logs enabled
    val allIncrementalPrefixes = incrementalClusterBase
      .filter('cluster_log_conf.isNotNull)
      .select('cluster_id, 'cluster_log_conf)

    // Build root level eventLog path prefix from clusterID and log conf
    // /some/log/prefix/cluster_id/eventlog
    val currentAndIncrementalLogRootPaths = currentlyDefinedClustersWithLogging
      .unionByName(allIncrementalPrefixes)
      .withColumn("s3", get_json_object('cluster_log_conf, "$.s3"))
      .withColumn("dbfs", get_json_object('cluster_log_conf, "$.dbfs"))
      .withColumn("destination",
        when('s3.isNotNull, regexp_replace(get_json_object('s3, "$.destination"), "\\/$", ""))
          when('dbfs.isNotNull, regexp_replace(get_json_object('dbfs, "$.destination"), "\\/$", ""))
      )
      .withColumn("topLevelTargets",
        array(col("destination"), col("cluster_id"),
          lit("eventlog"))
      ).withColumn("wildPath", concat_ws("/", 'topLevelTargets))
      .select('wildPath)

    // IF previously loaded log files
    // Scan for new files in historical cluster, log configs and append them to the incrementals
    val allEventLogPrefixes = if (processedEventLogFiles.exists) { // log files captured before
      // Captures Terminated clusters with new files
      // Assume cluster had log files during last run but was terminated between last run and current run and there
      // were no edits or restarts to this cluster -- this cluster would not be in either the snapshot or the
      // incremental audit logs -- thus it would be missed without loading these as well
      val allHistoricalEventLogRootPrefixes = processedEventLogFiles.asDF
        .withColumn("pathAr", split('filename, "/"))
        .withColumn("pathDepth", size('pathAr))
        .withColumn("clusterEventLogPrefix", new Column(Slice('pathAr.expr, lit(1).expr, ('pathDepth - lit(3)).expr))) // TODO - unsupported hack until DBR 8.0+
        .withColumn("wildPath", concat_ws("/", 'clusterEventLogPrefix))
        .select('wildPath)

      currentAndIncrementalLogRootPaths
        .unionByName(allHistoricalEventLogRootPrefixes)
        .distinct

    } else currentAndIncrementalLogRootPaths.distinct

    // all files considered for ingest
    allEventLogPrefixes
      .repartition(optimizeParCount)
      .as[String]
      .map(Helpers.parListFiles) // parallelized file lister since large / shared / long-running (months) clusters will have MANy paths
      .select(explode('value).alias("logPathPrefix"))
      .withColumn("logPathPrefix", concat_ws("/", 'logPathPrefix, lit("*"), lit("eventlo*")))
      .repartition(optimizeParCount)
      .as[String]
      .map(p => Helpers.globPath(p, Some(fromTimeEpochMillis), Some(untilTimeEpochMillis)))
      .select(explode('value).alias("simpleFileStatus"))
      .selectExpr("simpleFileStatus.*")
      .withColumnRenamed("pathString", "filename")
      .withColumn("fileCreateTS", from_unixtime('fileCreateEpochMS / lit(1000)).cast("timestamp"))
      .withColumn("fileCreateDate", 'fileCreateTS.cast("date"))

  }

  //  protected def collectEventLogPaths()(df: DataFrame): DataFrame = {
  //
  //    // Best effort to get currently existing cluster ids
  //    val colsDF = df.select($"cluster_log_conf.*")
  //    val cols = colsDF.columns.map(c => s"${c}.destination")
  //    val logsDF = df.select($"cluster_log_conf.*", $"cluster_id".alias("cluster_id"))
  //    cols.flatMap(
  //      c => {
  //        logsDF.select(col("cluster_id"), col(c)).filter(col("destination").isNotNull).distinct
  //          .select(
  //            array(regexp_replace(col("destination"), "\\/$", ""), col("cluster_id"),
  //              lit("eventlog"), lit("*"), lit("*"), lit("eventlo*"))
  //          ).rdd
  //          .map(r => r.getSeq[String](0).mkString("/")).collect()
  //      }).distinct
  //      .flatMap(Helpers.globPath)
  //      .toSeq.toDF("filename")
  //    setEventLogGlob(eventLogsPathGlobDF)
  //    df
  //  }

}
