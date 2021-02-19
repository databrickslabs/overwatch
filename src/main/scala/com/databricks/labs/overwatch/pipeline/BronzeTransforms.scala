package com.databricks.labs.overwatch.pipeline

import java.io.FileNotFoundException
import java.time.{LocalDate, LocalDateTime}

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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool


trait BronzeTransforms extends SparkSessionWrapper {

  import spark.implicits._

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

  @throws(classOf[NoNewDataException])
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

  private def datesStream(fromDate: LocalDate): Stream[LocalDate] = {
    fromDate #:: datesStream(fromDate plusDays 1)
  }

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
                                    organizationId: String
                                   ): DataFrame = {

    if (!validateCleanPaths(isFirstRun, ehConfig))
      throw new BadConfigException("Azure Event Hub Paths are nto empty on first run")

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
        Seq("No New Records").toDF("__OVERWATCHEMPTY")
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
      Seq("").toDF("__OVERWATCHEMPTY")
    }

  }

  private def appendNewFilesToTracker(database: Database,
                                      newFiles: DataFrame,
                                      trackerTarget: PipelineTable,
                                      orgId: String): Unit = {
    val fileTrackerDF = newFiles
      .withColumn("failed", lit(false))
      .withColumn("organization_id", lit(orgId))
    database.write(fileTrackerDF, trackerTarget)
  }

  private def retrieveNewValidSparkEventsWMeta(badRecordsPath: String,
                                               eventLogsDF: DataFrame,
                                               processedLogFiles: PipelineTable): DataFrame = {
    val validNewFiles = if (spark.catalog.tableExists(processedLogFiles.tableFullName)) {
      val alreadyProcessed = processedLogFiles.asDF.select('filename)
        .filter(!'failed)
        .distinct

      if (Helpers.pathExists(badRecordsPath)) {
        val badFiles = spark.read.format("json").load(s"${badRecordsPath}/*/*/")
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
                          organizationId: String)(eventLogsDF: DataFrame): DataFrame = {

    logger.log(Level.INFO, "Searching for Event logs")
    // Caching is done to ensure a single scan of the event log file paths
    val cachedEventLogs = eventLogsDF.cache()
    val eventLogsCount = cachedEventLogs.count() // eager force cache

    logger.log(Level.INFO, s"EVENT LOGS FOUND: Total Found --> ${eventLogsCount}")

    if (cachedEventLogs.take(1).nonEmpty) { // newly found file names
      val validNewFilesWMetaDF = retrieveNewValidSparkEventsWMeta(badRecordsPath, cachedEventLogs, processedLogFilesTracker)
      val pathsGlob = validNewFilesWMetaDF.select('fileName).as[String].collect
      if (pathsGlob.nonEmpty) { // new files less bad files and already-processed files
        logger.log(Level.INFO, s"VALID NEW EVENT LOGS FOUND: COUNT --> ${pathsGlob.length}")
        try {
          logger.log(Level.INFO, "Updating Tracker with new files")
          appendNewFilesToTracker(database, validNewFilesWMetaDF, processedLogFilesTracker, organizationId)
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

        logger.log(Level.INFO, "Attempting to load event log data")
        val baseEventsDF = try {
          spark.read.option("badRecordsPath", badRecordsPath)
            .json(pathsGlob: _*)
            .drop(dropCols: _*)
        } catch {
          /**
           * Event org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent has a duplicate column
           * "timestamp" where the type is a string and the column name is "timestamp". This conflicts with the rest
           * of the event log where the column name is "Timestamp" and its type is "Long"; thus, the catch for
           * the aforementioned event is specifically there to resolve the timestamp issue when this event is present.
           */
          case e: AnalysisException if (e.getMessage().trim
            .equalsIgnoreCase("""Found duplicate column(s) in the data schema: `timestamp`;""")) => {

            logger.log(Level.WARN, "Found duplicate column(s) in the data schema: `timestamp`; attempting to handle")

            val streamingQueryListenerTS = 'Timestamp.isNull && 'timestamp.isNotNull && 'Event === "org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent"

            // Enable Spark to read case sensitive columns
            spark.conf.set("spark.sql.caseSensitive", "true")

            // read the df and convert the timestamp column
            spark.read.option("badRecordsPath", badRecordsPath)
              .json(pathsGlob: _*)
              .drop(dropCols: _*)
              .withColumn("Timestamp",
                when(streamingQueryListenerTS,
                  TransformFunctions.stringTsToUnixMillis('timestamp)
                ).otherwise('Timestamp))
              .drop("timestamp")
          }
          case e: Throwable => {
            logger.log(Level.ERROR, "FAILED spark events bronze", e)
            Seq("").toDF("__OVERWATCHFAILURE")
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

        cachedEventLogs.unpersist()

        bronzeEventsFinal
      } else {
        val msg = "Path Globs Empty, exiting"
        println(msg)
        logger.log(Level.WARN, msg)
        Seq("No New Event Logs Found").toDF("__OVERWATCHEMPTY")
      }
    } else {
      val msg = "Event Logs DF is empty, Exiting"
      println(msg)
      logger.log(Level.WARN, msg)
      Seq("No New Event Logs Found").toDF("__OVERWATCHEMPTY")
    }
  }

  protected def collectEventLogPaths(fromTimeCol: Column,
                                     untilTimeCol: Column,
                                     fromTimeEpochMillis: Long,
                                     untilTimeEpochMillis: Long,
                                     clusterSpec: PipelineTable,
                                     clusterSnapshot: PipelineTable,
                                     isFirstRun: Boolean)(df: DataFrame): DataFrame = {

    logger.log(Level.INFO, "Collecting Event Log Paths Glob. This can take a while depending on the " +
      "number of new paths.")

    //    DEBUG
    //    println("DEBUG TIME COLS")
    //    Seq("").toDF("fromTSCol")
    //      .withColumn("fromTSCol", fromTimeCol)
    //      .withColumn("untilTSCol", untilTimeCol)
    //      .withColumn("fromMillis", lit(fromTimeEpochMillis))
    //      .withColumn("untilMillis", lit(untilTimeEpochMillis))
    //      .withColumn("fromMillisManualTS", from_unixtime('fromMillis / 1000))
    //      .withColumn("untilMillisManualTS", from_unixtime('untilMillis / 1000))
    //      .show(false)

    // Lookup null cluster_ids -- cluster_id and clusterId are both null during "create", AND "changeClusterAcl" actions
    // TODO -- upgrade to incrementalDF
    val latestSnapDate = clusterSnapshot.asDF.select(max('Pipeline_SnapTS).cast("date").cast("string"))
      .as[String].first
    val currentClustersWithLogs = clusterSnapshot.asDF
      .filter('Pipeline_SnapTS.cast("date") === latestSnapDate)
      .select(
        unix_timestamp('Pipeline_SnapTS).alias("timestamp"),
        'cluster_id,
        'cluster_name,
        to_json('cluster_log_conf).alias("cluster_log_conf")
      )
      .filter('cluster_id.isNotNull && 'cluster_log_conf.isNotNull)

    // Populating cluster Ids
    val dfClusterService = df
      .selectExpr("*", "requestParams.*").drop("requestParams")
      .filter('serviceName === "clusters")
      .withColumn("cluster_id",
        when('cluster_id.isNull, 'clusterId).otherwise('cluster_id)
      )
      .withColumn("cluster_id",
        when('cluster_id.isNull, get_json_object(to_json('response), "$.result.cluster_id")).otherwise('cluster_id)
      )
      .filter('cluster_id.isNotNull && 'cluster_log_conf.isNotNull)
      .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
      .unionByName(currentClustersWithLogs)
    //      .cache() //Not caching because delta cache is likely more efficient

    val clusterIDsWithNewData = dfClusterService
      .select('cluster_id)
      .distinct

    val newEventLogPrefixes = if (isFirstRun || !clusterSpec.exists) {
      dfClusterService
    } else {

      val historicalClustersWithNewData = clusterSpec.asDF
        //        .withColumn("date", toTS('timestamp, outputResultType = DateType))
        //        .filter('date.between(fromTimeCol.cast("date"), untilTimeCol.cast("date")))
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
        .join(clusterIDsWithNewData, Seq("cluster_id"))

      val logPrefixesWithNewData = dfClusterService
        .join(clusterIDsWithNewData, Seq("cluster_id"))
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
        .filter('cluster_log_conf.isNotNull)
        .unionByName(historicalClustersWithNewData)

      val existingLogPrefixes = clusterSpec.asDF
        .filter('cluster_id.isNotNull)
        .withColumn("date", TransformFunctions.toTS('timestamp, outputResultType = DateType))
        .filter('cluster_log_conf.isNotNull && 'actionName.isin("create", "edit"))
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)

      existingLogPrefixes
        .unionByName(logPrefixesWithNewData)
    }

    val newEventLogGlobs = newEventLogPrefixes
      .withColumn("s3", get_json_object('cluster_log_conf, "$.s3"))
      .withColumn("dbfs", get_json_object('cluster_log_conf, "$.dbfs"))
      .withColumn("destination",
        when('s3.isNotNull, regexp_replace(get_json_object('s3, "$.destination"), "\\/$", ""))
          when('dbfs.isNotNull, regexp_replace(get_json_object('dbfs, "$.destination"), "\\/$", ""))
      )
      .select(
        array(col("destination"), col("cluster_id"),
          lit("eventlog"), lit("*"), lit("*"), lit("eventlo*")).alias("wildPath")
      ).withColumn("wildPath", concat_ws("/", 'wildPath))
      .distinct

    // DEBUG
    //    println(s"COUNT: EventLogGlobs --> ${newEventLogGlobs.count()}")

    // ALERT! DO NOT DELETE THIS SECTION
    // NOTE: In DBR 6.6 (and probably others) there's a bug in the
    // org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:403)
    // that parses the entire spark plan to determine whether a DF is serializable. These DF plans are not but the result
    // is which requires that the DF be materialized first.
    // TODO -- Use Temp path from config or create one if not exist
    val tmpEventLogPathsDir = "/tmp/overwatch/bronze/sparkEventLogPaths"
    newEventLogGlobs.write
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .format("delta")
      .save(tmpEventLogPathsDir)

    // TODO -- use intelligent partitions count
    // TOOD -- Throw error on failure and bubble up to report
    val strategicPartitions = if (isFirstRun) 2000 else 1000
    val eventLogPaths = spark.read.format("delta").load(tmpEventLogPathsDir)
      .repartition(strategicPartitions)
      .as[String]
      .map(p => Helpers.globPath(p, Some(fromTimeEpochMillis), Some(untilTimeEpochMillis)))
      .filter(size('value) > 0)
      .select(explode('value))
      .select($"col._1".alias("filename"), $"col._2".alias("fileCreateEpochMS"), $"col._3".alias("succeeded"))
      .filter('succeeded)
      .drop("succeeded") // using failed as the status tracker col downstream
      .withColumn("fileCreateTS", from_unixtime('fileCreateEpochMS / lit(1000)).cast("timestamp"))
      .withColumn("fileCreateDate", 'fileCreateTS.cast("date"))

    // DEBUG
    //    println(s"COUNT: EventLogPaths --> ${eventLogPaths.count}")

    eventLogPaths
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
