package com.databricks.labs.overwatch.pipeline

import java.io.FileNotFoundException
import java.net.URI
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date

import com.databricks.labs.overwatch.utils.{OverwatchScope, SparkSessionWrapper}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataType, DateType, StringType, TimestampType}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


trait BronzeTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _newDataRetrieved: Boolean = true
  private var CLOUD_PROVIDER: String = "aws"

  //  case class ClusterEventsBuffer(clusterId: String, batch: Int, extraQuery: Map[String, Long])
  case class ClusterIdsWEventCounts(clusterId: String, count: Long)

  /**
   * Converts column of seconds/milliseconds/nanoseconds to timestamp
   *
   * @param rawVal          : Column of Longtype
   * @param inputResolution : String of milli, or second (nano to come)
   * @return
   */
  private def toTS(rawVal: Column, inputResolution: String = "milli", outputResultType: DataType = TimestampType): Column = {
    outputResultType match {
      case _: TimestampType => {
        if (inputResolution == "milli") {
          from_unixtime(rawVal.cast("double") / 1000).cast(outputResultType)
        } else { // Seconds for Now
          from_unixtime(rawVal).cast(outputResultType)
        }
      }
      case _: DateType => {
        if (inputResolution == "milli") {
          from_unixtime(rawVal.cast("double") / 1000).cast(outputResultType)
        } else { // Seconds for Now
          from_unixtime(rawVal).cast(outputResultType)
        }
      }
    }
  }

  private def structFromJson(df: DataFrame, c: String): Column = {
    require(df.schema.fields.map(_.name).contains(c), s"The dataframe does not contain col ${c}")
    require(df.schema.fields.filter(_.name == c).head.dataType.isInstanceOf[StringType], "Column must be a json formatted string")
    val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
    if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
      println(s"WARNING: The json schema for column ${c} was not parsed correctly, please review.")
    }
    from_json(col(c), jsonSchema).alias(c)
  }

  protected def setCloudProvider(value: String): this.type = {
    CLOUD_PROVIDER = value
    this
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
                                    isFirstRun: Boolean
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

  }

  protected def getAuditLogsDF(auditLogConfig: AuditLogConfig,
                               isFirstRun: Boolean,
                               untilTime: LocalDateTime,
                               fromTime: LocalDateTime,
                               auditRawLand: PipelineTable,
                               overwatchRunID: String
                              ): DataFrame = {
    if (CLOUD_PROVIDER == "azure") {
      val rawBodyLookup = spark.table(auditRawLand.tableFullName)
        .filter('Overwatch_RunID === lit(overwatchRunID))
      val schemaBuilders = spark.table(auditRawLand.tableFullName)
        .filter('Overwatch_RunID === lit(overwatchRunID))
        .withColumn("parsedBody", structFromJson(rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"))
        .selectExpr("streamRecord.*")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")


      spark.table(auditRawLand.tableFullName)
        .filter('Overwatch_RunID === lit(overwatchRunID))
        .withColumn("parsedBody", structFromJson(rawBodyLookup, "deserializedBody"))
        .select(explode($"parsedBody.records").alias("streamRecord"))
        .selectExpr("streamRecord.*")
        .withColumn("version", 'operationVersion)
        .withColumn("time", 'time.cast("timestamp"))
        .withColumn("timestamp", unix_timestamp('time) * 1000)
        .withColumn("date", 'time.cast("date"))
        .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"))
        .withColumn("userIdentity", structFromJson(schemaBuilders, "userIdentity"))
        .selectExpr("*", "properties.*").drop("properties")
        .withColumn("requestParams", structFromJson(schemaBuilders, "requestParams"))
        .withColumn("response", structFromJson(schemaBuilders, "response"))
        .drop("logId")

    } else {
      if (!isFirstRun) {

        // TODO -- fix this -- SO UGLY
        //  might be good to build from time in initializer better on a source case basis
        //  don't attempt this until after tests are in place
        val fromDT = fromTime.toLocalDate
        val untilDT = untilTime.toLocalDate
        // inclusive from exclusive to
        val datesGlob = datesStream(fromDT).takeWhile(_.isBefore(untilDT)).toArray
          .map(dt => s"${auditLogConfig.rawAuditPath.get}/date=${dt}")

        if (datesGlob.nonEmpty) {
          spark.read.json(datesGlob: _*)
            // When globbing the paths, the date must be reconstructed and re-added manually
            .withColumn("filename", split(input_file_name, "/"))
            .withColumn("date",
              split(expr("filter(filename, x -> x like ('date=%'))")(0), "=")(1).cast("date"))
            .drop("filename")
        } else {
          Seq("No New Records").toDF("FAILURE")
        }
      } else {
        spark.read.json(auditLogConfig.rawAuditPath.get)
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

        } else { None }

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

  protected def prepClusterEventLogs(auditLogsTable: PipelineTable,
                                     start_time: TimeTypes, end_time: TimeTypes,
                                     apiEnv: ApiEnv): DataFrame = {

    val extraQuery = Map(
      "start_time" -> start_time.asUnixTimeMilli, // 1588935326000L, //
      "end_time" -> end_time.asUnixTimeMilli, //1589021726000L //
      "limit" -> 500
    )

    val clusterIDs = auditLogsTable.asDF
      .filter(
        'date.between(start_time.asColumnTS.cast("date"), end_time.asColumnTS.cast("date")) &&
        'timestamp.between(lit(start_time.asUnixTimeMilli), lit(end_time.asUnixTimeMilli))
      )
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .filter('cluster_id.isNotNull)
      .select('cluster_id)
      .as[String]
      .collect()

//    val clusterIDs = """1004-141700-pins91,0819-200140-sag659,0918-161814-edits4,0814-173941-merit596,0711-120453-dotes18,0318-184242-dealt751,0711-120453-dotes18,1004-141700-pins91,0917-200813-ping33,0814-173941-merit596,0724-141619-this607,0724-141618-local604,0814-173940-envy590,0814-173943-arias608,0526-075326-mower2,0804-171430-picky968,0814-173941-newts595,0804-171429-howdy966,0917-200717-guilt32,0715-193200-wipes182,0917-200813-ping33,0814-173937-gouge579,0814-173941-merit596,1112-181811-jilts898,0814-173936-ovum572,0816-165511-songs94,0228-194538-shows5,0724-141622-brief621,0804-171429-crier963,0917-200813-ping33,0917-200717-guilt32,0616-182331-runt612,0917-200813-ping33,0526-075326-mower2,0716-160437-decaf361,0715-163318-piece155,0816-165511-songs94,0814-173936-alb571,0224-185747-jest918,0814-173938-vent583,0804-171422-dirk933,0814-173934-plum562,0715-163318-piece155,0817-161910-chomp205,0317-103623-rinse751,0917-200813-ping33,0715-163318-piece155,0724-141614-corn585,0917-200813-ping33,0816-165511-songs94,0814-173932-twos552,0724-141622-brief621,0819-200140-sag659,0317-103623-rinse751,0814-173936-alb571,0814-173936-newly574,0814-173938-fogs582,0917-200717-guilt32,0724-141619-coo609,0724-141617-hill600,0804-171425-nail948,0724-141622-brief621,1004-141700-pins91,1112-181811-jilts898,0310-052403-teen785,0715-163318-piece155,0814-173934-plum562,0918-161814-edits4,0526-075326-mower2,0816-165511-songs94,0207-162257-admit3,0828-205028-loon632,0827-184015-caned397,0724-141618-sears605,0819-200138-bacon649,0724-141614-corn585,0814-173938-vent583,0814-173938-stick580,0724-141615-union592,0828-150942-swath577,0724-141620-avast612,0831-192834-trims236,0109-132912-eves7,0804-171430-picky968,0814-173932-twos552,0724-141622-dozed622,0825-130849-toss837,0711-120453-dotes18,0917-200813-ping33,0814-173943-flirt607,0828-150942-swath577,0903-222609-bawdy413,0827-172349-hoses361,0804-171430-plug969,0819-201432-weigh671,0819-200140-sag659,0724-141616-acts595,0819-200142-moire665,0814-173936-newly574,0724-141617-any599,0724-141616-acts595,0831-192836-thou242,0716-160437-decaf361,0814-173942-tutor601,0828-205028-yoked630,0804-171429-crier963,0826-204714-scow178,0804-171422-dirk933,0819-200138-bacon649,0827-184426-dried398,0724-141613-inter583,0814-173933-flap557,0317-103623-rinse751,1004-141700-pins91,0724-141615-clef591,0825-130152-keen830,0224-185747-jest918,0827-172430-zaps375,0819-200142-moire665,0814-173938-stick580,0715-163318-piece155,0814-173941-newts595,0804-171425-nail948,0814-173933-jowls558,0814-173936-ovum572,0903-222609-bawdy413,0716-134539-twain7,0804-171430-picky968,0826-204714-scow178,0724-141618-local604,0724-141614-iota586,0825-130153-admit834,0831-192835-ducts237,1004-141700-pins91,0310-052403-teen785,1004-141700-pins91,0724-141619-matzo606,0814-173932-wises553,0825-130849-toss837,0109-132912-eves7,0715-163318-piece155,0814-173939-relic586,0724-141618-loses603,0724-141619-this607,0724-141617-hill600,0828-205027-cuter626,0228-194538-shows5,0813-191921-wadi350,0814-173942-tutor601,0814-173933-tier560,0109-132912-eves7,0616-182331-runt612,0814-173937-slier575,0724-141622-dozed622,0814-173931-wen550,0804-171432-now977,0526-075326-mower2,0724-141622-dozed622,0831-192834-trims236,0904-192312-gawk638,0814-173935-swank565,0917-200717-guilt32,0825-130849-gowns841,0804-171426-oases952,0224-185747-jest918,0902-145706-divan132,0816-165511-songs94,0814-173943-arias608,0814-173940-mown589,0917-200717-guilt32,0819-201432-weigh671,0804-171426-oases952,0616-182331-runt612,0310-052403-teen785,0827-184426-dried398,0724-141618-jag602,0816-165511-songs94,0724-141618-sears605,1004-141700-pins91,0904-143329-bungs571,0904-192312-gawk638,0903-222352-hath412,0814-173937-slier575,0814-173936-ovum572,0827-184015-caned397,0804-171432-now977,0814-173943-fibs606,0814-173941-merit596,0724-141615-clef591,0724-141623-made625,0825-130152-brag831,1004-141700-pins91,0926-173736-zinc29,1004-141700-pins91,0317-103623-rinse751,0814-173940-shuck591,0819-200142-toll667,0902-113358-nubs96,0825-130152-keen830,0825-130849-net839,0902-145706-divan132,0804-171429-howdy966,1112-181811-jilts898,0715-193200-wipes182,0814-173933-jowls558,0814-173934-plum562,0819-200138-uses648,0917-200813-ping33,0814-173942-pint603,0603-180134-oven821,0828-150942-swath577,0715-193200-wipes182,0724-141618-jag602,0926-173736-zinc29,0716-160437-celli362,0724-141619-this607,0831-192835-cents240,0814-173943-fibs606""".split(",")

    val batchSize = 500000D
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
        val tdf = spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
          .select(col("events.*"))

        SchemaTools.scrubSchema(tdf)
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
    val clusterEventsDF = spark.read.format("delta").load(tmpClusterEventsPath)
    val clusterEventsCaptured = clusterEventsDF.count
    if (clusterEventsCaptured > 0) {
      logger.log(Level.INFO, s"CLUSTER EVENTS CAPTURED: ${clusterEventsCaptured}")
      clusterEventsDF
    } else Seq("").toDF("FAILURE")

  }

  private def appendNewFilesToTracker(database: Database,
                                      newFiles: Array[String],
                                      trackerTarget: PipelineTable): Unit = {
    database.write(newFiles.toSeq.toDF("filename"), trackerTarget)
  }

  // Todo -- Put back to private
  private def getUniqueSparkEventsFiles(badRecordsPath: String,
                                        eventLogsDF: DataFrame,
                                        processedLogFiles: PipelineTable): Array[String] = {
    if (spark.catalog.tableExists(processedLogFiles.tableFullName)) {
      val alreadyProcessed = processedLogFiles.asDF.select('filename)
        .distinct
      val badFiles = spark.read.format("json").load(s"${badRecordsPath}/*/*/")
        .select('path.alias("filename"))
        .distinct
      eventLogsDF.except(alreadyProcessed.unionByName(badFiles)).as[String].collect()
    } else {
      eventLogsDF.select('filename)
        .distinct.as[String].collect()
    }
  }

  private def groupFilename(filename: Column): Column = {
    val segmentArray = split(filename, "/")
    val byCluster = array_join(slice(segmentArray, 1, 3), "/").alias("byCluster")
    val byClusterHost = array_join(slice(segmentArray, 1, 5), "/").alias("byDriverHost")
    val bySparkContextID = array_join(slice(segmentArray, 1, 6), "/").alias("bySparkContext")
    struct(filename, byCluster, byClusterHost, bySparkContextID).alias("filnameGroup")
  }

  def generateEventLogsDF(database: Database,
                          badRecordsPath: String,
                          processedLogFiles: PipelineTable)(eventLogsDF: DataFrame): DataFrame = {

    if (eventLogsDF.take(1).nonEmpty) {
      val pathsGlob = getUniqueSparkEventsFiles(badRecordsPath, eventLogsDF, processedLogFiles)
      appendNewFilesToTracker(database, pathsGlob, processedLogFiles)
      val dropCols = Array("Classpath Entries", "System Properties", "sparkPlanInfo", "Spark Properties",
        "System Properties", "HadoopProperties", "Hadoop Properties", "SparkContext Id")

      val baseEventsDF =
        spark.read.option("badRecordsPath", badRecordsPath)
          .json(pathsGlob: _*)
          .drop(dropCols: _*)

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

      if (baseEventsDF.columns.count(_.toLowerCase().replace(" ", "") == "stageid") > 1) {
        SchemaTools.scrubSchema(baseEventsDF
          .withColumn("progress", progressCol)
          .withColumn("filename", input_file_name)
          .withColumn("pathSize", size(split('filename, "/")))
          .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
          .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
          .withColumn("StageID", stageIDColumnOverride)
          .drop("pathSize", "Stage ID")
          .withColumn("filenameGroup", groupFilename('filename))
          .withColumn("Downstream_Processed", lit(false))
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
          .withColumn("Downstream_Processed", lit(false))
        )
      }
    } else {
      Seq("No New Event Logs Found").toDF("FAILURE")
    }
  }

  def saveAndLoadTempEvents(database: Database, tempTarget: PipelineTable)(df: DataFrame): DataFrame = {
    database.write(df, tempTarget)
    tempTarget.asDF
  }


  protected def collectEventLogPaths(fromTimeCol: Column,
                                     untilTimeCol: Column,
                                     clusterSpec: PipelineTable,
                                     isFirstRun: Boolean)(df: DataFrame): DataFrame = {

    // GZ files -- very compressed, need to get as much parallelism as possible
    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 48)

    logger.log(Level.INFO, "Collecting Event Log Paths Glob. This can take a while depending on the " +
      "number of new paths.")
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(128))

    val cluster_id_gen_w = Window.partitionBy('cluster_name)
      .orderBy('timestamp)
      .rowsBetween(Window.currentRow, Window.unboundedFollowing)

    // Lookup null cluster_ids
    val cluster_id_gen = first('cluster_id, ignoreNulls = true).over(cluster_id_gen_w)

    val clusterIDsWithNewData = df
      .filter('date.between(fromTimeCol.cast("date"), untilTimeCol.cast("date")))
      .selectExpr("*", "requestParams.*")
      .filter('serviceName === "clusters" && 'cluster_id.isNotNull)
      .select('cluster_id).distinct

    val newEventLogPrefixes = if (isFirstRun) {
      df
        .filter('date.between(fromTimeCol.cast("date"), untilTimeCol.cast("date")))
        .selectExpr("*", "requestParams.*")
        .filter('serviceName === "clusters")
        .join(clusterIDsWithNewData, Seq("cluster_id"))
        .filter('cluster_log_conf.isNotNull)
    } else {

      val historicalClustersWithNewData = clusterSpec.asDF
        .withColumn("date", toTS('timestamp, outputResultType = DateType))
        .filter('date.between(fromTimeCol.cast("date"), untilTimeCol.cast("date")))
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
        .join(clusterIDsWithNewData, Seq("cluster_id"))


      val logPrefixesWithNewData = df
        .filter('date.between(fromTimeCol.cast("date"), untilTimeCol.cast("date"))) //Partition filter
        .selectExpr("*", "requestParams.*")
        .filter('serviceName === "clusters")
        .join(clusterIDsWithNewData, Seq("cluster_id"))
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
        .filter('cluster_log_conf.isNotNull)
        .unionByName(historicalClustersWithNewData)

      val existingLogPrefixes = clusterSpec.asDF
        .filter('cluster_id.isNotNull)
        .withColumn("date", toTS('timestamp, outputResultType = DateType))
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
      .withColumn("cluster_id", cluster_id_gen)
      .select(
        array(col("destination"), col("cluster_id"),
          lit("eventlog"), lit("*"), lit("*"), lit("eventlo*")).alias("wildPath")
      ).withColumn("wildPath", concat_ws("/", 'wildPath))
      .distinct

    // ALERT! DO NOT DELETE THIS SECTION
    // NOTE: In DBR 6.6 (and probably others) there's a bug in the
    // org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:403)
    // that parses the entire spark plan to determine whether a DF is serializable. These DF plans are not but the result
    // is which requires that the DF be materialized first.
    val tmpEventLogPathsDir = "/tmp/overwatch/bronze/sparkEventLogPaths"
    newEventLogGlobs.write
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .format("delta")
      .save(tmpEventLogPathsDir)

    // TODO -- use intelligent partitions count
    val strategicPartitions = if (isFirstRun) 2000 else 1000
    spark.read.format("delta").load(tmpEventLogPathsDir)
      .repartition(strategicPartitions)
      .as[String]
      .map(p => Helpers.globPath(p))
      .filter(size('value) > 0)
      .select(explode('value).alias("filename"))

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
