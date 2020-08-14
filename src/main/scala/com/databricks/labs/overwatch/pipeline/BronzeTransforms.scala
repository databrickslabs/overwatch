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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.Trigger

import scala.util.Random


trait BronzeTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _newDataRetrieved: Boolean = true
  private var CLOUD_PROVIDER: String = "aws"

  /**
   * Converts column of seconds/milliseconds/nanoseconds to timestamp
   *
   * @param rawVal: Column of Longtype
   * @param inputResolution: String of milli, or second (nano to come)
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
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(64))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport
    //    DEBUG
    //    val idsPar = Array("0827-194754-tithe1")
    // removing parallelization for now to see if it fixes some weird errors
    // CONFIRMED -- Parallelizing this breaks the token cipher
    val results = idsPar.flatMap(id => {
      val rawQuery = Map(
        idsKey -> id
      )

      val query = if (extraQuery.nonEmpty) {
        extraQuery.get ++ rawQuery
      } else rawQuery

      val apiCall = ApiCall(endpoint, apiEnv, Some(query))
      if (apiType == "post") apiCall.executePost().asStrings
      else apiCall.executeGet().asStrings
    }).toArray
    if (results.isEmpty) {
      setNewDataRetrievedFlag(false)
      //      throw new NoNewDataException(s"${endpoint} returned no new data, skipping") // TODO -- BUBBLE THIS UP
      Array()
    }
    else results
  }

  // TODO -- For Paths
//  def getChildDetail(df: StructType, c: Column): Array[Column] = {
//    df.fields.filter(_.name == "children").map(f => {
//      println(s"Top Level f type - ${f.dataType.typeName}")
//      f.dataType match {
//        case dt: ArrayType => {
//          println(s"dt element type - ${dt.elementType.typeName}")
//          dt.elementType match {
//            case et: StructType => {
//              when(size(c.getField("children")) > 0,
//                getChildDetail(et, c.getField("children")(0)))
//                  struct(
//                    c.getField("estRowCount"),
//                    c.getField("metadata")
//                  ).alias("childDetail")
//            }
//            case _ => {
//              struct(
//                c.getField("estRowCount"),
//                c.getField("metadata")
//              ).alias("childDetail")
//            }
//          }
//        }
//      }
//    })
//  }

//  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
//    val x = schema.fields.flatMap(f => {
//      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
//
//
////      f.dataType match {
////        case st: StructType => flattenSchema(st, colName)
////        case at: ArrayType =>
////          val st = at.elementType.asInstanceOf[StructType]
////          flattenSchema(st, colName)
////        case _ => Array(new Column(colName).alias(colName))
////      }
//    })
//  }

  private def datesStream(fromDate: LocalDate): Stream[LocalDate] = {
    fromDate #:: datesStream(fromDate plusDays 1 )
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
        val today = untilTime.toLocalDate
        // inclusive from exclusive to
        val datesGlob = datesStream(fromDT).takeWhile(_.isBefore(today)).toArray
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

  protected def prepClusterEventLogs(auditLogsTable: PipelineTable,
                                      start_time: TimeTypes, end_time: Long,
                                     apiEnv: ApiEnv): DataFrame = {

    val extraQuery = Map(
      "start_time" -> start_time.asUnixTimeMilli, // 1588935326000L, //
      "end_time" -> end_time //1589021726000L //
    )

    val clusterIDs = auditLogsTable.asDF
      .filter('date >= start_time.asColumnTS.cast("date") &&
        'timestamp >= lit(start_time.asUnixTimeMilli))
      .filter('serviceName === "clusters" && !'actionName.isin("changeClusterAcl"))
      .selectExpr("*", "requestParams.*").drop("requestParams", "Overwatch_RunID")
      .filter('cluster_id.isNotNull)
      .select('cluster_id)
      .as[String]
      .collect()

    // TODO -- add assertion that df count == total count from API CALL
    val clusterEvents = apiByID("clusters/events", apiEnv, "post",
      clusterIDs, "cluster_id", Some(extraQuery))

    if (newDataRetrieved) {
      spark.read.json(Seq(clusterEvents: _*).toDS()).select(explode('events).alias("events"))
        .select(col("events.*"))
    } else clusterEvents.toSeq.toDF("FAILURE")

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
      .filter('date > fromTimeCol.cast("date"))
      .selectExpr("*", "requestParams.*")
      .filter('serviceName === "clusters" && 'cluster_id.isNotNull)
      .select('cluster_id).distinct

    val newEventLogPrefixes = if (isFirstRun) {
      df
        .filter('date > fromTimeCol.cast("date")) //Partition filter
        .selectExpr("*", "requestParams.*")
        .filter('serviceName === "clusters")
        .join(clusterIDsWithNewData, Seq("cluster_id"))
        .filter('cluster_log_conf.isNotNull)
    } else {

      val historicalClustersWithNewData = clusterSpec.asDF
        .withColumn("date", toTS('timestamp, outputResultType = DateType))
        .filter('date > fromTimeCol.cast("date"))
        .select('timestamp, 'cluster_id, 'cluster_name, 'cluster_log_conf)
        .join(clusterIDsWithNewData, Seq("cluster_id"))


      val logPrefixesWithNewData = df
        .filter('date > fromTimeCol.cast("date")) //Partition filter
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
          lit("eventlog"), lit("*"), lit("*"), lit("eventlo*"))
      ).rdd
      .map(r => r.getSeq[String](0).mkString("/")).collect()
      .distinct.par

    newEventLogGlobs.tasksupport = taskSupport

    logger.log(Level.INFO, s"Building Blob for ${newEventLogGlobs.length} wildcard paths.")
    val r = new Random(42L)
    newEventLogGlobs.map(glob => {
      // Sleep each thread between 0 and 60 seconds to spread the driver load to launch all the threaded readers
      val delay = r.nextInt(60 * 1000)
      Thread.sleep(delay)
      glob
    }).flatMap(Helpers.globPath).toArray.toSeq.toDF("filename")
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
