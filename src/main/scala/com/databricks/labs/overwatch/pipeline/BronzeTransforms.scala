package com.databricks.labs.overwatch.pipeline

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
import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Random


trait BronzeTransforms extends SparkSessionWrapper {

  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _jobIDs: Array[Long] = Array()
  private var _newDataRetrieved: Boolean = true
  private var _eventLogGlob: DataFrame = _

  private def setJobIDs(value: Array[Long]): this.type = {
    _jobIDs = value
    this
  }

  protected def setNewDataRetrievedFlag(value: Boolean): this.type = {
    _newDataRetrieved = value
    this
  }

  protected def setEventLogGlob(value: DataFrame): this.type = {
    _eventLogGlob = value
    this
  }

  protected def sparkEventsLogGlob: DataFrame = _eventLogGlob

  protected def newDataRetrieved: Boolean = _newDataRetrieved

  private def jobIDs: Array[Long] = _jobIDs

  @throws(classOf[NoNewDataException])
  private def apiByID[T](endpoint: String, apiEnv: ApiEnv,
                         apiType: String,
                         ids: Array[T], idsKey: String,
                         extraQuery: Option[Map[String, Any]] = None): Array[String] = {
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(4))
    val idsPar = ids.par
    idsPar.tasksupport = taskSupport
    //    DEBUG
    //    val idsPar = Array("0827-194754-tithe1")
    // removing parallelization for now to see if it fixes some weird errors
    // CONFIRMED -- Parallelizing this breaks the token cipher
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
    }) //.toArray
    if (results.isEmpty) {
      setNewDataRetrievedFlag(false)
      //      throw new NoNewDataException(s"${endpoint} returned no new data, skipping") // TODO -- BUBBLE THIS UP
      Array()
    }
    else results
  }

  private def datesStream(fromDate: LocalDate): Stream[LocalDate] = {
    fromDate #:: datesStream(fromDate plusDays 1 )
  }

  protected def getAuditLogsDF(auditLogPath: String,
                               isFirstRun: Boolean,
                               snapTime: LocalDateTime,
                               fromTime: LocalDateTime): DataFrame = {
    if (!isFirstRun) {
      val fromDT = fromTime.toLocalDate
      val yesterdate = snapTime.toLocalDate
      val datesGlob = datesStream(fromDT).takeWhile(_.isBefore(yesterdate)).toArray
        .map(dt => s"${auditLogPath}/date=${dt}")

      if (datesGlob.nonEmpty) {
        spark.read.json(datesGlob: _*)
      } else {
        Seq("No New Records").toDF("FAILURE")
      }
    } else {
      spark.read.json(auditLogPath)
    }

  }

  // TODO -- Get from audit if exists
  protected def collectJobsIDs()(df: DataFrame): DataFrame = {
    setJobIDs(df.select('job_id).distinct().as[Long].collect())
    df
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

      // Temporary Solution for Speculative Tasks bad Schema - SC-38615
      val stageIDColumnOverride: Column = if (baseEventsDF.columns.contains("Stage ID")) {
        when('StageID.isNull && $"Stage ID".isNotNull, $"Stage ID").otherwise('StageID)
      } else 'StageID

      if (baseEventsDF.columns.count(_.toLowerCase().replace(" ", "") == "stageid") > 1) {
        SchemaTools.scrubSchema(baseEventsDF
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
          .withColumn("filename", input_file_name)
          .withColumn("pathSize", size(split('filename, "/")))
          .withColumn("SparkContextId", split('filename, "/")('pathSize - lit(2)))
          .withColumn("clusterId", split('filename, "/")('pathSize - lit(5)))
          .drop("pathSize")
          .withColumn("filenameGroup", groupFilename('filename))
        )
      }
    } else {
      Seq("No New Event Logs Found").toDF("FAILURE")
    }
  }

  def saveAndLoadTempEvents(database: Database, tempTarget: PipelineTable)(df: DataFrame): DataFrame = {
    database.write(df, tempTarget)
    tempTarget.asDF
      .withColumn("Downstream_Processed", lit(false))
  }


  protected def collectEventLogPaths(fromTimeCol: Column,
                                     clusterSpec: PipelineTable,
                                     isFirstRun: Boolean)(df: DataFrame): DataFrame = {

    // GZ files -- very compressed, need to get as much parallelism as possible
    if (isFirstRun) spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 48)
    else spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 1)

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
        .withColumn("date", from_unixtime('timestamp.cast("double") / 1000).cast("date"))
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
        .withColumn("date", from_unixtime('timestamp.cast("double") / 1000).cast("date"))
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
