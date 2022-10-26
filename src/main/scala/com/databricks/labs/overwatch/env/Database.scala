package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper, WriteMode}
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, filter, lit}
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row}

import java.util
import java.util.UUID

class Database(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _


  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  /**
   * register an Overwatch target table in the configured Overwatch deployment
   * @param target Pipeline table (i.e. target) as per the Overwatch deployed config
   */
  def registerTarget(target: PipelineTable): Unit = {
    if (!target.exists(catalogValidation = true) && target.exists(pathValidation = true)) {
      val createStatement = s"create table ${target.tableFullName} " +
        s"USING DELTA location '${target.tableLocation}'"
      val logMessage = s"CREATING TABLE: ${target.tableFullName} at ${target.tableLocation}\n$createStatement\n\n"
      logger.log(Level.INFO, logMessage)
      if (config.debugFlag) println(logMessage)
      spark.sql(createStatement)
    }
  }

  def getDatabaseName: String = _databaseName

  // TODO -- Move this to post processing and improve
  //  all rollbacks should be completed during post processing from full inventory in par
  def rollbackTarget(target: PipelineTable): Unit = {

    // TODO -- on failure this causes multiple deletes unnecessarily -- improve this
    if (target.tableFullName matches ".*spark_.*_silver") {
      val sparkSilverTables = Array(
        "spark_executors_silver", "spark_Executions_silver", "spark_jobs_silver",
        "spark_stages_silver", "spark_tasks_silver"
      )
      val spark_silver_failMsg = s"Spark Silver FAILED: Rolling back all Spark Silver tables."
      if (config.debugFlag) println(spark_silver_failMsg)
      logger.log(Level.WARN, spark_silver_failMsg)

      sparkSilverTables.foreach(tbl => {
        val tableFullName = s"${config.databaseName}.${tbl}"
        val rollbackSql =
          s"""
             |delete from ${tableFullName}
             |where Overwatch_RunID = '${config.runID}'
             |""".stripMargin
        logger.log(Level.INFO, s"Rollback Statement to Execute: ${rollbackSql}")
        spark.sql(rollbackSql)
      })
    } else {
      val rollbackSql =
        s"""
           |delete from ${target.tableFullName}
           |where Overwatch_RunID = '${config.runID}'
           |""".stripMargin
      val rollBackMsg = s"Executing Rollback: STMT: ${rollbackSql}"
      if (config.debugFlag) println(rollBackMsg)
      logger.log(Level.WARN, rollBackMsg)
      spark.sql(rollbackSql)
    }

    // Specific Rollback logic
    if (target.name == "spark_events_bronze") {
      val eventsFileTrackerTable = s"${config.databaseName}.spark_events_processedfiles"
      val updateStmt =
        s"""
           |update ${eventsFileTrackerTable}
           |set failed = true
           |where Overwatch_RunID = '${config.runID}'
           |""".stripMargin
      val fileFailMsg = s"Failing Files for ${config.runID}.\nSTMT: $updateStmt"
      logger.log(Level.WARN, fileFailMsg)
      if (config.debugFlag) {
        println(updateStmt)
        println(fileFailMsg)
      }
      spark.sql(updateStmt)
    }
  }

  private def initializeStreamTarget(df: DataFrame, target: PipelineTable): Unit = {
    val dfWSchema = spark.createDataFrame(new util.ArrayList[Row](), df.schema)
    val staticDFWriter = target.copy(checkpointPath = None).writer(dfWSchema)
    staticDFWriter
      .asInstanceOf[DataFrameWriter[Row]]
      .save(target.tableLocation)

    registerTarget(target)
  }

  private def getQueryListener(query: StreamingQuery, minEventsPerTrigger: Long): StreamingQueryListener = {
    val streamManager = new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
        if (config.debugFlag) {
          println(query.status.prettyJson)
        }
        if (queryProgress.progress.numInputRows <= minEventsPerTrigger) {
          query.stop()
        }
      }
    }
    streamManager
  }

  /**
   * It's often more efficient to write a temporary version of the data to be merged than to compare complex
   * pipelines multiple times. This function simplifies the logic to write the df to temp storage and
   * read it back as a simple scan for deduping and merging
   * NOTE: this may be moved outside of database.scala if usage is valuable in other contexts
   * @param df Dataframe to persist and load as fresh
   * @param target target the df represents
   * @return
   */
  private def persistAndLoad(df: DataFrame, target: PipelineTable): DataFrame = {
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
    val tempPrefix = target.config.tempWorkingDir
    val tempSuffix = UUID.randomUUID().toString.replace("-", "")
    val dfTempPath = s"${tempPrefix}/${target.name.toLowerCase}/$tempSuffix"

    logger.info(
      s"""
         |Writing intermediate dataframe '${target.tableFullName}' to temporary path '$dfTempPath'
         |to optimize downstream performance.
         |""".stripMargin)
    df.write.format("delta").save(dfTempPath)

    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 16) // maximize parallelism on re-read and let
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "true")
    // AQE bring it back down
    spark.read.format("delta").load(dfTempPath)
  }

  private def getPartitionedDateField(df: DataFrame, partitionFields: Seq[String]): Option[String] = {
    df.schema.filter(f => partitionFields.map(_.toLowerCase).contains(f.name.toLowerCase))
      .filter(_.dataType.typeName == "date")
      .map(_.name).headOption
  }

  // TODO - refactor this write function and the writer from the target
  //  write function has gotten overly complex
  def write(df: DataFrame, target: PipelineTable, pipelineSnapTime: Column, maxMergeScanDates: Array[String] = Array()): Boolean = {
    // df = finalDF(clsd) have 49 rows
    // target = cluster_state_detail_silver have 9 rows
    var finalSourceDF: DataFrame = df

    // apend metadata to source DF
    finalSourceDF = if (target.withCreateDate) finalSourceDF.withColumn("Pipeline_SnapTS", pipelineSnapTime) else finalSourceDF
    finalSourceDF = if (target.withOverwatchRunID) finalSourceDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalSourceDF
    finalSourceDF = if (target.workspaceName) finalSourceDF.withColumn("workspace_name", lit(config.workspaceName)) else finalSourceDF
    val beforeDedupsDF = finalSourceDF

    // if target is to be deduped, dedup it by keys
    finalSourceDF = if (!target.permitDuplicateKeys) {
      val afterDedupsDF = finalSourceDF.dedupByKey(target.keys, target.incrementalColumns)

      println("beforeDedupsDF count is",beforeDedupsDF.count())
      println("afterDedupsDF count is",afterDedupsDF.count())
      if (beforeDedupsDF.count() == afterDedupsDF.count()){
        println("dedupByKey is working fine for clsf")
      }
      else println("dedupByKey not working fine for clsf")

      finalSourceDF.dedupByKey(target.keys, target.incrementalColumns)

    } else finalSourceDF // Problem arises here
    // ***** finalDF.filter('cluster_id === "0207-100712-yd99nxu4" && 'state_start_date >= "2022-10-13").orderBy('timestamp_state_start).count() is becoming 0

    val finalDF = if (target.persistBeforeWrite) persistAndLoad(finalSourceDF, target) else finalSourceDF

    // ON FIRST RUN - WriteMode is automatically overwritten to APPEND
    if (target.writeMode == WriteMode.merge) { // DELTA MERGE / UPSERT
      val deltaTarget = DeltaTable.forPath(target.tableLocation).alias("target")
        val updatesDF = finalDF.alias("updates")

      val immutableColumns = (target.keys ++ target.incrementalColumns).distinct

      val datePartitionFields = getPartitionedDateField(finalDF, target.partitionBy)
      val explicitDatePartitionCondition = if (datePartitionFields.nonEmpty & maxMergeScanDates.nonEmpty) {
        s" AND target.${datePartitionFields.get} in (${maxMergeScanDates.mkString("'", "', '", "'")})"
      } else ""
      val mergeCondition: String = immutableColumns.map(k => s"updates.$k = target.$k").mkString(" AND ")  + " " +
        s"AND target.organization_id = '${config.organizationId}'" +  // force partition filter for concurrent merge
        explicitDatePartitionCondition // force right side scan to only scan relevant dates

      val mergeDetailMsg =
        s"""
           |Beginning upsert to ${target.tableFullName}.
           |MERGE CONDITION: $mergeCondition
           |""".stripMargin
      logger.log(Level.INFO, mergeDetailMsg)

      // DEBUG
//      println(s"DEBUG - FinalDF Count = ${finalDF.count()}")
      println(mergeDetailMsg)
      import spark.implicits._
      if (target.name == "cluster_state_detail_silver") {
        println("DEBUG: DATABASE FINALSOURCEDF: clsd DF for specific cluster\n")
        finalSourceDF
          .filter('organization_id === "2222170229861029" && 'cluster_id === "0207-100712-yd99nxu4" && 'state_start_date >= "2022-10-13")
          .select('organization_id, 'cluster_id, 'unixTimeMS_state_start, 'timestamp, 'timestamp_state_start, 'state, 'state_start_date)
          .orderBy('unixTimeMS_state_start)
          .show(false)

      println("DEBUG: DATABASE FINALDF: clsd DF for specific cluster\n")
        finalDF
          .filter('organization_id === "2222170229861029" && 'cluster_id === "0207-100712-yd99nxu4" && 'state_start_date >= "2022-10-13")
          .select('organization_id, 'cluster_id, 'unixTimeMS_state_start, 'timestamp, 'timestamp_state_start, 'state, 'state_start_date)
          .orderBy('unixTimeMS_state_start)
          .show(false)
      }

      spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", config.runID)
      // TODO -- when DBR 9.1 LTS GA, use LSM (low-shuffle-merge) to improve pipeline
      deltaTarget
        .merge(updatesDF, mergeCondition)
        .whenMatched
        .updateAll()
        .whenNotMatched
        .insertAll()
        .execute()

      spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")

    } else {
      logger.log(Level.INFO, s"Beginning write to ${target.tableFullName}")
      if (target.checkpointPath.nonEmpty) { // STREAMING WRITER

        val msg = s"Checkpoint Path Set: ${target.checkpointPath.get} - proceeding with streaming write"
        logger.log(Level.INFO, msg)
        if (config.debugFlag) println(msg)

        val beginMsg = s"Stream to ${target.tableFullName} beginning."
        if (config.debugFlag) println(beginMsg)
        logger.log(Level.INFO, beginMsg)
        if (!spark.catalog.tableExists(config.databaseName, target.name)) {
          initializeStreamTarget(finalDF, target)
        }
        val streamWriter = target.writer(finalDF)
          .asInstanceOf[DataStreamWriter[Row]]
          .option("path", target.tableLocation)
          .start()
        val streamManager = getQueryListener(streamWriter, config.auditLogConfig.azureAuditLogEventhubConfig.get.minEventsPerTrigger)
        spark.streams.addListener(streamManager)
        val listenerAddedMsg = s"Event Listener Added.\nStream: ${streamWriter.name}\nID: ${streamWriter.id}"
        if (config.debugFlag) println(listenerAddedMsg)
        logger.log(Level.INFO, listenerAddedMsg)

        streamWriter.awaitTermination()
        spark.streams.removeListener(streamManager)

      } else { // DF Standard Writer append/overwrite
        target.writer(finalDF).asInstanceOf[DataFrameWriter[Row]].save(target.tableLocation)
      }
      logger.log(Level.INFO, s"Completed write to ${target.tableFullName}")
    }
    registerTarget(target)
    true
  }

}

object Database {

  def apply(config: Config): Database = {
    new Database(config).setDatabaseName(config.databaseName)
  }

}
