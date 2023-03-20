package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.{PipelineFunctions, PipelineTable}
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper, WriteMode, MergeScope}
import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row}

import java.util
import java.util.UUID
import scala.annotation.tailrec
import scala.util.Random

class Database(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _


  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  /**
   * register an Overwatch target table in the configured Overwatch deployment
   *
   * @param target Pipeline table (i.e. target) as per the Overwatch deployed config
   */
  def registerTarget(target: PipelineTable): Unit = {
    if (!target.exists(catalogValidation = true) && target.exists(pathValidation = true)) {
      val createStatement = s"create table if not exists ${target.tableFullName} " +
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
   *
   * @param df     Dataframe to persist and load as fresh
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

    // maximize parallelism on re-read and let AQE bring it back down
    spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 16)
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "true")
    spark.read.format("delta").load(dfTempPath)
  }

  private def getPartitionedDateField(df: DataFrame, partitionFields: Seq[String]): Option[String] = {
    df.schema.filter(f => partitionFields.map(_.toLowerCase).contains(f.name.toLowerCase))
      .filter(_.dataType.typeName == "date")
      .map(_.name).headOption
  }

  /**
   * Add metadata, cleanse duplicates, and persistBeforeWrite for perf as needed based on target
   *
   * @param df               original dataframe that is to be written
   * @param target           target to which the df is to be written
   * @param pipelineSnapTime pipelineSnapTime
   * @return
   */
  private def preWriteActions(df: DataFrame, target: PipelineTable, pipelineSnapTime: Column): DataFrame = {
    var finalSourceDF: DataFrame = df
    finalSourceDF = if (target.withCreateDate) finalSourceDF.withColumn("Pipeline_SnapTS", pipelineSnapTime) else finalSourceDF
    finalSourceDF = if (target.withOverwatchRunID) finalSourceDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalSourceDF
    finalSourceDF = if (target.workspaceName) finalSourceDF.withColumn("workspace_name", lit(config.workspaceName)) else finalSourceDF

    // if target is to be deduped, dedup it by keys
    finalSourceDF = if (!target.permitDuplicateKeys) finalSourceDF.dedupByKey(target.keys, target.incrementalColumns) else finalSourceDF

    // always persistAndLoad when parallelism > 1 to reduce table lock times
    // don't persist and load metadata tables with fixed schemas (isEvolvingSchema false)
    val isParallelNotStreamingNotMeta = SparkSessionWrapper.parSessionsOn && !target.isStreaming && !target.isEvolvingSchema
    if (target.persistBeforeWrite || isParallelNotStreamingNotMeta) persistAndLoad(finalSourceDF, target) else finalSourceDF
  }

  /**
   *
   * @param deltaTarget Delta table to which to write
   * @param updatesDF DF to be merged into the delta table
   * @param mergeCondition merge logic as a string
   * @param target Pipeline Table target to write to
   * @return
   */
  private def deriveDeltaMergeBuilder(
                                     deltaTarget: DeltaTable,
                                     updatesDF: DataFrame,
                                     mergeCondition: String,
                                     target: PipelineTable
                                     ): DeltaMergeBuilder = {

    val mergeScope = target.mergeScope
    logger.log(Level.INFO, s"BEGINNING MERGE for target ${target.tableFullName}. \nMERGE SCOPE: " +
      s"$mergeScope")

    if (mergeScope == MergeScope.insertOnly) {
      deltaTarget
        .merge(updatesDF, mergeCondition)
        .whenNotMatched
        .insertAll()
    } else if (mergeScope == MergeScope.updateOnly) {
      deltaTarget
        .merge(updatesDF, mergeCondition)
        .whenMatched
        .updateAll()
    }
    else if (mergeScope == MergeScope.full) {
      deltaTarget
        .merge(updatesDF, mergeCondition)
        .whenMatched
        .updateAll()
        .whenNotMatched
        .insertAll()
    } else {
      throw new Exception("Merge Scope Not Supported")
    }
  }

  // TODO - refactor this write function and the writer from the target
  //  write function has gotten overly complex
  /**
   * Write the dataframe to the target
   *
   * @param df                 df with data to be written
   * @param target             target to where data should be saved
   * @param pipelineSnapTime   pipelineSnapTime
   * @param maxMergeScanDates  perf -- max dates to scan on a merge write. If populated this will filter the target source
   *                           df to minimize the right-side scan.
   * @param preWritesPerformed whether pre-write actions have already been completed prior to calling write. This is
   *                           defaulted to false and should only be set to true in specific circumstances when
   *                           preWriteActions was called prior to calling the write function. Typically used for
   *                           concurrency locking.
   * @return
   */
  def write(df: DataFrame, target: PipelineTable, pipelineSnapTime: Column, maxMergeScanDates: Array[String] = Array(), preWritesPerformed: Boolean = false): Unit = {
    val finalSourceDF: DataFrame = df

    // append metadata to source DF and cleanse as necessary
    val finalDF = if (!preWritesPerformed) {
      preWriteActions(finalSourceDF, target, pipelineSnapTime)
    } else {
      finalSourceDF
    }

    // ON FIRST RUN - WriteMode is automatically overwritten to APPEND
    if (target.writeMode == WriteMode.merge) { // DELTA MERGE / UPSERT
      val deltaTarget = DeltaTable.forPath(spark,target.tableLocation).alias("target")
      val updatesDF = finalDF.alias("updates")

      val immutableColumns = (target.keys ++ target.incrementalColumns).distinct

      val datePartitionFields = getPartitionedDateField(finalDF, target.partitionBy)
      val explicitDatePartitionCondition = if (datePartitionFields.nonEmpty & maxMergeScanDates.nonEmpty) {
        s" AND target.${datePartitionFields.get} in (${maxMergeScanDates.mkString("'", "', '", "'")})"
      } else ""
      val mergeCondition: String = immutableColumns.map(k => s"updates.$k = target.$k").mkString(" AND ") + " " +
        s"AND target.organization_id = '${config.organizationId}'" + // force partition filter for concurrent merge
        explicitDatePartitionCondition // force right side scan to only scan relevant dates

      val mergeDetailMsg =
        s"""
           |Beginning upsert to ${target.tableFullName}.
           |MERGE CONDITION: $mergeCondition
           |""".stripMargin
      logger.log(Level.INFO, mergeDetailMsg)
      spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", config.runID)
      // TODO -- when DBR 9.1 LTS GA, use LSM (low-shuffle-merge) to improve pipeline
      deriveDeltaMergeBuilder(deltaTarget, updatesDF, mergeCondition, target)
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
        try {
          target.writer(finalDF).asInstanceOf[DataFrameWriter[Row]].save(target.tableLocation)
        } catch {
          case e: Throwable =>
            val fullMsg = PipelineFunctions.appendStackStrace(e, "Exception in writeMethod:")
            logger.log(Level.ERROR, s"""Exception while writing to ${target.tableFullName}"""" + Thread.currentThread().getName + " Error msg:" + fullMsg)
            throw e
        }
      }
      logger.log(Level.INFO, s"Completed write to ${target.tableFullName}")
    }
    registerTarget(target)
  }

  /**
   * Function forces the thread to sleep for some specified time.
   * @param tableName Name of table for logging only
   * @param minimumSeconds minimum number of seconds for which to sleep
   * @param maxRandomSeconds max random seconds to add
   */
  private def coolDown(tableName: String, minimumSeconds: Long, maxRandomSeconds: Long): Unit = {
    val rnd = new scala.util.Random
    val number: Long = ((rnd.nextFloat() * maxRandomSeconds) + minimumSeconds).toLong * 1000L
    logger.log(Level.INFO,"Slowing parallel writes to " + tableName + "sleeping..." + number +
      " thread name " + Thread.currentThread().getName)
    Thread.sleep(number)
  }

  /**
   * Perform write retry after cooldown for legacy deployments
   * race conditions can occur when multiple workspaces attempt to modify the schema at the same time, when this
   * occurs, simply retry the write after some cooldown
   * @param inputDf DF to write
   * @param target target to where to write
   * @param pipelineSnapTime pipelineSnapTime
   * @param maxMergeScanDates same as write function
   */
  private def performRetry(inputDf: DataFrame,
                   target: PipelineTable,
                   pipelineSnapTime: Column,
                   maxMergeScanDates: Array[String] = Array()): Unit = {
    @tailrec def executeRetry(retryCount: Int): Unit = {
      val rerunFlag = try {
        write(inputDf, target, pipelineSnapTime, maxMergeScanDates)
        false
      } catch {
        case e: Throwable =>
          val exceptionMsg = e.getMessage.toLowerCase()
          logger.log(Level.WARN,
            s"""
               |DELTA Table Write Failure:
               |$exceptionMsg
               |Will Retry After a small delay.
               |This is usually caused by multiple writes attempting to evolve the schema simultaneously
               |""".stripMargin)
          if (exceptionMsg != null && (exceptionMsg.contains("concurrent") || exceptionMsg.contains("conflicting")) && retryCount < 5) {
            coolDown(target.tableFullName, 30, 30)
            true
          } else {
            throw e
          }
      }
      if (retryCount < 5 && rerunFlag) executeRetry(retryCount + 1)
    }

    try {
      executeRetry(1)
    } catch {
      case e: Throwable =>
        throw e
    }

  }

  /**
   * Used for multithreaded multiworkspace deployments
   * Check if a table is locked and if it is wait until max timeout for it to be unlocked and fail the write if
   * the timeout is reached
   * Table Lock Timeout can be overridden by setting cluster spark config overwatch.tableLockTimeout -- in milliseconds
   * default table lock timeout is 20 minutes or 1200000 millis
   *
   * @param tableName name of the table to be written
   * @return
   */
  private def targetNotLocked(tableName: String): Boolean = {
    val defaultTimeout: String = "1200000"
    val timeout = spark(globalSession = true).conf.getOption("overwatch.tableLockTimeout").getOrElse(defaultTimeout).toLong
    val timerStart = System.currentTimeMillis()

    @tailrec def testLock(retryCount: Int): Boolean = {
      val currWaitTime = System.currentTimeMillis() - timerStart
      val withinTimeout = currWaitTime < timeout
      if (SparkSessionWrapper.globalTableLock.contains(tableName)) {
        if (withinTimeout) {
          logger.log(Level.WARN, s"TABLE LOCKED: $tableName for $currWaitTime -- waiting for parallel writes to complete")
          coolDown(tableName, retryCount * 5, 2) // add 5 to 7 seconds to sleep for each lock test
          testLock(retryCount + 1)
        } else {
          throw new Exception(s"TABLE LOCK TIMEOUT - The table $tableName remained locked for more than the configured " +
            s"max timeout of $timeout millis. This may be increased by setting the following spark config in the cluster" +
            s"to something higher than the default (20 minutes). Usually only necessary for historical loads. \n" +
            s"overwatch.tableLockTimeout")
        }
      } else true // table not locked
    }

    testLock(retryCount = 1)
  }

  /**
   * Wrapper for the write function
   * If legacy deployment retry delta write in event of race condition
   * If multiworkspace -- implement table locking to alleviate race conditions on parallelize workspace loads
   *
   * @param df
   * @param target
   * @param pipelineSnapTime
   * @param maxMergeScanDates
   * @param daysToProcess
   * @return
   */
  private[overwatch] def writeWithRetry(df: DataFrame,
                                        target: PipelineTable,
                                        pipelineSnapTime: Column,
                                        maxMergeScanDates: Array[String] = Array(),
                                        daysToProcess: Option[Int] = None): Unit = {

    // needsCache when it's a small number of days and not in parallel and not autoOptimize
    //  when in parallel disable cache because it will always use persistAndLoad to reduce table lock times.
    //  persist and load will all be able to happen in parallel to temp location and use a simple read/write to
    //  merge into target rather than locking the target for the entire time all the transforms are being executed.
    val needsCache = daysToProcess.getOrElse(1000) < 5 &&
      !target.autoOptimize &&
      !SparkSessionWrapper.parSessionsOn &&
      target.isEvolvingSchema // don't cache small meta tables

    logger.log(Level.INFO, s"PRE-CACHING TARGET ${target.tableFullName} ENABLED: $needsCache")
    val inputDf = if (needsCache) {
      logger.log(Level.INFO, "Persisting data :" + target.tableFullName)
      df.persist()
    } else df
    if (needsCache) inputDf.count()

    if (target.config.isMultiworkspaceDeployment && target.requiresLocking) { // multi-workspace ++ locking
      val withMetaDf = preWriteActions(df, target, pipelineSnapTime)
      if (targetNotLocked(target.tableFullName)) {
        try {
          SparkSessionWrapper.globalTableLock.add(target.tableFullName)
          write(withMetaDf, target, pipelineSnapTime, maxMergeScanDates, preWritesPerformed = true)
        } catch {
          case e: Throwable => throw e
        } finally {
          SparkSessionWrapper.globalTableLock.remove(target.tableFullName)
        }
      }
    } else { // not multiworkspace -- or if multiworkspace and does not require locking
      performRetry(inputDf, target, pipelineSnapTime, maxMergeScanDates)
    }
  }


}

object Database {

  def apply(config: Config): Database = {
    new Database(config).setDatabaseName(config.databaseName)
  }

}
