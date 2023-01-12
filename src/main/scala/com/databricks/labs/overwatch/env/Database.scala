package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline.{PipelineFunctions, PipelineTable}
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper, WriteMode}
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row}

import java.util
import java.util.UUID
import scala.annotation.tailrec

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
    var finalSourceDF: DataFrame = df

    // apend metadata to source DF
    finalSourceDF = if (target.withCreateDate) finalSourceDF.withColumn("Pipeline_SnapTS", pipelineSnapTime) else finalSourceDF
    finalSourceDF = if (target.withOverwatchRunID) finalSourceDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalSourceDF
    finalSourceDF = if (target.workspaceName) finalSourceDF.withColumn("workspace_name", lit(config.workspaceName)) else finalSourceDF

    // if target is to be deduped, dedup it by keys
    finalSourceDF = if (!target.permitDuplicateKeys) finalSourceDF.dedupByKey(target.keys, target.incrementalColumns) else finalSourceDF

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
    true
  }

  /**
   * Check if a table is locked and if it is wait until max timeout for it to be unlocked and fail the write if
   * the timeout is reached
   * @param tableName name of the table to be written
   * @param timeout timeout // TODO -- max this externally configurable -- historical loads for long-running modules may lock a table for a long time
   * @return
   */
  private def targetNotLocked(tableName: String, timeout: Long = 1200000): Boolean = {
    val timerStart = System.currentTimeMillis()

    @tailrec def testLock(retryCount: Int): Boolean = {
      val currWaitTime = System.currentTimeMillis() - timerStart
      val withinTimeout = currWaitTime < timeout
      if (SparkSessionWrapper.globalTableLock.contains(tableName)) {
        if (withinTimeout) {
          logger.log(Level.WARN, s"TABLE LOCKED: $tableName for $currWaitTime -- waiting for parallel writes to complete")
          Thread.sleep(retryCount * 10000) // add 10 seconds to sleep for each lock test
          testLock(retryCount + 1)
        } else {
          throw new Exception(s"TABLE LOCK TIMEOUT - The table $tableName remained locked for more than the configured " +
            s"max timeout of $timeout millis")
        }
      } else true // table not locked
    }
    testLock(retryCount = 1)
  }


  def performRetry(inputDf: DataFrame,
                   target: PipelineTable,
                   pipelineSnapTime: Column,
                   maxMergeScanDates: Array[String] = Array()): this.type = {
    @tailrec def executeRetry(retryCount: Int): this.type = {
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
               |Attempting Retry
               |""".stripMargin)
          val concurrentWriteFailure = exceptionMsg.contains("concurrent") ||
            exceptionMsg.contains("conflicting") ||
            exceptionMsg.contains("all nested columns must match")
          if (exceptionMsg != null && concurrentWriteFailure && retryCount < 5) {
            coolDown(target.tableFullName)
            true
          } else {
            throw e
          }
      }
      if (retryCount < 5 && rerunFlag) executeRetry(retryCount + 1) else this
    }
    try {
      executeRetry(1)
    } catch {
      case e: Throwable =>
        throw e
    }

  }

  def writeWithRetry(df: DataFrame,
                     target: PipelineTable,
                     pipelineSnapTime: Column,
                     maxMergeScanDates: Array[String] = Array(),
                     daysToProcess: Option[Int] = None): Boolean = {

    val needsCache = daysToProcess.getOrElse(1000) < 5 && !target.autoOptimize
    val inputDf = if (needsCache) {
      logger.log(Level.INFO, "Persisting data :" + target.tableFullName)
      df.persist()
    } else df
    if (needsCache) inputDf.count()
//    performRetry(inputDf,target, pipelineSnapTime, maxMergeScanDates)
    // TODO -- build the finalDF here and use persistAndLoad here as well to ensure that is done before the write
    //   target gets locked. Due to lazy execution, if not persist, the entire module timer may be executed
    //   from the write function
    if (targetNotLocked(target.tableFullName)) {
      try {
        SparkSessionWrapper.globalTableLock.add(target.tableFullName)
        write(inputDf, target, pipelineSnapTime, maxMergeScanDates)
      } catch {
        case e: Throwable => throw e
      } finally {
        SparkSessionWrapper.globalTableLock.remove(target.tableFullName)
      }
    }
    true
  }

  /**
   * Function forces the thread to sleep for a random 30-60 seconds.
   * @param tableName
   */
  private def coolDown(tableName: String): Unit = {
    val rnd = new scala.util.Random
    val number:Long = ((rnd.nextFloat() * 30) + 30 + (rnd.nextFloat() * 30)).toLong*1000
    logger.log(Level.INFO,"DELTA WRITE COOLDOWN: Slowing multithreaded writing for " +
      tableName + "sleeping..." + number + " thread name " + Thread.currentThread().getName)
    Thread.sleep(number)
  }

}

object Database {

  def apply(config: Config): Database = {
    new Database(config).setDatabaseName(config.databaseName)
  }

}
