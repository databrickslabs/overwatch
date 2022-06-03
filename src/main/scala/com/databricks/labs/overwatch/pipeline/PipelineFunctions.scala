package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils._
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import TransformFunctions._

import java.io.{PrintWriter, StringWriter}
import java.net.URI

object PipelineFunctions {
  private val logger: Logger = Logger.getLogger(this.getClass)

  private val uriSchemeRegex = "^([a-zA-Z][-.+a-zA-Z0-9]*):/.*".r

  /**
   * parses the value for the connection string from the scope/key defined if the pattern matches {{secrets/scope/key}}
   * otherwise return the true string value
   * https://docs.databricks.com/security/secrets/secrets.html#store-the-path-to-a-secret-in-a-spark-configuration-property
   * @param connectionString
   * @return
   */
  def parseEHConnectionString(connectionString: String): String = {
    val secretsRE = "\\{\\{secrets/([^/]+)/([^}]+)\\}\\}".r

    val retrievedConnectionString = secretsRE.findFirstMatchIn(connectionString) match {
      case Some(i) =>
        dbutils.secrets.get(i.group(1), i.group(2))
      case None =>
        connectionString
    }
    if (!retrievedConnectionString.matches("^Endpoint=sb://.*;SharedAccessKey=.*$")) {
      throw new BadConfigException(s"Retrieved EH Connection string is not in the correct format.")
    } else retrievedConnectionString
  }

  /**
   * Ensure no duplicate slashes in path and default to dbfs:/ URI prefix where no uri specified to result in
   * fully qualified URI for db location
   * @param rawPathString
   * @return
   */
  def cleansePathURI(rawPathString: String): String = {
    uriSchemeRegex.findFirstMatchIn(rawPathString) match {
      case Some(i) =>
        val schema = i.group(1).toLowerCase
        if (schema == "dbfs") {
          rawPathString.replaceAllLiterally("//", "/")
        } else {
          new URI(rawPathString).normalize().toString
        }
      case None =>
        "dbfs:%s".format(rawPathString.replaceAllLiterally("//", "/"))
    }
  }

  def addNTicks(ts: Column, n:Int, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 + n) / 1000).cast("timestamp")
      case _: DateType =>
        date_add(ts, n)
      case _: DoubleType =>
        ts + s"0.00$n".toDouble
      case _: LongType =>
        ts + n
      case _: IntegerType =>
        ts + n
      case _ => throw
        new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }

  def subtractNTicks(ts: Column, n: Int, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 - n) / 1000).cast("timestamp")
      case _: DateType =>
        date_sub(ts, n)
      case _: DoubleType =>
        ts - s"0.00$n".toDouble
      case _: LongType =>
        ts - n
      case _: IntegerType =>
        ts - n
      case _ => throw
        new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }

  def getSourceDFParts(df: DataFrame): Int = if (!df.isStreaming) df.rdd.partitions.length else 200

  /**
   * if intelligent scaling is enabled, cluster will scale to the best known appropriate size for the work being
   * done. If the cluster is completing modules extremely rapidly, this could cause scaling issues and it's
   * recommended to disable intelligent scaling; this feature is meant for large workloads.
   *
   * Resulting cluster size is bound by configured minimum/maximum core count allotted by user
   * and defaulted to 4/512 respectfully if enabled. Default is disabled.
   *
   * new cluster scale will be minimum core count * workload scale coefficient * user specified coefficient multiplier
   * (defaulted to 1.0).
   *
   * 4 core minimum * 4 workload coefficient * 1.0 user multiplier == 16 target cores. If in bounds, will set otherwise,
   * will use maximum specified cores.
   * @param pipeline
   * @param scaleCoefficient
   */
  def scaleCluster(pipeline: Pipeline, scaleCoefficient: Double): Unit = {
    val maxCoreCount = pipeline.getConfig.intelligentScaling.maximumCores
    val baseCoreCount = pipeline.getConfig.intelligentScaling.minimumCores
    val userCoeffMultiplier = pipeline.getConfig.intelligentScaling.coeff
    val coresPerWorker = pipeline.getCoresPerWorker
    if (pipeline.getConfig.intelligentScaling.enabled) {
      val nodeCountUpperBound = Math.floor(maxCoreCount / coresPerWorker).toInt
      val newCoreCount = Math.min(Math.max(baseCoreCount, Math.ceil(baseCoreCount * scaleCoefficient * userCoeffMultiplier).toInt), maxCoreCount)
      val newNodeCount = Math.min(Math.floor(newCoreCount / coresPerWorker), nodeCountUpperBound).toInt
      if (newNodeCount != pipeline.getNumberOfWorkerNodes) {
        logger.log(Level.INFO, s"Cluster Scaling: Max Core Count set to $maxCoreCount")
        logger.log(Level.INFO, s"Cluster Scaling: Max Nodes --> $nodeCountUpperBound")
        logger.log(Level.INFO, s"Cluster Scaling: New Target Node Count --> $newNodeCount")
        pipeline.workspace.resizeCluster(pipeline.config.apiEnv, newNodeCount)
      }
    }
  }

  /**
   * converts an epoch millisecond timestamp of long type to timestamp and preserves milliseconds
   * @param c name of column as a string
   * @return converted string
   */
  def epochMilliToTs(c: String): Column = {
    val defaultAlias = if (c.contains(".")) c.split("\\.").takeRight(1).head else c
    to_timestamp(col(c) / 1000.0).alias(defaultAlias)
  }

  /**
   * converts timestamp to epoch and retains milliseconds
   * @param c name of column as a string
   * @return converted string
   */
  def tsToEpochMilli(c: String): Column = {
    val defaultAlias = if (c.contains(".")) c.split("\\.").takeRight(1).head else c
    ((unix_timestamp(col(c).cast("timestamp")) +
      (substring(col(c), -4, 3).cast("double") / 1000))*1000)
      .alias(defaultAlias)
  }

//  def optimizeWritePartitions(
//                               df: DataFrame,
//                               target: PipelineTable,
//                               spark: SparkSession,
//                               config: Config,
//                               moduleName: String,
//                               currentClusterCoreCount: Int
//                             ): DataFrame = {
//
//    var mutationDF = df
//    mutationDF = if (target.zOrderBy.nonEmpty) {
//      mutationDF.moveColumnsToFront(target.zOrderBy ++ target.statsColumns)
//    } else mutationDF
//
//   val targetShufflePartitions = if (!target.tableFullName.toLowerCase.endsWith("_bronze")) {
//      val targetShufflePartitionSizeMB = 128.0
//      val readMaxPartitionBytesMB = spark.conf.get("spark.sql.files.maxPartitionBytes")
//        .replace("b", "").toDouble / 1024 / 1024
//
//      val partSizeNoramlizationFactor = targetShufflePartitionSizeMB / readMaxPartitionBytesMB
//
//      val sourceDFParts = getSourceDFParts(df)
//      // TODO -- handle streaming until Module refactor with source -> target mappings
//      val finalDFPartCount = if (target.checkpointPath.nonEmpty && config.cloudProvider == "azure") {
//        target.name match {
//          case "audit_log_bronze" => // TODO -- check to ensure this should be audit_log_bronze, not audit_log_raw_events
//            target.asDF.rdd.partitions.length * target.shuffleFactor
//          case _ => sourceDFParts / partSizeNoramlizationFactor * target.shuffleFactor
//        }
//      } else {
//        sourceDFParts / partSizeNoramlizationFactor * target.shuffleFactor
//      }
//
//      val estimatedFinalDFSizeMB = finalDFPartCount * readMaxPartitionBytesMB.toInt
//      val targetShufflePartitionCount = math.min(math.max(100, finalDFPartCount), 20000).toInt
//
//      if (config.debugFlag) {
//        println(s"DEBUG: Source DF Partitions: ${sourceDFParts}")
//        println(s"DEBUG: Target Shuffle Partitions: ${targetShufflePartitionCount}")
//        println(s"DEBUG: Max PartitionBytes (MB): $readMaxPartitionBytesMB")
//      }
//
//      logger.log(Level.INFO, s"$moduleName: " +
//        s"Final DF estimated at ${estimatedFinalDFSizeMB} MBs." +
//        s"\nShufflePartitions: ${targetShufflePartitionCount}")
//
//      targetShufflePartitionCount
//    } else {
//      Math.max(currentClusterCoreCount * 2, spark.conf.get("spark.sql.shuffle.partitions").toInt)
//    }
//
//    spark.conf.set("spark.sql.shuffle.partitions",targetShufflePartitions)
//
//    /**
//     * repartition partitioned tables that are not auto-optimized into the range partitions for writing
//     * without this the file counts of partitioned tables will be extremely high
//     * also generate noise to prevent skewed partition writes into extremely low cardinality
//     * organization_ids/dates per run -- usually 1:1
//     * noise currently hard-coded to 32 -- assumed to be sufficient in most, if not all, cases
//     */
//
//    if (target.partitionBy.nonEmpty) {
//      if (target.partitionBy.contains("__overwatch_ctrl_noise") && !target.autoOptimize) {
//        logger.log(Level.INFO, s"${target.tableFullName}: generating partition noise")
//        mutationDF = mutationDF.withColumn("__overwatch_ctrl_noise", (rand() * lit(32)).cast("int"))
//      }
//
//      if (!target.autoOptimize) {
//        logger.log(Level.INFO, s"${target.tableFullName}: shuffling into $targetShufflePartitions " +
//          s" output partitions defined as ${target.partitionBy.mkString(", ")}")
//        mutationDF = mutationDF.repartition(targetShufflePartitions, target.partitionBy map col: _*)
//      }
//    }
//
//    mutationDF
//
//  }

  def applyFilters(df: DataFrame, filters: Seq[Column], debugFlag: Boolean, module: Option[Module] = None): DataFrame = {
    if (module.nonEmpty) {
      val filterLogMessageSB: StringBuilder = new StringBuilder
      filterLogMessageSB.append(s"APPLIED FILTERS:\nMODULE_ID: ${module.get.moduleId}\nMODULE_NAME: ${module.get.moduleName}\nFILTERS:\n")
      filters.map(_.expr).foreach(filterLogMessageSB.append)
      val filterLogMessage = filterLogMessageSB.toString()
      logger.log(Level.INFO, filterLogMessage)
      if (debugFlag) println(filterLogMessage)
    }
    filters.foldLeft(df) {
      case (rawDF, filter) =>
        rawDF.filter(filter)
    }
  }

  def cleanseCorruptAuditLogs(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val flatFieldNames = SchemaTools.getAllColumnNames(df.select(col("requestParams")).schema)
    val corruptedNames = Array("requestParams.DataSourceId", "requestParams.DashboardId", "requestParams.AlertId")
    if (corruptedNames.length != corruptedNames.diff(flatFieldNames).length) { // df has at least one of the corrupted names
      logger.warn("Handling corrupted source audit log field requestParams.DataSourceId")
      spark.conf.set("spark.sql.caseSensitive", "true")
    val rpFlatFields = flatFieldNames
        .filterNot(fName => corruptedNames.contains(fName))

      val cleanRPFields = rpFlatFields.map {
        case "requestParams.dataSourceId" =>
          if (flatFieldNames.contains("requestParams.DataSourceId")) {
            when('actionName === "executeFastQuery", $"requestParams.DataSourceId")
              .when('actionName === "executeAdhocQuery", $"requestParams.dataSourceId")
              .otherwise(lit(null).cast("string"))
              .alias("dataSourceId")
          } else $"requestParams.dataSourceId"
        case "requestParams.dashboardId" =>
          if (flatFieldNames.contains("requestParams.DashboardId")) {
            when(
              'actionName.isin("createRefreshSchedule", "deleteRefreshSchedule", "updateRefreshSchedule"),
              $"requestParams.DashboardId"
            ).otherwise($"requestParams.dashboardId")
              .alias("dashboardId")
          } else $"requestParams.dashboardId"
        case "requestParams.alertId" =>
          if (flatFieldNames.contains("requestParams.AlertId")) {
            when(
              'actionName.isin("createRefreshSchedule", "deleteRefreshSchedule", "updateRefreshSchedule"),
              $"requestParams.AlertId"
            ).otherwise($"requestParams.alertId")
              .alias("alertId")
          } else $"requestParams.alertId"
        case fName => col(fName)
      }

      df.withColumn("requestParams", struct(cleanRPFields: _*))
    } else df
  }

  // TODO -- handle complex data types such as structs with format "jobRunTime.startEpochMS"
  //  currently filters with nested columns aren't supported
  def withIncrementalFilters(
                              df: DataFrame,
                              module: Option[Module],
                              filters: Seq[IncrementalFilter],
                              globalFilters: Seq[Column] = Seq(),
                              debugFlag: Boolean
                            ): DataFrame = {
    val parsedFilters = filters.map(filter => {
      val f = filter.cronField
      val cName = f.name
      val low = filter.low
      val high = filter.high
      col(cName) >= low && col(cName) < high
    })

    val allFilters = parsedFilters ++ globalFilters

    applyFilters(df, allFilters, debugFlag, module)

  }

  def setSparkOverrides(spark: SparkSession, sparkOverrides: Map[String, String],
                        debugFlag: Boolean = false): Unit = {
    logger.info(
      s"""
         |SPARK OVERRIDES BEING SET:
         |${sparkOverrides.mkString("\n")}
         |""".stripMargin)
    sparkOverrides foreach { case (k, v) =>
      try {
        val opt = spark.conf.getOption(k)
        if (debugFlag) { // if debug write out the override
          if (opt.isEmpty || opt.get != v) {
            val sparkConfSetMsg = s"Overriding $k from ${opt.getOrElse("UNDEFINED")} --> $v"
            println(sparkConfSetMsg)
            logger.log(Level.INFO, sparkConfSetMsg)
          }
        }
        // if sparkConf can be modified OR is unset, set spark conf
        if (spark.conf.isModifiable(k) || opt.isEmpty) spark.conf.set(k, v)
      } catch {
        case e: Throwable =>
          if (debugFlag)
            println(s"Failed Setting $k", e)
          logger.log(Level.WARN, s"Failed trying to set $k", e)
      }
    }
  }

  def appendStackStrace(e: Throwable, customMsg: String = ""): String = {
    val sw = new StringWriter
    sw.append(customMsg + "\n")
    sw.append(e.getMessage + "\n")
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  /**
   * There are currently two slow-changing input tables in the Overwatch pipeline, instanceDetails and dbuCostDetails.
   * These tables track costs through time for DBUs and compute costs. These tables cannot skip time or have overlapping
   * time for any keys as this will cause data quality issues. There must be only one active record per key and
   * there must be no gaps between dates. ActiveUntil expires on 06-01-2021 for node type X then there must be
   * another record with activeUntil beginning on 06-01-2021 and a null activeUntil for this validation to pass.
   *
   * @param target Pipeline Target to be validated
   * @param fromCol column name of the active start date or time
   * @param untilCol column name of the expiry date or time, null == active
   * @param isActiveCol same as untilCol == null but more obvious for users
   * @param snapDate snapshot date of the pipeline "yyyy-MM-dd" format
   */
  def validateType2Input(
                           target: PipelineTable,
                           fromCol: String,
                           untilCol: String,
                           isActiveCol: String,
                           snapDate: String = java.time.LocalDate.now.toString
                         ): Unit = {
    val df = target.asDF
    val keyCols = target.keys.map(k => lower(trim(col(k))).alias(k))

    // only one active record per key is permitted
    val maxActiveCountByKey = df.filter(col(isActiveCol)).groupBy(keyCols: _*)
      .count().select(max(col("count"))).collect().map(_.getLong(0)).head
    if (maxActiveCountByKey > 1) {
      df
        .filter(col(isActiveCol))
        .orderBy(keyCols :+ col("pipeline_snapTS").desc: _*)
        .show(20, false)
      throw new BadConfigException(
        s"Multiple active records found in ${target.tableFullName}. Only one record may be active " +
          s"for each sku at a time. Please review this table and correct it."
      )
    }

    // no time gaps or overlaps permitted for any key
    val w = Window.partitionBy(keyCols: _*).orderBy(col(fromCol))
    val wKeyCheck = Window
      .partitionBy(keyCols ++ Array(col(fromCol), col(untilCol)): _*)
      .orderBy(col(fromCol), col(untilCol))
    val dfCheck = df
      .withColumn(untilCol, coalesce(col(untilCol), lit(snapDate)))
      .withColumn("previousUntil", lag(col(untilCol), 1).over(w))
      .withColumn("rnk", rank().over(wKeyCheck))
      .withColumn("rn", row_number().over(wKeyCheck))
      .withColumn("isValid",
        when(col("previousUntil").isNull && col(isActiveCol), lit(true))
          .otherwise(
            col(fromCol) === col("previousUntil") && // current from must equal previous until
              coalesce(max(col(isActiveCol)).over(w.rowsBetween(Window.unboundedPreceding, -1)), lit(true)) === lit(false) // all prior records must be inactive
          )
      )
      .filter(col(isActiveCol))
      .filter(!col("isValid") || col("rnk") > 1 || col("rn") > 1)

    if (!dfCheck.isEmpty) {
      val dfCheckReportCols = keyCols ++ Array(
        col("rnk"),
        col("rn"),
        col("previousUntil"),
        datediff(col(fromCol), col("previousUntil")).alias("daysBetweenCurrentAndPrevious")
      )
      throw new InvalidType2Input(target, dfCheck.select(dfCheckReportCols: _*), fromCol, untilCol)
    }
  }

  def getPipelineTarget(pipeline: Pipeline, targetName: String): PipelineTable = {
    val pipelineTargets = pipeline match {
      case bronze: Bronze => bronze.getAllTargets
      case silver: Silver => silver.getAllTargets
      case gold: Gold => gold.getAllTargets
      case _ => throw new Exception("Pipeline type must be an Overwatch Bronze, Silver, or Gold Pipeline instance")
    }

    val filteredTarget = pipelineTargets.find(_.name.toLowerCase == targetName.toLowerCase)

    filteredTarget.getOrElse(throw new Exception(s"NO TARGET FOUND: No targets exist for lower " +
      s"case $targetName.\nPotential targets include ${pipelineTargets.map(_.name).mkString(", ")}"))

  }

  def getPipelineModule(pipeline: Pipeline, moduleId: Integer): Module = {
    val pipelineModules = pipeline match {
      case bronze: Bronze => bronze.getAllModules
      case silver: Silver => silver.getAllModules
      case gold: Gold => gold.getAllModules
      case _ => throw new Exception("Pipeline type must be an Overwatch Bronze, Silver, or Gold Pipeline instance")
    }

    val filteredModule = pipelineModules.find(_.moduleId == moduleId)
    filteredModule.getOrElse(throw new Exception(s"NO MODULE FOUND: ModuleID $moduleId does not exist. Available " +
      s"modules include ${pipelineModules.map(m => s"\n(${m.moduleId}, ${m.moduleName})").mkString("\n")}"))
  }

  private[overwatch] def deriveSKU(isAutomated: Column, sparkVersion: Column): Column = {
    val isJobsLight = sparkVersion.like("apache_spark_%")
    when(isAutomated && isJobsLight, "jobsLight")
      .when(isAutomated && !isJobsLight, "automated")
      .when(!isAutomated, "interactive")
      .otherwise("unknown")
  }

  def fillForward(colToFillName: String, w: WindowSpec, orderedLookups: Seq[Column] = Seq[Column]()) : Column = {
    val colToFill = col(colToFillName)
    if (orderedLookups.nonEmpty){ // TODO -- omit nulls from lookup
      val coalescedLookup = colToFill +: orderedLookups.map(lookupCol => {
        last(lookupCol, true).over(w)
      })
      coalesce(coalescedLookup: _*).alias(colToFillName)
    } else {
      coalesce(colToFill, last(colToFill, true).over(w)).alias(colToFillName)
    }
  }

  def getDeltaHistory(spark: SparkSession, target: PipelineTable, n: Int = 9999): DataFrame = {
    import spark.implicits._
    DeltaTable.forPath(target.tableLocation).history(n)
      .select(
        'version,
        'timestamp,
        'operation,
        'clusterId,
        'operationMetrics,
        'userMetadata
      )
  }

  def getTargetWriteMetrics(
                             spark: SparkSession,
                             target: PipelineTable,
                             snapTime: TimeTypes,
                             runId: String,
                             operations: Array[String] = Array("WRITE", "MERGE")
                           ): Map[String, String] = {
    import spark.implicits._
    getDeltaHistory(spark, target)
      .filter('operation.isin(operations: _*))
      .filter('timestamp > snapTime.asColumnTS && 'userMetadata === runId)
      .withColumn("rnk", rank().over(Window.orderBy('timestamp)))
      .filter('rnk === 1)
      .as[DeltaHistory]
      .collect()
      .headOption
      .map(_.operationMetrics)
      .getOrElse(Map[String, String]())

  }

  def getLastOptimized(spark: SparkSession, target: PipelineTable): Long = {
    import spark.implicits._
    getDeltaHistory(spark, target)
      .filter('operation === "OPTIMIZE")
      .select(max(unix_timestamp('timestamp) * 1000))
      .as[Option[Long]]
      .collect()
      .head
      .getOrElse(0L)
  }

}
