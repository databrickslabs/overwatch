package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, IncrementalFilter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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

    secretsRE.findFirstMatchIn(connectionString) match {
      case Some(i) =>
        dbutils.secrets.get(i.group(1), i.group(2))
      case None =>
        connectionString
    }
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
   * converts a long type
   * @param epochColName
   * @return
   */
  def epochMilliToTs(epochColName: String): Column = {
    from_unixtime(col(epochColName).cast("double") / 1000).cast("timestamp").alias("epochColName")
  }

  def optimizeWritePartitions(
                               df: DataFrame,
                               target: PipelineTable,
                               spark: SparkSession,
                               config: Config,
                               moduleName: String,
                               currentClusterCoreCount: Int
                             ): DataFrame = {

    var mutationDF = df
    mutationDF = if (target.zOrderBy.nonEmpty) {
      TransformFunctions.moveColumnsToFront(mutationDF, target.zOrderBy ++ target.statsColumns)
    } else mutationDF

   val targetShufflePartitions = if (!target.tableFullName.toLowerCase.endsWith("_bronze")) {
      val targetShufflePartitionSizeMB = 128.0
      val readMaxPartitionBytesMB = spark.conf.get("spark.sql.files.maxPartitionBytes")
        .replace("b", "").toDouble / 1024 / 1024

      val partSizeNoramlizationFactor = targetShufflePartitionSizeMB / readMaxPartitionBytesMB

      val sourceDFParts = getSourceDFParts(df)
      // TODO -- handle streaming until Module refactor with source -> target mappings
      val finalDFPartCount = if (target.checkpointPath.nonEmpty && config.cloudProvider == "azure") {
        target.name match {
          case "audit_log_bronze" => spark.table(s"${config.databaseName}.audit_log_raw_events")
            .rdd.partitions.length * target.shuffleFactor
          case _ => sourceDFParts / partSizeNoramlizationFactor * target.shuffleFactor
        }
      } else {
        sourceDFParts / partSizeNoramlizationFactor * target.shuffleFactor
      }

      val estimatedFinalDFSizeMB = finalDFPartCount * readMaxPartitionBytesMB.toInt
      val targetShufflePartitionCount = math.min(math.max(100, finalDFPartCount), 20000).toInt

      if (config.debugFlag) {
        println(s"DEBUG: Source DF Partitions: ${sourceDFParts}")
        println(s"DEBUG: Target Shuffle Partitions: ${targetShufflePartitionCount}")
        println(s"DEBUG: Max PartitionBytes (MB): $readMaxPartitionBytesMB")
      }

      logger.log(Level.INFO, s"$moduleName: " +
        s"Final DF estimated at ${estimatedFinalDFSizeMB} MBs." +
        s"\nShufflePartitions: ${targetShufflePartitionCount}")

      targetShufflePartitionCount
    } else {
      Math.max(currentClusterCoreCount * 2, spark.conf.get("spark.sql.shuffle.partitions").toInt)
    }

    spark.conf.set("spark.sql.shuffle.partitions",targetShufflePartitions)

    /**
     * repartition partitioned tables that are not auto-optimized into the range partitions for writing
     * without this the file counts of partitioned tables will be extremely high
     * also generate noise to prevent skewed partition writes into extremely low cardinality
     * organization_ids/dates per run -- usually 1:1
     * noise currently hard-coded to 32 -- assumed to be sufficient in most, if not all, cases
     */

    if (target.partitionBy.nonEmpty) {
      if (target.partitionBy.contains("__overwatch_ctrl_noise") && !target.autoOptimize) {
        logger.log(Level.INFO, s"${target.tableFullName}: generating partition noise")
        mutationDF = mutationDF.withColumn("__overwatch_ctrl_noise", (rand() * lit(32)).cast("int"))
      }

      if (!target.autoOptimize) {
        logger.log(Level.INFO, s"${target.tableFullName}: shuffling into $targetShufflePartitions " +
          s" output partitions defined as ${target.partitionBy.mkString(", ")}")
        mutationDF = mutationDF.repartition(targetShufflePartitions, target.partitionBy map col: _*)
      }
    }

    mutationDF

  }

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

  // TODO -- handle complex data types such as structs with format "jobRunTime.startEpochMS"
  //  currently filters with nested columns aren't supported
  def withIncrementalFilters(
                              df: DataFrame,
                              module: Option[Module],
                              filters: Seq[IncrementalFilter],
                              globalFilters: Seq[Column] = Seq(),
                              dataFrequency: Frequency,
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
    logger.log(Level.INFO, s"SETTING SPARK OVERRIDES:\n${sparkOverrides.mkString(", ")}")
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
}
