package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, Frequency, IncrementalFilter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object PipelineFunctions {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Ensure no duplicate slashes in path and default to dbfs:/ URI prefix where no uri specified to result in
   * fully qualified URI for db location
   * @param rawPathString
   * @return
   */
  def cleansePathURI(rawPathString: String): String = {
    if (rawPathString.replaceAllLiterally("//", "/").split("/")(0).isEmpty) {
      s"dbfs:${rawPathString}"
    } else {
      rawPathString
    }.replaceAllLiterally("//", "/")
  }

  def addOneTick(ts: Column, dataFrequency: Frequency, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 + 1) / 1000).cast("timestamp")
      case _: DateType =>
        if (dataFrequency == Frequency.daily) ts else date_add(ts, 1)
      case _: DoubleType =>
        ts + 0.001d
      case _: LongType =>
        ts + 1
      case _: IntegerType =>
        ts + 1
      case _ => throw
        new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }

  def getSourceDFParts(df: DataFrame): Int = if (!df.isStreaming) df.rdd.partitions.length else 200

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

  def applyFilters(df: DataFrame, filters: Seq[Column], module: Option[Module] = None): DataFrame = {
    if (module.nonEmpty) {
      val filterLogMessageSB: StringBuilder = new StringBuilder
      filterLogMessageSB.append(s"APPLIED FILTERS:\nMODULE_ID: ${module.get.moduleId}\nMODULE_NAME: ${module.get.moduleName}\nFILTERS:\n")
      filters.map(_.expr).foreach(filterLogMessageSB.append)
      val filterLogMessage = filterLogMessageSB.toString()
      logger.log(Level.INFO, filterLogMessage)
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
                              dataFrequency: Frequency
                            ): DataFrame = {
    val parsedFilters = filters.map(filter => {
      val f = filter.cronField
      val cName = f.name
      val low = filter.low
      val high = filter.high
      val dt = f.dataType
      dt match {
        case _: TimestampType =>
          col(cName).between(PipelineFunctions.addOneTick(low, dataFrequency), high)
        case _: DateType => {
          col(cName).between(
            PipelineFunctions.addOneTick(low.cast(DateType), dataFrequency, DateType),
            if (dataFrequency == Frequency.daily) date_sub(high.cast(DateType), 1) else high.cast(DateType)
          )
        }
        case _: LongType =>
          col(cName).between(PipelineFunctions.addOneTick(low, dataFrequency, LongType), high.cast(LongType))
        case _: IntegerType =>
          col(cName).between(PipelineFunctions.addOneTick(low, dataFrequency, IntegerType), high.cast(IntegerType))
        case _: DoubleType =>
          col(cName).between(PipelineFunctions.addOneTick(low, dataFrequency, DoubleType), high.cast(DoubleType))
        case _ =>
          throw new IllegalArgumentException(s"IncreasingID Type: ${dt.typeName} is Not supported")
      }
    })

    val allFilters = parsedFilters ++ globalFilters

    applyFilters(df, allFilters, module)

  }

  def setSparkOverrides(spark: SparkSession, sparkOverrides: Map[String, String],
                        debugFlag: Boolean = false): Unit = {
    sparkOverrides foreach { case (k, v) =>
      try {
        val opt = spark.conf.getOption(k)
        if (debugFlag) { // if debug write out the override
          if (opt.isEmpty || opt.get != v) {
            println(s"Overriding $k from ${opt.getOrElse("UNDEFINED")} --> $v")
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
