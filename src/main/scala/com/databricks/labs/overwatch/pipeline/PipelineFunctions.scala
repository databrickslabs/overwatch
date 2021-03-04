package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, Frequency, IncrementalFilter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, date_add, date_sub, lit, rand}

object PipelineFunctions {
  private val logger: Logger = Logger.getLogger(this.getClass)

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
                               moduleName: String
                             ): DataFrame = {

    val sourceDFParts = getSourceDFParts(df)
    var mutationDF = df
    mutationDF = if (target.zOrderBy.nonEmpty) {
      TransformFunctions.moveColumnsToFront(mutationDF, target.zOrderBy ++ target.statsColumns)
    } else mutationDF

    val targetShufflePartitionSizeMB = 128.0
    val readMaxPartitionBytesMB = spark.conf.get("spark.sql.files.maxPartitionBytes").toDouble / 1024 / 1024
    val partSizeNoramlizationFactor = targetShufflePartitionSizeMB / readMaxPartitionBytesMB

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
      println(s"DEBUG: Max PartitionBytes (MB): ${
        spark.conf.get("spark.sql.files.maxPartitionBytes").toInt / 1024 / 1024
      }")
    }

    logger.log(Level.INFO, s"$moduleName: " +
      s"Final DF estimated at ${estimatedFinalDFSizeMB} MBs." +
      s"\nShufflePartitions: ${targetShufflePartitionCount}")

    spark.conf.set("spark.sql.shuffle.partitions", targetShufflePartitionCount)

    // repartition partitioned tables that are not auto-optimized into the range partitions for writing
    // without this the file counts of partitioned tables will be extremely high
    // also generate noise to prevent skewed partition writes into extremely low cardinality
    // organization_ids/dates per run -- usually 1:1
    // noise currently hard-coded to 32 -- assumed to be sufficient in most, if not all, cases

    if (target.partitionBy.nonEmpty) {
      if (target.partitionBy.contains("__overwatch_ctrl_noise") && !target.autoOptimize) {
        logger.log(Level.INFO, s"${target.tableFullName}: generating partition noise")
        mutationDF = mutationDF.withColumn("__overwatch_ctrl_noise", (rand() * lit(32)).cast("int"))
      }

      if (!target.autoOptimize) {
        logger.log(Level.INFO, s"${target.tableFullName}: shuffling into $targetShufflePartitionCount " +
          s" output partitions defined as ${target.partitionBy.mkString(", ")}")
        mutationDF = mutationDF.repartition(targetShufflePartitionCount, target.partitionBy map col: _*)
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
  def withIncrementalFilters(
                              df: DataFrame,
                              module: Module,
                              filters: Seq[IncrementalFilter],
                              globalFilters: Option[Seq[Column]] = None,
                              dataFrequency: Frequency
                            ): DataFrame = {
    val parsedFilters = filters.map(filter => {
      val c = filter.cronColName
      val low = filter.low
      val high = filter.high
      val dt = df.schema.fields.filter(_.name == c).head.dataType
      dt match {
        case _: TimestampType =>
          col(c).between(PipelineFunctions.addOneTick(low, dataFrequency), high)
        case _: DateType => {
          col(c).between(
            PipelineFunctions.addOneTick(low.cast(DateType), dataFrequency, DateType),
            if (dataFrequency == Frequency.daily) date_sub(high.cast(DateType), 1) else high.cast(DateType)
          )
        }
        case _: LongType =>
          col(c).between(PipelineFunctions.addOneTick(low, dataFrequency, LongType), high.cast(LongType))
        case _: IntegerType =>
          col(c).between(PipelineFunctions.addOneTick(low, dataFrequency, IntegerType), high.cast(IntegerType))
        case _: DoubleType =>
          col(c).between(PipelineFunctions.addOneTick(low, dataFrequency, DoubleType), high.cast(DoubleType))
        case _ =>
          throw new IllegalArgumentException(s"IncreasingID Type: ${dt.typeName} is Not supported")
      }
    })

    val allFilters = parsedFilters ++ globalFilters.getOrElse(Seq())

    applyFilters(df, allFilters, Some(module))

  }

  def isIgnorableException(e: Exception): Boolean = {
    val message = e.getMessage()
    message.contains("Cannot modify the value of a static config")
  }

  def setSparkOverrides(spark: SparkSession, sparkOverrides: Map[String, String],
                        debugFlag: Boolean = false): Unit = {
    sparkOverrides foreach { case (k, v) =>
      try {
        if (debugFlag) {
          val opt = spark.conf.getOption(k)
          if (opt.isEmpty || opt.get != v) {
            println(s"Overriding $k from $opt --> $v")
          }
        }
        spark.conf.set(k, v)
      } catch {
        case e: AnalysisException =>

          if (!isIgnorableException(e)) {
            logger.log(Level.WARN, s"Cannot Set Spark Param: $k", e)
            if (debugFlag)
              println(s"Failed Setting $k", e)
          }
        case e: Throwable =>
          if (debugFlag)
            println(s"Failed Setting $k", e)
          logger.log(Level.WARN, s"Failed trying to set $k", e)
      }
    }
  }
}
