package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, Frequency, IncrementalFilter, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}

// TODO -- Add rules: Array[Rule] to enable Rules engine calculations in the append
//  also add ruleStrateg: Enum(Kill, Quarantine, Ignore) to determine when to require them
//  Perhaps add the strategy into the Rule definition in the Rules Engine
case class PipelineTable(
                          name: String,
                          keys: Array[String],
                          config: Config,
                          incrementalColumns: Array[String] = Array(),
                          dataFrequency: Frequency = Frequency.milliSecond,
                          format: String = "delta", // TODO -- Convert to Enum
                          mode: String = "append", // TODO -- Convert to Enum
                          _databaseName: String = "default",
                          autoOptimize: Boolean = false,
                          autoCompact: Boolean = false,
                          partitionBy: Array[String] = Array(),
                          statsColumns: Array[String] = Array(),
                          shuffleFactor: Double = 1.0,
                          optimizeFrequency: Int = 24 * 7,
                          zOrderBy: Array[String] = Array(),
                          vacuum: Int = 24 * 7, // TODO -- allow config overrides -- no vacuum == 0
                          enableSchemaMerge: Boolean = true,
                          sparkOverrides: Map[String, String] = Map[String, String](),
                          withCreateDate: Boolean = true,
                          withOverwatchRunID: Boolean = true,
                          isTemp: Boolean = false,
                          checkpointPath: Option[String] = None
                        ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var currentSparkOverrides: Map[String, String] = sparkOverrides

  import spark.implicits._

  private val databaseName = if(_databaseName == "default") config.databaseName else config.consumerDatabaseName

  //  col("c").get
  private val (catalogDB, catalogTable) = if (!config.isFirstRun) {
    val dbCatalog = try {
      Some(spark.sessionState.catalog.getDatabaseMetadata(config.databaseName))
    } catch {
      case e: Throwable => None
    }

    val dbTable = try {
      Some(spark.sessionState.catalog.getTableMetadata(new TableIdentifier(name, Some(config.databaseName))))
    } catch {
      case e: Throwable => None
    }
    (dbCatalog, dbCatalog)
  } else (None, None)

  val tableFullName: String = s"${databaseName}.${name}"

  if (autoOptimize) {
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    println(s"Setting Auto Optimize for ${name}")
  }
  else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")

  if (autoCompact) {
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
    println(s"Setting Auto Compact for ${name}")
  }
  else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "false")

  /**
   * This EITHER appends/changes the spark overrides OR sets them. This can only set spark params if updates
   * are not passed --> setting the spark conf is really mean to be private action
   * @param updates spark conf updates
   */
  private[overwatch] def setSparkOverrides(updates: Map[String, String] = Map()): Unit = {
    if (updates.nonEmpty) {
      currentSparkOverrides = currentSparkOverrides ++ updates
    }
    if (sparkOverrides.nonEmpty && updates.isEmpty) {
      PipelineFunctions.setSparkOverrides(spark, currentSparkOverrides, config.debugFlag)
    }
  }

  // TODO -- Add partition filter
//  private def buildIncrementalDF(df: DataFrame, filter: IncrementalFilter): DataFrame = {
//    val low = filter.low
//    val high = filter.high
//    val incrementalFilters = incrementalColumns.map(c => {
//      df.schema.fields.filter(_.name == c).head.dataType match {
//        case _: TimestampType => col(c).between(addOneTick(low.asColumnTS), high.asColumnTS)
//        case _: DateType => {
//          val maxVal = df.select(max(c)).as[String].collect().head
//          col(c).between(addOneTick(lit(maxVal).cast("date"), DateType), high.asColumnTS.cast("date"))
//        }
//        case _: LongType => col(c).between(low.asUnixTimeMilli + 1, high.asUnixTimeMilli)
//        case _: DoubleType => col(c).between(low.asUnixTimeMilli + 0.001, high.asUnixTimeMilli)
//        case _: BooleanType => col(c) === lit(false)
//        case dt: DataType =>
//          throw new IllegalArgumentException(s"IncreasingID Type: ${dt.typeName} is Not supported")
//      }
//    })
//
//    incrementalFilters.foldLeft(df) {
//      case (rawDF, incrementalFilter) =>
//        rawDF.filter(incrementalFilter)
//    }
//
//  }

  def exists: Boolean = {
    spark.catalog.tableExists(tableFullName)
  }

  def asDF: DataFrame = {
    try {
      spark.table(tableFullName)
    } catch {
      case e: AnalysisException =>
        logger.log(Level.WARN, s"WARN: ${tableFullName} does not exist will attempt to continue", e)
        Array(s"Could not retrieve ${tableFullName}").toSeq.toDF("ERROR")
    }
  }

  // TODO - REMOVE try/catch???
//  def asIncrementalDF(moduleID: Int): DataFrame = {
//    try {
//      buildIncrementalDF(spark.table(tableFullName), moduleID)
//    } catch {
//      case e: AnalysisException =>
//        logger.log(Level.WARN, s"WARN: ${tableFullName} does not exist will attempt to continue", e)
//        Array(s"Could not retrieve ${tableFullName}").toSeq.toDF("ERROR")
//    }
//  }

  def asIncrementalDF(filters: Seq[IncrementalFilter]): DataFrame = {
    PipelineFunctions.withIncrementalFilters(spark.table(tableFullName), filters)
  }

  def writer(df: DataFrame): Any = {
    setSparkOverrides()
    val f = if (config.isLocalTesting && !config.isDBConnect) "parquet" else format
    if (checkpointPath.nonEmpty) {
      val streamWriterMessage = s"DEBUG: PipelineTable - Checkpoint for ${tableFullName} == ${checkpointPath.get}"
      if (config.debugFlag) println(streamWriterMessage)
      logger.log(Level.INFO, streamWriterMessage)
      var streamWriter = df.writeStream.outputMode(mode).format(f).option("checkpointLocation", checkpointPath.get)
        .queryName(s"StreamTo_${name}")
      streamWriter = if (partitionBy.nonEmpty) streamWriter.partitionBy(partitionBy: _*) else streamWriter
      streamWriter = if (mode == "overwrite") streamWriter.option("overwriteSchema", "true")
      else if (enableSchemaMerge && mode != "overwrite")
        streamWriter
          .option("mergeSchema", "true")
      else streamWriter
      streamWriter
    } else {
      var dfWriter = df.write.mode(mode).format(f)
      dfWriter = if (partitionBy.nonEmpty) dfWriter.partitionBy(partitionBy: _*) else dfWriter
      dfWriter = if (mode == "overwrite") dfWriter.option("overwriteSchema", "true")
      else if (enableSchemaMerge && mode != "overwrite")
        dfWriter
          .option("mergeSchema", "true")
      else dfWriter
      dfWriter
    }

    // TODO - Validate proper repartition to minimize files per partition. Could be autoOptimize


  }


}
