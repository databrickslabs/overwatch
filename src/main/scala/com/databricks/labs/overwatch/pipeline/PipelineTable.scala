package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, Frequency, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, DataFrameWriter, Row}

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
                          isTemp: Boolean = false
                        ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var currentSparkOverrides: Map[String, String] = sparkOverrides
  import spark.implicits._

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

  val tableFullName: String = s"${config.databaseName}.${name}"

  if (autoOptimize) spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
  else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")

  if (autoCompact) spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
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
      currentSparkOverrides foreach { case (k, v) =>
      try {
          spark.conf.set(k, v)
      } catch {
          case e: AnalysisException => logger.log(Level.WARN, s"Cannot Set Spark Param: ${k}", e)
          case e: Throwable => logger.log(Level.ERROR, s"Failed trying to set $k", e)
        }
      }
    }
  }

  private def addOneTick(ts: Column, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 + 1) / 1000).cast("timestamp")
      case _: DateType =>
        date_add(ts, 1)
      case _: DoubleType =>
        ts + lit(0.001)
      case _: LongType =>
        ts + 1
      case _: IntegerType =>
        ts + 1
      case _ => throw new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }

  private def buildIncrementalDF(df: DataFrame, moduleID: Int): DataFrame = {
    val from = config.fromTime(moduleID)
    val until = config.pipelineSnapTime
    val incrementalFilters = incrementalColumns.map(c => {
      df.schema.fields.filter(_.name == c).head.dataType match {
        case dt: TimestampType => col(c).between(addOneTick(from.asColumnTS), until.asColumnTS)
        case dt: DateType => col(c).between(addOneTick(from.asColumnTS), until.asColumnTS)
        case dt: LongType => col(c).between(from.asUnixTimeMilli + 1, until.asUnixTimeMilli)
        case dt: DoubleType => col(c).between(from.asUnixTimeMilli + 0.001, until.asUnixTimeMilli)
        case dt: BooleanType => col(c) === lit(false)
        case dt: DataType =>
          throw new IllegalArgumentException(s"IncreasingID Type: ${dt.typeName} is Not supported")
      }
    })

    incrementalFilters.foldLeft(df) {
      case (rawDF, incrementalFilter) =>
        rawDF.filter(incrementalFilter)
    }

  }

  def asDF: DataFrame = {
    try{
      spark.table(tableFullName)
    } catch {
      case e: AnalysisException =>
        logger.log(Level.WARN, s"WARN: ${tableFullName} does not exist will attempt to continue", e)
        Array(s"Could not retrieve ${tableFullName}").toSeq.toDF("ERROR")
    }
  }

  def asIncrementalDF(moduleID: Int): DataFrame = {
    try{
      buildIncrementalDF(spark.table(tableFullName), moduleID)
    } catch {
      case e: AnalysisException =>
        logger.log(Level.WARN, s"WARN: ${tableFullName} does not exist will attempt to continue", e)
        Array(s"Could not retrieve ${tableFullName}").toSeq.toDF("ERROR")
    }
  }

  def writer(df: DataFrame): DataFrameWriter[Row] = {
    setSparkOverrides()
    val f = if (config.isLocalTesting && !config.isDBConnect) "parquet" else format
    var writer = df.write.mode(mode).format(f)
    // TODO - Validate proper repartition to minimize files per partition. Could be autoOptimize
    writer = if (partitionBy.nonEmpty) writer.partitionBy(partitionBy: _*) else writer
    writer = if (mode == "overwrite") writer.option("overwriteSchema", "true")
    else if (enableSchemaMerge && mode != "overwrite")
      writer.option("mergeSchema", "true")
    else writer
    writer
  }


}
