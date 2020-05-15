package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, Frequency, SchemaTools, SparkSessionWrapper}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

// TODO -- Add rules: Array[Rule] to enable Rules engine calculations in the append
//  also add ruleStrateg: Enum(Kill, Quarantine, Ignore) to determine when to require them
//  Perhaps add the strategy into the Rule definition in the Rules Engine
case class PipelineTable(
                          name: String,
                          keys: Array[String],
                          incrementalFromColumn: String, // TODO -- Change to CDC incrementing ID (i.e. allow appender to handle ints/etc
                          config: Config,
                          dataFrequency: Frequency = Frequency.milliSecond,
                          format: String = "delta", // TODO -- Conver to Enum
                          mode: String = "append", // TODO -- Convert to Enum
                          autoOptimize: Boolean = false,
                          autoCompact: Boolean = false,
                          partitionBy: Array[String] = Array(),
                          statsColumns: Array[String] = Array(),
                          zOrderBy: Array[String] = Array(),
                          vacuum: Int = 24 * 7, // TODO -- allow config overrides -- no vacuum == 0
                          enableSchemaMerge: Boolean = true,
                          withCreateDate: Boolean = true,
                          withOverwatchRunID: Boolean = true
                ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)


  private val (catalogDB, catalogTable) = if (!config.isFirstRun) {
    val dbCatalog = try{
      Some(spark.sessionState.catalog.getDatabaseMetadata(config.databaseName))
    } catch {
      case e: Throwable => None
    }

    val dbTable = try{
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


  def asDF: DataFrame = {
    spark.table(tableFullName)
  }

  // TODO - set autoOptimizeConfigs
  def writer(df: DataFrame): DataFrameWriter[Row] = {
    val f = if (config.isLocalTesting) "parquet" else format
    var writer = df.write.mode(mode).format(f)
    // TODO - Validate proper repartition to minimize files per partition. Could be autoOptimize
    writer = if(partitionBy.nonEmpty) writer.partitionBy(partitionBy: _*) else writer
    writer = if(enableSchemaMerge) writer.option("mergeSchema", "true") else writer
    writer
  }


}
