package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Dataset, Row}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class Database(config: Config) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _

  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  def getDatabaseName: String = _databaseName

  def write(df: DataFrame, target: PipelineTable): Boolean = {

    var finalDF: DataFrame = df
    finalDF = if (target.withCreateDate) finalDF.withColumn("Pipeline_SnapTS", config.pipelineSnapTime.asColumnTS) else finalDF
    finalDF = if (target.withOverwatchRunID) finalDF.withColumn("Overwatch_RunID", lit(config.runID)) else finalDF
    finalDF = SchemaTools.scrubSchema(finalDF)

    try {
      logger.log(Level.INFO, s"Beginning write to ${target.tableFullName}")
      target.writer(finalDF).saveAsTable(target.tableFullName)
//      if (!config.isLocalTesting) target.writer(finalDF).saveAsTable(target.tableFullName)
//      else target.writer(finalDF).saveAsTable(target.tableFullName)
      logger.log(Level.INFO, s"Completed write to ${target.tableFullName}")
      true
    } catch {
      case e: Throwable => logger.log(Level.ERROR, s"Failed to write to ${_databaseName}", e); false
    }
  }

  def readTable(tableName: String): DataFrame = {
    spark.table(s"${_databaseName}.${tableName}")
  }

  def readPath(path: String, format: String = "delta"): DataFrame = {
    spark.read.format(format).load(path)
  }

  def tableExists(tableName: String): Boolean = {
    spark.catalog.tableExists(_databaseName, tableName)
  }

  //TODO -- Add dbexists func

}

object Database {

  def apply(config: Config): Database = {
    new Database(config).setDatabaseName(config.databaseName)
  }

}
