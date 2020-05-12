package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.utils.{Config, SchemaTools, SparkSessionWrapper}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class Database extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var _databaseName: String = _

  def setDatabaseName(value: String): this.type = {
    _databaseName = value
    this
  }

  def getDatabaseName: String = _databaseName

  def doAutoCompact = ???

  def doAutoOptimize = ???

  def write(inputDF: DataFrame, tableName: String, format: String = "delta",
            mode: String = "append", autoOptimize: Boolean = false, autoCompact: Boolean = false,
            partitionBy: Array[String] = Array(),
            withCreateDate: Boolean = false, withOverwatchRunID: Boolean = false): Boolean = {

    var finalDF: DataFrame = inputDF
    if (withCreateDate) finalDF = finalDF.withColumn("Pipeline_SnapTS", Config.pipelineSnapTime.asColumnTS)
    if (withOverwatchRunID) finalDF = finalDF.withColumn("Overwatch_RunID", lit(Config.runID))

    finalDF = SchemaTools.scrubSchema(finalDF)

    try {
      logger.log(Level.INFO, s"Beginning write to ${_databaseName}.${tableName}")
      if (autoCompact) doAutoCompact
      if (autoOptimize) doAutoOptimize

      // TODO - Validate proper repartition to minimize files per partition. Could be autoOptimize
      if (!partitionBy.isEmpty) {
        finalDF.write.format(format).mode(mode)
          .partitionBy(partitionBy: _*)
          .saveAsTable(s"${_databaseName}.${tableName}")
        true
      } else {
        finalDF.write.format(format).mode(mode).option("mergeSchema", "true")
          .saveAsTable(s"${_databaseName}.${tableName}")
        true
      }
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

  def apply(databaseName: String): Database = {
    new Database().setDatabaseName(databaseName)
  }

}
