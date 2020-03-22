package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.log4j.{Level, Logger}

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

  def write(df: DataFrame, tableName: String, format: String = "delta",
            mode: String = "append", autoOptimize: Boolean = false,
            autoCompact: Boolean = false, partitionBy: Array[String] = Array()): Boolean = {

    try {
      logger.log(Level.INFO, s"Beginning write to ${_databaseName}.${tableName}")
      if (autoCompact) doAutoCompact
      if (autoOptimize) doAutoOptimize

      // TODO - Validate proper repartition to minimize files per partition. Could be autoOptimize
      if (!partitionBy.isEmpty) {
        df.write.format(format).mode(mode)
          .partitionBy(partitionBy: _*)
          .saveAsTable(s"${_databaseName}.${tableName}")
        true
      } else {
        df.write.format(format).mode(mode).saveAsTable(s"${_databaseName}.${tableName}")
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

}

object Database {

  def apply(databaseName: String): Database = {
    new Database().setDatabaseName(databaseName)
  }

}
