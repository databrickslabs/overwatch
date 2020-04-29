package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
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

  def sanitizeFieldName(s: String): String = {
    s.replaceAll("[^a-zA-Z0-9_]", "")
  }

  def sanitizeFields(field: StructField): StructField = {
    field.copy(name = sanitizeFieldName(field.name), dataType = sanitizeSchema(field.dataType))
  }

  def generateUniques(fields: Array[StructField]): Array[StructField] = {
    val r = new scala.util.Random(10)
    val fieldNames = fields.map(_.name)
    val dups = fieldNames.diff(fieldNames.distinct)
    val dupCount = dups.length
    if (dupCount == 0) {
      fields
    } else {
      val uniqueSuffixes = (0 to dupCount + 10).map(_ => r.alphanumeric.take(6).mkString("")).distinct
      fields.zipWithIndex.map(f => {
        f._1.copy(name = f._1.name + "_" + uniqueSuffixes(f._2))
      })
    }
  }

  def sanitizeSchema(dataType: DataType): DataType = {
    dataType match {
      case dt: StructType =>
        val dtStruct = dt.asInstanceOf[StructType]
        dtStruct.copy(fields = generateUniques(dtStruct.fields).map(sanitizeFields))
      case dt: ArrayType =>
        val dtArray = dt.asInstanceOf[ArrayType]
        dtArray.copy(elementType = sanitizeSchema(dtArray.elementType))
      case _ => dataType
    }
  }

  def write(inputDF: DataFrame, tableName: String, format: String = "delta",
            mode: String = "append", autoOptimize: Boolean = false,
            autoCompact: Boolean = false, partitionBy: Array[String] = Array(),
            withCreateDate: Boolean = false): Boolean = {

    var finalDF: DataFrame = inputDF
    if (withCreateDate) finalDF = finalDF.withColumn("CreateDate",
      from_unixtime(lit(Config.pipelineSnapTime.asUnixTime)))
    finalDF = spark.createDataFrame(finalDF.rdd, sanitizeSchema(finalDF.schema).asInstanceOf[StructType])

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
        finalDF.write.format(format).mode(mode).saveAsTable(s"${_databaseName}.${tableName}")
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
