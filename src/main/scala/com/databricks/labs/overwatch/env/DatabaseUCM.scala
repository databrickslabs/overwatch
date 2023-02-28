package com.databricks.labs.overwatch.env

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
//import com.sun.prism.impl.Disposer.Target
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

import java.util

class DatabaseUCM(config: Config) extends Database(config) {

  override def getStatementsForRegisterTarget(target: PipelineTable): (String, String) = {
    val createStatement = s"create table if not exists ${target.tableFullName} " + s"USING DELTA "
    val logMessage = s"CREATING TABLE: ${target.tableFullName} \n$createStatement\n\n"

    (createStatement, logMessage)
  }

  override private[env] def getDeltaTable(target: PipelineTable): DeltaTable = {
    val deltaTarget = DeltaTable.forName(target.tableFullName).alias("target")
    deltaTarget
  }

  override private[env] def getStreamWriterObject(finalDF: DataFrame, target: PipelineTable): StreamingQuery = {
    val streamWriter = target.writer(finalDF)
      .asInstanceOf[DataStreamWriter[Row]]
      .toTable(target.tableFullName)

    streamWriter
  }

  override private[env] def targetWriter(finalDF: DataFrame, target: PipelineTable): Unit = {
    target.writer(finalDF).asInstanceOf[DataFrameWriter[Row]].saveAsTable(target.tableFullName)
  }

  //  override private[env] def tableNameForExistsCheck(target: PipelineTable, config: Config): String = {
  //    val tableString = s"${config.catalogName}.${config.databaseName}.${target.name}"
  //    tableString
  //  }

  override private[env] def tableNameForExistsCheck(target: PipelineTable): Boolean = {
    target.exists(pathValidation = false, catalogValidation = true)
  }

  override private[env] def initializeStreamTarget(df: DataFrame, target: PipelineTable): Unit = {
    val dfWSchema = spark.createDataFrame(new util.ArrayList[Row](), df.schema)
    val staticDFWriter = target.copy(checkpointPath = None).writer(dfWSchema)
    staticDFWriter
      .asInstanceOf[DataFrameWriter[Row]]
      .saveAsTable(target.tableFullName)

    registerTarget(target)
  }
}
