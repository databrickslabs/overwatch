package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.pipeline.{PipelineTable, Schema}
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.{DataTarget, SparkSessionWrapper, TimeTypesConstants}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, count, from_unixtime, length, lit, when}
import org.apache.spark.sql.types.{DateType, LongType, MapType, StringType, TimestampType}
import org.apache.log4j.{Level, Logger}

import java.sql.Timestamp


trait ValidationUtils extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  /**
   * Deletes all targets states silver and gold. Used to recalculate modules from scratch after snapshotting bronze.
   * Warning: Should be applied to snapshotted pipeline_report table only!
   * TODO: list of modules to delete should be taken from pipeline definition, but is not exposed as a list as of now.
   *
   * @param target
   * @return
   */
  protected def resetPipelineReportState(pipelineStateTable: PipelineTable): Unit = {

    try {
      val sql = s"""delete from ${pipelineStateTable.tableFullName} where moduleID >= 2000"""
      println(s"deleting silver and gold module state entries: $sql")
      spark.sql(sql)
    } catch {
      case e: Throwable => {
        val errMsg = s"FAILED to updated pipeline state: ${e.getMessage}"
        println(errMsg)
        logger.log(Level.ERROR, errMsg, e)
      }
    }
  }

  protected def tsFilter(c: Column, startCompare: Column, endCompare: Column): Column = c.between(startCompare, endCompare)

  protected def tsTojsql(ts: Column): java.sql.Timestamp =
    new java.sql.Timestamp(Seq(("")).toDF("ts").withColumn("ts", ts.cast("long")).as[Long].first)

  /**
   * outputs column converted to normalized filtering
   * TODO: find better approach to deal with incrementalcol being a nested field
   * @param df
   * @param tsCol
   * @return
   */
  protected def colToTS(df: DataFrame, tsCol: String): Column = {
    val fields = df.schema.fields

    val Array(tsCol_splitted, rest @ _*) = tsCol split('.') // if the field is complex, split.
    val restField = rest.mkString

    assert(fields.map(_.name.toLowerCase).contains(tsCol_splitted.toLowerCase))
    assert(!restField.contains('.')) // support 1 level deep only

    val _f = fields.find(_.name.toLowerCase == tsCol_splitted.toLowerCase).get // get main field

    val f = if (!restField.isEmpty) {
      df.withColumn("__overwatch_incremental_col", col(tsCol))
        .schema.fields.find(_.name == "__overwatch_incremental_col").get
    } else _f

    val c = col(f.name)
    f.dataType match {
      case _: LongType =>
        when(length(c) === 13, from_unixtime(c.cast("long") / lit(1000)).cast("timestamp"))
          .otherwise(from_unixtime(c).cast("timestamp"))
      case _: DateType => c.cast("timestamp")
      case _: TimestampType => c
      case _ => throw new Exception(s"${f.dataType} type not supported")
    }
  }

  /**
   * gets a pipeline table dataframe and outputs it after filtering, used to compare with recalculated tables.
   * TODO: find better approach to deal with incrementalcol being a nested field
   * @param target
   * @param tableName
   * @param startCompare
   * @param endCompare
   * @return
   */
  protected def getFilteredDF(target: PipelineTable, tableName: String, startCompare: Column, endCompare: Column): DataFrame = {
    val baseDF = spark.table(tableName)
    // TODO - TOMES - must add global and partition filters where relevant.
    if (target.incrementalColumns.isEmpty) {
      baseDF
    } else {
      //println(s"DEBUG: adding df filter  (${tableName}) between (${startCompare}) and (${endCompare})")
      target.incrementalColumns.foldLeft(baseDF)((df, incColName) => {
        if (colToTS(df, incColName).toString == "__overwatch_incremental_col") {
          df.withColumn("__overwatch_incremental_col", col(incColName))
            .filter(tsFilter(colToTS(df, incColName), startCompare, endCompare))
        } else
          df.filter(tsFilter(colToTS(df, incColName), startCompare, endCompare))
      })
    }
  }

  def assertDataFrameDataEquals(targetDetail: ModuleTarget, sourceDB: String): ValidationReport = {
    val module = targetDetail.module
    val sourceTarget = targetDetail.target.copy(_databaseName = sourceDB)
    val snapTarget = targetDetail.target
    val expected = sourceTarget.asIncrementalDF(module, sourceTarget.incrementalColumns)

    val result = snapTarget.asIncrementalDF(module, sourceTarget.incrementalColumns)

    val expectedCol = "assertDataFrameNoOrderEquals_expected"
    val actualCol = "assertDataFrameNoOrderEquals_actual"

    val controlColumns = Array("__overwatch_ctrl_noise", "Pipeline_SnapTS", "Overwatch_RunID")
    val requiredFields = expected.schema.fields.filter(_.dataType != MapType(StringType, StringType))
      .filterNot(f => controlColumns.map(_.toLowerCase).contains(f.name.toLowerCase))

    val validationStatus = s"VALIDATING TABLE: ${snapTarget.tableFullName}\nFROM TIME: ${module.fromTime.asTSString} " +
      s"\nUNTIL TIME: ${module.untilTime.asTSString}\nFOR FIELDS: ${requiredFields.map(_.name).mkString(", ")}"
    logger.log(Level.INFO, validationStatus)

    val validationReport =
      try {

        val expectedElementsCount = expected
          .groupBy(requiredFields.map(_.name) map col: _*)
          .agg(count(lit(1)).as(expectedCol))
        val resultElementsCount = result
          .groupBy(requiredFields.map(_.name) map col: _*)
          .agg(count(lit(1)).as(actualCol))

        val diff = expectedElementsCount
          .join(resultElementsCount, expected.columns, "full_outer")
          .filter(col(expectedCol) =!= col(actualCol))

        diff
          .select(
            lit(sourceTarget.tableFullName).alias("tableSourceName"),
            lit(snapTarget.tableFullName).alias("tableSnapName"),
            col(expectedCol).alias("tableSourceCount"),
            col(actualCol).alias("tableSnapCount"),
            (col(expectedCol) - col(actualCol)).alias("totalDiscrepancies"),
            module.fromTime.asColumnTS.alias("from"),
            module.untilTime.asColumnTS.alias("until"),
            lit("SUCCESS").alias("message")
          ).as[ValidationReport].first()
      } catch {
        case e: Throwable => {
          val errMsg = s"FAILED VALIDATION RUN: ${snapTarget.tableFullName} --> ${e.getMessage}"
          logger.log(Level.ERROR, errMsg, e)
          ValidationReport(
            sourceTarget.tableFullName,
            snapTarget.tableFullName, 0L, 0L, 0L,
            new Timestamp(module.fromTime.asUnixTimeMilli),
            new Timestamp(module.untilTime.asUnixTimeMilli),
            errMsg
          )
        }
      }

    validationReport
  }
}
