package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.databricks.labs.overwatch.utils.{DataTarget, SparkSessionWrapper, TimeTypesConstants}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, count, from_unixtime, length, lit, when}
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}

trait ValidationUtils extends SparkSessionWrapper {

  import spark.implicits._

  /**
   * Deletes all targets states silver and gold. Used to recalculate modules from scratch after snapshotting bronze.
   * Warning: Should be applied to snapshotted pipeline_report table only!
   * TODO: list of modules to delete should be taken from pipeline definition, but is not exposed as a list as of now.
   *
   * @param target
   * @return
   */
  protected def resetPipelineReportState(dbname: String): Unit = {
    assert(dbname != "overwatch_etl")

    try {
      val sql = s"""delete from ${dbname}.pipeline_report where not moduleName like 'Bronze%'"""
      println(s"deleting bronze and silver module state entries: $sql")
      spark.sql(sql)
    } catch {
      case e: Throwable => println(e.printStackTrace())
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
}
