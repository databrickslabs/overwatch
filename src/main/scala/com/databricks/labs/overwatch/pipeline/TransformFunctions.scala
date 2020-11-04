package com.databricks.labs.overwatch.pipeline

import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, from_unixtime, last, lit, struct}
import org.apache.spark.sql.types._

object TransformFunctions {

  /**
   * First shot At Silver Job Runs
   *
   * @param df
   * @return
   */

  //TODO -- jobs snapshot api seems to be returning more data than audit logs, review
  //  example = maxRetries, max_concurrent_runs, jar_task, python_task
  //  audit also seems to be missing many job_names (if true, retrieve from snap)
  def getJobsBase(df: DataFrame): DataFrame = {
    df.filter(col("serviceName") === "jobs")
      .selectExpr("*", "requestParams.*").drop("requestParams")
  }


  /**
   * Converts column of seconds/milliseconds/nanoseconds to timestamp
   *
   * @param rawVal          : Column of LongType
   * @param inputResolution : String of milli, or second (nano to come)
   * @return
   */
  def toTS(rawVal: Column, inputResolution: String = "milli", outputResultType: DataType = TimestampType): Column = {
    outputResultType match {
      case _: TimestampType | DateType =>
        if (inputResolution == "milli") {
          from_unixtime(rawVal.cast("double") / 1000).cast(outputResultType)
        } else { // Seconds for Now
          from_unixtime(rawVal).cast(outputResultType)
        }

      case _ =>
        throw new IllegalArgumentException(s"Unsupported outputResultType: $outputResultType")
    }
  }

  def subtractTime(start: Column, end: Column, inputResolution: String = "milli"): Column = {
    val runTimeMS = end - start
    val runTimeS = runTimeMS / 1000
    val runTimeM = runTimeS / 60
    val runTimeH = runTimeM / 60
    struct(
      start.alias("startEpochMS"),
      toTS(start, inputResolution).alias("startTS"),
      end.alias("endEpochMS"),
      toTS(end, inputResolution).alias("endTS"),
      lit(runTimeMS).alias("runTimeMS"),
      lit(runTimeS).alias("runTimeS"),
      lit(runTimeM).alias("runTimeM"),
      lit(runTimeH).alias("runTimeH")
    ).alias("RunTime")
  }

  // Does not remove null structs
  def removeNullCols(df: DataFrame): (Seq[Column], DataFrame) = {
    val cntsDF = df.summary("count").drop("summary")
    val nonNullCols = cntsDF
      .collect().flatMap(r => r.getValuesMap[Any](cntsDF.columns).filter(_._2 != "0").keys).map(col)
    val complexTypeFields = df.schema.fields
      .filter(f => f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType])
      .map(_.name).map(col)
    val cleanDF = df.select(nonNullCols ++ complexTypeFields: _*)
    (nonNullCols ++ complexTypeFields, cleanDF)
  }

  private def unionWithMissingAsNull(baseDF: DataFrame, lookupDF: DataFrame): DataFrame = {
    val baseCols = baseDF.columns
    val lookupCols = lookupDF.columns
    val missingBaseCols = lookupCols.diff(baseCols)
    val missingLookupCols = baseCols.diff(lookupCols)
    val df1Complete = missingBaseCols.foldLeft(baseDF) {
      case (df, c) =>
        df.withColumn(c, lit(null))
    }
    val df2Complete = missingLookupCols.foldLeft(lookupDF) {
      case (df, c) =>
        df.withColumn(c, lit(null))
    }

    df1Complete.unionByName(df2Complete)
  }

  def fillFromLookupsByTS(primaryDF: DataFrame, primaryOnlyNoNulls: String,
                          columnsToLookup: Array[String], w: WindowSpec,
                          lookupDF: DataFrame*): DataFrame = {
    val finalDFWNulls = lookupDF.foldLeft(primaryDF) {
      case (primaryDF, lookup) =>
        unionWithMissingAsNull(primaryDF, lookup)
    }

    columnsToLookup.foldLeft(finalDFWNulls) {
      case (df, c) =>
        val dt = df.schema.fields.filter(_.name == c).head.dataType
        df.withColumn(c, coalesce(last(col(c), ignoreNulls = true).over(w), lit(0).cast(dt)))
    }.filter(col(primaryOnlyNoNulls).isNotNull)

  }

}
