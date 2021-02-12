package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, IncrementalFilter, Module, SparkSessionWrapper}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

import java.util.UUID

object TransformFunctions extends SparkSessionWrapper {

  import spark.implicits._

  implicit class DataFrameTransforms(df: DataFrame) {

    def dropDupColumnByAlias(dropAlias: String, columnNames: String*): DataFrame = {
      columnNames.foldLeft(df) {
        case (mutDF, k) => {
          mutDF.drop(col(s"${dropAlias}.${k}"))
        }
      }
    }

    def joinWithLag(
                     df2: DataFrame,
                     usingColumns: Seq[String],
                     lagDateColumnName: String,
                     laggingSide: String = "left",
                     joinType: String = "inner"
                   ): DataFrame = {
      require(laggingSide == "left" || laggingSide == "right", s"laggingSide must be either 'left' or 'right'; received ${laggingSide}")
      val (left, right) = if(laggingSide == "left") {
        (df.alias("laggard"), df2.alias("driver"))
      } else {
        (df.alias("driver"), df2.alias("laggard"))
      }

      val joinExpr = usingColumns.map(c => {
        val matchCol = col(s"driver.${c}") === col(s"laggard.${c}")
        if (c == lagDateColumnName) {
          matchCol || col(s"driver.${c}") === date_sub(col(s"laggard.${c}"), 1)
        } else matchCol
      }).reduce((x, y) => x && y)

      left.join(right, joinExpr, joinType)
        .dropDupColumnByAlias("laggard", usingColumns: _*)
    }

    def joinAsOf(
                  lookupDF: DataFrame,
                  usingColumns: Seq[String],
                  orderByColumns: Seq[String],
                  lookupColumns: Seq[String],
                  permitFillBackward: Boolean = false,
                  rowsBefore: Long = Window.unboundedPreceding,
                  rowsAfter: Long = Window.unboundedFollowing
                ): DataFrame = {

      val controlColName = "__ctrlCol__"
      val drivingDF = df
        .withColumn(controlColName, lit(0).cast("int"))
      val baseWSpec = Window.partitionBy(usingColumns map col: _*)
        .orderBy(orderByColumns map col: _*)
      val beforeWSpec = baseWSpec.rowsBetween(rowsBefore, Window.currentRow)
      val afterWSpec = baseWSpec.rowsBetween(Window.currentRow, rowsAfter)


      val drivingCols = drivingDF.columns
      val lookupDFCols = lookupDF.columns
      val missingBaseCols = lookupDFCols.diff(drivingCols)
      val missingLookupCols = drivingCols.diff(lookupDFCols)

      val df1Complete = missingBaseCols.foldLeft(drivingDF) {
        case (dfBuilder, c) =>
          dfBuilder.withColumn(c, lit(null))
      }
      val df2Complete = missingLookupCols.foldLeft(lookupDF) {
        case (dfBuilder, c) =>
          dfBuilder.withColumn(c, lit(null))
      }

      val masterDF = df1Complete.unionByName(df2Complete)

      lookupColumns.foldLeft(masterDF) {
        case (dfBuilder, c) => {
          val dt = dfBuilder.schema.fields.filter(_.name == c).head.dataType
          val asOfDF = dfBuilder.withColumn(c,
            coalesce(last(col(c), ignoreNulls = true).over(beforeWSpec), lit(null).cast(dt))
          )
          if (permitFillBackward) {
            asOfDF.withColumn(c,
              coalesce(first(col(c), ignoreNulls = true).over(afterWSpec), lit(null).cast(dt))
            )
          } else asOfDF
        }
      }.filter(col(controlColName).isNotNull)
        .drop(controlColName)
    }
  }

  /**
   * Retrieve DF alias
   * @param ds
   * @return
   */
  def getAlias(ds: Dataset[_]): Option[String] = ds.queryExecution.analyzed match {
    case SubqueryAlias(alias, _) => Some(alias.identifier)
    case _ => None
  }

  /**
   * Simplify code for join conditions where begin events may be on the previous date BUT the date column is a
   * partition column so the explicit date condition[s] must be in the join clause to ensure
   * dynamic partition pruning (DPP)
   * join column names must match on both sides of the join
   * @param dateColumnName date column of DateType -- this column should also either be a partition or indexed col
   * @param alias DF alias of latest event
   * @param lagAlias DF alias potentially containing lagging events
   * @param usingColumns Seq[String] for matching column names
   * @return
   */
  def joinExprMinusOneDay(dateColumnName: String, alias: String, lagAlias: String, usingColumns: Seq[String]): Column = {
    usingColumns.map(c => {
      val matchCol = col(s"${alias}.${c}") === col(s"${lagAlias}.${c}")
      if (c == dateColumnName) {
        matchCol || col(s"${alias}.${c}") === date_sub(col(s"${lagAlias}.${c}"), 1)
      } else matchCol
    }).reduce((x, y) => x && y)
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

  /**
   *
   * Generates a complex time struct to simplify time conversions.
   * TODO - Currently ony supports input as a unix epoch time in milliseconds, check for column input type
   * and support non millis (Long / Int / Double / etc.)
   * This function should also support input column types of timestamp and date as well for robustness

   * @param start : Column of LongType with start time in milliseconds
   * @param end : Column of LongType with end time  in milliseconds
   * @param inputResolution : String of milli, or second (nano to come)
   * @return
   *
   * TODO: should we check for the start < end?
   */
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

  /**
   *
   * Warning Does not remove null structs, arrays, etc.
   *
   * TODO: think, do we need to return the list of the columns - it could be inferred from DataFrame itself
   * TODO: fix its behaviour with non-string & non-numeric fields - for example, it will remove Boolean columns
   *
   * @param df dataframe to more data
   * @return
   *
   */
  def removeNullCols(df: DataFrame): (Seq[Column], DataFrame) = {
    val cntsDF = df.summary("count").drop("summary")
    val nonNullCols = cntsDF.collect()
      .flatMap(r => r.getValuesMap[Any](cntsDF.columns).filter(_._2 != "0").keys)
      .map(col)
    val complexTypeFields = df.schema.fields
      .filter(f => f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType]  || f.dataType.isInstanceOf[MapType])
      .map(_.name).map(col)
    val columns = nonNullCols ++ complexTypeFields
    val cleanDF = df.select(columns: _*)
    (columns, cleanDF)
  }

  /**
   *
   * @param baseDF
   * @param lookupDF
   * @return
   */
  def unionWithMissingAsNull(baseDF: DataFrame, lookupDF: DataFrame): DataFrame = {
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

  /**
   * This is an AS OF lookup function -- return the most recent slow-changing dim as of some timestamp. The window
   * partition column[s] act like the join keys. The Window partition column must be present in driving and lookup DF.
   * EX: Get latest columnsToLookup by Window's Partition column[s] as of latest Window's OrderByColumn
   * @param primaryDF Driving dataframe to which the lookup values are to be added
   * @param primaryOnlyNoNulls Non-null column present only in the primary DF, not the lookup[s]
   * @param columnsToLookup Column names to be looked up -- must be in driving DF AND all lookup DFs from which the value is to be looked up
   * @param w Window spec to partition/sort the lookups. The partition and sort columns must be present in all DFs
   * @param lookupDF One more more dataframes from which to lookup the values
   * @return
   */
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
        df.withColumn(c, coalesce(last(col(c), ignoreNulls = true).over(w), lit(null).cast(dt)))
    }.filter(col(primaryOnlyNoNulls).isNotNull)
  }

  /**
   * Delta, by default, calculates statistics on the first 32 columns and there's no way to specify which columns
   * on which to calc stats. Delta can be configured to calc stats on less than 32 columns but it still starts
   * from left to right moving to the nth position as configured. This simplifies the migration of columns to the
   * front of the dataframe to allow them to be "indexed" in front of others.
   *
   * TODO -- Validate order of columns in Array matches the order in the dataframe after the function call.
   * If input is Array("a", "b", "c") the first three columns should match that order. If it's backwards, the
   * array should be reversed before progressing through the logic
   *
   * TODO -- change colsToMove to the Seq[String]....
   * TODO: checks for empty list, for existence of columns, etc.
   *
   * @param df Input dataframe
   * @param colsToMove Array of column names to be moved to front of schema
   * @return
   */
  def moveColumnsToFront(df: DataFrame, colsToMove: Array[String]): DataFrame = {
    val allNames = df.schema.names
    val newColumns = (colsToMove ++ (allNames.diff(colsToMove))).map(col)
    df.select(newColumns: _*)
  }

  /**
   * Converts string ts column from standard spark ts string format to unix epoch millis. The input column must be a
   * string and must be in the format of yyyy-dd-mmTHH:mm:ss.SSSz
   * @param tsStringCol
   * @return
   */
  def stringTsToUnixMillis(tsStringCol: Column): Column = {
    ((unix_timestamp(tsStringCol.cast("timestamp")) * 1000) + substring(tsStringCol,-4,3)).cast("long")
  }

  private val applicableWorkers = when('type === "RESIZING" && 'target_num_workers < 'current_num_workers, 'target_num_workers).otherwise('current_num_workers)

  def getNodeInfo(nodeType: String, metric: String, multiplyTime: Boolean): Column = {
    val baseMetric = if ("driver".compareToIgnoreCase(nodeType) == 0) {
      col(s"driverSpecs.${metric}")
    } else if("worker".compareToIgnoreCase(nodeType) == 0) {
      col(s"workerSpecs.${metric}") * applicableWorkers
    } else {
      throw new Exception("nodeType must be either 'driver' or 'worker'")
    }

    if (multiplyTime) {
      when('type === "TERMINATING", lit(0))
        .otherwise(round(baseMetric * 'uptime_in_state_S, 2)).alias(s"${nodeType}_${baseMetric}S")
    } else {
      when('type === "TERMINATING", lit(0))
        .otherwise(round(baseMetric, 2).alias(s"${nodeType}_${baseMetric}"))
    }
  }

}
