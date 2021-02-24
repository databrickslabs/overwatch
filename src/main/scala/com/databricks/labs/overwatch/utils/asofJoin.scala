package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

/**
 * Essentially a fork from dblabs tempo project
 * Added a few features and made one or two minor changes noted below
 * Plan to use tempo as a dependency when tempo is ready and has these new, needed features
 *
 * Some significant refactoring can be done to simplify and reduce the code here but left it broken out to
 * simplify the merge into tempo. Also, didn't want to spend too much time on it since the ultimate goal is to use
 * tempo as a dep when it's ready.
 */
object asofJoin {

  def checkEqualPartitionCols(leftTSDF: TSDF, rightTSDF: TSDF): Boolean = {
    leftTSDF.partitionCols.zip(rightTSDF.partitionCols).forall(p => p._1 == p._2)
  }

  def addPrefixToColumns(tsdf: TSDF, col_list: Seq[StructField], prefix: String): TSDF = {

    // Prefix all column
    val prefixedDF = col_list.foldLeft(tsdf.df)((df, colStruct) => df
      .withColumnRenamed(colStruct.name, prefix + colStruct.name))

    // update ts_colName if it is in col_list
    val ts_colName = if (col_list.contains(tsdf.tsColumn)) prefix + tsdf.tsColumn.name else tsdf.tsColumn.name

    // update any partition column names that are in col_list
    val partitionColstrings = tsdf.partitionCols
      .foldLeft(Seq[String]())((newPartCols, PartCol) =>
        newPartCols :+ (if (col_list.contains(PartCol)) prefix + PartCol.name else PartCol.name))

    TSDF(prefixedDF, ts_colName, partitionColstrings: _*)
  }

  // Add columns from other DF ahead of combining them through Union
  // TODO: implement unit test
  def addColumnsFromOtherDF(tsdf: TSDF, other_cols: Seq[StructField]): TSDF = {

    val newDF = other_cols.foldLeft(tsdf.df)((df, colStruct) => df
      .withColumn(colStruct.name, lit(null)))

    // TODO: fix partition column names, it's not nice this way
    TSDF(newDF, tsdf.tsColumn.name, tsdf.partitionCols.map(_.name): _*)
  }

  /**
   * CHANGE: removed the "combined_ts" column and am using the left timestamp column as the time-series
   * for continuity throughout multiple joins
   * @param leftTSDF
   * @param rightTSDF
   * @return
   */
  def combineTSDF(leftTSDF: TSDF, rightTSDF: TSDF): TSDF = {

    //    val combinedTsCol = "combined_ts"

    val combinedDF = leftTSDF.df
      .unionByName(rightTSDF.df)
      .withColumn(leftTSDF.tsColumn.name,
        coalesce(col(leftTSDF.tsColumn.name), col(rightTSDF.tsColumn.name)))


    TSDF(combinedDF, leftTSDF.tsColumn.name, leftTSDF.partitionCols.map(_.name): _*)
  }

  /**
   * MAJOR CHANGE: This implementation was considerably altered from tempo's original to enable time-boxed
   * reverse and forward lookups. Additionally, there was a BUG?? in that the window was custom built inside this
   * function rather than being built from the TSDF definition
   *
   * Also added column typecasting to ensure left type == right type. Error handling and checks should be
   * implemented, currently if cannot cast, null is written silently.
   * @param tsdf
   * @param maxLookback
   * @param maxLookAhead
   * @param left_ts_col
   * @param rightCols
   * @return
   */
  def getLastRightRow(
                       tsdf: TSDF,
                       maxLookback: Long,
                       maxLookAhead: Long,
                       left_ts_col: StructField,
                       rightCols: Seq[StructField]
                     ): TSDF = {

    val beforeW: WindowSpec = tsdf.windowBetweenRows(maxLookback, Window.currentRow)
    val afterW: WindowSpec = tsdf.windowBetweenRows(Window.currentRow, maxLookAhead)

    val df = rightCols.foldLeft(tsdf.df) {
      case (dfBuilder, rightCol) => {
        val asofDF = dfBuilder.withColumn(rightCol.name,
          coalesce(last(col(rightCol.name), true).over(beforeW), lit(null).cast(rightCol.dataType))
        )
        if (maxLookAhead > 0L) {
          asofDF.withColumn(rightCol.name,
            coalesce(col(rightCol.name), first(col(rightCol.name), true).over(afterW), lit(null).cast(rightCol.dataType))
          )
        } else asofDF
      }
    }

    TSDF(df, left_ts_col.name, tsdf.partitionCols.map(_.name): _*)
  }

  /**
   * Critical performance for skewed windows -- time sliced (partitioned) windows to greatly improve parallelism
   * on skewed windows
   * @param combinedTSDF
   * @param tsPartitionVal
   * @param fraction
   * @return
   */
  def getTimePartitions(combinedTSDF: TSDF, tsPartitionVal: Int, fraction: Double): TSDF = {

    val tsColDouble = "ts_col_double"
    val tsColPartition = "ts_partition"
    val partitionRemainder = "partition_remainder"
    val isOriginal = "is_original"

    val partitionDF = combinedTSDF.df
      .withColumn(tsColDouble, col(combinedTSDF.tsColumn.name).cast("double"))
      .withColumn(tsColPartition, lit(tsPartitionVal) * (col(tsColDouble) / lit(tsPartitionVal)).cast("integer"))
      .withColumn(partitionRemainder, (col(tsColDouble) - col(tsColPartition)) / lit(tsPartitionVal))
      .withColumn(isOriginal, lit(1))
      .cache()

    // add [1 - fraction] of previous time partition to the next partition.
    val remainderDF = partitionDF
      .filter(col(partitionRemainder) >= lit(1 - fraction))
      .withColumn(tsColPartition, col(tsColPartition) + lit(tsPartitionVal))
      .withColumn(isOriginal, lit(0))

    val df = partitionDF.union(remainderDF).drop(partitionRemainder, tsColDouble)
    val newPartitionColNames = combinedTSDF.partitionCols.map(_.name) :+ tsColPartition

    partitionDF.unpersist()
    TSDF(df, combinedTSDF.tsColumn.name, newPartitionColNames: _*)
  }

  /**
   * NEW: The tempo tsdf join joins left and right and maintains all columns. When performing lookups, the columns
   * handling after several chained joins is a nightmare. This function merges all the columns from left to right
   * taking the first non-null value and keeping it through all future joins.
   * @param leftTSDF
   * @param rightTSDF
   * @param leftPrefix
   * @param rightPrefix
   * @param maxLookback
   * @param maxLookAhead
   * @param tsPartitionVal
   * @param fraction
   * @return
   */
  def lookupWhenExec(
                  leftTSDF: TSDF,
                  rightTSDF: TSDF,
                  leftPrefix: Option[String],
                  rightPrefix: String,
                  maxLookback: Long,
                  maxLookAhead: Long,
                  tsPartitionVal: Option[Int],
                  fraction: Double = 0.1
                ): TSDF = {

    if (!checkEqualPartitionCols(leftTSDF, rightTSDF)) {
      throw new IllegalArgumentException("Partition columns of left and right TSDF should be equal.")
    }

    // add Prefixes to TSDFs
    val prefixedLeftTSDF = leftPrefix match {
      case Some(prefix_value) => addPrefixToColumns(
        leftTSDF,
        leftTSDF.observationColumns :+ leftTSDF.tsColumn,
        prefix_value)
      case None => leftTSDF
    }

    val prefixedRightTSDF = addPrefixToColumns(
      rightTSDF,
      rightTSDF.observationColumns :+ rightTSDF.tsColumn,
      rightPrefix)

    // get all non-partition columns (including ts col)
    val leftCols = prefixedLeftTSDF.observationColumns :+ prefixedLeftTSDF.tsColumn
    val rightCols = prefixedRightTSDF.observationColumns :+ prefixedRightTSDF.tsColumn

    // perform asof join
    val combinedTSDF = combineTSDF(
      addColumnsFromOtherDF(prefixedLeftTSDF, rightCols),
      addColumnsFromOtherDF(prefixedRightTSDF, leftCols))

    // build left-driving unified output columns
    // structural cols
    val structuralCols = combinedTSDF.structuralColumns.map(f => col(f.name).alias(f.name))

    // dup cols - coalesce
    val dupColNames = leftTSDF.observationColumns.map(_.name).intersect(rightTSDF.observationColumns.map(_.name))

    // outerCols -- leftOnly + rightOnly -- rename
    val leftOnlyColNames = leftTSDF.observationColumns.map(_.name).diff(rightTSDF.observationColumns.map(_.name))
    val rightOnlyColNames = rightTSDF.observationColumns.map(_.name).diff(leftTSDF.observationColumns.map(_.name))
    val outerColNames = leftOnlyColNames.map(cName => col(leftPrefix.getOrElse("") + cName).alias(cName)) ++
      rightOnlyColNames.map(cName => col(rightPrefix + cName).alias(cName))

    val slimSelects = structuralCols ++ outerColNames ++ dupColNames.map(cName => col(cName))

    val asofTSDF: TSDF = tsPartitionVal match {
      case Some(partitionValue) => {
        val timePartitionedTSDF = getTimePartitions(combinedTSDF, partitionValue, fraction)

        val asofDF = getLastRightRow(
          timePartitionedTSDF,
          maxLookback,
          maxLookAhead,
          prefixedLeftTSDF.tsColumn,
          rightCols
        ).df
          .filter(col("is_original") === lit(1))
          .drop("ts_partition", "is_original")

        TSDF(asofDF, prefixedLeftTSDF.tsColumn.name, leftTSDF.partitionCols.map(_.name): _*)
      }
      case None => getLastRightRow(
        combinedTSDF,
        maxLookback,
        maxLookAhead,
        prefixedLeftTSDF.tsColumn,
        rightCols
      )
    }

    val lookupDF = dupColNames.foldLeft(asofTSDF.df)((df, c) => df
      .withColumn(c, coalesce(col(leftPrefix.getOrElse("") + c), col(rightPrefix + c))))
      .select(slimSelects: _*)

    TSDF(lookupDF, leftTSDF.tsColumn.name, leftTSDF.partitionCols.map(_.name): _*)
  }

  /**
   * Original asOfJoin implementation with ADDITION of enabling time-boxed reverse lookups as well as forward
   * lookups. The defaults remain as they were (unbound) but can be overridden. The first non-null value will be
   * taken from -Inf to Inf order
   * @param leftTSDF
   * @param rightTSDF
   * @param leftPrefix
   * @param rightPrefix
   * @param maxLookback
   * @param maxLookAhead
   * @param tsPartitionVal
   * @param fraction
   * @return
   */
    def asofJoinExec(
                      leftTSDF: TSDF,
                      rightTSDF: TSDF,
                      leftPrefix: Option[String],
                      rightPrefix: String,
                      maxLookback: Long,
                      maxLookAhead: Long,
                      tsPartitionVal: Option[Int],
                      fraction: Double = 0.1
                    ): TSDF= {

      if(!checkEqualPartitionCols(leftTSDF,rightTSDF)) {
        throw new IllegalArgumentException("Partition columns of left and right TSDF should be equal.")
      }

      // add Prefixes to TSDFs
      val prefixedLeftTSDF = leftPrefix match {
        case Some(prefix_value) => addPrefixToColumns(
          leftTSDF,
          leftTSDF.observationColumns :+ leftTSDF.tsColumn,
          prefix_value)
        case None => leftTSDF
      }

      val prefixedRightTSDF = addPrefixToColumns(
        rightTSDF,
        rightTSDF.observationColumns :+ rightTSDF.tsColumn,
        rightPrefix)

      // get all non-partition columns (including ts col)
      val leftCols = prefixedLeftTSDF.observationColumns :+ prefixedLeftTSDF.tsColumn
      val rightCols = prefixedRightTSDF.observationColumns :+ prefixedRightTSDF.tsColumn

      // perform asof join
      val combinedTSDF = combineTSDF(
        addColumnsFromOtherDF(prefixedLeftTSDF, rightCols),
        addColumnsFromOtherDF(prefixedRightTSDF, leftCols))

      val asofTSDF: TSDF = tsPartitionVal match {
        case Some(partitionValue) => {
          val timePartitionedTSDF = getTimePartitions(combinedTSDF,partitionValue,fraction)

          val asofDF = getLastRightRow(
            timePartitionedTSDF,
            maxLookback,
            maxLookAhead,
            prefixedLeftTSDF.tsColumn,
            rightCols
          ).df
            .filter(col("is_original") === lit(1))
            .drop("ts_partition","is_original")

          TSDF(asofDF, prefixedLeftTSDF.tsColumn.name, leftTSDF.partitionCols.map(_.name):_*)
        }
        case None => getLastRightRow(
          combinedTSDF,
          maxLookback,
          maxLookAhead,
          prefixedLeftTSDF.tsColumn,
          rightCols
        )
      }
      asofTSDF
    }

}


