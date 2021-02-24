package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField}

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

  def combineTSDF(leftTSDF: TSDF, rightTSDF: TSDF): TSDF = {

    //    val combinedTsCol = "combined_ts"

    val combinedDF = leftTSDF.df
      .unionByName(rightTSDF.df)
      .withColumn(leftTSDF.tsColumn.name,
        coalesce(col(leftTSDF.tsColumn.name), col(rightTSDF.tsColumn.name)))


    TSDF(combinedDF, leftTSDF.tsColumn.name, leftTSDF.partitionCols.map(_.name): _*)
  }

  //    def combineTSDF(leftTSDF: TSDF, lookupTSDF: TSDF, lookupColums: Seq[String]): TSDF = {
  //
  //      val controlCol = StructField("__control__", IntegerType, nullable = true)
  //      val combinedTsCol = "combined_ts"
  //
  //      val missingLeftColumns = lookupTSDF.schema.filterNot(leftTSDF.fields.contains(_))
  //      val missingLookupColumns = leftTSDF.schema.filterNot(lookupTSDF.fields.contains(_)) :+ controlCol
  //
  //
  //      val leftComplete = missingLeftColumns.foldLeft(leftTSDF.df)((dfBuilder, missingLeftField) => dfBuilder
  //        .withColumn(missingLeftField.name, lit(null).cast(missingLeftField.dataType)))
  //
  //      val lookupComplete = missingLookupColumns.foldLeft(lookupTSDF.df)((dfBuilder, missingLookupField) => dfBuilder
  //        .withColumn(missingLookupField.name, lit(null).cast(missingLookupField.dataType)))
  //
  //      val combinedDF = leftComplete
  //        .withColumn(controlCol.name, lit(1))
  //        .unionByName(lookupComplete)
  //        .withColumn(combinedTsCol,
  //          coalesce(col(leftTSDF.tsColumn.name), col(lookupTSDF.tsColumn.name)))
  //
  //      TSDF(combinedDF, combinedTsCol, leftTSDF.partitionCols.map(_.name): _*)
  //
  //
  //      assert(lookupColums.map(_.toLowerCase).diff(lookupColNames.map(_.toLowerCase)).isEmpty,
  //        s"Lookup column[s] missing from target DF: ${lookupColums.diff(lookupTSDF.schema.fieldNames)}"
  //      )
  //
  //    }

  def getLastRightRow(
                       tsdf: TSDF,
                       maxLookback: Long,
                       maxLookAhead: Long,
                       left_ts_col: StructField,
                       rightCols: Seq[StructField]
                     ): TSDF = {
    // TODO: Add functionality for sort_keys (@ricardo)


    //    val window_spec: WindowSpec =  Window
    //      .partitionBy(tsdf.partitionCols.map(x => col(x.name)):_*)
    //      .rowsBetween(maxLookback, maxLookAhead)
    //      .orderBy(tsdf.tsColumn.name)

    val beforeW: WindowSpec = tsdf.windowBetweenRows(maxLookback, Window.currentRow)
    val afterW: WindowSpec = tsdf.windowBetweenRows(Window.currentRow, maxLookAhead)

    //    val rightWindowSpec = windowSpec.orderBy()

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

    //    val df = rightCols
    //      .foldLeft(tsdf.df)((df, rightCol) =>
    //        df.withColumn(rightCol.name, last(df(rightCol.name), true).over(window_spec)))
    //      .filter(col(left_ts_col.name).isNotNull).drop(col(tsdf.tsColumn.name))

    TSDF(df, left_ts_col.name, tsdf.partitionCols.map(_.name): _*)
  }

  def getTimePartitions(combinedTSDF: TSDF, tsPartitionVal: Int, fraction: Double): TSDF = {

    val tsColDouble = "ts_col_double"
    val tsColPartition = "ts_partition"
    val partitionRemainder = "partition_remainder"
    val isOriginal = "is_original"

    val partitionDF = combinedTSDF.df
      .withColumn(tsColDouble, col(combinedTSDF.tsColumn.name).cast("double"))
      .withColumn(tsColPartition, lit(tsPartitionVal) * (col(tsColDouble) / lit(tsPartitionVal)).cast("integer"))
      .withColumn(partitionRemainder, (col(tsColDouble) - col(tsColPartition)) / lit(tsPartitionVal))
      .withColumn(isOriginal, lit(1)).cache()

    // add [1 - fraction] of previous time partition to the next partition.
    val remainderDF = partitionDF
      .filter(col(partitionRemainder) >= lit(1 - fraction))
      .withColumn(tsColPartition, col(tsColPartition) + lit(tsPartitionVal))
      .withColumn(isOriginal, lit(0))

    val df = partitionDF.union(remainderDF).drop(partitionRemainder, tsColDouble)
    val newPartitionColNames = combinedTSDF.partitionCols.map(_.name) :+ tsColPartition

    TSDF(df, combinedTSDF.tsColumn.name, newPartitionColNames: _*)
  }

  def asofJoinExec(
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
}

//  def asofJoinExec(
//                    leftTSDF: TSDF,
//                    rightTSDF: TSDF,
//                    leftPrefix: Option[String],
//                    rightPrefix: String,
//                    maxLookback: Long,
//                    maxLookAhead: Long,
//                    tsPartitionVal: Option[Int],
//                    fraction: Double = 0.1
//                  ): TSDF= {
//
//    if(!checkEqualPartitionCols(leftTSDF,rightTSDF)) {
//      throw new IllegalArgumentException("Partition columns of left and right TSDF should be equal.")
//    }
//
//    // add Prefixes to TSDFs
//    val prefixedLeftTSDF = leftPrefix match {
//      case Some(prefix_value) => addPrefixToColumns(
//        leftTSDF,
//        leftTSDF.observationColumns :+ leftTSDF.tsColumn,
//        prefix_value)
//      case None => leftTSDF
//    }
//
//    val prefixedRightTSDF = addPrefixToColumns(
//      rightTSDF,
//      rightTSDF.observationColumns :+ rightTSDF.tsColumn,
//      rightPrefix)
//
//    // get all non-partition columns (including ts col)
//    val leftCols = prefixedLeftTSDF.observationColumns :+ prefixedLeftTSDF.tsColumn
//    val rightCols = prefixedRightTSDF.observationColumns :+ prefixedRightTSDF.tsColumn
//
//    // perform asof join
//    val combinedTSDF = combineTSDF(
//      addColumnsFromOtherDF(prefixedLeftTSDF, rightCols),
//      addColumnsFromOtherDF(prefixedRightTSDF, leftCols))
//
//    val asofTSDF: TSDF = tsPartitionVal match {
//      case Some(partitionValue) => {
//        val timePartitionedTSDF = getTimePartitions(combinedTSDF,partitionValue,fraction)
//
//        val asofDF = getLastRightRow(
//          timePartitionedTSDF,
//          maxLookback,
//          maxLookAhead,
//          prefixedLeftTSDF.tsColumn,
//          rightCols
//        ).df
//          .filter(col("is_original") === lit(1))
//          .drop("ts_partition","is_original")
//
//        TSDF(asofDF, prefixedLeftTSDF.tsColumn.name, leftTSDF.partitionCols.map(_.name):_*)
//      }
//      case None => getLastRightRow(
//        combinedTSDF,
//        maxLookback,
//        maxLookAhead,
//        prefixedLeftTSDF.tsColumn,
//        rightCols
//      )
//    }
//    asofTSDF
//  }

