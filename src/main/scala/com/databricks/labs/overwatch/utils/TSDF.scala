package com.databricks.labs.overwatch.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import asofJoin._

sealed trait TSDF {
  val df: DataFrame

  val tsColumn: StructField

  val partitionCols: Seq[StructField]

  val isPartitioned: Boolean

  val schema: StructType

  val fields: Array[StructField]

  val structuralColumns: Seq[StructField]

  val observationColumns: Seq[StructField]

  protected def baseWindow(): WindowSpec

  def asofJoin(rightTSDF: TSDF,
               leftPrefix: String,
               rightPrefix: String = "right_",
               maxLookback: Long,
               maxLookAhead: Long,
               tsPartitionVal: Int = 0,
               fraction: Double = 0.1) : TSDF

  def windowBetweenRows(start: Long, end: Long): WindowSpec

//  def windowOverRows(length: Long, offset: Long = Window.currentRow): WindowSpec

  def windowBetweenRange(start: Long, end: Long): WindowSpec
//
//  def windowOverRange(length: Long, offset: Long = Window.currentRow): WindowSpec

}

private[overwatch] sealed class BaseTSDF(
                                      val df: DataFrame,
                                      val tsColumn: StructField,
                                      val partitionCols: StructField*
                             )
  extends TSDF {

  override val schema: StructType = df.schema
  override val fields: Array[StructField] = schema.fields

  assert(
    schema.contains(tsColumn),
    s"The provided DF does not contain the given timeseries column ${tsColumn.name}"
  )

  assert(
    TSDF.validTSColumnTypes.contains(tsColumn.dataType),
    s"The time series column's data type of ${tsColumn.dataType.typeName} is not supported. " +
      s"Supported types include ${TSDF.validTSColumnTypes.map(_.typeName).mkString(", ")}"
  )

  override val isPartitioned: Boolean = partitionCols.nonEmpty


  override val structuralColumns: Seq[StructField] = Seq(tsColumn) ++ partitionCols

  override val observationColumns: Seq[StructField] =
    schema.filter(!structuralColumns.contains(_))

  def partitionedBy(_partitionCols: String*): TSDF = {
    TSDF(df, tsColumn.name, _partitionCols: _*)
  }

  protected def baseWindow(): WindowSpec = {
    val w = Window.orderBy(tsColumn.name)

    if (this.isPartitioned) {
      w.partitionBy(partitionCols.map(_.name) map col: _*)
    } else w
  }

  override def windowBetweenRows(start: Long, end: Long): WindowSpec = {
    baseWindow().rowsBetween(start, end)
  }

  override def windowBetweenRange(start: Long, end: Long): WindowSpec = {
    baseWindow().rangeBetween(start, end)
  }



  def asofJoin(
                rightTSDF: TSDF,
                leftPrefix: String = "",
                rightPrefix: String = "right_",
                maxLookback: Long = Window.unboundedPreceding,
                maxLookAhead: Long = Window.currentRow,
                tsPartitionVal: Int = 0,
                fraction: Double = 0.1
              ): TSDF = {

    if(leftPrefix == "" && tsPartitionVal == 0) {
      asofJoinExec(this,rightTSDF, leftPrefix = None, rightPrefix, maxLookback, maxLookAhead, tsPartitionVal = None, fraction)
    }
    else if(leftPrefix == "") {
      asofJoinExec(this, rightTSDF, leftPrefix = None, rightPrefix, maxLookback, maxLookAhead, Some(tsPartitionVal), fraction)
    }
    else if(tsPartitionVal == 0) {
      asofJoinExec(this, rightTSDF, Some(leftPrefix), rightPrefix, maxLookback, maxLookAhead, tsPartitionVal = None)
    }
    else {
      asofJoinExec(this, rightTSDF, Some(leftPrefix), rightPrefix, maxLookback, maxLookAhead, Some(tsPartitionVal), fraction)
    }
  }

}

object TSDF {

  final val validTSColumnTypes = Seq[DataType](ByteType,
    ShortType,
    IntegerType,
    LongType,
    TimestampType,
    DateType)

  private[overwatch] def colByName(df: DataFrame)(colName: String): StructField =
    df.schema.find(_.name.toLowerCase() == colName.toLowerCase()).get

  def apply(
             df: DataFrame,
             tsColumnName: String,
             partitionColNames: String*): TSDF = {

    val colFinder = colByName(df) _
    val tsColumn = colFinder(tsColumnName)
    val partitionCols = partitionColNames.map(colFinder)
    new BaseTSDF(df, tsColumn, partitionCols: _*)

  }


}
