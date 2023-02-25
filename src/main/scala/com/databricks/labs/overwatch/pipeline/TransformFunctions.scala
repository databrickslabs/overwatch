package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset}

import java.time.LocalDate

object TransformFunctions {

  implicit class DataFrameTransforms(df: DataFrame) {
    private val logger: Logger = Logger.getLogger(this.getClass)

    def dropDupColumnByAlias(dropAlias: String, columnNames: String*): DataFrame = {
      columnNames.foldLeft(df) {
        case (mutDF, k) => {
          mutDF.drop(col(s"${dropAlias}.${k}"))
        }
      }
    }

    /**
     * drops columns that contain only nulls
     *
     * @param df dataframe to more data
     * @return
     *
     */
    def cullNull(): DataFrame = {
      val dfSchema = df.schema
      // df.summary doesn't display summaries for all data types, so the types that aren't displayed need to be
      // converted to a string to be analyzed by df.summary
      val summarySelects = dfSchema.map(f => {
        f.dataType.typeName match {
          case "struct" | "array" | "map" => to_json(col(f.name)).alias(f.name)
          case "date" | "timestamp" | "boolean" => col(f.name).cast("string").alias(f.name)
          case _ => col(f.name).alias(f.name)
        }
      })

      val cntsDF = df.select(summarySelects: _*).summary("count").drop("summary")
      val nonNullCols = cntsDF.collect()
        .flatMap(r => r.getValuesMap[Any](cntsDF.columns).filter(_._2 != "0").keys)
        .map(col)
      df.select(nonNullCols: _*)
    }

    def cullNestedColumns(structToModify: String, nestedFieldsToCull: Array[String]): DataFrame = {
      SchemaTools.cullNestedColumns(df, structToModify, nestedFieldsToCull)
    }

    def suffixDFCols(
                      suffix: String,
                      columnsToSuffix: Array[String] = Array(),
                      caseSensitive: Boolean = false
                    ): DataFrame = {
      val dfFields = if (caseSensitive) df.schema.fields.map(_.name) else df.schema.fields.map(_.name.toLowerCase)
      val allColumnsToSuffix = if (columnsToSuffix.isEmpty) {
        dfFields
      } else {
        if (caseSensitive) columnsToSuffix else columnsToSuffix.map(_.toLowerCase)
      }

      df.select(dfFields.map(fName => {
        if (allColumnsToSuffix.contains(fName)) col(fName).alias(s"${fName}${suffix}") else col(fName)
      }): _*)
    }

    def modifyStruct(changeInventory: Map[String, Column]): DataFrame = {
      df.select(SchemaTools.modifyStruct(df.schema, changeInventory): _*)
    }

    /**
     * Join left and right as normal with the added feature that "lagDays" will be allowed to match on the join
     * condition of the "laggingSide"
     * EX: startEvent.joinWithLag(endEvent, keyColumn[s], "laggingDateCol", lagDays = 30, joinType = "left")
     * The above example with match the join condition on all keyColumn[s] AND where left.laggingDateCol >=
     * date_sub(right.laggingDateCol, 30)
     *
     * @param df2               df to be joined
     * @param usingColumns      key columns ** less the lagDateColumn
     * @param lagDateColumnName name of the lag control column
     * @param laggingSide       which side of the join is the lagging side
     * @param lagDays           how many days to allow for lag
     * @param joinType          join type, one of left | inner | right
     * @return
     */
    def joinWithLag(
                     df2: DataFrame,
                     usingColumns: Seq[String],
                     lagDateColumnName: String,
                     laggingSide: String = "left",
                     lagDays: Int = 1,
                     joinType: String = "inner"
                   ): DataFrame = {
      require(laggingSide == "left" || laggingSide == "right", s"laggingSide must be either 'left' or 'right'; received $laggingSide")
      require(joinType == "left" || joinType == "right" || joinType == "inner", s"Only left, right, inner joins " +
        s"supported, you selected $joinType, switch to supported join type")
      require( // both sides contain the lagDateColumnName
        df.schema.fields.exists(_.name == lagDateColumnName) &&
          df2.schema.fields.exists(_.name == lagDateColumnName),
        s"$lagDateColumnName must exist on both sides of the join"
      )

      require( // lagDateColumn is date or timestamp type on both sides
        df.schema.fields
          .filter(f => f.name == lagDateColumnName)
          .exists(f => f.dataType == TimestampType || f.dataType == DateType) &&
          df2.schema.fields
            .filter(f => f.name == lagDateColumnName)
            .exists(f => f.dataType == TimestampType || f.dataType == DateType),
        s"$lagDateColumnName must be either a Date or Timestamp type on both sides of the join"
      )

      val rightSuffix = "_right__OVERWATCH_CONTROL_COL"
      val leftSuffix = "_left__OVERWATCH_CONTROL_COL"
      val allJoinCols = (usingColumns :+ lagDateColumnName).toArray
      val (left, right) = if (joinType == "left" || joinType == "inner") {
        (df, df2.suffixDFCols(rightSuffix, allJoinCols, caseSensitive = true))
      } else (df.suffixDFCols(leftSuffix, allJoinCols, caseSensitive = true), df2)

      val baseJoinCondition = if (joinType == "left" || joinType == "inner") {
        usingColumns.map(k => s"$k = ${k}${rightSuffix}").mkString(" AND ")
      } else usingColumns.map(k => s"$k = ${k}${leftSuffix}").mkString(" AND ")

      val joinConditionWLag = if (joinType == "left" || joinType == "inner") {
        if (laggingSide == "left") {
          expr(s"$baseJoinCondition AND ${lagDateColumnName} >= date_sub(${lagDateColumnName}${rightSuffix}, $lagDays)")
        } else {
          expr(s"$baseJoinCondition AND ${lagDateColumnName}${rightSuffix} >= date_sub(${lagDateColumnName}, $lagDays)")
        }
      } else {
        if (laggingSide == "left") {
          expr(s"$baseJoinCondition AND ${lagDateColumnName}${leftSuffix} >= date_sub(${lagDateColumnName}, $lagDays)")
        } else {
          expr(s"$baseJoinCondition AND ${lagDateColumnName} >= date_sub(${lagDateColumnName}${leftSuffix}, $lagDays)")
        }
      }

      logger.log(Level.INFO, s"LagJoin Condition: $joinConditionWLag")

      val joinResult = left.join(right, joinConditionWLag, joinType)
      val joinSelects = if (joinType == "left" || joinType == "inner") {
        joinResult.schema.fields.filterNot(_.name.endsWith(rightSuffix)).map(f => col(f.name))
      } else joinResult.schema.fields.filterNot(_.name.endsWith(leftSuffix)).map(f => col(f.name))

      joinResult.select(joinSelects: _*)
    }

    def requireFields(fieldName: Seq[String]): DataFrame = requireFields(false, fieldName: _*)

    def requireFields(caseSensitive: Boolean, fieldName: String*): DataFrame = {
      fieldName.map(f => {
        val fWithCase = if (caseSensitive) f else f.toLowerCase
        try {
          if (caseSensitive) {
            df.schema.fields.map(_.name).find(_ == fWithCase).get
          } else {
            df.schema.fields.map(_.name.toLowerCase).find(_ == fWithCase).get
          }
        } catch {
          case e: NoSuchElementException =>
            val errMsg = s"MISSING REQUIRED FIELD: $fWithCase."
            println(errMsg)
            logger.log(Level.ERROR, errMsg, e)
            throw new Exception(errMsg)
          case e: Throwable =>
            val errMsg = s"REQUIRED COLUMN FAILURE FOR: $fWithCase"
            println(errMsg)
            logger.log(Level.ERROR, errMsg, e)
            throw new Exception(errMsg)
        }
      })
      df
    }

    def scrubSchema: DataFrame = {
      SchemaScrubber.scrubSchema(df)
    }

    def scrubSchema(schemaScrubber: SchemaScrubber): DataFrame = {
      schemaScrubber.scrubSchema(df)
    }

    def toTSDF(
                timeSeriesColumnName: String,
                partitionByColumnNames: String*
              ): TSDF = {
      TSDF(df, timeSeriesColumnName, partitionByColumnNames: _*)
    }

    @transient
    def verifyMinimumSchema(
                             minimumRequiredSchema: StructType,
                             enforceNonNullCols: Boolean = true,
                             isDebug: Boolean = false
                           ): DataFrame = {
      verifyMinimumSchema(Some(minimumRequiredSchema), enforceNonNullCols, isDebug)
    }

    @transient
    def verifyMinimumSchema(
                             minimumRequiredSchema: Option[StructType],
                             enforceNonNullCols: Boolean,
                             isDebug: Boolean
                           ): DataFrame = {

      if (minimumRequiredSchema.nonEmpty) {
        // validate field requirements for fields present in the dataframe
        val validatedCols = SchemaTools
          .buildValidationRunner(df.schema, minimumRequiredSchema.get, enforceNonNullCols, isDebug)
          .map(SchemaTools.validateSchema(_, isDebug = isDebug))
        df.select(validatedCols.map(_.column): _*)
      } else {
        logger.log(Level.WARN, s"No Schema Found for verification.")
        df
      }
    }

    // Function used to view schema validation and returned columns
    def reviewSchemaValidations(
                                 minimumRequiredSchema: Option[StructType],
                                 enforceNonNullCols: Boolean = true
                               ): Seq[ValidatedColumn] = {

      if (minimumRequiredSchema.nonEmpty) {
        // validate field requirements for fields present in the dataframe
        SchemaTools.buildValidationRunner(df.schema, minimumRequiredSchema.get, enforceNonNullCols, isDebug = true)
          .map(SchemaTools.validateSchema(_, isDebug = true))
      } else {
        logger.log(Level.ERROR, s"No Schema Found for verification.")
        df.schema.fields.map(f => ValidatedColumn(col(f.name), Some(f)))
      }
    }

    /**
     * fills metadata columns in a dataframe using windows
     * the windows will use the keys and the incrementals to go back as far as needed to get a value
     * if a value cannot be filled from previous data, first future value will be used to fill
     *
     * @param fieldsToFill      Array of fields to fill
     * @param keys              keys by which to partition the window
     * @param incrementalFields fields by which to order the window
     * @param orderedLookups    Seq of columns that provide a secondary lookup for the value within the row
     * @param noiseBuckets      Optional number of buckets to split lookup window
     *                          creates an intermediate step that can help shrink skew on large datasets with
     *                          heavily skewed lookups
     * @return
     */
    def fillMeta(
                  fieldsToFill: Array[String],
                  keys: Seq[String],
                  incrementalFields: Seq[String],
                  orderedLookups: Seq[Column] = Seq[Column](),
                  noiseBuckets: Int = 1
                ) : DataFrame = {

      val dfFields = df.columns

      // generate noise as per the number of noise buckets created
      val noiseBucketCount = round(rand() * noiseBuckets, 0)
      val keysWithNoise = keys :+ "__overwatch_ctrl_noiseBucket"

      val dfc = df
        .withColumn("__overwatch_ctrl_noiseBucket", lit(noiseBucketCount))
        .cache()

      dfc.count()

      val wNoise = Window.partitionBy(keysWithNoise map col: _*).orderBy(incrementalFields map col: _*)
      val wNoisePrev = wNoise.rowsBetween(Window.unboundedPreceding, Window.currentRow)
      val wNoiseNext = wNoise.rowsBetween(Window.currentRow, Window.unboundedFollowing)

      val selectsWithFills = dfFields.map(f => {
        if(fieldsToFill.map(_.toLowerCase).contains(f.toLowerCase)) { // field to fill
          bidirectionalFill(f, wNoisePrev, wNoiseNext, orderedLookups)
        } else { // not a fill field just return original value
          col(f)
        }
      })

      val stepDF = dfc
        .select(selectsWithFills :+ col("__overwatch_ctrl_noiseBucket"): _*)

      val lookupSelects = (keys ++ fieldsToFill) ++ Array("unixTimeMS_state_start")
      val lookupTSDF = stepDF
        .select(lookupSelects map col: _*)
        .distinct
        .toTSDF("unixTimeMS_state_start", keys: _*)

      dfc.toTSDF("unixTimeMS_state_start", keys: _*)
        .lookupWhen(lookupTSDF, maxLookAhead = 1000L)
        .df
    }

    /**
     * remove dups via a window
     *
     * @param keys              seq of keys for the df
     * @param incrementalFields seq of incremental fields for the df
     * @return
     */
    def dedupByKey(
                    keys: Seq[String],
                    incrementalFields: Seq[String]
                  ): DataFrame = {
      //       val keysLessIncrementals = (keys.toSet -- incrementalFields.toSet).toArray
      val distinctKeys = (keys ++ incrementalFields).toSet.toArray
      val w = Window.partitionBy(distinctKeys map col: _*).orderBy(incrementalFields map col: _*)
      df
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter(col("rnk") === 1 && col("rn") === 1)
        .drop("rnk", "rn")
    }

    /**
     * Supports strings, numericals, booleans. Defined keys don't contain any other types thus this function should
     * ensure no nulls present for keys
     *
     * @return
     */
    def fillAllNAs: DataFrame = {
      df.na.fill(0).na.fill("0").na.fill(false)
    }

    /**
     *
     * @param name
     * @param caseSensitive
     * @return
     */
    def hasFieldNamed(name: String, caseSensitive: Boolean = false): Boolean = {
      val casedName = if (caseSensitive) name else name.toLowerCase
      SchemaTools.getAllColumnNames(df.schema).exists(c => {
        if (caseSensitive) c.startsWith(casedName) else c.toLowerCase.startsWith(casedName)
      })
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
     * @param df         Input dataframe
     * @param colsToMove Array of column names to be moved to front of schema
     * @return
     */
    def moveColumnsToFront(colsToMove: Array[String]): DataFrame = {
      val allNames = df.schema.names
      val newColumns = (colsToMove ++ allNames.diff(colsToMove)).map(col)
      df.select(newColumns: _*)
    }

    def moveColumnsToFront(colsToMove: String*): DataFrame = {
      val allNames = df.schema.names
      val newColumns = (colsToMove ++ allNames.diff(colsToMove)).map(col)
      df.select(newColumns: _*)
    }

    /**
     * appends fields to an existing struct
     *
     * @param structFieldName            name of struct to which namedColumns should be applied
     * @param namedColumns               Array of NamedColumn
     * @param overrideExistingStructCols Whether or not to override the value of existing struct field if it exists
     * @param newStructFieldName         If not provided, the original struct will be morphed, if a secondary struct is desired
     *                                   provide a name here and the original struct will not be altered.
     *                                   New, named struct will be added to the top level
     * @param caseSensitive              whether or not the field names are case sensitive
     * @return
     */
    def appendToStruct(
                        structFieldName: String,
                        namedColumns: Array[NamedColumn],
                        overrideExistingStructCols: Boolean = false,
                        newStructFieldName: Option[String] = None,
                        caseSensitive: Boolean = false
                      ): DataFrame = {
      require(df.hasFieldNamed(structFieldName, caseSensitive),
        s"ERROR: Dataframe must contain the struct field to be altered. " +
          s"$structFieldName was not found. Struct fields include " +
          s"${df.schema.fields.filter(_.dataType.typeName == "struct").map(_.name).mkString(", ")}"
      )

      val fieldToAlterTypeName = df.select(structFieldName).schema.fields.head.dataType.typeName
      require(fieldToAlterTypeName == "struct", s"ERROR: Field to alter must a struct but got $fieldToAlterTypeName")

      val targetStructFieldNames = df.select(s"$structFieldName.*").schema.fieldNames
      val missingFieldsToAdd = namedColumns.filterNot(fc => targetStructFieldNames.contains(fc.fieldName))
      val colsToAdd = if (overrideExistingStructCols) namedColumns else missingFieldsToAdd
      val alteredStructColumn = colsToAdd.foldLeft(col(structFieldName))((structCol, fc) => {
        structCol.withField(fc.fieldName, fc.column.alias(fc.fieldName))
      })

      df.withColumn(newStructFieldName.getOrElse(structFieldName), alteredStructColumn)
    }

  }

  private def bidirectionalFill(colToFillName: String, wPrev: WindowSpec, wNext: WindowSpec, orderedLookups: Seq[Column] = Seq[Column]()): Column = {
    val colToFill = col(colToFillName)
    if (orderedLookups.nonEmpty) { // TODO -- omit nulls from lookup
      val coalescedLookup = Array(colToFill) ++ orderedLookups.map(lookupCol => {
        last(lookupCol, true).over(wPrev)
      }) ++ orderedLookups.map(lookupCol => {
        first(lookupCol, true).over(wNext)
      })
      coalesce(coalescedLookup: _*).alias(colToFillName)
    } else {
      coalesce(colToFill, last(colToFill, true).over(wPrev), first(colToFill, true).over(wNext)).alias(colToFillName)
    }
  }

  object Costs {
    def compute(
                 isCloudBillable: Column,
                 computeCost_H: Column,
                 nodeCount: Column,
                 computeTime_H: Column,
                 smoothingCol: Option[Column] = None
               ): Column = {
      coalesce(
        when(isCloudBillable, computeCost_H * computeTime_H * nodeCount * smoothingCol.getOrElse(lit(1)))
          .otherwise(lit(0)),
        lit(0) // don't allow costs to be null (i.e. missing worker node type and/or single node workers
      )
    }

    /**
     * Calculates DBU Costs
     * @param isDatabricksBillable is state in DBU Billable State
     * @param dbus dbus from the state
     * @param dbuRate_H cost of a dbu per hour for the given sku
     * @param nodeCount number of nodes considered in the calculation
     * @param stateTime uptime (wall time) in state
     * @param smoothingCol coefficient derived from the smooth function
     * @return
     */
    def dbu(
             dbus: Column,
             dbuRate_H: Column,
             smoothingCol: Option[Column] = None
           ): Column = {

      //This is the default logic for DBU calculation
      dbus * dbuRate_H
//      val defaultCalculation = dbus * stateTime * nodeCount * dbuRate_H * smoothingCol.getOrElse(lit(1))
//      when(isDatabricksBillable, defaultCalculation).otherwise(lit(0.0))
    }

    /**
     * Will be improved upon and isn't used in this package but is a handy function for Overwatch users.
     * This will likely be refactored out to utils later when we create user utils functions area.
     *
     * @param df
     * @param metrics
     * @param by
     * @param precision
     * @param smoother
     * @throws org.apache.spark.sql.AnalysisException
     * @return
     */
    @throws(classOf[AnalysisException])
    def summarize(
                   df: DataFrame,
                   metrics: Array[String],
                   by: Array[String],
                   precision: Int,
                   smoother: Column
                 ): DataFrame = {
      metrics.foreach(m => require(df.schema.fieldNames.map(_.toLowerCase).contains(m.toLowerCase),
        s"Cost Summary Failed: Column $m does not exist in the dataframe provided."
      ))
      by.foreach(by => require(df.schema.fieldNames.map(_.toLowerCase).contains(by.toLowerCase),
        s"Cost Summary Failed: Grouping Column $by does not exist in the dataframe provided."
      ))

      val aggs = metrics.map(m => {
        greatest(round(sum(col(m) * smoother), precision), lit(0)).alias(m)
      })

      df
        .groupBy(by map col: _*)
        .agg(
          aggs.head, aggs.tail: _*
        )
    }
  }

  def isAutomated(clusterName: Column): Column = clusterName.like("job-%-run-%")

  /**
   * Retrieve DF alias
   *
   * @param ds
   * @return
   */
  def getAlias(ds: Dataset[_]): Option[String] = ds.queryExecution.analyzed match {
    case SubqueryAlias(alias, _) => Some(alias.name)
    case _ => None
  }

  def datesStream(fromDate: LocalDate): Stream[LocalDate] = {
    fromDate #:: datesStream(fromDate plusDays 1)
  }

  /**
   * Simplify code for join conditions where begin events may be on the previous date BUT the date column is a
   * partition column so the explicit date condition[s] must be in the join clause to ensure
   * dynamic partition pruning (DPP)
   * join column names must match on both sides of the join
   *
   * @param dateColumnName date column of DateType -- this column should also either be a partition or indexed col
   * @param alias          DF alias of latest event
   * @param lagAlias       DF alias potentially containing lagging events
   * @param usingColumns   Seq[String] for matching column names
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
   *
   * @param start           : Column of LongType with start time in milliseconds
   * @param end             : Column of LongType with end time  in milliseconds
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
   * @param baseDF
   * @param lookupDF
   * @return
   */
  def unionWithMissingAsNull(baseDF: DataFrame, lookupDF: DataFrame): DataFrame = {
    val baseFields = baseDF.schema
    val lookupFields = lookupDF.schema
    val missingBaseFields = lookupFields.filterNot(f => baseFields.map(_.name).contains(f.name))
    val missingLookupFields = baseFields.filterNot(f => lookupFields.map(_.name).contains(f.name))
    val df1Complete = missingBaseFields.foldLeft(baseDF) {
      case (df, f) =>
        df.withColumn(f.name, lit(null).cast(f.dataType))
    }
    val df2Complete = missingLookupFields.foldLeft(lookupDF) {
      case (df, f) =>
        df.withColumn(f.name, lit(null).cast(f.dataType))
    }

    df1Complete.unionByName(df2Complete)
  }

  /**
   * Converts string ts column from standard spark ts string format to unix epoch millis. The input column must be a
   * string and must be in the format of yyyy-dd-mmTHH:mm:ss.SSSz
   *
   * @param tsStringCol
   * @return
   */
  def stringTsToUnixMillis(tsStringCol: Column): Column = {
    ((unix_timestamp(tsStringCol.cast("timestamp")) * 1000) + substring(tsStringCol, -4, 3)).cast("long")
  }

  private val applicableWorkers = when(col("state") === "RESIZING" &&
    col("target_num_workers") < col("current_num_workers"), col("target_num_workers"))
    .otherwise(col("current_num_workers"))

  def getNodeInfo(nodeType: String, metric: String, multiplyTime: Boolean): Column = {
    val baseMetric = if ("driver".compareToIgnoreCase(nodeType) == 0) {
      col(s"driverSpecs.${metric}")
    } else if ("worker".compareToIgnoreCase(nodeType) == 0) {
      col(s"workerSpecs.${metric}") * applicableWorkers
    } else {
      throw new Exception("nodeType must be either 'driver' or 'worker'")
    }

    if (multiplyTime) {
      when(col("state") === "TERMINATING", lit(0))
        .otherwise(round(baseMetric * col("uptime_in_state_S"), 2)).alias(s"${nodeType}_${baseMetric}S")
    } else {
      when(col("state") === "TERMINATING", lit(0))
        .otherwise(round(baseMetric, 2).alias(s"${nodeType}_${baseMetric}"))
    }
  }

  def cluster_idFromAudit: Column = {
    //    import spark.implicits._
    when(
      col("serviceName") === "clusters" &&
        col("actionName").like("%Result"),
      col("requestParams.clusterId")
    )
      .when(
        col("serviceName") === "clusters" &&
          col("actionName").isin("permanentDelete", "delete", "resize", "edit"),
        col("requestParams.cluster_id")
      )
      .when(
        col("serviceName") === "clusters" &&
          col("actionName") === "create",
        get_json_object(col("response.result"), "$.cluster_id")
      )
      .otherwise(col("requestParams.cluster_id"))
  }

  def getClusterIdsWithNewEvents(filteredAuditLogDF: DataFrame,
                                 clusterSnapshotTable: DataFrame
                                ): DataFrame = {

    val newClustersIDs = filteredAuditLogDF
      .select(cluster_idFromAudit.alias("cluster_id"))
      .filter(col("cluster_id").isNotNull)
      .distinct()

    val latestSnapW = Window.partitionBy(col("organization_id")).orderBy(col("Pipeline_SnapTS").desc)
    // capture long-running clusters not otherwise captured from audit
    val currentlyRunningClusters = clusterSnapshotTable
      .withColumn("snapRnk", rank.over(latestSnapW))
      .filter(col("snapRnk") === 1)
      .filter(col("state") === "RUNNING")
      .select(col("cluster_id"))
      .distinct

    newClustersIDs
      .unionByName(currentlyRunningClusters)
      .distinct

  }

}
